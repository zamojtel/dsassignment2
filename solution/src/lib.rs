mod domain;

use std::time::Duration; 
use std::{collections::{HashMap, HashSet}, sync::Arc};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
pub use crate::domain::*;
use tokio::sync::mpsc;
pub use atomic_register_public::*;
pub use register_client_public::*;
pub use sectors_manager_public::*;
pub use transfer_public::*;
use uuid::Uuid;

// added
type RegisterMap = Arc<tokio::sync::RwLock<HashMap<SectorIdx,Arc<tokio::sync::Mutex<Box<dyn AtomicRegister>>>>>>;
// client implementation for sending system messages 
struct TcpRegisterClient {
    config: Arc<Configuration>,
    self_sender: mpsc::UnboundedSender<SystemRegisterCommand>,
}

#[async_trait::async_trait]
impl RegisterClient for TcpRegisterClient {
    async fn send(&self, msg: Send) {
        let target_rank = msg.target;

        if target_rank == self.config.public.self_rank {
            let cmd = msg.cmd.as_ref().clone();
            let _ = self.self_sender.send(cmd);
            return;
        }

        let target_idx = (target_rank - 1) as usize;
        if target_idx >= self.config.public.tcp_locations.len(){
            return;
        }

        let (host, port) = &self.config.public.tcp_locations[target_idx];
        let address = format!("{}:{}", host, port);
        
        let system_cmd = msg.cmd.as_ref().clone(); 
        let hmac_key = self.config.hmac_system_key;

        tokio::spawn(async move {
            loop {
                match TcpStream::connect(&address).await {
                    Ok(mut stream) => {
                        let cmd_wrapper = RegisterCommand::System(system_cmd.clone());
                        
                        if transfer_public::serialize_register_command(
                            &cmd_wrapper, 
                            &mut stream, 
                            &hmac_key
                        ).await.is_ok() {
                            break;
                        }
                    }
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                }
            }
        });
    }

    async fn broadcast(&self, msg: Broadcast){
        for i in 0..self.config.public.tcp_locations.len() {
            let target_rank = (i+1) as u8;
            self.send(Send {
                cmd: msg.cmd.clone(),
                target: target_rank, 
            }).await;
        }
    }
}

async fn send_client_response(
    response: &ClientCommandResponse,
    writer: &mut (dyn tokio::io::AsyncWrite + std::marker::Send + Unpin),
    hmac_key: &[u8]
) -> Result<(), transfer_public::EncodingError> {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;

    let config = bincode::config::standard()
        .with_big_endian()
        .with_fixed_int_encoding();

    let payload = bincode::serde::encode_to_vec(response, config)
        .map_err(EncodingError::BincodeError)?;
    
    let mut mac = HmacSha256::new_from_slice(hmac_key).expect("Invalid key length");
    mac.update(&payload);
    let tag = mac.finalize().into_bytes();

    let total_len = (payload.len() + tag.len()) as u64;

    writer.write_all(&total_len.to_be_bytes()).await.map_err(EncodingError::IoError)?;
    writer.write_all(&payload).await.map_err(EncodingError::IoError)?;
    writer.write_all(&tag).await.map_err(EncodingError::IoError)?;
    
    Ok(())
}

async fn get_or_create_register(
    sector_idx: SectorIdx,
    registers: &RegisterMap,
    config: &Arc<Configuration>,
    sectors_manager: &Arc<dyn SectorsManager>,
    self_sender: tokio::sync::mpsc::UnboundedSender<SystemRegisterCommand>,
) -> Arc<tokio::sync::Mutex<Box<dyn AtomicRegister>>> {
    // this is a fast check 
    // most of the times the rigister will be already created
    {
        let map = registers.read().await;
        if let Some(register) = map.get(&sector_idx){
            return register.clone()
        }
    }

    // and here's a very important part
    // some other thread could came before us and create another register 
    // to avoid creating a duplicated register we check once again

    let mut map = registers.write().await;

    if let Some(register) = map.get(&sector_idx) {
        return register.clone();
    }

    // here a new register is being created
    let register_client = Arc::new(TcpRegisterClient { 
        config: config.clone(),
        self_sender: self_sender, 
    });

    let new_register = build_atomic_register(
        config.public.self_rank,
        sector_idx,
        register_client,
        sectors_manager.clone(),
        config.public.tcp_locations.len() as u8
    ).await;

    let register_arc = Arc::new(tokio::sync::Mutex::new(new_register));
    map.insert(sector_idx, register_arc.clone());

    register_arc
}

async fn process_system_command(
    system_cmd: SystemRegisterCommand,
    registers: &RegisterMap,
    config: &Arc<Configuration>,
    sectors_manager: &Arc<dyn SectorsManager>,
    self_sender: &mpsc::UnboundedSender<SystemRegisterCommand>,
) {
    let sector_idx = system_cmd.header.sector_idx;
    let register = get_or_create_register(
        sector_idx,
        registers,
        config,
        sectors_manager,
        self_sender.clone()
    ).await;

    let mut guard = register.lock().await;
    guard.system_command(system_cmd).await;
}

async fn handle_tcp_connection(
    stream: TcpStream,
    sectors_manager: Arc<dyn SectorsManager>,
    config: Arc<Configuration>,
    registers: RegisterMap,
    self_sender: mpsc::UnboundedSender<SystemRegisterCommand>,
) -> Result<(), std::io::Error> {
    let (mut socket_reader, socket_writer) = stream.into_split();
    let socket_writer = Arc::new(tokio::sync::Mutex::new(socket_writer));

    loop {
        let (cmd, hmac_ok) = match transfer_public::deserialize_register_command(
            &mut socket_reader,
            &config.hmac_system_key,
            &config.hmac_client_key,
        ).await {
            Ok(x) => x,
            Err(DecodingError::IoError(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(_) => return Ok(()) // Drop connection on error
        };

        match cmd {
            RegisterCommand::Client(client_cmd) => {
                if !hmac_ok {
                    let response = ClientCommandResponse {
                        status: StatusCode::AuthFailure,
                        request_identifier: client_cmd.header.request_identifier,
                        op_return: match client_cmd.content {
                            ClientRegisterCommandContent::Read => OperationReturn::Read { read_data: zero_sector() },
                            ClientRegisterCommandContent::Write { .. } => OperationReturn::Write,
                        }
                    };
                    let writer = socket_writer.clone();
                    let key = config.hmac_client_key;
                    let mut guard = writer.lock().await;
                    let _ = send_client_response(&response, &mut *guard, &key).await;
                    continue; 
                }
                
                let sector_idx = client_cmd.header.sector_idx;

                if sector_idx >= config.public.n_sectors {
                    let response = ClientCommandResponse {
                        status: StatusCode::InvalidSectorIndex,
                        request_identifier: client_cmd.header.request_identifier,
                        op_return: match client_cmd.content {
                            ClientRegisterCommandContent::Read => OperationReturn::Read { read_data: zero_sector()},
                            ClientRegisterCommandContent::Write{ .. } => OperationReturn::Write,
                        }
                    };
                    
                    let writer = socket_writer.clone();
                    let key = config.hmac_client_key;
                    let mut guard = writer.lock().await;
                    let _ = send_client_response(&response,& mut *guard,&key).await;
                    continue;
                }

                let register = get_or_create_register(
                    sector_idx,
                    &registers,
                    &config,
                    &sectors_manager,
                    self_sender.clone()
                ).await;

                let writer = socket_writer.clone();
                let key = config.hmac_client_key;

                let callback = Box::new(move |response: ClientCommandResponse| 
                    -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + std::marker::Send>> {
                    Box::pin(async move {
                        let mut guard = writer.lock().await;
                        let _ = send_client_response(&response, &mut *guard, &key).await;
                    })
                });

                let mut guard = register.lock().await;
                guard.client_command(client_cmd, callback).await;
            },
            RegisterCommand::System(system_cmd) => {
                if !hmac_ok {
                    return Ok(());
                }
                process_system_command(system_cmd, &registers, &config, &sectors_manager, &self_sender).await;
            }
        }
    }
}

pub async fn run_register_process(config: Configuration) {
    let (host,port) = &config.public.tcp_locations[(config.public.self_rank-1) as usize];
    let ip_with_port = format!("{}:{}",host,port);

    let listener = TcpListener::bind(&ip_with_port).await.expect("Failed to bind TCP listener");
    
    let sectors_manager = build_sectors_manager(config.public.storage_dir.clone()).await;
    let config = Arc::new(config);
    let registers: RegisterMap = Arc::new(tokio::sync::RwLock::new(HashMap::new()));

    let (self_tx, mut self_rx) = mpsc::unbounded_channel::<SystemRegisterCommand>();

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((stream, _)) => {
                        let sectors_manager = Arc::clone(&sectors_manager);
                        let config = Arc::clone(&config);
                        let registers = Arc::clone(&registers);
                        let self_tx = self_tx.clone();

                        tokio::spawn(async move {
                            let _ = handle_tcp_connection(stream, sectors_manager, config, registers, self_tx).await;
                        });
                    }
                    Err(_) => {
                        continue;
                    }
                }
            }

            Some(system_cmd) = self_rx.recv() => {
                let sectors_manager = Arc::clone(&sectors_manager);
                let config = Arc::clone(&config);
                let registers = Arc::clone(&registers);
                let self_tx = self_tx.clone();

                tokio::spawn(async move {
                    process_system_command(system_cmd, &registers, &config, &sectors_manager, &self_tx).await;
                });
            }
        }
    }
}

enum OperationType {
    Read,
    Write(SectorVec),
}

struct RegisterValue{
    timestamp: u64,
    write_rank: u8,
    value: SectorVec,    
}

struct OperationState {
    request_id: u64,
    read_list: HashMap<u8,RegisterValue>,
    ack_list: HashSet<u8>,
    op_type: OperationType,
    write_msg_ident: Option<Uuid>,
    read_data: Option<SectorVec>,
    callback: Box<
    dyn FnOnce(ClientCommandResponse) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + std::marker::Send>>
        + std::marker::Send
        + std::marker::Sync,
    >,
}
struct AtomicRegisterNode{
    ident: u8,
    sector_idx: SectorIdx,
    register_client: Arc<dyn RegisterClient>,
    sectors_manager: Arc<dyn SectorsManager>,
    processes_count: u8,
    operation_states: HashMap<u64,OperationState>,
}


impl AtomicRegisterNode {
    pub fn prepare_write_data(self_ident: u8,state: &OperationState,best: &RegisterValue) -> (u64,u8,SectorVec){
        match &state.op_type {
            OperationType::Read =>{
                (best.timestamp,best.write_rank,best.value.clone())
            },
            OperationType::Write(sector_vec) => {
                (best.timestamp+1,self_ident,sector_vec.clone())
            }
        }
    }
}

#[async_trait::async_trait]
impl AtomicRegister for AtomicRegisterNode {
    async fn client_command(&mut self,cmd: ClientRegisterCommand,success_callback: Box<dyn FnOnce(ClientCommandResponse) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + std::marker::Send>> + std::marker::Send + std::marker::Sync>){
        let op_id = cmd.header.request_identifier;

        let op_type = match cmd.content {
            ClientRegisterCommandContent::Read => {OperationType::Read},
            ClientRegisterCommandContent::Write { data } => {OperationType::Write(data)}
        };

        let state = OperationState{
            request_id:op_id,
            read_list: HashMap::new(),
            ack_list: HashSet::new(),
            op_type:op_type,
            callback:success_callback,
            write_msg_ident: None,
            read_data: None,
        };

        self.operation_states.insert(op_id,state);

        let msg = SystemRegisterCommand {
            header:SystemCommandHeader{
                process_identifier: self.ident,
                msg_ident: Uuid::new_v4(),
                sector_idx: self.sector_idx
            },
            content: SystemRegisterCommandContent::ReadProc
        };
        
        self.register_client.broadcast(Broadcast { cmd: Arc::new(msg) }).await;
    }

    async fn system_command(&mut self, cmd: SystemRegisterCommand) {
        match cmd.content  {
            SystemRegisterCommandContent::ReadProc =>{
                let (timestamp,write_rank) = self.sectors_manager.read_metadata(self.sector_idx).await;

                let data = self.sectors_manager.read_data(self.sector_idx).await;
                let header = SystemCommandHeader{
                    process_identifier: self.ident,
                    msg_ident: cmd.header.msg_ident,
                    sector_idx: self.sector_idx,
                };

                let content = SystemRegisterCommandContent::Value { 
                    timestamp,
                    write_rank,
                    sector_data: data 
                };
                
                self.register_client.send(Send { 
                    cmd: Arc::new(SystemRegisterCommand { header, content }),
                    target: cmd.header.process_identifier, 
                }).await;

            },
            SystemRegisterCommandContent::Value{timestamp,write_rank,sector_data} =>{
                for state in self.operation_states.values_mut() {
                    let register_value = RegisterValue{
                        timestamp,
                        write_rank,
                        value: sector_data.clone()
                    };
 
                    let process_id = cmd.header.process_identifier;
                    state.read_list.insert(process_id, register_value);
 
                    if state.read_list.len() > (self.processes_count as usize) / 2 {
                        if let Some((_pid, best)) = state.read_list.iter().max_by_key(
                            |(_pid,rv)| (rv.timestamp,rv.write_rank)
                        ) {
                            let (cmd_timestamp, cmd_write_rank, cmd_sector_data) = AtomicRegisterNode::prepare_write_data(self.ident, state, best);

                            state.ack_list.clear();
                            let msg_ident = Uuid::new_v4();
                            state.write_msg_ident = Some(msg_ident);

                            if let OperationType::Read = state.op_type {
                                state.read_data = Some(cmd_sector_data.clone());
                            }

                            let header = SystemCommandHeader{
                                process_identifier: self.ident,
                                msg_ident: msg_ident,
                                sector_idx: self.sector_idx,
                            };

                            let content = SystemRegisterCommandContent::WriteProc { 
                                timestamp: cmd_timestamp,
                                write_rank: cmd_write_rank, 
                                data_to_write: cmd_sector_data // send new data
                            };

                            self.register_client.broadcast(Broadcast { cmd: Arc::new(
                                SystemRegisterCommand { 
                                    header,
                                    content 
                                })
                            }).await;
                        }
                    }   
                }
            },
            SystemRegisterCommandContent::WriteProc { timestamp, write_rank, data_to_write } => {
                let (current_timestamp,current_write_rank) = self.sectors_manager.read_metadata(self.sector_idx).await;
                if (current_timestamp,current_write_rank) < (timestamp,write_rank) {
                    let to_write = (data_to_write,timestamp,write_rank);
                    self.sectors_manager.write(self.sector_idx,&to_write).await;
                }

                let header = SystemCommandHeader{
                    process_identifier: self.ident,
                    msg_ident: cmd.header.msg_ident, 
                    sector_idx: self.sector_idx,
                };

                let content = SystemRegisterCommandContent::Ack;
                
                self.register_client.send(
                    Send { 
                        cmd: Arc::new(SystemRegisterCommand { header, content }),
                        target: cmd.header.process_identifier
                    }
                ).await;
            },
            SystemRegisterCommandContent::Ack => {
                let mut completed_op_id = None;
                for state in self.operation_states.values_mut() {
                    if state.write_msg_ident == Some(cmd.header.msg_ident) {
                        state.ack_list.insert(cmd.header.process_identifier);

                        if state.ack_list.len() > (self.processes_count as usize) / 2 {
                            completed_op_id = Some(state.request_id);
                        }
                        break;
                    }
                } 

                if let Some(op_id) = completed_op_id {
                    if let Some(state) = self.operation_states.remove(&op_id) {
                        let op_return = match state.op_type {
                            OperationType::Read => { 
                                OperationReturn:: Read {read_data: state.read_data.unwrap() }
                            },
                            OperationType::Write(_) => OperationReturn::Write,
                        };

                        let response = ClientCommandResponse {
                            status: StatusCode:: Ok,
                            request_identifier: state.request_id,
                            op_return
                        };

                        (state.callback)(response).await;
                    }
                }
            }
        }
    }
}

pub mod atomic_register_public {
    use crate::{
        AtomicRegisterNode, ClientCommandResponse, ClientRegisterCommand, RegisterClient, SectorIdx, SectorsManager, SystemRegisterCommand
    };
    use std::collections::HashMap;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

    #[async_trait::async_trait]
    pub trait AtomicRegister: Send + Sync {
        async fn client_command(
            &mut self,
            cmd: ClientRegisterCommand,
            success_callback: Box<
                dyn FnOnce(ClientCommandResponse) -> Pin<Box<dyn Future<Output = ()> + Send>>
                    + Send
                    + Sync,
            >,
        );
        async fn system_command(&mut self, cmd: SystemRegisterCommand);
    }

    pub async fn build_atomic_register(
        self_ident: u8,
        sector_idx: SectorIdx,
        register_client: Arc<dyn RegisterClient>,
        sectors_manager: Arc<dyn SectorsManager>,
        processes_count: u8,
    ) -> Box<dyn AtomicRegister> {
        Box::new(
        AtomicRegisterNode{
            ident:self_ident,
            sector_idx:sector_idx,
            register_client: register_client,
            sectors_manager: sectors_manager,
            processes_count: processes_count,
            operation_states: HashMap::new()
        })
    }
}


fn parse_meta(name: &str) -> Option<(u64,u64,u8)> {
    let mut it = name.split("_");
    let sector_index: u64 = it.next()?.parse().ok()?;
    let timestamp: u64 = it.next()?.parse().ok()?;
    let write_rank: u8 = it.next()?.parse().ok()?;
    if it.next().is_some() { return None; }
    Some((sector_index,timestamp,write_rank))
}

fn zero_sector() -> SectorVec {
    SectorVec(Box::new(serde_big_array::Array([0u8; SECTOR_SIZE])))
}

pub mod sectors_manager_public {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::RwLock;
    use crate::{SECTOR_SIZE, SectorIdx, SectorVec, parse_meta, zero_sector};
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;

    struct DiskSectorsManager {
        directory: PathBuf,
        // required security for threads
        metadata_cache: RwLock<HashMap<SectorIdx,(u64, u8)>>,
    }

    impl DiskSectorsManager{
        // async fn get_metadata(&self,idx: SectorIdx)-> Option<(u64,u8)> {
        //     let cache = self.metadata_cache.read().await;
        //     cache.get(&idx).copied()
        // }

        fn get_file_path(&self,file_name: &str) -> PathBuf {
            self.directory.join(file_name)
        }
    }

    #[async_trait::async_trait]
    pub trait SectorsManager: Send + Sync {
        /// Returns 4096 bytes of sector data by index.
        async fn read_data(&self, idx: SectorIdx) -> SectorVec;

        /// Returns timestamp and write rank of the process which has saved this data.
        /// Timestamps and ranks are relevant for atomic register algorithm, and are described
        /// there.
        async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8);

        /// Writes a new data, along with timestamp and write rank to some sector.
        async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8));
    }

    /// Path parameter points to a directory to which this method has exclusive access.
    pub async fn build_sectors_manager(path: PathBuf) -> Arc<dyn SectorsManager> {
        // recovery process
        let mut cache = HashMap::<SectorIdx,(u64, u8)>::new();
        let mut directory = tokio::fs::read_dir(&path).await.unwrap();

        while let Some(entry) = directory.next_entry().await.unwrap() {
            let name_os = entry.file_name();
            if let Some(name) = name_os.to_str() {
                if let Some((sector_index,timestamp,write_rank)) = parse_meta(name) {
                    cache.insert(sector_index,(timestamp,write_rank));
                }
            }
        }
       
        Arc::new(DiskSectorsManager {
            directory: path,
            metadata_cache:  RwLock::new(cache),
        })
    }

    #[async_trait::async_trait]
    impl SectorsManager for DiskSectorsManager {
        async fn read_data(&self, idx: SectorIdx) -> SectorVec {
            let (timestamp,write_rank) = self.read_metadata(idx).await;
            
            let file_name = format!("{}_{}_{}",idx,timestamp,write_rank);
            let file_path = self.get_file_path(&file_name);

            let mut file = match tokio::fs::File::open(&file_path).await {
                Ok(f) => f,
                Err(_e) => return zero_sector(),
            };

            let mut buffer = [0u8;SECTOR_SIZE];
            if file.read_exact(&mut buffer).await.is_err() {
                return zero_sector();
            }

            SectorVec(Box::new(serde_big_array::Array(buffer)))
        }
    
        async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8) {
            let cache = self.metadata_cache.read().await;
            match cache.get(&idx) {
                Some(&(a,b)) => (a,b),
                None => (0,0)
            }
        }

        async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8)){
            let (data,timestamp,write_rank) = sector;

            let target_file_name = format!("{}_{}_{}",idx,timestamp,write_rank);
            let temp_file_name = format!("tmp_{}_{}_{}",idx,timestamp,write_rank);

            let target_file_path = self.get_file_path(&target_file_name);
            let temp_file_path = self.get_file_path(&temp_file_name);

            let bytes: &[u8] = &data.0[..];

            let mut file = tokio::fs::File::create(&temp_file_path).await.unwrap();
            file.write_all(bytes).await.unwrap();
            file.sync_all().await.unwrap();

            tokio::fs::rename(&temp_file_path,&target_file_path).await.unwrap();

            let mut cache = self.metadata_cache.write().await;
            if let Some((old_timestamp,old_write_rank)) = cache.insert(idx,(*timestamp,*write_rank)) {
                let old_file = format!("{}_{}_{}",idx,old_timestamp,old_write_rank);
                let old_file_path = self.get_file_path(&old_file);
                tokio::fs::remove_file(old_file_path).await.unwrap();
            }
        }
    }

}


pub mod transfer_public {
    use crate::{RegisterCommand};
    use bincode::error::{DecodeError, EncodeError};
    use hmac::{Hmac,Mac};
    use sha2::Sha256;
    use std::io::Error;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

    type HmacSha256 = Hmac<Sha256>;

    #[derive(Debug)]
    pub enum EncodingError {
        IoError(Error),
        BincodeError(EncodeError),
    }

    #[derive(Debug, derive_more::Display)]
    pub enum DecodingError {
        IoError(Error),
        BincodeError(DecodeError),
        InvalidMessageSize,
    }

    pub async fn deserialize_register_command(
        data: &mut (dyn AsyncRead + Send + Unpin),
        hmac_system_key: &[u8; 64],
        hmac_client_key: &[u8; 32],
    ) -> Result<(RegisterCommand, bool), DecodingError> {
        let config = bincode::config::standard()
        .with_big_endian()
        .with_fixed_int_encoding();

        let mut length_buffor = [0u8;8];
        data.read_exact(&mut length_buffor).await.map_err(DecodingError::IoError)?;
        // from BigEndian 
        let length: u64 = u64::from_be_bytes(length_buffor); 

        if length<32{
            return Err(DecodingError::InvalidMessageSize)
        }

        let payload_length = std::cmp::max(0,length-32);
        let mut body_buffor = vec![0u8; payload_length as usize];
        data.read_exact(&mut body_buffor).await.map_err(DecodingError::IoError)?;

        let mut hmac_buffor = [0u8;32];
        data.read_exact(&mut hmac_buffor).await.map_err(DecodingError::IoError)?;

        let (cmd,used) = bincode::serde::decode_from_slice::<RegisterCommand,_>(
            body_buffor.as_slice(),
            config
        ).map_err(DecodingError::BincodeError)?;
        
        if used != body_buffor.len() {
            return Err(DecodingError::BincodeError(
                bincode::error::DecodeError::OtherString("trailing bytes".into()),
            ));
        }

        let key: &[u8] = match &cmd {
            RegisterCommand::System(_) => &hmac_system_key[..],
            RegisterCommand::Client(_) => &hmac_client_key[..],
        };
        
        let mut mac = HmacSha256::new_from_slice(key).expect("Hmac key init failed");
        mac.update(&body_buffor);
        let valid = mac.verify_slice(&hmac_buffor).is_ok();
        Ok((cmd,valid))
    }

    pub async fn serialize_register_command(
        cmd: &RegisterCommand,
        writer: &mut (dyn AsyncWrite + Send + Unpin),
        hmac_key: &[u8],
    ) -> Result<(), EncodingError> {
        let config = bincode::config::standard()
        .with_big_endian()
        .with_fixed_int_encoding();

    
        let payload: Vec<u8> = bincode::serde::encode_to_vec(cmd, config).map_err(EncodingError::BincodeError)?;

        let mut mac = HmacSha256::new_from_slice(hmac_key).expect("Hmac key should have correct length");
        mac.update(&payload);

        let tag = mac.finalize().into_bytes();

        let total_len = (payload.len() + tag.len()) as u64;
        writer.write_all(&total_len.to_be_bytes())
        .await
        .map_err(EncodingError::IoError)?;

        writer.write_all(&payload)
        .await
        .map_err(EncodingError::IoError)?;
        
        writer.write_all(tag.as_slice())
        .await
        .map_err(EncodingError::IoError)?;
    
        Ok(())
    }
}

pub mod register_client_public {
    use crate::SystemRegisterCommand;
    use std::sync::Arc;

    #[async_trait::async_trait]
    /// We do not need any public implementation of this trait. It is there for use
    /// in `AtomicRegister`. In our opinion it is a safe bet to say some structure of
    /// this kind must appear in your solution.
    pub trait RegisterClient: core::marker::Send + Sync {
        /// Sends a system message to a single process.
        async fn send(&self, msg: Send);

        /// Broadcasts a system message to all processes in the system, including self.
        async fn broadcast(&self, msg: Broadcast);
    }

    pub struct Broadcast {
        pub cmd: Arc<SystemRegisterCommand>,
    }

    pub struct Send {
        pub cmd: Arc<SystemRegisterCommand>,
        /// Identifier of the target process. Those start at 1.
        pub target: u8,
    }
}
