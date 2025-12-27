mod domain;

use std::{collections::{HashMap, HashSet}, path::PathBuf, sync::Arc, thread::current};

pub use crate::domain::*;
pub use atomic_register_public::*;
use base64::{read, write};
pub use register_client_public::*;
pub use sectors_manager_public::*;
use tokio::{net::TcpListener, sync::RwLock};
pub use transfer_public::*;
use uuid::Uuid;

pub async fn run_register_process(config: Configuration) {
    // unimplemented!()
    let (host,port) = &config.public.tcp_locations[(config.public.self_rank-1) as usize];
    let ip_with_port = format!("{}:{}",host,port);
    let socket = TcpListener::bind(ip_with_port).await.unwrap();

    let sectors_manager = build_sectors_manager(config.public.storage_dir.clone()).await;

}

// Added structs and functions
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
    // New Fields 
    write_msg_ident: Option<Uuid>,
    read_data: Option<SectorVec>,
    // ----------------
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
    // in this map every operation that is currently being executed 
    // is assigned with a unique number 
    operation_states: HashMap<u64,OperationState>, // request_id -> OperationState
    counter: u64, // for generating unique ids
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

// ---------------------------

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

        // we remember the state for every operation
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
                // gathering current data 
                let (current_timestamp,current_write_rank) = self.sectors_manager.read_metadata(self.sector_idx).await;
                // in rust tuples by default are compared lexicographically 
                // we can compare them like this 
                if (current_timestamp,current_write_rank) < (timestamp,write_rank) {
                    let to_write = (data_to_write,timestamp,write_rank);
                    self.sectors_manager.write(self.sector_idx,&to_write).await;
                }

                let header = SystemCommandHeader{
                    process_identifier: self.ident,
                    msg_ident: cmd.header.msg_ident, // zmienione na cmd.header.msg 
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

                // node knows that some operation has been finished by getting an ack message
                // then it can just return the answear to the client (one of possible many users)
                // if the operation has finished we remove it from the list
                // and invoke the callback function
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
        AtomicRegisterNode, ClientCommandResponse, ClientRegisterCommand, OperationState, RegisterClient, SectorIdx, SectorsManager, SystemRegisterCommand
    };
    use std::collections::HashMap;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

    #[async_trait::async_trait]
    pub trait AtomicRegister: Send + Sync {
        /// Handle a client command. After the command is completed, we expect
        /// callback to be called. Note that completion of client command happens after
        /// delivery of multiple system commands to the register, as the algorithm specifies.
        ///
        /// This function corresponds to the handlers of Read and Write events in the
        /// (N,N)-AtomicRegister algorithm.
        async fn client_command(
            &mut self,
            cmd: ClientRegisterCommand,
            success_callback: Box<
                dyn FnOnce(ClientCommandResponse) -> Pin<Box<dyn Future<Output = ()> + Send>>
                    + Send
                    + Sync,
            >,
        );

        /// Handle a system command.
        ///
        /// This function corresponds to the handlers of `SystemRegisterCommand` messages in the (N,N)-AtomicRegister algorithm.
        async fn system_command(&mut self, cmd: SystemRegisterCommand);
    }

    /// Idents are numbered starting at 1 (up to the number of processes in the system).
    /// Communication with other processes of the system is to be done by `register_client`.
    /// And sectors must be stored in the `sectors_manager` instance.
    ///
    /// This function corresponds to the handlers of Init and Recovery events in the
    /// (N,N)-AtomicRegister algorithm.
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
            operation_states: HashMap::new(),
            counter:0
        })
    }
}

// structs and helper functions 
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
    use base64::write;
    use hmac::digest::generic_array::arr;
    use hmac::digest::typenum::Zero;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::RwLock;
    use crate::{SECTOR_SIZE, SectorIdx, SectorVec, parse_meta, zero_sector};
    use std::collections::HashMap;
    use std::fmt::format;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;

    struct DiskSectorsManager {
        directory: PathBuf,
        // required security for threads
        metadata_cache: RwLock<HashMap<SectorIdx,(u64, u8)>>,
    }

    impl DiskSectorsManager{
        async fn get_metadata(&self,idx: SectorIdx)-> Option<(u64,u8)> {
            let cache = self.metadata_cache.read().await;
            cache.get(&idx).copied()
        }

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
    use crate::RegisterCommand;
    use bincode::error::{DecodeError, EncodeError};
    use std::io::Error;
    use tokio::io::{AsyncRead, AsyncWrite};
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
        unimplemented!()
    }

    pub async fn serialize_register_command(
        cmd: &RegisterCommand,
        writer: &mut (dyn AsyncWrite + Send + Unpin),
        hmac_key: &[u8],
    ) -> Result<(), EncodingError> {
        unimplemented!()
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
