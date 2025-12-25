mod domain;

use std::{collections::{HashMap, HashSet}, sync::Arc, thread::current};

pub use crate::domain::*;
pub use atomic_register_public::*;
use base64::write;
pub use register_client_public::*;
pub use sectors_manager_public::*;
pub use transfer_public::*;
use uuid::Uuid;

pub async fn run_register_process(config: Configuration) {
    unimplemented!()
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
            (ClientRegisterCommandContent::Read) => {OperationType::Read},
            (ClientRegisterCommandContent::Write { data }) => {OperationType::Write(data)}
        };

        let state = OperationState{
            request_id:op_id,
            read_list: HashMap::new(),
            ack_list: HashSet::new(),
            op_type:op_type,
            callback:success_callback,
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
            (SystemRegisterCommandContent::ReadProc) =>{
                
            },
            (SystemRegisterCommandContent::Value{timestamp,write_rank,sector_data}) =>{
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

                            let header = SystemCommandHeader{
                                process_identifier: self.ident,
                                msg_ident: Uuid::new_v4(),
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
            (SystemRegisterCommandContent::WriteProc { timestamp, write_rank, data_to_write }) => {
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
            (SystemRegisterCommandContent::Ack) => {
                // TODO
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

pub mod sectors_manager_public {
    use crate::{SectorIdx, SectorVec};
    use std::path::PathBuf;
    use std::sync::Arc;

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
        unimplemented!()
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
