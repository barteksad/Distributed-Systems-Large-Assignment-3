use std::{time::SystemTime, collections::{HashSet, HashMap}};

use async_channel::Sender;
use log::debug;
use uuid::Uuid;

use crate::{LogEntry, LogEntryContent, ServerConfig, StableStorage, ClientRequestResponse};

pub struct PersistentState {
    current_term: u64,
    voted_for: Option<Uuid>,
    log: Vec<LogEntry>,
    stable_storage: Box<dyn StableStorage>,
}

#[derive(Clone)]
pub struct ElectionTimeout;

#[derive(Clone)]
pub enum HeartbeatTimeout {
    First,
    NotFirst,
}

#[derive(Clone)]
pub struct Init;

/// State of a Raft process with a corresponding (volatile) information.
#[derive(Clone, Debug)]
pub enum ProcessType {
    Follower,
    Candidate { votes_received: HashSet<Uuid> },
    Leader { 
        next_index: HashMap<Uuid, u64>, 
        match_index: HashMap<Uuid, u64>, 
        heartbeats_received: HashSet<Uuid>,
        last_hearbeat_round_successful: bool,
        client_id2tx: HashMap<Uuid, Sender<ClientRequestResponse>>,
    },
}

impl Default for ProcessType {
    fn default() -> Self {
        ProcessType::Follower
    }
}

impl PersistentState {
    pub async fn new(
        stable_storage: Box<dyn StableStorage>,
        first_log_entry_timestamp: SystemTime,
        config: &ServerConfig,
    ) -> Self {
        // TODO restore the state from the stable storage
        PersistentState {
            current_term: 0,
            voted_for: None,
            log: Self::restore_logs(&stable_storage, config, first_log_entry_timestamp).await,
            stable_storage,
        }
    }

    async fn restore_logs(stable_storage: &Box<dyn StableStorage>, config: &ServerConfig, first_log_entry_timestamp: SystemTime) -> Vec<LogEntry> {
        let mut logs = vec![];
        let mut idx = 0;
        loop {
            let log = stable_storage.get(&format!("log_{}", idx)).await;
            match log {
                Some(maybe_log) => {
                    if let Some(log) = bincode::deserialize(&maybe_log).unwrap() {
                        logs.push(log);
                        idx += 1;
                    } else {
                        break;
                    }
                }
                None => break,
            }
        }
        if logs.is_empty() {
            logs.push(LogEntry {
                term: 0,
                timestamp: first_log_entry_timestamp,
                content: LogEntryContent::Configuration {
                    servers: config.servers.clone(),
                },
            });
        }
        debug!("Initial logs: {:?}", logs);
        logs
    }

    pub fn voted_for(&self) -> Option<Uuid> {
        self.voted_for
    }

    pub async fn set_voted_for(&mut self, new_vote: Option<Uuid>) {
        self.stable_storage
            .put("voted_for", &bincode::serialize(&new_vote).unwrap())
            .await
            .unwrap();
        self.voted_for = new_vote;
    }

    pub fn current_term(&self) -> u64 {
        self.current_term
    }

    pub async fn set_current_term(&mut self, new_term: u64) {
        self.stable_storage
            .put("current_term", &bincode::serialize(&new_term).unwrap())
            .await
            .unwrap();
        self.current_term = new_term;
    }

    pub fn log(&self) -> &Vec<LogEntry> {
        &self.log
    }

    pub async fn append_log(&mut self, log: LogEntry) {
        let next_idx = self.log.len();
        self.stable_storage
            .put(&format!("log_{}", next_idx), &bincode::serialize(&Some(log.clone())).unwrap())
            .await
            .unwrap();
        self.log.push(log);
    }

    pub async fn delete_logs_from(&mut self, idx: usize) {
        self.stable_storage
            .put(&format!("log_{}", idx), &bincode::serialize::<Option<LogEntry>>(&None).unwrap())
            .await
            .unwrap();
        self.log.truncate(idx);
    }
}
