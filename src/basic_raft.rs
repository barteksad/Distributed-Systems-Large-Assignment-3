use std::time::SystemTime;

use uuid::Uuid;

use crate::{LogEntry, LogEntryContent, ServerConfig, StableStorage};

pub struct PersistentState {
    current_term: u64,
    voted_for: Option<Uuid>,
    log: Vec<LogEntry>,
    stable_storage: Box<dyn StableStorage>,
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
            log: vec![LogEntry {
                term: 0,
                timestamp: first_log_entry_timestamp,
                content: LogEntryContent::Configuration {
                    servers: config.servers.clone(),
                },
            }],
            stable_storage,
        }
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

    pub async fn set_current_tern(&mut self, new_term: u64) {
        self.stable_storage
            .put("current_term", &bincode::serialize(&new_term).unwrap())
            .await
            .unwrap();
        self.current_term = new_term;
    }
}
