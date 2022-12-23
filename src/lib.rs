use std::{
    collections::HashSet,
    time::{Duration, SystemTime},
};

use basic_raft::{ElectionTimeout, Init, PersistentState, ProcessType};
use executor::{Handler, ModuleRef, System, TimerHandle};

pub use domain::*;
use rand::Rng;
use uuid::Uuid;

mod basic_raft;
mod domain;

pub struct Raft {
    config: ServerConfig,
    pstate: PersistentState,
    commit_index: u64,
    last_applied: u64,
    state_machine: Box<dyn StateMachine>,
    message_sender: Box<dyn RaftSender>,
    process_type: ProcessType,
    current_leader: Option<Uuid>,
    last_leader_timestamp: Option<SystemTime>,
    election_timer_handle: Option<TimerHandle>,
    self_ref: Option<ModuleRef<Self>>,
}

impl Raft {
    /// Registers a new `Raft` module in the `system`, initializes it and
    /// returns a `ModuleRef` to it.
    pub async fn new(
        system: &mut System,
        config: ServerConfig,
        first_log_entry_timestamp: SystemTime,
        state_machine: Box<dyn StateMachine>,
        stable_storage: Box<dyn StableStorage>,
        message_sender: Box<dyn RaftSender>,
    ) -> ModuleRef<Self> {
        let pstate = PersistentState::new(stable_storage, first_log_entry_timestamp, &config).await;
        let raft = Raft {
            config,
            pstate,
            commit_index: 0,
            last_applied: 0,
            state_machine,
            message_sender,
            process_type: ProcessType::default(),
            current_leader: None,
            last_leader_timestamp: None,
            election_timer_handle: None,
            self_ref: None,
        };

        let module_ref = system.register_module(raft).await;
        module_ref.send(Init).await;
        module_ref
    }

    async fn update_term(&mut self, new_term: u64) {
        assert!(self.pstate.current_term() < new_term);
        self.pstate.set_current_term(new_term).await;
        self.pstate.set_voted_for(None).await;
        self.current_leader = None;
    }

    async fn reset_election_timer(&mut self) {
        if let Some(handle) = self.election_timer_handle.take() {
            handle.stop().await;
        }

        let election_min = self.config.election_timeout_range.start().as_micros();
        let election_max = self.config.election_timeout_range.end().as_micros();
        let timeout = Duration::from_micros(
            rand::thread_rng()
                .gen_range(election_min..election_max)
                .try_into()
                .unwrap(),
        );

        self.election_timer_handle = Some(
            self.self_ref
                .as_ref()
                .unwrap()
                .request_tick(ElectionTimeout, timeout)
                .await,
        );
    }

    async fn handle_request_vote(
        &mut self,
        header: &RaftMessageHeader,
        args: RequestVoteArgs,
    ) -> Option<RaftMessageContent> {
        // (Chapter 4.2.3 of [1])
        if let Some(timestamp) = self.last_leader_timestamp {
            if timestamp.elapsed().unwrap() < *self.config.election_timeout_range.start() {
                return None;
            }
        }

        if let ProcessType::Leader { .. } = self.process_type {
            return None;
        }

        if header.term > self.pstate.current_term() {
            self.update_term(header.term).await;
        }

        let vote_granted = match (
            header.term < self.pstate.current_term(),
            self.pstate.voted_for(),
        ) {
            (false, None) => {
                // page 22 & 23
                let logs = self.pstate.log();
                let last_log_index = logs.len();
                let last_log_term = logs.last().map(|e| e.term).unwrap();
                if args.last_log_term > last_log_term
                    || (args.last_log_term == last_log_term
                        && args.last_log_index >= last_log_index)
                {
                    true
                } else {
                    false
                }
            }
            _ => false,
        };

        match vote_granted {
            true => {
                self.pstate.set_voted_for(Some(header.source)).await;
                self.reset_election_timer().await;

                Some(RaftMessageContent::RequestVoteResponse(
                    RequestVoteResponseArgs { vote_granted: true },
                ))
            }
            false => None,
        }
    }

    async fn handle_request_vote_response(
        &mut self,
        header: &RaftMessageHeader,
        args: RequestVoteResponseArgs
    ) {
        match &mut self.process_type {
            ProcessType::Follower => return,
            ProcessType::Leader { .. } => return,
            ProcessType::Candidate { votes_received } => {
                if header.term > self.pstate.current_term() {
                    self.update_term(header.term).await;
                    self.process_type = ProcessType::Follower;
                } else if args.vote_granted {
                    votes_received.insert(header.source);
                    if votes_received.len() > (self.config.servers.len() + 1) / 2 {
                        self.process_type = ProcessType::Leader {
                            next_index: self.config.servers.iter().map(|uid| (*uid, (self.pstate.log().len() + 1) as u64)).collect(),
                            match_index: self.config.servers.iter().map(|uid| (*uid, 0)).collect(),
                        }};
                        self.current_leader = Some(self.config.self_id);
                        self.last_leader_timestamp = None;
                    }
                }
            }
        }
    }


#[async_trait::async_trait]
impl Handler<RaftMessage> for Raft {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, msg: RaftMessage) {
        let reply_content = match msg.content {
            RaftMessageContent::RequestVote(args) => {
                self.handle_request_vote(&msg.header, args).await
            }
            RaftMessageContent::RequestVoteResponse(args) => {
                self.handle_request_vote_response(&msg.header, args).await;
                None
            },
            _ => unimplemented!(),
        };

        if let Some(content) = reply_content {
            self.message_sender
                .send(
                    &msg.header.source,
                    RaftMessage {
                        header: RaftMessageHeader {
                            source: self.config.self_id,
                            term: self.pstate.current_term(),
                        },
                        content,
                    },
                )
                .await;
        }
    }
}

#[async_trait::async_trait]
impl Handler<ClientRequest> for Raft {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, msg: ClientRequest) {
        todo!()
    }
}

// TODO you can implement handlers of messages of other types for the Raft struct.

#[async_trait::async_trait]
impl Handler<Init> for Raft {
    async fn handle(&mut self, self_ref: &ModuleRef<Self>, _msg: Init) {
        self.reset_election_timer().await;
    }
}

#[async_trait::async_trait]
impl Handler<ElectionTimeout> for Raft {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, _msg: ElectionTimeout) {
        if let ProcessType::Leader { .. } = self.process_type {
            unimplemented!()
        }
        self.update_term(self.pstate.current_term() + 1).await;
        self.pstate.set_voted_for(Some(self.config.self_id)).await;
        self.process_type = ProcessType::Candidate {
            votes_received: HashSet::from([self.config.self_id]),
        };

        if self.config.servers.len() == 1 {
            unimplemented!();
        } else {
            let logs = self.pstate.log();
            let msg = RaftMessage {
                header: RaftMessageHeader {
                    source: self.config.self_id,
                    term: self.pstate.current_term(),
                },
                content: RaftMessageContent::RequestVote(RequestVoteArgs {
                    last_log_index: logs.len(),
                    last_log_term: logs.last().map(|e| e.term).unwrap(),
                }),
            };

            for server in &self.config.servers {
                if *server != self.config.self_id {
                    self.message_sender.send(server, msg.clone()).await;
                }
            }
        }
    }
}
