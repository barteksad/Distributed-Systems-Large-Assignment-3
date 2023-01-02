use std::{
    cmp::min,
    collections::HashSet,
    ops::Sub,
    time::{Duration, SystemTime},
};

use basic_raft::{ElectionTimeout, HeartbeatTimeout, Init, PersistentState, ProcessType};
use executor::{Handler, ModuleRef, System, TimerHandle};

pub use domain::*;
use rand::Rng;
use uuid::Uuid;

mod basic_raft;
mod domain;

pub struct Raft {
    config: ServerConfig,
    pstate: PersistentState,
    commit_index: usize,
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

    async fn send_append_entries(&mut self) {
        if let ProcessType::Leader {
            next_index,
            match_index,
            ..
        } = &self.process_type
        {
            let current_term = self.pstate.current_term();
            let leader_id = self.config.self_id;
            let prev_log_index = self.pstate.log().len() - 1;
            let prev_log_term = self.pstate.log().last().map(|e| e.term).unwrap();
            let leader_commit = self.commit_index;

            for server in &self.config.servers {
                if server == &self.config.self_id {
                    continue;
                }

                let next_index = next_index.get(server).unwrap();
                let match_index = match_index.get(server).unwrap();
                let entries = match *next_index == (match_index + 1) {
                    true => {
                        let rhs: usize = (*next_index).try_into().unwrap();
                        let take_n = min(
                            self.config.append_entries_batch_size,
                            self.pstate.log().len() - rhs,
                        );
                        self.pstate
                            .log()
                            .iter()
                            .skip((*next_index).try_into().unwrap())
                            .take(take_n)
                            .cloned()
                            .collect()
                    }
                    false => vec![],
                };

                let msg = RaftMessage {
                    header: RaftMessageHeader {
                        term: current_term,
                        source: leader_id,
                    },
                    content: RaftMessageContent::AppendEntries(AppendEntriesArgs {
                        prev_log_index,
                        prev_log_term,
                        entries,
                        leader_commit,
                    }),
                };

                self.message_sender.send(server, msg).await;
            }
        } else {
            panic!("send_append_entries called on non-leader");
        }
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
        args: RequestVoteResponseArgs,
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
                        self.current_leader = Some(self.config.self_id);
                        self.last_leader_timestamp = None;
                        // When a server becomes a leader, it must append a NoOp entry to the log (nextIndex must be initialized with the index of this entry).
                        self.pstate
                            .append_log(LogEntry {
                                content: LogEntryContent::NoOp,
                                term: self.pstate.current_term(),
                                timestamp: SystemTime::now(),
                            })
                            .await;
                        self.process_type = ProcessType::Leader {
                            next_index: self
                                .config
                                .servers
                                .iter()
                                .map(|uid| {
                                    (*uid, (self.pstate.log().len() + 1).try_into().unwrap())
                                })
                                .collect(),
                            match_index: self.config.servers.iter().map(|uid| (*uid, 0)).collect(),
                            heartbeats_received: HashSet::from([self.config.self_id]),
                            last_hearbeat_round_successful: true,
                        }
                    };
                    self.self_ref
                        .as_ref()
                        .unwrap()
                        .send(HeartbeatTimeout::First)
                        .await;
                    self.self_ref
                        .as_ref()
                        .unwrap()
                        .request_tick(HeartbeatTimeout::NotFirst, self.config.heartbeat_timeout)
                        .await;
                }
            }
        }
    }

    async fn handle_append_entries(
        &mut self,
        header: &RaftMessageHeader,
        args: AppendEntriesArgs,
    ) -> Option<RaftMessageContent> {
        if header.term < self.pstate.current_term() {
            self.update_term(header.term).await;
            Some(RaftMessageContent::AppendEntriesResponse(AppendEntriesResponseArgs {
                success: false,
                last_verified_log_index: args.prev_log_index + args.entries.len(),
            }))
        } else {
            if header.term > self.pstate.current_term() {
                self.update_term(header.term).await;
            }
            self.reset_election_timer().await;
            self.current_leader = Some(header.source);
            self.last_leader_timestamp = Some(SystemTime::now());
            self.process_type = ProcessType::Follower;

            let prev_log_index = args.prev_log_index;
            match self.pstate.log().get(prev_log_index) {
                Some(LogEntry { content, term, timestamp }) => {
                    let last_verified_log_index = args.prev_log_index + args.entries.len();
                    let success = match *term == args.prev_log_term {
                        false => {
                            self.pstate.delete_logs_from(prev_log_index).await;
                            false
                        },
                        true => {
                            for entry in args.entries {
                                self.pstate.append_log(entry).await;
                            }
                            while self.commit_index < args.leader_commit {
                                match self.pstate.log().get(self.commit_index).unwrap() {
                                    LogEntry { content: LogEntryContent::Command { data, ..}, .. } => {
                                        self.state_machine.apply(data).await;
                                    },
                                    _ => (),
                                }
                                self.commit_index += 1;
                            }
                            true
                        }
                    };
                    return Some(RaftMessageContent::AppendEntriesResponse(AppendEntriesResponseArgs {
                        success,
                        last_verified_log_index,
                    }));
                },
                None => {
                    return Some(RaftMessageContent::AppendEntriesResponse(AppendEntriesResponseArgs {
                        success: false,
                        last_verified_log_index: args.prev_log_index + args.entries.len(),
                    }));
                },
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
            }
            RaftMessageContent::AppendEntries(args) => {
                self.handle_append_entries(&msg.header, args).await
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

#[async_trait::async_trait]
impl Handler<HeartbeatTimeout> for Raft {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, msg: HeartbeatTimeout) {
        self.send_append_entries().await;
        if let ProcessType::Leader { heartbeats_received, last_hearbeat_round_successful, .. } = &mut self.process_type {
            let majority_count = self.config.servers.len()/2 + self.config.servers.len() % 2;
            *last_hearbeat_round_successful = match (msg, heartbeats_received.len() > majority_count) {
                (HeartbeatTimeout::First, _) => true,
                (HeartbeatTimeout::NotFirst, val) => val,
            };

            heartbeats_received.clear();
            heartbeats_received.insert(self.config.self_id);
        } else {
            panic!("HeartbeatTimeout received by non-leader")
        }
    }
}
