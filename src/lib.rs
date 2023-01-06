use std::{
    cmp::{max, min},
    collections::{HashMap, HashSet},
    time::{Duration, SystemTime},
};

use basic_raft::{ElectionTimeout, HeartbeatTimeout, Init, PersistentState, ProcessType};
use executor::{Handler, ModuleRef, System, TimerHandle};

pub use domain::*;
use log::{debug, error, info};
use rand::Rng;
use uuid::Uuid;

mod basic_raft;
mod domain;

pub struct Raft {
    config: ServerConfig,
    pstate: PersistentState,
    commit_index: usize,
    last_applied: usize,
    state_machine: Box<dyn StateMachine>,
    message_sender: Box<dyn RaftSender>,
    process_type: ProcessType,
    current_leader: Option<Uuid>,
    last_leader_timestamp: Option<SystemTime>,
    election_timer_handle: Option<TimerHandle>,
    heartbeat_timer_handle: Option<TimerHandle>,
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
            heartbeat_timer_handle: None,
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
                .gen_range(election_min..=election_max)
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

    async fn send_append_entries(&mut self, target_one: Option<Uuid>) {
        if let ProcessType::Leader {
            next_index,
            match_index,
            ..
        } = &self.process_type
        {
            let current_term = self.pstate.current_term();
            let leader_id = self.config.self_id;
            let leader_commit = self.commit_index;

            let targets: Vec<&Uuid> = match target_one.as_ref() {
                Some(target) => vec![&target],
                None => self
                    .config
                    .servers
                    .iter()
                    .filter(|server| **server != self.config.self_id)
                    .collect(),
            };

            for server in targets {
                if *server == self.config.self_id {
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

                let prev_log_term = self
                    .pstate
                    .log()
                    .get(*match_index as usize)
                    .map(|e| e.term)
                    .unwrap();

                // Used after receiving successful AppendEntries response
                if target_one.is_some() && entries.is_empty() {
                    continue;
                }

                let msg = RaftMessage {
                    header: RaftMessageHeader {
                        term: current_term,
                        source: leader_id,
                    },
                    content: RaftMessageContent::AppendEntries(AppendEntriesArgs {
                        prev_log_index: *match_index as usize,
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
            if timestamp.elapsed().unwrap() <= *self.config.election_timeout_range.start() {
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
                self.reset_election_timer().await;
                self.pstate.set_voted_for(Some(header.source)).await;

                Some(RaftMessageContent::RequestVoteResponse(
                    RequestVoteResponseArgs { vote_granted: true },
                ))
            }
            false => Some(RaftMessageContent::RequestVoteResponse(
                RequestVoteResponseArgs {
                    vote_granted: false,
                },
            )),
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
                    if 2 * votes_received.len() > self.config.servers.len() {
                        self.become_leader().await;
                    };
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
            // self.update_term(header.term).await;
            Some(RaftMessageContent::AppendEntriesResponse(
                AppendEntriesResponseArgs {
                    success: false,
                    last_verified_log_index: args.prev_log_index + args.entries.len(),
                },
            ))
        } else {
            if header.term > self.pstate.current_term() {
                self.update_term(header.term).await;
            }
            self.reset_election_timer().await;
            self.current_leader = Some(header.source);
            self.last_leader_timestamp = Some(SystemTime::now());
            self.process_type = ProcessType::Follower;
            if let Some(timer_handle) = self.heartbeat_timer_handle.take() {
                timer_handle.stop().await;
            }

            let prev_log_index = args.prev_log_index;
            match self.pstate.log().get(prev_log_index) {
                Some(LogEntry { term, .. }) => {
                    let last_verified_log_index = args.prev_log_index + args.entries.len();
                    let success = match *term == args.prev_log_term {
                        false => {
                            self.pstate.delete_logs_from(prev_log_index).await;
                            false
                        }
                        true => {
                            for entry in args.entries {
                                self.pstate.append_log(entry).await;
                            }
                            if args.leader_commit > self.commit_index {
                                self.commit_index =
                                    min(args.leader_commit, self.pstate.log().len());
                            }

                            while self.last_applied <= self.commit_index {
                                match self.pstate.log().get(self.last_applied) {
                                    Some(LogEntry {
                                        content: LogEntryContent::Command { data, .. },
                                        ..
                                    }) => {
                                        self.state_machine.apply(data).await;
                                        self.last_applied += 1;
                                    }
                                    None => break,
                                    _ => self.last_applied += 1,
                                }
                            }
                            true
                        }
                    };
                    return Some(RaftMessageContent::AppendEntriesResponse(
                        AppendEntriesResponseArgs {
                            success,
                            last_verified_log_index,
                        },
                    ));
                }
                None => {
                    return Some(RaftMessageContent::AppendEntriesResponse(
                        AppendEntriesResponseArgs {
                            success: false,
                            last_verified_log_index: args.prev_log_index + args.entries.len(),
                        },
                    ));
                }
            }
        }
    }

    async fn handle_append_entries_response(
        &mut self,
        header: &RaftMessageHeader,
        args: AppendEntriesResponseArgs,
    ) -> Option<RaftMessageContent> {
        if header.term > self.pstate.current_term() {
            self.update_term(header.term).await;
            self.process_type = ProcessType::Follower;
            if let Some(timer_handle) = self.heartbeat_timer_handle.take() {
                timer_handle.stop().await;
            }
        } else {
            match &mut self.process_type {
                ProcessType::Leader {
                    next_index,
                    match_index,
                    heartbeats_received,
                    client_id2tx,
                    sessions,
                    duplicated_commands,
                    ..
                } => {
                    heartbeats_received.insert(header.source);
                    match args.success {
                        false => {
                            if let Some(next_index_val) = next_index.get_mut(&header.source) {
                                if next_index_val > &mut 0 {
                                    *next_index_val -= 1;
                                }
                            }
                            self.send_append_entries(Some(header.source)).await;
                        }
                        true => {
                            if let Some(next_index_val) = next_index.get_mut(&header.source) {
                                *next_index_val =
                                    u64::try_from(args.last_verified_log_index).unwrap() + 1;
                            }
                            if let Some(match_index_val) = match_index.get_mut(&header.source) {
                                *match_index_val = args.last_verified_log_index.try_into().unwrap();
                            }
                            for new_commit_index in self.commit_index..=args.last_verified_log_index
                            {
                                let mut count = 1;
                                for (_, match_index) in match_index.iter() {
                                    if *match_index >= new_commit_index.try_into().unwrap() {
                                        count += 1;
                                    }
                                }
                                if 2 * count > self.config.servers.len() {
                                    match self.pstate.log().get(new_commit_index) {
                                        Some(LogEntry { term, .. }) => {
                                            if term == &self.pstate.current_term() {
                                                self.commit_index = new_commit_index;
                                            }
                                        }
                                        None => break,
                                    }
                                } else {
                                    break;
                                }
                            }
                            // Uuid::from_u128(self.pstate.log().len() as u128)
                            while self.last_applied <= self.commit_index {
                                match self.pstate.log().get(self.last_applied) {
                                    Some(LogEntry {
                                        timestamp,
                                        content:
                                            LogEntryContent::Command {
                                                data,
                                                client_id,
                                                sequence_num,
                                                ..
                                            },
                                        ..
                                    }) => {
                                        debug!("Committing command by leader");
                                        if let Some(tx) = client_id2tx.get(client_id) {
                                            match sessions.get_mut(client_id) {
                                                Some(session)
                                                    if session.last_activity.elapsed().unwrap()
                                                        < self.config.session_expiration
                                                        && session
                                                            .lowest_sequence_num_without_response
                                                            <= *sequence_num =>
                                                {
                                                    let output_to_send = match session
                                                        .responses
                                                        .get(sequence_num)
                                                    {
                                                        Some(response) => {
                                                            error!("Sending response from session");
                                                            response.clone()
                                                        }
                                                        None => {
                                                            debug!("Sending response from state machine");
                                                            error!("Store response in session seq: {} client: {}", sequence_num, client_id);
                                                            let output = self
                                                            .state_machine
                                                            .apply(data)
                                                            .await;
                                                            session.responses.insert(
                                                                *sequence_num,
                                                                output.clone(),
                                                            );
                                                            error!("duplication: {:?}", session.responses);
                                                            output
                                                        }
                                                    };
                                                    let n_duplicates = duplicated_commands.remove(&(
                                                        *client_id,
                                                        *sequence_num,
                                                    )).unwrap();
                                                    for _ in 0..n_duplicates {
                                                        
                                                        if let Err(e) = tx
                                                        .send(ClientRequestResponse::CommandResponse(
                                                            CommandResponseArgs {
                                                                content:
                                                                CommandResponseContent::CommandApplied {
                                                                    output: output_to_send.clone(),
                                                                },
                                                                client_id: *client_id,
                                                                sequence_num: *sequence_num,
                                                            },
                                                        ))
                                                        .await
                                                        
                                                        {
                                                            debug!(
                                                                "Failed to send command response to client: {}",
                                                                e
                                                            );
                                                        }
                                                    }
                                                    session.last_activity = *timestamp;
                                                }
                                                _ => {
                                                    debug!("Client session expired");
                                                    sessions.remove(client_id);
                                                    if let Err(e) = tx
                                                        .send(ClientRequestResponse::CommandResponse(
                                                            CommandResponseArgs {
                                                                content:
                                                                    CommandResponseContent::SessionExpired,
                                                                client_id: *client_id,
                                                                sequence_num: *sequence_num,
                                                            },
                                                        ))
                                                        .await
                                                    {
                                                        debug!(
                                                            "Failed to send command response to client: {}",
                                                            e
                                                        );
                                                    }
                                                }
                                            }
                                        } else {
                                            self.state_machine.apply(data).await;
                                        }
                                        self.last_applied += 1;
                                    }
                                    Some(LogEntry {
                                        timestamp,
                                        content: LogEntryContent::RegisterClient,
                                        ..
                                    }) => {
                                        debug!("Committing register client by leader");
                                        let client_id = Uuid::from_u128(self.last_applied as u128);
                                        if let Some(tx) = client_id2tx.get(&client_id) {
                                            sessions.insert(
                                                client_id,
                                                ClientSession {
                                                    last_activity: *timestamp,
                                                    responses: HashMap::new(),
                                                    lowest_sequence_num_without_response: 0,
                                                },
                                            );
                                            if let Err(e) = tx.send(ClientRequestResponse::RegisterClientResponse(RegisterClientResponseArgs {
                                            content: RegisterClientResponseContent::ClientRegistered { client_id }
                                        })).await {
                                            error!("Failed to send register response to client: {}", e);
                                        }
                                        } else {
                                            error!("No tx for client_id: {}", client_id);
                                            panic!();
                                        }
                                        self.last_applied += 1;
                                    }
                                    None => break,
                                    _ => self.last_applied += 1,
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        None
    }

    async fn become_leader(&mut self) {
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
                .map(|uid| (*uid, (self.pstate.log().len() - 1).try_into().unwrap()))
                .collect(),
            match_index: self.config.servers.iter().map(|uid| (*uid, 0)).collect(),
            heartbeats_received: HashSet::from([self.config.self_id]),
            last_hearbeat_round_successful: true,
            client_id2tx: HashMap::new(),
            sessions: HashMap::new(),
            duplicated_commands: HashMap::new(),
        };
        self.self_ref
            .as_ref()
            .unwrap()
            .send(HeartbeatTimeout::First)
            .await;
        self.heartbeat_timer_handle = Some(
            self.self_ref
                .as_ref()
                .unwrap()
                .request_tick(HeartbeatTimeout::NotFirst, self.config.heartbeat_timeout)
                .await,
        );
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
            }
            RaftMessageContent::AppendEntriesResponse(args) => {
                self.handle_append_entries_response(&msg.header, args).await;
                None
            }
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
        match msg.content {
            ClientRequestContent::Command {
                command,
                client_id,
                sequence_num,
                lowest_sequence_num_without_response,
            } => match &mut self.process_type {
                ProcessType::Leader {
                    client_id2tx,
                    sessions,
                    duplicated_commands,
                    ..
                } => {
                    if let Some(client_session) = sessions.get_mut(&client_id) {
                        if client_session.last_activity.elapsed().unwrap()
                            > self.config.session_expiration
                            || sequence_num < client_session.lowest_sequence_num_without_response
                        {
                            info!("Session expired");
                            info!("New command with sequence_num: {}, lowest_sequence_num_without_response: {} ", sequence_num, client_session.lowest_sequence_num_without_response);
                            // sessions.remove(&client_id);
                            if let Err(e) = msg
                                .reply_to
                                .send(ClientRequestResponse::CommandResponse(
                                    CommandResponseArgs {
                                        client_id,
                                        sequence_num,
                                        content: CommandResponseContent::SessionExpired,
                                    },
                                ))
                                .await
                            {
                                error!("Failed to send command response to client: {}", e);
                            };
                            return;
                        } else {
                            client_session.lowest_sequence_num_without_response = max(
                                lowest_sequence_num_without_response,
                                client_session.lowest_sequence_num_without_response,
                            );
                            error!("stored commands: {:?}", client_session.responses);
                            if let Some(responses) = client_session.responses.get(&sequence_num) {
                                error!("Duplicated command: {} {}", client_id, sequence_num);
                                return if let Err(e) = msg
                                    .reply_to
                                    .send(ClientRequestResponse::CommandResponse(
                                        CommandResponseArgs {
                                            client_id,
                                            sequence_num,
                                            content: CommandResponseContent::CommandApplied {
                                                output: responses.clone(),
                                            },
                                        },
                                    ))
                                    .await
                                {
                                    error!("Failed to send command response to client: {}", e);
                                };
                            } else {
                                if let Some(duplicated_command) =
                                    duplicated_commands.get_mut(&(client_id, sequence_num))
                                {
                                    *duplicated_command += 1;
                                } else {
                                    duplicated_commands.insert((client_id, sequence_num), 1);
                                    self.pstate
                                        .append_log(LogEntry {
                                            term: self.pstate.current_term(),
                                            timestamp: SystemTime::now(),
                                            content: LogEntryContent::Command {
                                                data: command,
                                                client_id,
                                                sequence_num,
                                                lowest_sequence_num_without_response: client_session.lowest_sequence_num_without_response,
                                            },
                                        })
                                        .await;
                                }
                                client_id2tx.insert(client_id, msg.reply_to);
                            }
                        }
                    } else {
                        if let Err(e) = msg
                            .reply_to
                            .send(ClientRequestResponse::CommandResponse(
                                CommandResponseArgs {
                                    client_id,
                                    sequence_num,
                                    content: CommandResponseContent::SessionExpired,
                                },
                            ))
                            .await
                        {
                            error!("Failed to send command response to client: {}", e);
                        };
                    }
                }
                _ => {
                    if let Err(e) = msg
                        .reply_to
                        .send(ClientRequestResponse::CommandResponse(
                            CommandResponseArgs {
                                content: CommandResponseContent::NotLeader {
                                    leader_hint: self.current_leader,
                                },
                                client_id,
                                sequence_num,
                            },
                        ))
                        .await
                    {
                        debug!("Failed to send command response to client: {}", e);
                    }
                }
            },
            ClientRequestContent::Snapshot => unimplemented!("Snapshots omitted"),
            ClientRequestContent::AddServer { .. } => {
                unimplemented!("Cluster membership changes omitted")
            }
            ClientRequestContent::RemoveServer { .. } => {
                unimplemented!("Cluster membership changes omitted")
            }
            ClientRequestContent::RegisterClient => match &mut self.process_type {
                ProcessType::Leader { client_id2tx, .. } => {
                    self.pstate
                        .append_log(LogEntry {
                            term: self.pstate.current_term(),
                            timestamp: SystemTime::now(),
                            content: LogEntryContent::RegisterClient,
                        })
                        .await;
                    client_id2tx.insert(
                        Uuid::from_u128((self.pstate.log().len() - 1) as u128),
                        msg.reply_to,
                    );
                }
                _ => {
                    msg.reply_to
                        .send(ClientRequestResponse::RegisterClientResponse(
                            RegisterClientResponseArgs {
                                content: RegisterClientResponseContent::NotLeader {
                                    leader_hint: self.current_leader,
                                },
                            },
                        ))
                        .await
                        .unwrap();
                }
            },
        }
    }
}

// TODO you can implement handlers of messages of other types for the Raft struct.

#[async_trait::async_trait]
impl Handler<Init> for Raft {
    async fn handle(&mut self, self_ref: &ModuleRef<Self>, _msg: Init) {
        self.self_ref = Some(self_ref.clone());
        self.reset_election_timer().await;
    }
}

#[async_trait::async_trait]
impl Handler<ElectionTimeout> for Raft {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, _msg: ElectionTimeout) {
        if let ProcessType::Leader {
            last_hearbeat_round_successful,
            ..
        } = &mut self.process_type
        {
            if !*last_hearbeat_round_successful {
                debug!("Leader heartbeat round not successful!, leader steps down");
                self.process_type = ProcessType::Follower;
                self.heartbeat_timer_handle.take().unwrap().stop().await;
                self.current_leader = None;
                return;
            } else {
                *last_hearbeat_round_successful = false;
                return;
            }
        }
        self.update_term(self.pstate.current_term() + 1).await;
        self.pstate.set_voted_for(Some(self.config.self_id)).await;
        self.process_type = ProcessType::Candidate {
            votes_received: HashSet::from([self.config.self_id]),
        };

        if self.config.servers.len() == 1 {
            self.become_leader().await;
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
        if let ProcessType::Leader {
            heartbeats_received,
            last_hearbeat_round_successful,
            ..
        } = &mut self.process_type
        {
            *last_hearbeat_round_successful = match (
                msg,
                2 * heartbeats_received.len() > self.config.servers.len(),
            ) {
                (HeartbeatTimeout::First, _) => true,
                (HeartbeatTimeout::NotFirst, val) => val,
            };
            heartbeats_received.clear();
            heartbeats_received.insert(self.config.self_id);
            self.send_append_entries(None).await;
        } else {
            if let Some(heartbeat_timer_handle) = self.heartbeat_timer_handle.take() {
                heartbeat_timer_handle.stop().await;
            }
        }
    }
}
