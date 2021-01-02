// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"
	"sort"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

// VoteType represents the current situation of vote stage.
type VoteType uint64

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	//random value of election interval
	randomElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
	electionElapsed  int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	// Your Code Here (2A).
	// 2020-12-23: doing project2Aa
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	raft := &Raft{
		id:               c.ID,
		Term:             0,                       //InitialTerm is 0
		Vote:             None,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress),
		State:            StateFollower,           //InitialState is follower
		votes:            make(map[uint64]bool),
		Lead:             None,                    //InitialLeader is None
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,          //other members are 0 automatically
		heartbeatElapsed: 0,
		electionElapsed:  0,
		//Will be used in 3a
		leadTransferee:   0,
		PendingConfIndex: 0,
	}
	// generate empty peerID -> Progress map from config
	lastIndex, _ := c.Storage.LastIndex()
	for _, peer := range c.peers {
		if peer == c.ID {
			raft.Prs[peer] = &Progress{Next: lastIndex + 1, Match: lastIndex}
		} else {
			raft.Prs[peer] = &Progress{Next: lastIndex + 1}
		}
	}
	//create a random value in [baseElectionTimeout, 2*baseElectionTimeout - 1]
	raft.randomElectionTimeout = raft.electionTimeout + rand.Intn(raft.electionTimeout)
	//update members according to hardState's info
	hardState, _ , _ := c.Storage.InitialState()
	raft.Term, raft.Vote, raft.RaftLog.committed = hardState.GetTerm(), hardState.GetVote(), hardState.GetCommit()
	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	//! Your Code Here (2A).
	prevIndex := r.Prs[to].Next - 1
	prevTerm, err := r.RaftLog.Term(prevIndex)
	if err != nil {
		panic(err)
	}
	entries := make([]*pb.Entry, 0)
	// Get offset of the entry that will be append firstly in entries through its Index - r.RaftLog.firstEntry
	for i := r.RaftLog.GetEntryOffset(r.Prs[to].Next); i < len(r.RaftLog.entries); i++ {
		entries = append(entries, &r.RaftLog.entries[i])
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: prevTerm,
		Index:   prevIndex,
		Entries: entries,
		Commit:  r.RaftLog.committed,
	}
	//push message into r.msgs
	r.msgs = append(r.msgs, msg)
	return true
}

// sendAppendResponse sends a response of Append to the given peer
func (r *Raft) sendAppendResponse(to uint64, reject bool, logTerm uint64, logIndex uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: logTerm,
		Index:   logIndex,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	//! Your Code Here (2A).
	// Leader send a MessageType_MsgHeartbeat
	// using empty entry append RPC as a heartbeat to all followers.
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit: min(r.RaftLog.committed, r.Prs[to].Match),
	}
	//push message into r.msgs
	r.msgs = append(r.msgs, msg)
}

// sendHeartbeatResponse sends a heartbeat response to the given peer.
func (r *Raft) sendHeartbeatResponse(to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	//push message into r.msgs
	r.msgs = append(r.msgs, msg)
}

// sendRequestVote sends a vote request to the given peer
func (r *Raft) sendRequestVote(to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
		LogTerm: r.RaftLog.LastTerm(),
	}
	//push message into r.msgs
	r.msgs = append(r.msgs, msg)
}

// sendRequestVoteResponse sends a vote response to the given peer
func (r *Raft) sendRequestVoteResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	//! Your Code Here (2A).
	// Only leader keeps heartbeatElapsed.
	if r.State == StateLeader {
		r.heartbeatTick()
	} else {
		r.elcetionTick()
	}
}

// elcetionTick is the action only for candidates and followers
func (r *Raft) elcetionTick() {
	r.electionElapsed++
	// It is the time for follower to start a new election.
	if r.electionElapsed >= r.randomElectionTimeout {
		r.electionElapsed = 0
		r.Step(pb.Message{ MsgType: pb.MessageType_MsgHup, From: r.id, Term: r.Term })
	}
}

// heartbeatTick is the action only for the leader
func (r *Raft) heartbeatTick() {
	r.heartbeatElapsed++
	// It is the time for leader to send a heartbeat.
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{ MsgType: pb.MessageType_MsgBeat, From: r.id, Term: r.Term })
	}
}

// becomeFollower transform its state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	//! Your Code Here (2A).
	r.State = StateFollower
	r.Lead = lead
	r.Term = term
	r.Vote = None
	r.votes = make(map[uint64]bool)
}

// becomeCandidate transform its state to candidate
func (r *Raft) becomeCandidate() {
	//! Your Code Here (2A).
	// votes for itself
	r.State = StateCandidate
	r.Lead = None
	r.Term++
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
}

// becomeLeader transform its state to leader
func (r *Raft) becomeLeader() {
	//! Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.Vote = None
	r.votes = make(map[uint64]bool)
	r.heartbeatElapsed = 0
	// Leader appends a noop entry, data is nil
	lastIndex := r.RaftLog.LastIndex()
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{Term: r.Term, Index: lastIndex + 1})
	// broadcast this noop entry as a heartbeat
	for peer := range r.Prs {
		if peer == r.id {
			r.Prs[peer] = &Progress{Next: lastIndex + 2, Match: lastIndex + 1}
		} else {
			r.Prs[peer] = &Progress{Next: lastIndex + 1}
		}
	}
	r.broadcastAppend()
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	//! Your Code Here (2A).
	// r.term left behind, listen to others directly, update itself
	if m.Term > r.Term {
		if m.MsgType == pb.MessageType_MsgAppend ||
			m.MsgType == pb.MessageType_MsgHeartbeat ||
			m.MsgType == pb.MessageType_MsgSnapshot {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}
	}
	// different state has different messages and actions
	switch r.State {
	case StateFollower:
		r.stepStateFollower(m)
	case StateCandidate:
		r.stepStateCandidate(m)
	case StateLeader:
		r.stepStateLeader(m)
	}
	return nil
}

func (r *Raft) stepStateFollower(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startNewElection(m)
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	}
}

func (r *Raft) stepStateCandidate(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startNewElection(m)
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		if m.Term == r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgSnapshot:
		if m.Term == r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		if m.Term == r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	}
}

// Leader will send a MessageType_MsgHeartbeat
// with m.Index = 0, m.LogTerm=0 and empty entries as heartbeat to all followers.
func (r *Raft) stepStateLeader(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
	case pb.MessageType_MsgBeat:
		r.broadcastHeartbeat()
	case pb.MessageType_MsgPropose:
		r.handlePropose(m)
	case pb.MessageType_MsgAppend:
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	}
}

// startNewElection begins a new election by broadcasting vote requests
func (r *Raft) startNewElection(m pb.Message) {
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.becomeCandidate()
	if len(r.Prs) == 1 {
		r.becomeLeader()
	}
	for peer := range r.Prs {
		if peer != r.id {
			r.sendRequestVote(peer)
		}
	}
}

//broadcastHeartbeat send heartbeat to every peer of this leader
func (r *Raft) broadcastHeartbeat() {
	// 2020-12-23: doing project2Aa
	for peer := range r.Prs {
		if peer != r.id {
			r.sendHeartbeat(peer)
		}
	}
}

//broadcastAppend send Append to every peer of this leader
func (r *Raft) broadcastAppend() {
	// 2020-12-23: doing project2Aa
	for peer := range r.Prs {
		if peer != r.id {
			r.sendAppend(peer)
		}
	}
}

//handleRequestVote is the action of follower to Vote for others
func (r *Raft) handleRequestVote(m pb.Message) {
	// m.term left behind, r will reject it!
	if m.Term < r.Term {
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	// m's state must be ahead of r and r need have remain vote for m.From
	isAhead := m.LogTerm > r.RaftLog.LastTerm() || (m.LogTerm == r.RaftLog.LastTerm() && m.Index >= r.RaftLog.LastIndex())
	remainVote := (r.Vote == None  || (r.Vote != None && r.Vote == m.From))
	if isAhead && remainVote {
		r.Vote = m.From
		r.sendRequestVoteResponse(m.From, false)
	} else {
		r.sendRequestVoteResponse(m.From, true)
	}
}

//handleRequestVoteResponse is the action of candidate to judge whether it will be a leader or folower
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	r.votes[m.From] = !m.Reject
	approval, against := 0, 0
	for _, val := range r.votes {
		if val {
			approval++
		} else {
			against++
		}
	}
	if approval > len(r.Prs)/2 {
		r.becomeLeader()
	} else if against > len(r.Prs)/2 {
		r.becomeFollower(r.Term, None)
	}
}

// handlePropose is for leader to append data to the leader's log entries
func (r *Raft) handlePropose(m pb.Message) {
	lastIndex := r.RaftLog.LastIndex()
	for i, entry := range m.Entries {
		entry.Term = r.Term
		entry.Index = lastIndex + uint64(i) + 1
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
	//After proposing, lastIndex (MatchIndex) is updated now
	lastIndex = r.RaftLog.LastIndex()
	r.Prs[r.id].Match = lastIndex
	r.Prs[r.id].Next = lastIndex + 1
	r.broadcastAppend()
	//- If there is only one node, just commit this entry.
	if len(r.Prs) == 1 {
		r.RaftLog.committed = lastIndex
	}
}

// handleAppendEntries is for followers to handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	//! Your Code Here (2A).
	if m.Term < r.Term {
		// m.term left behind, will be rejected
		r.sendAppendResponse(m.From, true, None, None)
		return
	}
	lastIndex := r.RaftLog.LastIndex()
	if m.Index > lastIndex {
		// the sender's appendEntry index is greater than the receiver's lastIndex
		r.sendAppendResponse(m.From, true, None, lastIndex+1)
		return
	}
	logTerm, err := r.RaftLog.Term(m.Index)
	if err != nil {
		panic(err)
	}
	// Follower's can't find a log entry with the same Index and term as the one in AppendEntries RPC.
	// In follower's log entries, m.Index correspond logTerm != m.LogTerm , Refuse this AppendEntries RPC
	// Traverse the un-compacted log entries of follower, find the proper right Index with the same logTerm.
	if logTerm != m.LogTerm {
		var rightIndex uint64
		for i := 0; i < len(r.RaftLog.entries); i++ {
			if r.RaftLog.entries[i].Term == logTerm {
				rightIndex = uint64(i) + r.RaftLog.firstEntryIndex
				break
			}
		}
		r.sendAppendResponse(m.From, true, logTerm, rightIndex)
		return
	}
	// Find matched log entry (the same logIndex and the same logTerm) successfully, append m.entries now!
	// When AppendEntries RPC is valid, the follower will delete the existing conflict entry and others that follow it,
	// It will append any new entry which is not in the log.
	for i, entry := range m.Entries {
		if entry.Index <= r.RaftLog.LastIndex() {
			logTerm, err := r.RaftLog.Term(entry.Index)
			if err != nil {
				panic(err)
			}
			// resolve Term conflict entry and delete other entries after it
			// And update the storage content via change stabled value.
			// else do nothing (same, no conflict)
			if logTerm != entry.Term {
				offset := entry.Index - r.RaftLog.firstEntryIndex
				r.RaftLog.entries[offset] = *entry
				r.RaftLog.entries = r.RaftLog.entries[ :offset+1]
				if r.RaftLog.stabled >= entry.Index {
					r.RaftLog.stabled = entry.Index - 1
				}
			}
		} else {
			// append latter every new entries after this new entry
			for ; i < len(m.Entries); i++ {
				r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[i])
			}
			break
		}
	}
	// If m.Commit > r.RaftLog.committed, set it as min(m.Commit, index of the newest last entry).
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, m.Index + uint64(len(m.Entries)))
	}
	r.sendAppendResponse(m.From, false, None, r.RaftLog.LastIndex())
}

// handleAppendResponse is for leader to handle the follower's response to its append RPC
func (r *Raft) handleAppendResponse(m pb.Message) {
	if m.Term < r.Term {
		return
	}
	if m.Reject {
		// compulsory handling of inconsistencies by leaders, cover conflict log entry
		offset := -1
		for i := 0; i < len(r.RaftLog.entries); i++ {
			if r.RaftLog.entries[i].Term > m.LogTerm {
				offset = i
				break
			}
		}
		//if there is an entry that matches the same logTerm and the same index,set nextIndex as the index after that
		if offset > 0 && r.RaftLog.entries[offset - 1].Term == m.LogTerm {
			r.Prs[m.From].Next = uint64(offset) + r.RaftLog.firstEntryIndex
		} else {
			//set this peer's nextIndex as the rightIndex send by peer.
			r.Prs[m.From].Next = m.Index
		}
		r.sendAppend(m.From)
		return
	}
	// append RPC is accepted.
	r.Prs[m.From].Match = m.Index
	r.Prs[m.From].Next = m.Index + 1
	// leader commit
	r.leaderCommit()
}

// leaderCommit updates the leader's committed index
func (r *Raft) leaderCommit() {
	matchResult := make(uint64Slice, 0)
	for _, progress := range r.Prs {
		matchResult = append(matchResult,progress.Match)
	}
	sort.Sort(matchResult)
	midIndex := matchResult[(len(r.Prs) - 1)/2]
	logTerm, err := r.RaftLog.Term(midIndex)
	if err != nil {
		panic(err)
	}
	// only log entries in current term can be committed by leader.
	if logTerm == r.Term && midIndex > r.RaftLog.committed {
		r.RaftLog.committed = midIndex
		for peer := range r.Prs {
			if peer != r.id {
				r.sendAppend(peer)
			}
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	// update itself
	if m.Term >= r.Term {
		r.RaftLog.committed = m.Commit
		r.sendHeartbeatResponse(m.From)
		r.becomeFollower(m.Term, m.From)
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
