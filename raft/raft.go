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
	electionElapsed int

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
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	// 2020-12-23: doing project2Aa
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
		leadTransferee:   0,
		PendingConfIndex: 0,                       //Will be used in 3a
	}
	for _, peer := range c.peers {
		raft.votes[peer] = false
		raft.Prs[peer] = &Progress{}
	}
	//create a random value in [baseElectionTimeout, 2*baseElectionTimeout - 1]
	raft.randomElectionTimeout = raft.electionTimeout + rand.Intn(raft.electionTimeout)
	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	// 2020-12-23: doing project2Aa
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
	}
	//push message into r.msgs
	r.msgs = append(r.msgs, msg)
}

//sendHeartbeatResponse is the followers' response of Leader's Heartbeat
func (r *Raft) sendHeartbeatResponse(to uint64, reject bool) {
	// 2020-12-23: doing project2Aa
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	}
	//push message into r.msgs
	r.msgs = append(r.msgs, msg)
}

//sendRequestVote sends a vote request to the given peer
func (r *Raft) sendRequestVote(to uint64) {
	// 2020-12-23: doing project2Aa
	lastIndex := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.Term(lastIndex)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: lastLogTerm,
		Index:   lastIndex,
	}
	//push message into r.msgs
	r.msgs = append(r.msgs, msg)
}

//sendRequestVoteResponse is the followers' response of RequestVote to them
func (r *Raft) sendRequestVoteResponse(to uint64, reject bool) {
	// 2020-12-23: doing project2Aa
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	}
	//push message into r.msgs
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	// 2020-12-23: doing project2Aa
	switch r.State {
	case StateFollower:
		r.electionElapsed++
		//If an election timeout happened, pass 'MessageType_MsgHup' to its Step method and start a new election.
		if r.electionElapsed >= r.randomElectionTimeout {
			r.electionElapsed = 0
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, From: r.id})
		}
	case StateCandidate:
		r.electionElapsed++
		//If an election timeout happened, pass 'MessageType_MsgHup' to its Step method and start a new election.
		if r.electionElapsed >= r.randomElectionTimeout {
			r.electionElapsed = 0
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, From: r.id})
		}
	case StateLeader:
		r.heartbeatElapsed++
		//leader sends a heartbeat signal to its followers.
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat, From: r.id})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	// 2020-12-23: doing project2Aa
	// When leader's term >= r.term, recognize other legal leader and r become a follower again
	r.State = StateFollower
	r.Term = term
	r.Vote = None
	r.Lead = lead
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	// 2020-12-23: doing project2Aa
	r.State = StateCandidate
	r.Term = r.Term + 1
	r.Lead = None
	r.Vote = r.id
	for peer := range r.votes {
		if peer == r.id {
			r.votes[peer] = true
		} else {
			r.votes[peer] = false
		}
	}

}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term !!!
	// 2020-12-23: doing project2Aa
	r.State = StateLeader
	r.Lead = r.id
	//noop entry
	for peer := range r.Prs {
		r.Prs[peer] = &Progress{}
	}
	// r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{Term: r.Term, Index: r.RaftLog.LastIndex()})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// 2020-12-23: doing project2Aa
	// r.term left behind, listen to others directly, update itself
	if m.Term > r.Term {
		if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}
	}
	// different state has different messages and actions
	switch r.State {
	case StateFollower:
		r.stepInStateFollower(m)
	case StateCandidate:
		r.stepInStateCandidate(m)
	case StateLeader:
		r.stepInStateLeader(m)
	}
	return nil
}

func (r *Raft) stepInStateFollower(m pb.Message) {
	// 2020-12-23: doing project2Aa
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startNewElection()
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgAppend:
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
	}
}

func (r *Raft) stepInStateCandidate(m pb.Message) {
	// 2020-12-23: doing project2Aa
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startNewElection()
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgAppend:
		//r.term left behind, listen to others directly
		if m.Term >= r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgSnapshot:
		if m.Term >= r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		if m.Term >= r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	}
}

func (r *Raft) stepInStateLeader(m pb.Message) {
	// 2020-12-23: doing project2Aa
	switch m.MsgType {
	case pb.MessageType_MsgHup:
	case pb.MessageType_MsgBeat:
		r.broadcastHeartbeat()
	case pb.MessageType_MsgAppend:
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
	}
}

// startNewElection begins a new election by broadcasting vote requests
func (r *Raft) startNewElection() {
	// 2020-12-23: doing project2Aa
	r.becomeCandidate()
	//reset ticks()
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	if len(r.votes) == 1 {
		r.becomeLeader()
		return
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

//handleRequestVote is the action of follower to Vote for others
func (r *Raft) handleRequestVote(m pb.Message) {
	// 2020-12-23: doing project2Aa
	//m's term behind or outRange or r has already voted (Can't vote it!!!)
	lastIndex := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.Term(lastIndex)
	outRange := lastLogTerm > m.LogTerm || (lastLogTerm == m.LogTerm && lastIndex > m.Index)
	if m.Term >= r.Term && !outRange && (r.Vote == None || r.Vote == m.From) {
		r.Vote = m.From
		r.sendRequestVoteResponse(m.From, false)
	} else {
		r.sendRequestVoteResponse(m.From, true)
	}
}

//handleRequestVoteResponse is the action of candidate to judge whether it can be a leader
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	// 2020-12-23: doing project2Aa
	if m.Term < r.Term {
		return
	}
	r.votes[m.From] = !m.Reject
	approval := 0
	for _, val := range r.votes {
		if val {
			approval++
		}
	}
	// when approvals are bigger than half of its peers, becomeLeader!
	// when oppositions are bigger than half of its peers, becomeFollower ???
	if approval > len(r.Prs)/2 {
		r.becomeLeader()
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	//2020-12-22: doing project2Aa
	if m.Term != None && m.Term < r.Term {
		r.sendHeartbeatResponse(m.From, true)
		return
	}
	//Confirm its leader and become a follower
	r.Lead = m.From
	r.becomeFollower(m.Term, m.From)
	r.sendHeartbeatResponse(m.From, false)
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

