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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
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

	Term uint64 // currentTerm
	Vote uint64 // votedFor

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	// idOfPeer -> (nextIndex, matchIndex)
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
	res := Raft{
		id:               c.ID,
		Term:             0,
		Vote:             0,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress, 0),
		State:            StateFollower,
		votes:            make(map[uint64]bool, 0),
		msgs:             make([]pb.Message, 0),
		Lead:             0,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   0,
		PendingConfIndex: 0,
	}
	res.RaftLog.applied = c.Applied
	for _, pId := range c.peers {
		res.Prs[pId] = &Progress{}
	}

	return &res
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	r.sendMsg(pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: r.RaftLog.LastTerm(),
		Index:   r.RaftLog.LastIndex(),
		Entries: r.RaftLog.EntriesToSend(r.Prs[to].Next),
		Commit:  r.RaftLog.committed,
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.sendMsg(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.heartbeatElapsed += 1
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			_ = r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
				To:      r.id,
				From:    r.id,
				Term:    r.Term,
			})
			r.heartbeatElapsed = 0
		}
	} else {
		// candidate and follower
		r.electionElapsed += 1
		// factor: [1, 3)
		factor := rand.Float32()*2 + 1
		timeout := int(float32(r.electionTimeout) * factor)
		if r.electionElapsed >= timeout {
			_ = r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
				To:      r.id,
				From:    r.id,
				Term:    r.Term,
			})
			r.electionElapsed = 0
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Vote = None
	r.Lead = lead
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term += 1
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.votes = make(map[uint64]bool, 0)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.Lead = r.id

	// leader state
	for _, p := range r.Prs {
		p.Next = r.RaftLog.LastIndex() + 1
		p.Match = 0
	}

	// send heartbeat
	for pId, _ := range r.Prs {
		if pId != r.id {
			r.sendHeartbeat(pId)
		}
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {

	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startElection()
	case pb.MessageType_MsgBeat: // ignore
	case pb.MessageType_MsgPropose: // redirect to leader
		m.To = r.Lead
		r.sendMsg(m)
	case pb.MessageType_MsgAppend:
		if m.Term >= r.Term {
			r.handleAppendEntries(m)
		} else {
			r.rejectAppendEntries(m)
		}
	case pb.MessageType_MsgRequestVote:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse: // ignore
	//case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse: // ignore
	//case pb.MessageType_MsgTransferLeader:
	//case pb.MessageType_MsgTimeoutNow:
	default:
		log.Errorf("[%s]Not implemented msgType: %s", r.State, m.MsgType)
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {

	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startElection()
	case pb.MessageType_MsgBeat: // ignore
	case pb.MessageType_MsgPropose: // ignore
	case pb.MessageType_MsgAppend:
		if m.Term >= r.Term {
			// revert to follower
			r.becomeFollower(m.Term, m.From)
			r.handleAppendEntries(m)
		} else {
			r.rejectAppendEntries(m)
		}

	case pb.MessageType_MsgRequestVote:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.votes[m.From] = !m.Reject
		quorum := len(r.Prs) / 2
		voteCnt := 0
		for _, v := range r.votes {
			if v {
				voteCnt++
			}
			if voteCnt > quorum {
				r.becomeLeader()
				return nil
			}
		}
	//case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, m.From)
			r.handleHeartbeat(m)
		}
	case pb.MessageType_MsgHeartbeatResponse: // ignore
	//case pb.MessageType_MsgTransferLeader:
	//case pb.MessageType_MsgTimeoutNow:
	default:
		log.Errorf("[%s]Not implemented msgType: %s", r.State, m.MsgType)
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup: // ignore
	case pb.MessageType_MsgBeat:
		for pId := range r.Prs {
			if pId != r.id {
				r.sendHeartbeat(pId)
			}
		}
	case pb.MessageType_MsgPropose:
		r.handlePropose(m)
	case pb.MessageType_MsgAppend:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, m.From)
			r.handleAppendEntries(m)
		}
	case pb.MessageType_MsgAppendResponse:
		//todo
	case pb.MessageType_MsgRequestVote:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse: // ignore
	//case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
	case pb.MessageType_MsgHeartbeatResponse:
	//case pb.MessageType_MsgTransferLeader:
	//case pb.MessageType_MsgTimeoutNow:
	default:
		log.Errorf("[%s]Not implemented msgType: %s", r.State, m.MsgType)
	}
	return nil
}

func (r *Raft) startElection() {
	r.becomeCandidate()

	// special case: just 1 node
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}

	// vote for itself
	r.Vote = r.id
	r.votes[r.id] = true
	// send msg to others for vote
	for pId := range r.Prs {
		if pId != r.id {
			r.sendMsg(pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				To:      pId,
				From:    r.id,
				Term:    r.Term,
			})
		}
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	if r.canVote(m) {
		r.Vote = m.From
		r.Term = m.Term
		r.sendMsg(pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  false,
		})
	} else {
		r.sendMsg(pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		})
	}
}
func (r *Raft) canVote(m pb.Message) bool {
	// cannot vote for a stale term
	if m.Term < r.Term {
		return false
	}

	// only vote for one candidate in a term
	if r.Vote != None && r.Vote != m.From {
		return false
	}

	// compare whose log is more up-to-date
	selfIndex := r.RaftLog.LastIndex()
	selfTerm := r.RaftLog.LastTerm()

	// candidate's term is lower
	if m.LogTerm < selfTerm {
		return false
	}

	// same term, but candidate's log is shorter
	if m.LogTerm == selfTerm && m.Index < selfIndex {
		return false
	}

	return true
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	r.heartbeatElapsed = 0
	r.Term = m.Term

	t, err := r.RaftLog.Term(m.Index)
	if err != nil {
		//log.Error("cannot get term")
	}
	if t == m.LogTerm {
		for _, entry := range m.Entries {
			// todo overwrite
			r.RaftLog.entries = append(r.RaftLog.entries, *entry)
		}

		// advance commit
		if r.RaftLog.LastIndex() < m.Commit {
			r.RaftLog.committed = r.RaftLog.LastIndex()
		} else {
			r.RaftLog.committed = m.Commit
		}

		r.sendMsg(pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Commit:  r.RaftLog.committed,
			Reject:  true,
		})

	}

	// todo reject

}

func (r *Raft) rejectAppendEntries(m pb.Message) {
	r.sendMsg(pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  true,
	})
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.heartbeatElapsed = 0
	r.sendMsg(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
	})
}

func (r *Raft) handlePropose(m pb.Message) {
	for _, e := range m.Entries {
		r.RaftLog.entries = append(r.RaftLog.entries, *e)
	}
	for pId := range r.Prs {
		if pId != r.id {
			r.sendAppend(pId)
		}
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

// just add msg in r.msgs now
func (r *Raft) sendMsg(m pb.Message) {
	r.msgs = append(r.msgs, m)
}
