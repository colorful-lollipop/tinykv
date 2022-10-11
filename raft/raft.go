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

	"github.com/pingcap-incubator/tinykv/com"
	"github.com/pingcap-incubator/tinykv/global"
	"github.com/pingcap-incubator/tinykv/log"
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

var (
	MsgBeat      = pb.Message{MsgType: pb.MessageType_MsgBeat}
	MsgHup       = pb.Message{MsgType: pb.MessageType_MsgHup}
	EmptyEntry   = pb.Entry{}
	EmptyEntries = []*pb.Entry{&EmptyEntry}
)

var msgRespMap = map[pb.MessageType]pb.MessageType{
	pb.MessageType_MsgAppend:      pb.MessageType_MsgAppendResponse,
	pb.MessageType_MsgRequestVote: pb.MessageType_MsgRequestVoteResponse,
	pb.MessageType_MsgHeartbeat:   pb.MessageType_MsgHeartbeatResponse,
}

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
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
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
	//num of supporter in election of condidate
	supporterNum int
	//num of protecter in election of condidate
	// protecterNum int

	//

}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err.Error())
	}
	Prs := make(map[uint64]*Progress)
	if len(c.peers) == 0 {
		c.peers = confState.Nodes
	}
	for _, v := range c.peers {
		Prs[v] = new(Progress)
	}
	return &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          newLog(c.Storage),
		Prs:              Prs,
		State:            StateFollower,
		votes:            make(map[uint64]bool, len(c.peers)),
		msgs:             make([]pb.Message, 0),
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  -rand.Intn(c.ElectionTick),
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	lastIndex := r.RaftLog.LastIndex()
	firstIndex := r.RaftLog.FirstIndex()
	entries := make([]*pb.Entry, 0, lastIndex+1-r.Prs[to].Next)
	for i := r.Prs[to].Next; i <= lastIndex; i++ {
		entries = append(entries, &r.RaftLog.entries[i-firstIndex])
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: r.RaftLog.GetTerm(r.Prs[to].Next - 1),
		Index:   r.Prs[to].Next - 1,
		Entries: entries,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs,
		pb.Message{
			MsgType: pb.MessageType_MsgHeartbeat,
			To:      to,
			From:    r.id,
			Term:    r.Term,
			LogTerm: r.RaftLog.GetTerm(r.Prs[to].Next - 1),
			Index:   r.Prs[to].Next - 1,
			Commit:  r.RaftLog.committed,
		})
}

// sendRequestVote sends a RequestVote RPC to the given peer.
func (r *Raft) sendRequestVote(to uint64) {
	r.msgs = append(r.msgs,
		pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      to,
			From:    r.id,
			Term:    r.Term,
			LogTerm: r.RaftLog.LastTerm(),
			Index:   r.RaftLog.LastIndex(),
		})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower, StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			r.electionElapsed = -rand.Intn(r.electionTimeout)
			r.Step(MsgHup)
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.Step(MsgBeat)
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	if global.DEBUG {

	}
	r.Term = term
	r.Lead = lead
	r.State = StateFollower
	r.Vote = None
	r.electionElapsed = -rand.Intn(r.electionTimeout)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term++
	r.Lead = None
	r.State = StateCandidate
	r.electionElapsed = -rand.Intn(r.electionTimeout)
	r.Vote = r.id
	// ClearMap(r.votes)
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true

}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.Lead = r.id
	r.State = StateLeader
	r.heartbeatElapsed = 0
	// ClearMap(r.votes)
	// r.votes = make(map[uint64]bool)
	for id := range r.Prs {
		if r.Prs[id] == nil {
			r.Prs[id] = new(Progress)
		}
		r.Prs[id].Next = r.RaftLog.LastIndex() + 1
		r.Prs[id].Match = 0
	}
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{Term: r.Term, Index: r.RaftLog.LastIndex() + 1})
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if m.Term < r.Term && m.Term != 0 {
		r.quickReject(&m)
		return nil
	}
	switch r.State {
	case StateFollower:
		r.stepFollower(&m)
	case StateCandidate:
		r.stepCandidate(&m)
	case StateLeader:
		r.stepLeader(&m)
	}
	return nil
}

// Step the entrance of handle message as a Follower
func (r *Raft) stepFollower(m *pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startElection()
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		r.handleMsgAppend(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	}
}

// Step the entrance of handle message as a Candidate
func (r *Raft) stepCandidate(m *pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startElection()
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		if m.Term < r.Term {
			return
		}
		r.becomeFollower(m.Term, m.From)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	}
}

// Step the entrance of handle message as a Leader
func (r *Raft) stepLeader(m *pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
	case pb.MessageType_MsgBeat:
		for k := range r.Prs {
			if k == r.id {
				continue
			}
			r.sendHeartbeat(k)
		}
	case pb.MessageType_MsgPropose:
		r.handleMsgPropose(m)
	case pb.MessageType_MsgAppend:
		if m.Term <= r.Term {
			return
		}
		r.becomeFollower(m.Term, m.From)
	case pb.MessageType_MsgAppendResponse:
		r.handleMsgAppendResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	//FIXME:
	r.handleMsgAppend(&m)
}

// extra func
func (r *Raft) handleMsgPropose(m *pb.Message) {
	nextIndex := r.RaftLog.LastIndex() + 1
	for _, v := range m.Entries {
		v.Term = r.Term
		v.Index = nextIndex
		nextIndex++
		r.RaftLog.entries = append(r.RaftLog.entries, *v)
	}

	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
	pProgress := r.Prs[r.id]
	pProgress.Match = nextIndex - 1
	pProgress.Next = nextIndex
	if len(r.Prs) == 1 {
		r.RaftLog.committed = pProgress.Match
	}
}

// handleRequestVote handle RequestVote RPC request
func (r *Raft) handleRequestVote(m *pb.Message) {
	//TODO:
	reject := true
	if m.Term > r.Term ||
		(m.Term == r.Term && (r.Vote == None || r.Vote == m.From)) &&
			m.Term >= r.RaftLog.LastTerm() && m.Index >= r.RaftLog.LastIndex() {
		r.becomeFollower(m.Term, m.From)
		if m.LogTerm > r.RaftLog.LastTerm() || m.LogTerm == r.RaftLog.LastTerm() && m.Index >= r.RaftLog.LastIndex() {
			r.Vote = m.From
			reject = false

		}
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
		//TODO:
	}
	r.msgs = append(r.msgs, msg)
}

// handleRequestVoteResponse handle RequestVote RPC Response
func (r *Raft) handleRequestVoteResponse(m *pb.Message) {
	r.votes[m.From] = !m.Reject
	if m.Reject {
		return
	}
	r.supporterNum++
	if r.supporterNum > len(r.Prs)/2 {
		r.becomeLeader()
		// return
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m *pb.Message) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  true,
		//TODO: orther data
	}
	switch {
	case m.Term == r.Term:
		if com.DEBUG && r.Term != None && r.Term != m.From {
			panic("split-brain")
		}
		fallthrough
	case m.Term > r.Term:
		msg.Reject = false
		r.becomeFollower(m.Term, m.From)
	case m.Term < r.Term:
	}
	r.msgs = append(r.msgs, msg)
}
func (r *Raft) handleMsgAppendResponse(m *pb.Message) {
	if m.Reject {
		//TODO:
		pProgress := r.Prs[m.From]
		pProgress.Next = m.Index + 1
		r.sendAppend(m.From)
		return
	}
	pProgress := r.Prs[m.From]
	pProgress.Match = m.Index
	pProgress.Next = m.Index + 1
	//find half matched
	halfmatched := r.findHalfMatched()
	//TODO:copy
	// r.RaftLog.applied = halfmatched
	halfTerm, _ := r.RaftLog.Term(halfmatched)
	if halfmatched > r.RaftLog.committed && halfTerm == r.Term {
		r.RaftLog.committed = halfmatched
		for peer := range r.Prs {
			if peer != r.id {
				r.sendAppend(peer)
			}
		}
	}

}

func (r *Raft) handleMsgAppend(m *pb.Message) {
	r.Term = m.Term
	r.Lead = m.From
	r.electionElapsed = 0
	reject := true
	var index uint64 = 0
	if m.Index <= r.RaftLog.LastIndex() {
		term, _ := r.RaftLog.Term(m.Index)
		//TODO:
		if term == m.LogTerm {
			for _, entry := range m.Entries {
				if entry.Index <= r.RaftLog.LastIndex() {
					logTerm, _ := r.RaftLog.Term(entry.Index)
					if logTerm != entry.Term {
						// 覆盖冲突的日志
						r.RaftLog.entries[entry.Index-r.RaftLog.FirstIndex()] = *entry
						r.RaftLog.entries = r.RaftLog.entries[:entry.Index-r.RaftLog.FirstIndex()+1]
						r.RaftLog.stabled = entry.Index - 1
					}
				} else {
					// 追加日志
					r.RaftLog.entries = append(r.RaftLog.entries, *entry)
				}
			}
			reject = false
			if m.Commit > r.RaftLog.committed {
				r.RaftLog.committed = m.Commit
			}
			index = r.RaftLog.LastIndex()
		} else {
			index = min(r.RaftLog.LastIndex(), m.Index-1)
		}
	} else {
		index = min(r.RaftLog.LastIndex(), m.Index-1)
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  reject,
		Index:   index,
	}
	r.msgs = append(r.msgs, msg)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// startElection run for election in condidate
func (r *Raft) startElection() {
	r.becomeCandidate()
	r.supporterNum = 1
	r.votes[r.id] = true
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendRequestVote(id)

	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

// raft will reject when msg.Term < this raft Term  anyway
func (r *Raft) quickReject(m *pb.Message) {
	if com.DEBUG {
		if _, ok := msgRespMap[m.MsgType]; !ok {
			log.Debug("unkonw MsgType", m.MsgType)
		}
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: msgRespMap[m.MsgType],
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  true,
	})
}
func (r *Raft) findHalfMatched() uint64 {
	arr := make([]uint64, len(r.Prs))
	i := 0
	for _, k := range r.Prs {
		arr[i] = k.Match
		i++
	}
	sort.Slice(arr, func(i, j int) bool { return arr[i] < arr[j] })
	return arr[(len(arr)-1)/2]
}
