package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"
import "time"
import "math/rand"

//import "fmt"

//import "sync/atomic"

import "bytes"
import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	Term         int
}

// log entry

type Log_entry struct {
	COMMAND interface{}
	//index   int
	TERM int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	state int // 0 -> leader , 1 -> cadi , 2 -> follower
	/* Persister */
	term     int
	votedFor int
	logs     []Log_entry
	/**************/

	has_leader bool
	alive      bool

	commitIndex int
	lastApplied int

	cond      *sync.Cond
	apply_num int
	//logs_len int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func (rf *Raft) Unlock() {
	////////fmt.Printf("%d Unlock\n", rf.me)
	rf.mu.Unlock()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.term
	isleader = rf.state == 0
	rf.Unlock()
	return term, isleader
}

func (rf *Raft) GetInterState() (int, int, bool) {

	var term int
	var alive bool
	var state int
	// Your code here (2A).
	rf.mu.Lock()

	state = rf.state
	term = rf.term
	alive = rf.alive
	//	isleader = rf.state == 0

	rf.Unlock()
	return state, term, alive
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	logs := new([]Log_entry)

	if d.Decode(&term) != nil || d.Decode(&votedFor) != nil || d.Decode(logs) != nil {

	} else {
		rf.term = term
		rf.votedFor = votedFor
		rf.logs = *logs
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	Server       int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) Is_up_to_date(index, term int) bool {
	maxIndex := len(rf.logs) - 1
	var current_term int
	if maxIndex >= 0 {
		current_term = rf.logs[maxIndex].TERM
	} else {
		current_term = -1
	}
	if current_term > term {
		return true
	}

	if current_term == term {
		return maxIndex > index
	}

	return false
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//////////fmt.Printf("%d start call %d RequestVote\n", args.Server, rf.me)
	////////fmt.Printf("%d ()()()()()(() %d\n", args.Server, rf.me)

	rf.mu.Lock()
	////////fmt.Printf("%d RequestVote get lock\n", rf.me)
	////////fmt.Printf("iiiiiiiiiiii\n")
	defer rf.Unlock()
	////////fmt.Printf("%d start call %d RequestVote\n", args.Server, rf.me)

	if args.Term < rf.term {
		reply.Term = rf.term
		reply.VoteGranted = false
		//////////fmt.Printf("()()()()()(()\n")
		return
	}

	up_to_date := rf.Is_up_to_date(args.LastLogIndex, args.LastLogTerm)
	////////fmt.Printf("%d QQQQQQQQQQQQQQ %d QQQ %d QQQQ %d\n", rf.me, args.Server, args.LastLogIndex, args.LastLogTerm, rf.logs)

	if args.Term > rf.term {
		rf.term = args.Term
		rf.state = 2
		rf.votedFor = -1
	}

	reply.Term = rf.term

	if rf.state == 0 || rf.state == 1 {
		//////////fmt.Printf("[][][][][][][]\n")
		reply.VoteGranted = false
		return
	}

	if up_to_date == false && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	} else {
		reply.VoteGranted = false
	}
	////////fmt.Printf("%d: return RequestVote %d -- %d -- %d\n", rf.me, reply.VoteGranted, up_to_date, rf.votedFor)
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args *AppendArgs, reply *AppendReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

/*
func (rf *Raft) election_Vote(server int, args *RequestVoteArgs) bool {
	reply := new(RequestVoteReply)
	for {
		ok := rf.sendRequestVote(server, args, reply)
		if ok == true {

		}
	}
}
*/

func (rf *Raft) CheckStateAndTransfer(state int) bool {

	if state == 2 || (state == 1 && rf.state == 2) || (state == 0 && rf.state == 1) {
		rf.state = state
		return true
	}

	return false
}

func (rf *Raft) election() {

	//defer rf.mu.Unlock()
	rf.state = 1
	rf.term += 1

	////////fmt.Printf("%d: election start!!\n", rf.me)
	//args := new(RequestVoteArgs)
	//reply := new(RequestVoteReply)

	peers_num := len(rf.peers)

	//success_count := new(int32)
	success_count := 1
	//////////fmt.Printf("zzzzzzzzzzzz\n")
	for i := 0; i < peers_num; i++ {
		if rf.me == i {
			rf.votedFor = i
			//atomic.AddInt32(success_count, int32(1))
			//////////fmt.Printf("\n%d: get %d vote,  need %d, has %d!!!!!!!!!!!\n", rf.me, i, peers_num/2, *success_count)
			continue
		}
		server := i
		go func() {
			var args RequestVoteArgs
			var reply RequestVoteReply

			rf.mu.Lock()
			args.Term = rf.term
			args.CandidateId = rf.me
			args.Server = rf.me
			args.LastLogIndex = len(rf.logs) - 1
			if args.LastLogIndex >= 0 {
				args.LastLogTerm = rf.logs[args.LastLogIndex].TERM
			} else {
				args.LastLogTerm = -1
			}

			////////fmt.Printf("%d: send %d election get lock\n", rf.me, server)
			rf.Unlock()

			ok := rf.sendRequestVote(server, &args, &reply)

			rf.mu.Lock()
			////////fmt.Printf("%d election get lock 2\n", rf.me)
			////////fmt.Printf("%d sendRequestVote to %d\n", rf.me, server)
			if ok {
				if reply.Term > rf.term {
					rf.term = reply.Term
					rf.state = 2
					rf.votedFor = -1

				} else if reply.VoteGranted == true {
					//	////////fmt.Printf("%d get RequestVote from %d, has %d, need %d\n",
					//		rf.me, server, *success_count, int32(peers_num/2))
					success_count += 1
					if success_count > peers_num/2 {
						ok := rf.CheckStateAndTransfer(0)
						if ok {
							//						fmt.Printf("%d become a leader %d\n", rf.me, success_count)
							go rf.sendHeartBeat()
							//rf.mu.Unlock()
							return
						}
					}
				}
			}
			rf.Unlock()
		}()
	}
	////////fmt.Printf("%d: election end!!\n", rf.me)
}

func (rf *Raft) GetIndexTerm(index int) int {
	return rf.logs[index].TERM
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.Unlock()

	if rf.state != 0 {
		isLeader = false
		//	fmt.Printf("%d not leader\n", rf.me, command)
		//	fmt.Printf("\n")
		return index, term, isLeader
	}

	//fmt.Printf("%d Start a command\n", rf.me, command)
	//fmt.Printf("\n")
	term = rf.term
	rf.logs = append(rf.logs, Log_entry{command, rf.term})
	rf.persist()
	index = len(rf.logs)
	//startIndex := index
	peers_num := len(rf.peers)
	success_count := 1

	for i := 0; i < peers_num; i++ {
		server := i
		if i != rf.me {
			var args AppendArgs
			var reply AppendReply
			args.Server = rf.me
			args.Term = rf.term
			args.LeaderId = rf.me
			//args.LeaderCommit = rf.commitIndex
			//args.PrevLogIndex =
			startIndex := max(0, index-5)
			go func() {
				retry := 2
				for {
					rf.mu.Lock()
					////////fmt.Printf("%d SendOrders get lock\n", rf.me)
					if rf.alive == false || rf.term != args.Term || rf.state != 0 {
						rf.mu.Unlock()
						break
					}
					var entries []Log_entry
					//fmt.Printf("%d: index is %d, len(logs) is %d\n", rf.me, index, len(rf.logs))
					for i := startIndex; i < index; i += 1 {
						entries = append(entries, rf.logs[i])
					}
					args.Entries = entries
					args.PrevLogIndex = startIndex - 1
					args.LeaderCommit = rf.commitIndex
					if args.PrevLogIndex >= 0 {
						args.PrevLogTerm = rf.logs[args.PrevLogIndex].TERM
					}

					//fmt.Printf("%d tttttttttttttttt %d %d\n", rf.me, startIndex, index)
					rf.Unlock()
					//////fmt.Printf("\n")
					//				fmt.Printf("%d sendOrder to %d has %d times\n", rf.me, server, retry, entries)
					//////fmt.Printf("\n")
					//////////fmt.Printf("iiiiiiiiiii: %d\n", len(args.Entries))
					ok := rf.sendAppendEntry(server, &args, &reply)

					rf.mu.Lock()
					////////fmt.Printf("%d SendOrders get lock2\n", rf.me)
					if ok {
						if reply.Term > rf.term {
							rf.term = reply.Term
							rf.state = 2
							rf.votedFor = -1

							//fmt.Printf("%d zzzzzzzzzzzzzz %d\n", rf.me, rf.term)
							rf.Unlock()

							break
						} else {
							if reply.Success {
								success_count += 1

								//fmt.Printf("%d: oooooooooooooooooooooooo: %d, %d, %d\n",
								//rf.me, success_count, server, index)
								if success_count > peers_num/2 {
									if rf.commitIndex < index-1 {
										rf.commitIndex = max(rf.commitIndex, index-1)
										//rf.persist()
										//fmt.Printf("%d NNNNNNNNNNNNNNNN %d\n", rf.me, index)
										rf.cond.Signal()
									}
								}
								rf.Unlock()
								break
							} else {

								startIndex = max(0, index-5*retry)
								if startIndex != 0 {
									retry *= 2
								}
							}
						}
					} else {
						//rf.Unlock()
						//break
						//////////fmt.Printf("%d ddddddd %d\n", rf.me, server)

						retry += 1
					}

					/*if retry == 20 {
						////////fmt.Printf("%d cccccccccccc %d\n", rf.me, server)

						//rf.Unlock()
						break
					}*/
					rf.Unlock()
				}
			}()
		}
	}

	return index - 1, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	//rf.state = 2
	rf.alive = false
}

type AppendArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log_entry
	LeaderCommit int
	Server       int
}

type AppendReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntry(args *AppendArgs, reply *AppendReply) {
	/*if args == nil {
		////////fmt.Printf("??????????????????\n")
	}*/

	rf.mu.Lock()
	////////fmt.Printf("%d AppendEntry get lock\n", rf.me)
	defer rf.Unlock()

	if rf.term > args.Term {
		reply.Success = false
		reply.Term = rf.term
		//rf.votedFor = -1
		return
	}
	//////////fmt.Printf("%d: fffffffffffffffffffffffffff\n")
	if rf.term < args.Term {
		rf.votedFor = -1
	}

	rf.has_leader = true
	rf.term = args.Term
	rf.state = 2

	if args.PrevLogIndex == -100 {
		reply.Success = true
		reply.Term = rf.term
		if args.LeaderCommit == -1 || len(rf.logs) <= args.LeaderCommit || rf.logs[args.LeaderCommit].TERM != args.PrevLogTerm {
			return
		}

		if rf.commitIndex < min(args.LeaderCommit, len(rf.logs)-1) {
			rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)
			//rf.persist()
			rf.cond.Signal()
		}
		return
	}
	////////fmt.Print("%d : %d  ------%d------%d\n", args.Server, rf.me, args.PrevLogIndex, len(rf.logs))
	if args.PrevLogIndex < len(rf.logs) &&
		(args.PrevLogIndex == -1 || rf.logs[args.PrevLogIndex].TERM == args.PrevLogTerm) {

		targetIndex := args.PrevLogIndex + len(args.Entries)

		if !(targetIndex < len(rf.logs) && rf.logs[targetIndex].TERM == args.Term) {

			////fmt.Printf("\n")
			////fmt.Printf("%d: iiiiiiiiiiiiii %d\n", rf.me, args.PrevLogIndex)
			rf.logs = rf.logs[:args.PrevLogIndex+1]

			////fmt.Printf("\n")
			////fmt.Printf("%d: %d uuuuuu %d uuuuuuu %d uuu %d uuu %d\n",
			//	rf.me, len(rf.logs), args.PrevLogIndex, rf.commitIndex, len(args.Entries), args.LeaderCommit)
			for j := 0; j < len(args.Entries); j++ {
				rf.logs = append(rf.logs, args.Entries[j])
			}
			rf.persist()
			////fmt.Printf("\n")
			////fmt.Printf("%d: ss \n", rf.me, rf.logs)
			////fmt.Printf("\n")
			////////fmt.Printf("%d ;;;;;;;;;; %d ;;;;; %d \n", len(rf.logs), rf.commitIndex, args.LeaderCommit)
			if rf.commitIndex < min(args.LeaderCommit, len(rf.logs)-1) {
				////fmt.Printf("\n")
				////fmt.Printf("%d sssssssss\n", rf.me, rf.logs)
				////fmt.Printf("\n")
				rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)
				rf.cond.Signal()
			}
		}
		reply.Success = true
		reply.Term = rf.term
		return
	}
	reply.Success = false
	reply.Term = rf.term
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

var r *rand.Rand

func (rf *Raft) applyEntry(applyCh chan ApplyMsg) {
	//= rf.commitIndex
	for {
		//var lastapply int
		//	var commitindex int
		rf.cond.L.Lock()
		//	for rf.lastApplied > rf.commitIndex {
		rf.cond.Wait()
		//	}
		//	commitindex = rf.commitIndex
		rf.cond.L.Unlock()
		//logs := rf.logs
		////fmt.Printf("\n")
		rf.apply_num += 1
		////fmt.Printf("%d ccccccccccc %d\n", rf.me, rf.apply_num, rf.logs)
		////fmt.Printf("\n")
		for ; rf.lastApplied <= rf.commitIndex; rf.lastApplied++ {

			////////fmt.Printf("%d: %d :::::::::::::::::::::::::::: %d ::: %d\n", rf.me, rf.lastApplied, rf.commitIndex, len(rf.logs))
			rf.mu.Lock()
			var command interface{}
			if rf.lastApplied < len(rf.logs) {
				command = rf.logs[rf.lastApplied].COMMAND
			} else {
				////fmt.Printf("%d: zzzzzzzzzzzz %d\n", rf.me, len(rf.logs))
				rf.mu.Unlock()
				break
			}
			rf.mu.Unlock()

			applyCh <- ApplyMsg{true, command, rf.lastApplied, rf.term}
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = 2
	rf.votedFor = -1
	rf.term = 0
	rf.has_leader = false
	rf.alive = true
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	////fmt.Printf("\n")
	////fmt.Printf("%d cxzczxc:\n %d", rf.term, rf.votedFor, rf.logs)
	////fmt.Printf("\n")
	rf.cond = sync.NewCond(&sync.Mutex{})
	rf.commitIndex = -1
	rf.lastApplied = 0
	rf.apply_num = 0
	go rf.applyEntry(applyCh)
	////////fmt.Printf("%d activity ****************************\n", rf.me)
	if r == nil {
		r = rand.New(rand.NewSource(88))
	}

	go func() {
		for {
			dura := 600 + r.Int()%600
			time.Sleep(time.Duration(dura) * time.Millisecond)

			////////fmt.Printf("%d sleep start %d!\n", rf.me, dura)
			rf.mu.Lock()
			////fmt.Printf("%d Sleep start get lock, the state is %d\n", rf.me, rf.state)
			if rf.alive == false {
				////////fmt.Printf("%d dead ###########################################################\n", rf.me)
				rf.Unlock()
				return
			}

			////////fmt.Printf("%d-----------------%d---------%d------ %d --- %d\n", rf.me, rf.has_leader, rf.state, rf.term)
			if len(rf.logs) > 0 {
				////////fmt.Printf("%d\n", rf.logs[len(rf.logs)-1])
			}
			if rf.has_leader == false && rf.state != 0 {
				rf.election()
				rf.Unlock()
				continue
			}

			rf.has_leader = false
			////////fmt.Printf("%d sleep end %d!!!!!!!!!\n", rf.me, dura)

			rf.Unlock()
		}
	}()

	return rf
}

func (rf *Raft) sendHeartBeat() {
	//args := new(AppendArgs)
	//reply := new(AppendReply)

	////////fmt.Printf("kaishi\n")
	is_init := true
	for {
		//rf.mu.Lock()
		//defer rf.mu.Unlock()
		if is_init == false {
			rf.mu.Lock()
			////////fmt.Printf("%d sendHearBeat get lock\n", rf.me)
		}

		if rf.alive == false || rf.state != 0 {
			rf.Unlock()
			////////fmt.Printf("%d term is change!!!!!!!\n", rf.me)
			return
		}

		peers_num := len(rf.peers)

		getReply := 1
		for i := 0; i < peers_num; i++ {
			server := i
			if i != rf.me {
				var args AppendArgs
				var reply AppendReply
				args.Server = rf.me
				args.Term = rf.term
				args.PrevLogIndex = -100
				args.LeaderCommit = rf.commitIndex
				if rf.commitIndex >= 0 {
					args.PrevLogTerm = rf.logs[rf.commitIndex].TERM
				}
				go func() {
					ok := rf.sendAppendEntry(server, &args, &reply)

					//		rf.mu.Lock()
					//////////fmt.Printf("%d sendhearorder get lock\n", rf.me)
					//////////fmt.Printf("%d send Append to %d\n", rf.me, server)
					if ok {
						getReply += 1
						if reply.Term > rf.term {
							rf.term = reply.Term
							rf.state = 2
							rf.votedFor = -1
						}
					}
					//		rf.mu.Unlock()
				}()
			}
		}
		//if is_init == false {
		rf.Unlock()
		//}
		is_init = false
		//////////fmt.Printf("%d Unlock\n", rf.me)
		time.Sleep(200 * time.Millisecond)
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
