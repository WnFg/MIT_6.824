package raftkv

import (
	//	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op    string
	Key   string
	Value string
	Cli   int64
	Req   int
}

type Client struct {
	Req int
	Ret string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate     int // snapshot if log grows this big
	Ret              map[int]string
	cond             *sync.Cond
	commitIndex      int
	KeyValueDataBase map[string]string
	Clis             map[int64]Client
	NowTime          int
	Que              []interface{}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	name := args.Cli
	//req := args.Req

	index, term, isLeader := kv.rf.Start(Op{"Get", args.Key, "", args.Cli, args.Req})

	if isLeader == false {
		reply.WrongLeader = true
		reply.Err = "It's not a leader!"
		return
	}

	//	fmt.Printf("Leader: get %s\n", args.Key)
	now := kv.NowTime
	du := 0
	kv.cond.L.Lock()
	for du <= 2 && isLeader && index > kv.commitIndex {
		kv.cond.Wait()
		_, isLeader = kv.rf.GetState()
		du = kv.NowTime - now
		//famt.Printf("lock end: get %s %v\n", args.Key, isLeader)
	}
	kv.cond.L.Unlock()

	//	fmt.Printf("Leader end: get %s %v\n", args.Key, isLeader)

	if du > 2 || isLeader == false {
		//fmt.Printf("Leader end: get %s %v %d\n", args.Key, isLeader, du)

		reply.WrongLeader = true
		reply.Err = "It's not a leader!"
		return
	}

	term1 := kv.rf.GetIndexTerm(index)
	if term != term1 {
		reply.WrongLeader = true
		reply.Err = "It's not a leader!"
		return
	}

	reply.WrongLeader = false
	reply.Value = kv.Clis[name].Ret
	reply.Err = "ok"
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	index, term, isLeader := kv.rf.Start(Op{args.Op, args.Key, args.Value, args.Cli, args.Req})

	if isLeader == false {
		reply.WrongLeader = true
		reply.Err = "It's not a leader!"
		return
	}
	//fmt.Printf("Leader: set %s\n", args.Key)
	now := kv.NowTime
	du := 0
	kv.cond.L.Lock()
	for du <= 2 && isLeader && index > kv.commitIndex {
		kv.cond.Wait()
		_, isLeader = kv.rf.GetState()
		du = kv.NowTime - now
		//famt.Printf("lock end: get %s %v\n", args.Key, isLeader)
	}
	kv.cond.L.Unlock()

	//	fmt.Printf("Leader end: get %s %v\n", args.Key, isLeader)

	if du > 2 || isLeader == false {
		//fmt.Printf("Leader end: get %s %v %d\n", args.Key, isLeader, du)

		reply.WrongLeader = true
		reply.Err = "It's not a leader!"
		return
	}

	term1 := kv.rf.GetIndexTerm(index)
	if term != term1 {
		reply.WrongLeader = true
		reply.Err = "It's not a leader!"
		return
	}

	//	fmt.Printf("Leader %d: %s, %s , %s\n", kv.me, args.Op, args.Key, args.Value)
	reply.WrongLeader = false
	reply.Err = "ok"
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) Pt() {
	//fmt.Printf("databases: %v\n", kv.KeyValueDataBase)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.KeyValueDataBase = make(map[string]string)
	kv.Ret = make(map[int]string)
	// You may need initialization code here.
	kv.commitIndex = -1
	kv.cond = sync.NewCond(&sync.Mutex{})
	kv.Clis = make(map[int64]Client)
	kv.NowTime = 0
	go func() {
		//fmt.Printf("ssssss: \n")
		for apply := range kv.applyCh {
			//		kv.Que = append(kv.Que, apply.Command)
			//		fmt.Printf("%d: now state: %v\n", kv.me, kv.Que)
			op := apply.Command.(Op)
			//fmt.Printf("server: %d\n", op.Req)
			cli, ok := kv.Clis[op.Cli]
			if !ok {
				kv.Clis[op.Cli] = Client{-1, ""}
				cli = Client{-1, ""}
			}
			switch op.Op {
			case "Get":
				if cli.Req != op.Req {
					value, ok := kv.KeyValueDataBase[op.Key]
					if ok == true {
						kv.Clis[op.Cli] = Client{op.Req, value}
					} else {
						kv.Clis[op.Cli] = Client{op.Req, ""}
					}
				}
				break
			case "Put":
				if cli.Req != op.Req {
					kv.Clis[op.Cli] = Client{op.Req, ""}
					kv.KeyValueDataBase[op.Key] = op.Value
				}
				break
			case "Append":
				if cli.Req != op.Req {
					kv.Clis[op.Cli] = Client{op.Req, ""}
					value, ok := kv.KeyValueDataBase[op.Key]
					if ok == false {
						kv.KeyValueDataBase[op.Key] = op.Value
					} else {
						kv.KeyValueDataBase[op.Key] = value + op.Value
					}
				}
			}
			kv.commitIndex = apply.CommandIndex
			kv.cond.Signal()
		}
	}()

	go func() {
		for {
			time.Sleep(1 * time.Second)
			kv.NowTime += 1
			kv.cond.Signal()
		}
	}()
	return kv
}
