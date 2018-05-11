package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"

//import "fmt"

type Clerk struct {
	servers []*labrpc.ClientEnd
	req     int
	me      int64
	leader  int
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.req = 0
	ck.me = nrand()
	//	fmt.Printf("make clerk %v\n", ck.me)
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	i := ck.leader
	servers := len(ck.servers)
	req := ck.req
	ck.req += 1
	for {
		var args GetArgs
		var reply GetReply
		args.Key = key
		args.Req = req
		args.Cli = ck.me
		server := ck.servers[i%servers]
		//	fmt.Printf("zzz2: %d, req: %d\n", i%servers, req)

		ok := server.Call("KVServer.Get", &args, &reply)
		if ok && reply.WrongLeader == false {
			ck.leader = i
			return reply.Value
		}
		//	fmt.Printf("iii2: %d: %s, %s\n", i, key, "Get")
		i += 1
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	i := ck.leader
	servers := len(ck.servers)
	req := ck.req
	ck.req += 1
	for {
		var args PutAppendArgs
		var reply PutAppendReply
		args.Op = op
		args.Key = key
		args.Value = value
		args.Req = req
		args.Cli = ck.me
		server := ck.servers[i%servers]
		//	fmt.Printf("zzz1: %d, req: %d\n", i%servers, req)
		ok := server.Call("KVServer.PutAppend", &args, &reply)
		if ok == false {
			//fmt.Printf("%s,  %s,  %s, \n", key, value, op)
		}
		if ok && reply.WrongLeader == false {
			ck.leader = i
			break
		}
		//	fmt.Printf("iii1: %d: %s,  %s,  %s, \n", i, key, value, op)

		i += 1
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
