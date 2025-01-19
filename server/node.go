package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/marcelloh/fastdb"
)

type Node struct {
	id       int
	peerIds  []int
	leaderId int

	electionMutex    sync.Mutex
	isElectionGoging bool

	internalListener net.Listener
	internalServer   *rpc.Server

	leaderListener net.Listener
	leaderServer   *rpc.Server
}

type InternalRPC struct {
	node *Node
}

type LeaderRPC struct {
	node  *Node
	db    *fastdb.DB
	mutex sync.Mutex
}

type SetKeyArgs struct {
	BucketName string
	Key        int
	Value      string
}

type GetKeyArgs struct {
	BucketName string
	Key        int
}

type DeleteKeyArgs struct {
	BucketName string
	Key        int
}

type Server struct {
	db    *fastdb.DB
	mutex sync.Mutex
}

type Response struct {
	Data    string
	Message string
}

type EmptyRequest struct{}

func (server *LeaderRPC) SetKey(agrs *SetKeyArgs, reply *Response) error {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	server.db.Set(agrs.BucketName, agrs.Key, []byte(agrs.Value))
	reply.Message = "OK"
	return nil
}

func (server *LeaderRPC) GetKey(args *GetKeyArgs, reply *Response) error {
	data, isExist := server.db.Get(args.BucketName, args.Key)
	if !isExist {
		reply.Message = "Not found"
	} else {
		reply.Data = string(data)
	}
	return nil
}

func (server *LeaderRPC) DeleteKey(args *DeleteKeyArgs, reply *Response) error {
	ok, err := server.db.Del(args.BucketName, args.Key)
	if !ok {
		reply.Message = "Faild to delete key" + string(err.Error())
	} else {
		reply.Message = "OK"
	}
	return nil
}

func (server *LeaderRPC) GetStoreInfo(args EmptyRequest, reply *Response) error {
	reply.Data = server.db.Info()
	return nil
}

// Bully election
// There are some essential api
// Ping: To check if master is up/down
// StartElection: Start election process. If there is any ongoing process, ignore
// Election: call from lower id node to higer node
// NotifyLeader: Call be highest node. If current master is lower than received information. update

func (node *Node) callToPeer(peerId int, method string, args interface{}, reply interface{}) error {
	var port int
	if peerId == node.leaderId {
		port = 8000
	} else {
		port = 8000 + peerId
	}
	address := fmt.Sprintf("localhost:%d", port)
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		log.Printf("[Node %d] Could not connect to peer %d at %d: %v\n",
			node.id, peerId, port, err)
		return err
	}
	log.Printf("[Node %d] Calling method %s", node.id, method)
	defer client.Close()
	return client.Call(method, args, reply)

}

func (node *Node) startElection() {
	node.electionMutex.Lock()
	if node.isElectionGoging {
		node.electionMutex.Unlock()
		return
	}
	node.isElectionGoging = true
	node.electionMutex.Unlock()

	higherIds := []int{}
	for _, peerId := range node.peerIds {
		if peerId > node.id {
			higherIds = append(higherIds, peerId)
		}
	}
	var isFoundHigherActiveNode bool = false
	for _, nextId := range higherIds {
		var electionReply ElectionRes
		err := node.callToPeer(
			nextId,
			"InternalRPC.Election",
			ElectionReq{node.id},
			&electionReply,
		)
		if err != nil {
			log.Println("Has error while trying to get key", err)
		} else if electionReply.Ack {
			// Have another higher node to delegage. Abort
			isFoundHigherActiveNode = true
			break
		}
	}

	if !isFoundHigherActiveNode {
		// TODO: promote to leader and notified all other node
		node.becomeLeader()
	}
}

// func (rpcNode *InternalRPC) StartElection(args ElectionReq, reply *ElectionRes) error {
// 	n := rpcNode.node
// 	n.electionMutex.Lock()

// 	if rpcNode.node.isElectionGoging {
// 		n.electionMutex.Unlock()
// 		reply.Ack = true
// 		return nil
// 	}

// 	rpcNode.node.isElectionGoging = true
// 	n.electionMutex.Unlock()

// 	higherIds := []int{}
// 	for _, peerId := range rpcNode.node.peerIds {
// 		if peerId > rpcNode.node.id {
// 			higherIds = append(higherIds, peerId)
// 		}
// 	}
// 	var isFoundHigherActiveNode bool = false
// 	for _, nextId := range higherIds {
// 		var electionReply ElectionRes
// 		log.Printf("[Node %d] StartElection: Call Election to node %d", rpcNode.node.id, nextId)
// 		err := rpcNode.node.callToPeer(
// 			nextId,
// 			"Election",
// 			ElectionReq{rpcNode.node.id},
// 			&electionReply,
// 		)
// 		if err != nil {
// 			log.Fatal("Has error while trying to get key", err)
// 		} else if electionReply.Ack {
// 			// Have another higher node to delegage. Abort
// 			isFoundHigherActiveNode = true
// 			break
// 		}
// 	}

// 	if !isFoundHigherActiveNode {
// 		n.becomeLeader()
// 	}
// 	return nil
// }

type ElectionReq struct {
	SenderID int
}

type ElectionRes struct {
	Ack bool
}

type NotifyLeaderReq struct {
	LeaderId int
}

type NotifyLeaderRes struct {
	Ack bool
}

func (rpcNode *InternalRPC) Election(args ElectionReq, reply *ElectionRes) error {
	n := rpcNode.node
	n.electionMutex.Lock()
	defer n.electionMutex.Unlock()
	// When you receive this. response with OK then continue to call Election to the next node
	reply.Ack = true

	// Check if this node is start election process or not. If not, start one
	if !rpcNode.node.isElectionGoging {
		rpcNode.node.startElection()
	}
	return nil
}

func (rpcNode *InternalRPC) NotifyLeader(args NotifyLeaderReq, reply *NotifyLeaderRes) error {
	newLeader := args.LeaderId
	reply.Ack = true
	log.Printf("[Node %d] Receive NotifyLeader", rpcNode.node.id, newLeader)
	// If leaderId is smaller than current leaderId, no need to change

	rpcNode.node.leaderId = newLeader
	rpcNode.node.isElectionGoging = false

	log.Printf("[Node %d] Acknowledge that leader is now %d", rpcNode.node.id, rpcNode.node.leaderId)
	return nil
}

// Heartbeat check
type HeartbeatReq struct {
	SenderID int
}
type HeartbeatRes struct {
	Alive bool
}

// Heartbeat: backups call coordinator to confirm it's alive
func (rpcNode *LeaderRPC) Heartbeat(req HeartbeatReq, res *HeartbeatRes) error {
	n := rpcNode.node
	n.electionMutex.Lock()
	defer n.electionMutex.Unlock()
	if n.id == n.leaderId {
		res.Alive = true
	} else {
		res.Alive = false
	}
	return nil
}

func (node *Node) becomeLeader() {
	node.electionMutex.Lock()
	defer node.electionMutex.Unlock()
	log.Printf("[Node %d] I am now the leader.\n", node.id)
	node.leaderId = node.id
	store, err := fastdb.Open(":memory:", 100)
	if err != nil {
		message := fmt.Sprintf("[Node %d] Failed to open db", node.id)
		log.Fatal(message)
	}
	node.startLeaderServer(store)
	// Notify to other nodes
	for _, nextId := range node.peerIds {
		log.Printf("[Node %d] Notify to node %d that leader is %d", node.id, nextId, node.id)
		var notifyLeaderRes NotifyLeaderRes
		// Don't care about ack
		err := node.callToPeer(
			nextId,
			"InternalRPC.NotifyLeader",
			NotifyLeaderReq{node.id},
			&notifyLeaderRes,
		)
		if err != nil {
			log.Println(err)
		}
	}
}

func (node *Node) startInternalServer() {
	node.internalServer = rpc.NewServer()
	node.internalServer.Register(&InternalRPC{node: node})

	port := 8000 + node.id
	addr := fmt.Sprintf(":%d", port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("[Node %d] Cannot listen on %s: %v", node.id, addr, err)
	}
	node.internalListener = l
	log.Printf("[Node %d] Internal server listening on %s", node.id, addr)
	go func() {
		// Continuous loop
		for {
			conn, _ := l.Accept()
			go node.internalServer.ServeConn(conn)
		}
	}()
}

func (node *Node) startLeaderServer(store *fastdb.DB) {
	node.leaderServer = rpc.NewServer()
	node.leaderServer.Register(&LeaderRPC{
		node: node,
		db:   store,
	})

	port := 8000
	addr := fmt.Sprintf(":%d", port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("[Node %d] Cannot listen on %s: %v", node.id, addr, err)
	}
	node.leaderListener = l
	log.Printf("[Node %d] Leader server listening on %s", node.id, addr)
	go func() {
		// Continuous loop
		for {
			conn, err := l.Accept()
			if err != nil {
				log.Printf("[Node %d] Leader listener closed: %v", node.id, err)
				return
			}
			go node.leaderServer.ServeConn(conn)
		}
	}()
}

func (node *Node) startHeartbeatRoutine() {
	log.Printf("[Node %d] Start Heartbeat Rountine", node.id)
	go func() {
		for {
			time.Sleep(3 * time.Second)

			node.electionMutex.Lock()
			leader := node.leaderId
			node.electionMutex.Unlock()
			// log.Printf("[Node %d] Heartbeat: Current leader is %d", node.id, node.leaderId)
			if leader == -1 || leader == node.id {
				continue
			}
			var heartbeatRes HeartbeatRes

			err := node.callToPeer(
				leader,
				"LeaderRPC.Heartbeat",
				HeartbeatReq{node.id},
				&heartbeatRes,
			)
			if err != nil || !heartbeatRes.Alive {
				go node.startElection()
			}
		}
	}()
}
