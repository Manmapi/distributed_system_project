package main

import (
	"log"
	"net"
	"net/http"
	"net/rpc"

	"sync"

	"github.com/marcelloh/fastdb"
)

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

func (server *Server) SetKey(agrs *SetKeyArgs, reply *Response) error {
	server.mutex.Lock()
	server.db.Set(agrs.BucketName, agrs.Key, []byte(agrs.Value))
	reply.Message = "OK"
	defer server.mutex.Unlock()
	return nil
}

func (server *Server) GetKey(args *GetKeyArgs, reply *Response) error {
	data, isExist := server.db.Get(args.BucketName, args.Key)
	if !isExist {
		reply.Message = "Not found"
	} else {
		reply.Data = string(data)
	}
	return nil
}

func (server *Server) DeleteKey(args *DeleteKeyArgs, reply *Response) error {
	ok, err := server.db.Del(args.BucketName, args.Key)
	if !ok {
		reply.Message = "Faild to delete key" + string(err.Error())
	} else {
		reply.Message = "OK"
	}
	return nil
}

func (server *Server) GetStoreInfo(args EmptyRequest, reply *Response) error {
	reply.Data = server.db.Info()
	return nil
}

func main() {
	store, err := fastdb.Open(":memory:", 100)
	if err != nil {
		log.Fatalf("Failed to open db: %v", err)
	}
	defer store.Close()
	// Create a new RPC server
	server := &Server{db: store}
	// Register RPC server

	rpc.Register(server)
	rpc.HandleHTTP()

	// Listen for requests on port 1234
	l, e := net.Listen("tcp", ":2233")
	if e != nil {
		log.Fatal("listen error:", e)
	}

	http.Serve(l, nil)
}
