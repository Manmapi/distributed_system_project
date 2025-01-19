package main

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	// 1) Parse environment variables
	nodeIDStr := os.Getenv("NODE_ID")
	if nodeIDStr == "" {
		log.Fatal("Missing environment variable NODE_ID")
	}
	nodeID, err := strconv.Atoi(nodeIDStr)
	if err != nil {
		log.Fatalf("Invalid NODE_ID=%s: %v", nodeIDStr, err)
	}

	peersEnv := os.Getenv("PEERS")
	if peersEnv == "" {
		log.Println("No PEERS environment var. This node has no peers.")
	}

	var peerIDs []int
	if peersEnv != "" {
		for _, pStr := range strings.Split(peersEnv, ",") {
			pStr = strings.TrimSpace(pStr)
			if pStr == "" {
				continue
			}
			pid, err := strconv.Atoi(pStr)
			if err == nil && pid != nodeID {
				peerIDs = append(peerIDs, pid)
			}
		}
	}

	// 2) Create the node
	node := &Node{
		id:       nodeID,
		peerIds:  peerIDs,
		leaderId: -1,

		isElectionGoging: false,
	}

	node.startInternalServer()

	time.Sleep(2 * time.Second)

	node.startHeartbeatRoutine()

	go func() {
		time.Sleep(3 * time.Second)
		node.electionMutex.Lock()
		if node.leaderId == -1 {
			node.electionMutex.Unlock()
			go node.startElection()
		} else {
			node.electionMutex.Unlock()
		}
	}()

	// Keep this node running
	select {}
}
