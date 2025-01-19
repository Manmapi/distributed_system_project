package main

import (
	"flag"
	"log"
	"strconv"
	"strings"
	"time"
)

func main() {
	// 1) Parse environment variables
	nodeID := (flag.Int("id", 1, "Node ID"))
	peersStr := flag.String("peers", "", "Comma-separated list of peer IDs")
	flag.Parse()

	if *peersStr == "" {
		log.Fatal("Peers must be specified via -peers")
	}

	var peerIDs []int
	for _, p := range strings.Split(*peersStr, ",") {
		pid, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil {
			log.Fatalf("Invalid peer ID: %s", p)
		}
		if pid != *nodeID {
			peerIDs = append(peerIDs, pid)
		}
	}

	// 2) Create the node
	node := &Node{
		id:       *nodeID,
		peerIds:  peerIDs,
		leaderId: -1,

		isElectionGoging: false,
	}

	node.startInternalServer()

	time.Sleep(2 * time.Second)

	node.startHeartbeatRoutine()

	go func() {
		time.Sleep(5 * time.Second)
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
