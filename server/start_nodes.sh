#!/usr/bin/env bash


echo "Starting Node 1..."
go run main.go node.go -id=1 -peers=2,3
PID1=$!

echo "Starting Node 2..."
go run main.go node.go -id=2 -peers=1,3
PID2=$!

echo "Starting Node 3..."
go run main.go node.go -id=3 -peers=1,2
PID3=$!

echo "Starting Node 4..."
go run main.go node.go -id=4 -peers=1,2,3 &
PID4=$!

echo "All nodes launched. PIDs: $PID1, $PID2, $PID3, $PID4"

# Wait for all background jobs to finish (or Ctrl+C to kill them).
wait
