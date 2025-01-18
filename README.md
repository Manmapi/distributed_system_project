# Go Key-Value Storage

This project demonstrates a simple server-client application for key-value storage written in Go. The instructions below will guide you through running both the server and the client.

## Prerequisites

- [Golang](https://go.dev/)

## Getting Started

1. **Navigate to the project's root folder** (referred to as the "initial location of the source code").

## Running the Server

1. Open a terminal in the project's **root folder**.
2. Run the following command:
   ```bash
   cd server && go run main.go
   ```
3. The server will start listening on the configured port (for example, `localhost:8080`) and will log the status in the terminal.

## Running the Client

1. Open another terminal in the project's **root folder** (or use the same terminal in a new tab if you prefer).
2. Run the following command:
   ```bash
   cd client && go run main.go
   ```
3. The client will connect to the server and will print out its log or status messages in the terminal.
