package main

import (
	"context"
	"log"
	"net"
	"sync"
	chat "chitchat/grpc"
	"google.golang.org/grpc"
	"fmt"
)

type chitChatServer struct { 
	chat.UnimplementedChitChatServer //required by gRPC
	clients map[string]chat.ChitChat_JoinServer //stores each client's stream
	clock int64 //Lamport clock
	mu sync.Mutex //protects the clients map
}

  
// incrementClock increments the Lamport clock and returns the new value.
// Called BEFORE sending/broadcasting (Lamport Rule 1)
func (s *chitChatServer) incrementClock() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clock++
	return s.clock
}

// updateClock updates the clock when receiving a message from a client.
// Lamport Rule 2: clock = max(local, received) + 1
func (s *chitChatServer) updateClock(receivedTime int64) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if receivedTime > s.clock {
		s.clock = receivedTime
	}
	s.clock++
	return s.clock
}

func (s *chitChatServer) broadcast(content string, timestamp int64) {
	//Create message to send
	msg := &chat.BroadcastMessage{ //pointer to a BroadcastMessage struct defined in proto file
		Content: content,
		Timestamp: timestamp,
	}

	//lock while iterating ovver the clients map
	s.mu.Lock()
	defer s.mu.Unlock()

	//send the message to each connected client
	//loop through the clients map and send the message to each client's open stream
	for name, stream := range s.clients {
		if err := stream.Send(msg); err != nil {
			log.Printf("[Server] Error sending to %s: %v", name, err)
		}
	}
	log.Printf("[Server] Broadcasted at Lamport time: %d: %s", timestamp, content)
}

func (s *chitChatServer) Join(req *chat.JoinRequest, stream chat.ChitChat_JoinServer) error {
	// 1. Get client name from request
  	// 2. Add client to the map
  	// 3. Broadcast "Participant X joined Chit Chat at Lamport time L"
  	// 4. BLOCK - keep the goroutine alive while client is connected
  	// 5. When client disconnects, clean up

	// 1. Get client name from request
	clientName := req.Name

	log.Printf("[Server] Client %s is joining...", clientName)

	// 2. Add client to the map
	s.mu.Lock()
	s.clients[clientName] = stream
	s.mu.Unlock()

	// 3. Broadcast "Participant X joined Chit Chat at Lamport time L"
	timestamp := s.incrementClock() //increment clock before broadcasting
	joinMessage := fmt.Sprintf("Participant %s joined Chit Chat at Lamport time %d", clientName, timestamp)
	s.broadcast(joinMessage, timestamp)

	log.Printf("[Server] Client %s joined successfully at Lamport time %d.", clientName, timestamp)

	// 4. BLOCK - keep the goroutine alive while client is connected
	// pretty sure this is right
	<-stream.Context().Done()

	// 5. When client disconnects, clean up if needed
	s.mu.Lock()
	_, stillExists := s.clients[clientName]
	if stillExists {
		delete(s.clients, clientName)
		s.mu.Unlock()

		timestamp = s.incrementClock() //increment clock before broadcasting
		leaveMessage := fmt.Sprintf("Participant %s left Chit Chat at Lamport time %d", clientName, timestamp)
		s.broadcast(leaveMessage, timestamp)

		log.Printf("[Server] Client %s disconnected at Lamport time %d.", clientName, timestamp)
	} else {
		s.mu.Unlock() //already called Leave(), nothing to doo
	}

	return nil
}

func (s *chitChatServer) Publish(ctx context.Context, msg *chat.ChatMessage) (*chat.PublishResponse, error) {
	// 1. Receive the message from the client
	// 2. Update Lamport clock with received timestamp
	// 3. Broadcast the message to all clients
	// 4. Return a response

	// 1. Receive the message from the client
	clientName := msg.Name
	content := msg.Content
	receivedTimestamp := msg.Timestamp

	log.Printf("[Server] Received message from %s at Lamport time %d: %s", clientName, receivedTimestamp, content)

	//2.update Lamport clock with received timestamps
	timestamp := s.updateClock(receivedTimestamp)

	//3.broadcast to everyone	
	message := fmt.Sprintf("%s: %s", clientName, content)
	s.broadcast(message, timestamp)

	//return empty response
	return &chat.PublishResponse{}, nil //(response, error)
}

func (s *chitChatServer) Leave(ctx context.Context, req *chat.LeaveRequest) (*chat.LeaveResponse, error) {
	// 1. Get client name from request
	// 2. Remove client from the map
	// 3. Broadcast "Participant X left Chit Chat at Lamport time L"
	// 4. Return a response

	// 1. Get client name from request
	clientName := req.Name

	log.Printf("[Server] Client %s is leaving...", clientName)

	// 2. Remove client from the map if they exist. If not, just log and return.
	s.mu.Lock()
	_, stillExists := s.clients[clientName]
	if stillExists {
		delete(s.clients, clientName)
		s.mu.Unlock()

		// 3. Broadcast "Participant X left Chit Chat at Lamport time L"
		timestamp := s.incrementClock() //increment clock before broadcasting
		leaveMessage := fmt.Sprintf("Participant %s left Chit Chat at Lamport time %d", clientName, timestamp)
		s.broadcast(leaveMessage, timestamp)

		log.Printf("[Server] Client %s left successfully at Lamport time %d.", clientName, timestamp)
	} else {
		s.mu.Unlock() //already handled in Join(), nothing to do
		log.Printf("[Server] Client %s was not found in the clients map.", clientName)
	}

	// 4. Return a response
	return &chat.LeaveResponse{}, nil
}

func main() {
	// Step 1: Set up logging
	// Step 2: Create TCP listener on port 5050
	// Step 3: Create gRPC server
	// Step 4: Create our chat server with initialized map
	// Step 5: Register our server with gRPC
	// Step 6: Start serving (blocks forever)


  	// Step 1: Set up logging
  	log.SetFlags(log.Ltime) //show only time in logs
  	log.Println("[Server] Starting Chit Chat server...")

  	// Step 2: Create TCP listener on port 5050
  	listener, err := net.Listen("tcp", ":5050") //net.Listen opens a network port and waits for incoming connections
  	if err != nil {
  		log.Fatalf("[Server] Failed to listen on port 5050: %v", err)
  	}
  	log.Println("[Server] Listening on port 5050")

  	// Step 3: Create gRPC server
  	grpcServer := grpc.NewServer()

  	// Step 4: Create chat server with map
  	server := &chitChatServer{ //gRPC expects a pointer to a struct that implements the service interface
  		clients: make(map[string]chat.ChitChat_JoinServer), //empty map to hold client streams
  		clock:   0,
  	}

  	// Step 5: Register server with gRPC
  	chat.RegisterChitChatServer(grpcServer, server)

  	log.Println("[Server] Ready to accept connections")

  	// Step 6: Start serving 
  	if err := grpcServer.Serve(listener); err != nil {
  		log.Fatalf("[Server] Failed to serve: %v", err)
  	}
  }
