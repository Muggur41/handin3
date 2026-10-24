package main

import (
	"bufio"
	"context"
	"flag"
	"log"
	"os"
	"strings"
	"sync"

	// Import the generated gRPC code from the .proto file
	chat "chitchat/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client struct holds the clients state
type Client struct {
	name string                  // The participant name
	lamportTime int64            // Our local Lamport clock
	mu sync.Mutex              	 // Lock to protect lamportTime from concurrent access
	client chat.ChitChatClient   // The gRPC client stub, for calling publish() and leave()
	stream chat.ChitChat_JoinClient // The server-streaming connection for receiving broadcasts
}

// incrementClock safely increments our Lamport clock and returns the new value
// Rule 1: Before sending any message, increment the clock
func (c *Client) incrementClock() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lamportTime++
	return c.lamportTime
}

// updateClock updates our clock when receiving a message
// Rule 2: clock = max(local_clock, received_timestamp) + 1
func (c *Client) updateClock(receivedTime int64) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if receivedTime > c.lamportTime {
		c.lamportTime = receivedTime
	}
	c.lamportTime++
	return c.lamportTime
}

// getClock safely returns the current Lamport time
// not sure if this is needed
func (c *Client) getClock() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lamportTime
}

// listenForBroadcasts runs in a goroutine and continuously listens for
// messages from the server's stream. When a message arrives, it:
// 1. Updates the Lamport clock
// 2. Displays the message
// 3. Logs the message
func (c *Client) listenForBroadcasts() {
	for {
		// Recv() blocks until a message arrives or the stream closes
		msg, err := c.stream.Recv()
		if err != nil {
			// Stream closed (server shutdown or we left)
			log.Printf("[Client %s] Stream closed: %v", c.name, err)
			return
		}

		// Update our Lamport clock with the received timestamp
		newTime := c.updateClock(msg.Timestamp)

		// Display the message to the user
		// Format: [Lamport Time L] message content
		log.Printf("[Client %s] [Lamport Time %d] %s", c.name, msg.Timestamp, msg.Content)

		// Note: The log.Printf already logs to stdout
		// In a real system, you might also write to a file
		_ = newTime // We updated the clock; newTime is our new local time
	}
}

// publishMessage sends a chat message to the server
func (c *Client) publishMessage(content string) error {
	// Validate message length (max 128 characters - S3)
	if len(content) > 128 {
		log.Printf("[Client %s] Message too long (max 128 characters)", c.name)
		return nil
	}

	// Rule 1: Increment clock before sending
	timestamp := c.incrementClock()

	// Create and send the message
	_, err := c.client.Publish(context.Background(), &chat.ChatMessage{
		Name: c.name,
		Content: content,
		Timestamp: timestamp,
	})

	if err != nil {
		log.Printf("[Client %s] Failed to publish message: %v", c.name, err)
		return err
	}

	log.Printf("[Client %s] Published message at Lamport time %d", c.name, timestamp)
	return nil
}

// leave notifies the server that we're leaving and closes the connection
func (c *Client) leave() error {
	// Increment clock before sending leave request
	timestamp := c.incrementClock()

	_, err := c.client.Leave(context.Background(), &chat.LeaveRequest{
		Name: c.name,
		Timestamp: timestamp,
	})

	if err != nil {
		log.Printf("[Client %s] Error during leave: %v", c.name, err)
		return err
	}

	log.Printf("[Client %s] Left chat at Lamport time %d", c.name, timestamp)
	return nil
}

func main() {
	// Parse command line arguments
	// Usage: go run client.go -name "Alice" -server "localhost:5050"
	name := flag.String("name", "Anonymous", "Your name in the chat")
	serverAddr := flag.String("server", "localhost:5050", "Server address")
	flag.Parse()

	// Set up logging with timestamps and client name
	log.SetFlags(log.Ltime) // Include time in log messages

	log.Printf("[Client %s] Starting client...", *name)

	// Connect to the server
	// We use insecure credentials for simplicity (no TLS)
	conn, err := grpc.NewClient(*serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("[Client %s] Failed to connect to server: %v", *name, err)
	}
	defer conn.Close()

	log.Printf("[Client %s] Connected to server at %s", *name, *serverAddr)

	// Create the gRPC client stub
	grpcClient := chat.NewChitChatClient(conn)

	// Create our client instance
	client := &Client{
		name:        *name,
		lamportTime: 0, // Start with Lamport time 0
		client:      grpcClient,
	}

	// Join the chat - this opens a server-streaming connection
	// The server will send us all broadcast messages through this stream
	stream, err := grpcClient.Join(context.Background(), &chat.JoinRequest{
		Name: *name,
	})
	if err != nil {
		log.Fatalf("[Client %s] Failed to join chat: %v", *name, err)
	}
	client.stream = stream

	log.Printf("[Client %s] Joined chat successfully", *name)

	// Start a goroutine to listen for incoming broadcasts
	// this should work
	go client.listenForBroadcasts()

	// Main loop: read user input and send messages
	log.Printf("[Client %s] Type messages and press Enter to send. Type 'quit' to leave.", *name)

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		input := strings.TrimSpace(scanner.Text())

		if input == "" {
			continue // Ignore empty lines
		}

		if strings.ToLower(input) == "quit" {
			// User wants to leave
			client.leave()
			break
		}

		// Send the message
		client.publishMessage(input)
	}

	log.Printf("[Client %s] Shutting down", *name)
}
