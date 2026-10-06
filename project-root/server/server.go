package main

import (
	"context"
	"log"
	"net"
	"sync"
	chat "chitchat/grpc"
	"google.golang.org/grpc"
)

type chitChatRoute struct {
	chat.UnimplementedChitChatServer //required by gRPC
	clients map[string]chat.ChitChat_JoinServer //stores each client's stream
	clock int64 //Lamport clock
	mu sync.Mutex //protects the clients map
}

func (s *chitChatRoute) Join(req *chat.JoinRequest, stream chat.ChitChat_JoinServer) error {
	//streaming method
}
