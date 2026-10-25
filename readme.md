How to Run:

Go needs to be installed and the proto code already generated.

To generate the proto code (if needed):
cd project-root
protoc --go_out=. --go-grpc_out=. grpc/chitchat.proto

To start the server:
cd project-root
go run ./server/

To start a client (new terminal for each client):
cd project-root
go run ./client/ -name "name"

You can run multiple clients by opening multiple terminals and using different names.

Usage

Once connected, type a message and press enter to send it.
Type "quit" to leave the chat.
