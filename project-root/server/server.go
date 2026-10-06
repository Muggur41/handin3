package main

import (
	proto "ITUServer/grpc"
	"context"
	"errors"
	"log"
	"net"

	"google.golang.org/grpc"
)

type ITU_databaseServer struct {
	proto.UnimplementedITUDatabaseServer
	students []*proto.Student
}

func (s *ITU_databaseServer) GetStudents(ctx context.Context, in *proto.Empty) (*proto.Students, error) {
	return &proto.Students{Students: s.students}, nil
}

func (s *ITU_databaseServer) GetStudent(ctx context.Context, in *proto.GetStudentRequest) (*proto.Student, error) {
	for _, student := range s.students{
		if student.Id == in.Id {
			return student, nil
		}
	}
	return nil, errors.New("No such student")
}

func (s *ITU_databaseServer) AddStudent(ctx context.Context, in *proto.Student) (*proto.Empty, error) {
	s.students = append(s.students, in)
	return &proto.Empty{}, nil
}

func main() {
	server := &ITU_databaseServer{students: []*proto.Student{}}
	server.students = append(server.students, &proto.Student{Name:"John", Id: 1} )
	server.students = append(server.students, &proto.Student{Name:"Jane", Id: 2} )
	server.students = append(server.students, &proto.Student{Name:"Bob", Id: 3} )
	server.students = append(server.students, &proto.Student{Name:"Alice", Id:4} )

	server.start_server()
}

func (s *ITU_databaseServer) start_server() {
	grpcServer := grpc.NewServer()
	listener, err := net.Listen("tcp", ":5050")
	if err != nil {
		log.Fatalf("Did not work")
	}

	proto.RegisterITUDatabaseServer(grpcServer, s)

	err = grpcServer.Serve(listener)

	if err != nil {
		log.Fatalf("Did not work")
	}

}
