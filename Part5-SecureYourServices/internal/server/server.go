package server

import (
	"context"

	log_v1 "github.com/distributed_service_go/Part5-SecureYourServices/api/v1"
	"google.golang.org/grpc"
)

type Config struct {
	CommitLog CommitLog
}

type CommitLog interface {
	Append(*log_v1.Record) (uint64, error)
	Read(uint64) (*log_v1.Record, error)
}

var _ log_v1.LogServer = (*grpcServer)(nil)

type grpcServer struct {
	log_v1.UnimplementedLogServer
	*Config
}

func newgrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: config,
	}
	return srv, nil
}

func (s *grpcServer) Produce(ctx context.Context, req *log_v1.ProduceRequest) (*log_v1.ProduceResponse, error) {
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &log_v1.ProduceResponse{Offset: offset}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *log_v1.ConsumeRequest) (*log_v1.ConsumeResponse, error) {
	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &log_v1.ConsumeResponse{Record: record}, nil
}

func (s *grpcServer) ProduceStream(
	stream log_v1.Log_ProduceStreamServer,
) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}

		if err = stream.Send(res); err != nil {
			return err
		}
	}
}

func (s *grpcServer) ConsumeStream(
	req *log_v1.ConsumeRequest,
	stream log_v1.Log_ConsumeStreamServer,
) error {

	for {
		select {
		case <-stream.Context().Done():
			return nil

		default:
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case log_v1.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}
			if err = stream.Send(res); err != nil {
				return err
			}
			req.Offset++
		}
	}

}

func NewGRPCServer(config *Config, opts ...grpc.ServerOption) (*grpc.Server, error) {
	gsrv := grpc.NewServer()
	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}
	log_v1.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}
