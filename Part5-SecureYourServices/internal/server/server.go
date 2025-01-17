package server

import (
	"context"

	api_v1 "github.com/distributed_service_go/Part5-SecureYourServices/api/v1"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	objectWildcard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

var _ api_v1.LogServer = (*grpcServer)(nil)

type Config struct {
	CommitLog
	Authorizer Authorizer
}

type Authorizer interface {
	Authorize(subject, object, action string) error
}

type CommitLog interface {
	Append(*api_v1.Record) (uint64, error)
	Read(uint64) (*api_v1.Record, error)
}

var _ api_v1.LogServer = (*grpcServer)(nil)

type grpcServer struct {
	api_v1.UnimplementedLogServer
	*Config
}

func newgrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: config,
	}
	return srv, nil
}

func (s *grpcServer) Produce(ctx context.Context, req *api_v1.ProduceRequest) (*api_v1.ProduceResponse, error) {
	if err := s.Authorizer.Authorize(
		subject(ctx), objectWildcard, produceAction,
	); err != nil {
		return nil, err
	}

	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api_v1.ProduceResponse{Offset: offset}, nil

}

func (s *grpcServer) Consume(ctx context.Context, req *api_v1.ConsumeRequest) (*api_v1.ConsumeResponse, error) {
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		consumeAction,
	); err != nil {
		return nil, err
	}

	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &api_v1.ConsumeResponse{Record: record}, nil
}

func (s *grpcServer) ProduceStream(
	stream api_v1.Log_ProduceStreamServer,
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
	req *api_v1.ConsumeRequest,
	stream api_v1.Log_ConsumeStreamServer,
) error {

	for {
		select {
		case <-stream.Context().Done():
			return nil

		default:
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case api_v1.ErrOffsetOutOfRange:
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

	opts = append(opts,
		grpc.StreamInterceptor(
			grpc_middleware.ChainStreamServer(
				grpc_auth.StreamServerInterceptor(authenticate),
			)),
		grpc.UnaryInterceptor(
			grpc_middleware.ChainUnaryServer(
				grpc_auth.UnaryServerInterceptor(authenticate),
			)))
	gsrv := grpc.NewServer(opts...)
	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}
	api_v1.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

func authenticate(ctx context.Context) (context.Context, error) {
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(
			codes.Unknown,
			"couldn't find peer info",
		).Err()
	}

	if peer.AuthInfo == nil {
		return context.WithValue(ctx, subjectContextKey{}, ""), nil
	}
	tlsInfo := peer.AuthInfo.(credentials.TLSInfo)
	subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, subjectContextKey{}, subject)

	return ctx, nil
}

func subject(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}

type subjectContextKey struct{}
