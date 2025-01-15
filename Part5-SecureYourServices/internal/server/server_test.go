package server

import (
	"context"
	"net"
	"os"
	"testing"

	log_v1 "github.com/distributed_service_go/Part5-SecureYourServices/api/v1"
	"github.com/distributed_service_go/Part5-SecureYourServices/internal/config"
	"github.com/distributed_service_go/Part5-SecureYourServices/internal/log"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client log_v1.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                    testProduceConsumeStream,
		"consume past log boundary fails":                    testConsumePastBoundary,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, config)
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (
	client log_v1.LogClient,
	cfg *Config,
	teardown func(),
) {

	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile: config.ClientCertFile, // 추가
		KeyFile:  config.ClientKeyFile,  // 추가
		CAFile:   config.CAFile,
	})
	require.NoError(t, err)
	clientCreds := credentials.NewTLS(clientTLSConfig)

	cc, err := grpc.NewClient(
		l.Addr().String(),
		grpc.WithTransportCredentials(clientCreds),
	)
	require.NoError(t, err)

	client = log_v1.NewLogClient(cc)

	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: l.Addr().String(),
		Server:        true,
	})

	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	dir, err := os.MkdirTemp("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	cfg = &Config{
		CommitLog: clog,
	}

	if fn != nil {
		fn(cfg)
	}
	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go func() {
		server.Serve(l)
	}()

	return client, cfg, func() {
		server.Stop()
		cc.Close()
		l.Close()

	}
}

func testProduceConsume(t *testing.T, client log_v1.LogClient, config *Config) {

	ctx := context.Background()

	want := &log_v1.Record{
		Value: []byte("hello world"),
	}

	produce, err := client.Produce(
		ctx,
		&log_v1.ProduceRequest{
			Record: want,
		},
	)

	require.NoError(t, err)

	consume, err := client.Consume(ctx, &log_v1.ConsumeRequest{
		Offset: produce.Offset,
	})

	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)

}

func testConsumePastBoundary(
	t *testing.T,
	client log_v1.LogClient,
	config *Config,
) {
	ctx := context.Background()

	produce, err := client.Produce(ctx, &log_v1.ProduceRequest{
		Record: &log_v1.Record{
			Value: []byte("hello world"),
		},
	})
	require.NoError(t, err)

	consume, err := client.Consume(
		ctx, &log_v1.ConsumeRequest{
			Offset: produce.Offset + 1,
		})

	if consume != nil {
		t.Fatal("consume not nil")
	}

	got := status.Code(err)

	want := status.Code(log_v1.ErrOffsetOutOfRange{}.GRPStatus().Err())

	if got != want {
		t.Fatalf("got err: %v, want: %v", got, want)
	}

}
func testProduceConsumeStream(
	t *testing.T,
	client log_v1.LogClient,
	config *Config,
) {
	ctx := context.Background()

	records := []*log_v1.Record{{
		Value:  []byte("first message"),
		Offset: 0,
	}, {
		Value:  []byte("second message"),
		Offset: 1,
	}}

	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)

		for offset, record := range records {
			err = stream.Send(&log_v1.ProduceRequest{Record: record})

			require.NoError(t, err)
			res, err := stream.Recv()
			require.NoError(t, err)
			if res.Offset != uint64(offset) {
				t.Fatalf(
					"got offset: %d, want: %d",
					res.Offset,
					offset,
				)
			}

		}
	}

	{
		stream, err := client.ConsumeStream(
			ctx,
			&log_v1.ConsumeRequest{Offset: 0},
		)

		require.NoError(t, err)

		for i, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, res.Record, &log_v1.Record{
				Value:  record.Value,
				Offset: uint64(i),
			})
		}
	}

}
