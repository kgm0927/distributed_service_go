package server

import (
	"context"
	"flag"

	"net"
	"os"
	"testing"
	"time"

	api_v1 "github.com/distributed_service_go/Part7-ServerSideServiceDiscovery/api/v1"
	"github.com/distributed_service_go/Part7-ServerSideServiceDiscovery/internal/auth"
	"github.com/distributed_service_go/Part7-ServerSideServiceDiscovery/internal/config"
	"github.com/distributed_service_go/Part7-ServerSideServiceDiscovery/internal/log"
	"github.com/stretchr/testify/require"
	"go.opencensus.io/examples/exporter"
	"go.uber.org/zap"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

var debug = flag.Bool("debug", false, "Enable observability for debugging.")

func TestMain(m *testing.M) {
	flag.Parse()
	if *debug {
		logger, err := zap.NewDevelopment()
		if err != nil {
			panic(err)
		}
		zap.ReplaceGlobals(logger)
	}
	os.Exit(m.Run())
}

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		rootClient api_v1.LogClient,
		nobodyClient api_v1.LogClient,
		config *Config,
	){
		// ...
		"produce/consume a message to/from the log succeeeds": testProduceConsume,
		"produce/consume stream succeeds":                     testProduceConsumeStream,
		"consume past log boundary fails":                     testConsumePastBoundary,
		"unauthorized fails":                                  testUnauthorized,
	} {
		t.Run(scenario, func(t *testing.T) {
			rootClient, nobodyClient, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, rootClient, nobodyClient, config)
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (
	rootClient api_v1.LogClient,
	nobodyClient api_v1.LogClient,
	cfg *Config,
	teardown func(),
) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	newClient := func(crtPath, keyPath string) (
		*grpc.ClientConn,
		api_v1.LogClient,
		[]grpc.DialOption,
	) {
		tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
			CertFile: crtPath,
			KeyFile:  keyPath,
			CAFile:   config.CAFile,
			Server:   false,
		})
		require.NoError(t, err)
		tlsCreds := credentials.NewTLS(tlsConfig)
		opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
		conn, err := grpc.NewClient(l.Addr().String(), opts...)
		require.NoError(t, err)
		client := api_v1.NewLogClient(conn)
		return conn, client, opts
	}

	var rootConn *grpc.ClientConn
	rootConn, rootClient, _ = newClient(
		config.RootClientCertFile,
		config.RootClientKeyFile,
	)

	var nobodyConn *grpc.ClientConn
	nobodyConn, nobodyClient, _ = newClient(
		config.NobodyClientCertFile,
		config.NobodyClientKeyFile,
	)

	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile: config.ServerCertFile,
		KeyFile:  config.ServerKeyFile,
		CAFile:   config.CAFile,
		Server:   true,
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	dir, err := os.MkdirTemp("", "server-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	authorizer := auth.New(config.ACLModelFile, config.ACLPolicyFile)

	var telemetryExporter *exporter.LogExporter
	if *debug {
		metricsLogFile, err := os.CreateTemp("", "metrics-*.log")
		require.NoError(t, err)
		t.Logf("metrics log file: %s", metricsLogFile.Name())

		traceLogFile, err := os.CreateTemp("", "trace-*.log")
		require.NoError(t, err)
		t.Logf("traces log file: %s", traceLogFile.Name())

		telemetryExporter, err = exporter.NewLogExporter(exporter.Options{
			MetricsLogFile:    metricsLogFile.Name(),
			TracesLogFile:     traceLogFile.Name(),
			ReportingInterval: time.Second,
		})
		require.NoError(t, err)
		err = telemetryExporter.Start()
		require.NoError(t, err)
	}
	cfg = &Config{
		CommitLog:  clog,
		Authorizer: authorizer,
	}
	if fn != nil {
		fn(cfg)
	}

	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go func() {
		server.Serve(l)
	}()

	return rootClient, nobodyClient, cfg, func() {
		server.Stop()
		rootConn.Close()
		nobodyConn.Close()
		l.Close()
		if telemetryExporter != nil {
			time.Sleep(1500 * time.Millisecond)
			telemetryExporter.Stop()
			telemetryExporter.Close()
		}
	}
}
func testProduceConsume(t *testing.T, client, _ api_v1.LogClient, config *Config) {
	ctx := context.Background()

	want := &api_v1.Record{
		Value: []byte("hello world"),
	}

	produce, err := client.Produce(
		ctx,
		&api_v1.ProduceRequest{
			Record: want,
		},
	)
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api_v1.ConsumeRequest{
		Offset: produce.Offset,
	})
	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

func testConsumePastBoundary(
	t *testing.T,
	client, _ api_v1.LogClient,
	config *Config,
) {
	ctx := context.Background()

	produce, err := client.Produce(ctx, &api_v1.ProduceRequest{
		Record: &api_v1.Record{
			Value: []byte("hello world"),
		},
	})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api_v1.ConsumeRequest{
		Offset: produce.Offset + 1,
	})
	if consume != nil {
		t.Fatal("consume not nil")
	}
	got := status.Code(err)
	want := status.Code(api_v1.ErrOffsetOutOfRange{}.GRPStatus().Err())
	if got != want {
		t.Fatalf("got err: %v, want: %v", got, want)
	}
}

func testProduceConsumeStream(
	t *testing.T,
	client, _ api_v1.LogClient,
	config *Config,
) {
	ctx := context.Background()

	records := []*api_v1.Record{{
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
			err = stream.Send(&api_v1.ProduceRequest{
				Record: record,
			})
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
			&api_v1.ConsumeRequest{Offset: 0},
		)
		require.NoError(t, err)

		for i, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, res.Record, &api_v1.Record{
				Value:  record.Value,
				Offset: uint64(i),
			})
		}
	}
}
func testUnauthorized(
	t *testing.T,
	_,
	client api_v1.LogClient,
	config *Config,
) {
	ctx := context.Background()
	produce, err := client.Produce(ctx,
		&api_v1.ProduceRequest{
			Record: &api_v1.Record{
				Value: []byte("hello world"),
			},
		},
	)
	if produce != nil {
		t.Fatalf("produce response should be nil")
	}
	gotCode, wantCode := status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code: %d, want: %d", gotCode, wantCode)
	}
	consume, err := client.Consume(ctx, &api_v1.ConsumeRequest{
		Offset: 0,
	})
	if consume != nil {
		t.Fatalf("consume response should be nil")
	}
	gotCode, wantCode = status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code: %d, want: %d", gotCode, wantCode)
	}
}

// func setupTest1(t *testing.T, fn func(*Config)) (
// 	client api_v1.LogClient,
// 	cfg *Config,
// 	teardown func(),
// ) {
// 	t.Helper()

// 	t.Helper()

// 	l, err := net.Listen("tcp", "127.0.0.1:0")
// 	require.NoError(t, err)

// 	clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
// 		CAFile: config.CAFile,
// 	})
// 	require.NoError(t, err)

// 	clientCreds := credentials.NewTLS(clientTLSConfig)
// 	cc, err := grpc.Dial(
// 		l.Addr().String(),
// 		grpc.WithTransportCredentials(clientCreds),
// 	)
// 	require.NoError(t, err)

// 	client = api_v1.NewLogClient(cc)

// 	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
// 		CertFile:      config.ServerCertFile,
// 		KeyFile:       config.ServerKeyFile,
// 		CAFile:        config.CAFile,
// 		ServerAddress: l.Addr().String(),
// 	})
// 	require.NoError(t, err)
// 	serverCreds := credentials.NewTLS(serverTLSConfig)

// 	dir, err := os.MkdirTemp("", "server-test")
// 	require.NoError(t, err)

// 	clog, err := log.NewLog(dir, log.Config{})
// 	require.NoError(t, err)

// 	cfg = &Config{
// 		CommitLog: clog,
// 	}
// 	if fn != nil {
// 		fn(cfg)
// 	}
// 	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
// 	require.NoError(t, err)

// 	go func() {
// 		server.Serve(l)
// 	}()

// 	return client, cfg, func() {
// 		server.Stop()
// 		cc.Close()
// 		l.Close()
// 	}
// }

// func setupTest2(t *testing.T, fn func(*Config)) (
// 	client api_v1.LogClient,
// 	cfg *Config,
// 	teardown func(),
// ) {
// 	t.Helper()

// 	l, err := net.Listen("tcp", "127.0.0.1:0")
// 	require.NoError(t, err)

// 	clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
// 		CertFile: config.ClientCertFile,
// 		KeyFile:  config.ClientKeyFile,
// 		CAFile:   config.CAFile,
// 	})
// 	require.NoError(t, err)

// 	clientCreds := credentials.NewTLS(clientTLSConfig)
// 	cc, err := grpc.NewClient(
// 		l.Addr().String(),
// 		grpc.WithTransportCredentials(clientCreds),
// 	)
// 	require.NoError(t, err)

// 	client = api_v1.NewLogClient(cc)

// 	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
// 		CertFile:      config.ServerCertFile,
// 		KeyFile:       config.ServerKeyFile,
// 		CAFile:        config.CAFile,
// 		ServerAddress: l.Addr().String(),
// 		Server:        true,
// 	})
// 	require.NoError(t, err)
// 	serverCreds := credentials.NewTLS(serverTLSConfig)

// 	dir, err := os.MkdirTemp("", "server-test")
// 	require.NoError(t, err)

// 	clog, err := log.NewLog(dir, log.Config{})
// 	require.NoError(t, err)

// 	cfg = &Config{
// 		CommitLog: clog,
// 	}
// 	if fn != nil {
// 		fn(cfg)
// 	}
// 	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
// 	require.NoError(t, err)

// 	go func() {
// 		server.Serve(l)
// 	}()

// 	return client, cfg, func() {
// 		server.Stop()
// 		cc.Close()
// 		l.Close()
// 	}
// }
