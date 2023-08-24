package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/apache/arrow/go/v13/arrow/ipc"
	"github.com/spf13/cobra"
	"github.com/ydb-platform/ydb/library/go/core/log"
	api_common "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/api/common"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/config"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	api_service "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/prototext"
)

const (
	tableName    = "primitives"
	outputFormat = api_service_protos.TReadSplitsRequest_ARROW_IPC_STREAMING
)

func newConfigFromPath(configPath string) (*config.ClientConfig, error) {
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("read file %v: %w", configPath, err)
	}

	var cfg config.ClientConfig

	if err := prototext.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("prototext unmarshal `%v`: %w", string(data), err)
	}

	return &cfg, nil
}

func runClient(_ *cobra.Command, args []string) error {
	logger, err := utils.NewDevelopmentLogger()
	if err != nil {
		return fmt.Errorf("zap new: %w", err)
	}

	configPath := args[0]

	cfg, err := newConfigFromPath(configPath)
	if err != nil {
		return fmt.Errorf("unknown instance: %w", err)
	}

	if err := callServer(logger, cfg); err != nil {
		return fmt.Errorf("call server: %w", err)
	}

	return nil
}

func makeConnection(logger log.Logger, cfg *config.ClientConfig) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption

	if cfg.Tls != nil {
		logger.Info("client will use TLS connections")

		caCrt, err := os.ReadFile(cfg.Tls.Ca)
		if err != nil {
			return nil, err
		}

		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(caCrt) {
			return nil, fmt.Errorf("failed to add server CA's certificate")
		}

		tlsCfg := &tls.Config{
			RootCAs: certPool,
		}

		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	} else {
		logger.Info("client will use insecure connections")

		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.Dial(utils.EndpointToString(cfg.Endpoint), opts...)
	if err != nil {
		return nil, fmt.Errorf("grpc dial: %w", err)
	}

	return conn, nil
}

func callServer(logger log.Logger, cfg *config.ClientConfig) error {
	conn, err := makeConnection(logger, cfg)
	if err != nil {
		return fmt.Errorf("grpc dial: %w", err)
	}

	defer utils.LogCloserError(logger, conn, "connection close")

	connectorClient := api_service.NewConnectorClient(conn)

	// DescribeTable
	schema, err := describeTable(logger, connectorClient, cfg.DataSourceInstance)
	if err != nil {
		return fmt.Errorf("describe table: %w", err)
	}

	// ListSplits - we want to SELECT *
	splits, err := listSplits(logger, schema, connectorClient, cfg.DataSourceInstance)
	if err != nil {
		return fmt.Errorf("list splits: %w", err)
	}

	// ReadSplits
	if err := readSplits(logger, splits, outputFormat, connectorClient, cfg.DataSourceInstance); err != nil {
		return fmt.Errorf("read splits: %w", err)
	}

	return nil
}

func describeTable(
	logger log.Logger,
	connectorClient api_service.ConnectorClient,
	dsi *api_common.TDataSourceInstance,
) (*api_service_protos.TSchema, error) {
	req := &api_service_protos.TDescribeTableRequest{Table: tableName, DataSourceInstance: dsi}
	logger.Debug("DescribeTable", log.String("request", req.String()))

	resp, err := connectorClient.DescribeTable(context.TODO(), req)
	if err != nil {
		return nil, fmt.Errorf("describe table: %w", err)
	}

	if utils.IsSuccess(resp.Error) {
		logger.Debug("DescribeTable", log.String("response", resp.String()))
		return resp.Schema, nil
	}
	logger.Error("DescribeTable", log.String("response", resp.String()))

	return nil, utils.NewSTDErrorFromAPIError(resp.Error)
}

func listSplits(
	logger log.Logger,
	schema *api_service_protos.TSchema,
	connectorClient api_service.ConnectorClient,
	dsi *api_common.TDataSourceInstance,
) ([]*api_service_protos.TSplit, error) {
	items := []*api_service_protos.TSelect_TWhat_TItem{}

	for _, column := range schema.Columns {
		items = append(items, &api_service_protos.TSelect_TWhat_TItem{
			Payload: &api_service_protos.TSelect_TWhat_TItem_Column{Column: column},
		})
	}

	req := &api_service_protos.TListSplitsRequest{
		Selects: []*api_service_protos.TSelect{
			{
				DataSourceInstance: nil,
				What:               &api_service_protos.TSelect_TWhat{Items: items},
				From:               &api_service_protos.TSelect_TFrom{Table: tableName},
			},
		},
		DataSourceInstance: dsi,
	}
	logger.Debug("ListSplits", log.String("request", req.String()))

	streamListSplits, err := connectorClient.ListSplits(context.TODO(), req)
	if err != nil {
		return nil, fmt.Errorf("list splits: %w", err)
	}

	var splits []*api_service_protos.TSplit

	for {
		resp, err := streamListSplits.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}

			return nil, fmt.Errorf("stream list splits: %w", err)
		}

		if !utils.IsSuccess(resp.Error) {
			logger.Error("ListSplits", log.String("response", resp.String()))
			return splits, utils.NewSTDErrorFromAPIError(resp.Error)
		}

		logger.Debug("ListSplits", log.String("response", resp.String()))
		splits = append(splits, resp.Splits...)
	}

	if len(splits) != 1 {
		return nil, fmt.Errorf("too many splits")
	}

	return splits, nil
}

func readSplits(
	logger log.Logger,
	splits []*api_service_protos.TSplit,
	format api_service_protos.TReadSplitsRequest_EFormat,
	connectorClient api_service.ConnectorClient,
	dsi *api_common.TDataSourceInstance,
) error {
	req := &api_service_protos.TReadSplitsRequest{Splits: splits, Format: format, DataSourceInstance: dsi}
	logger.Debug("ReadSplits", log.String("request", req.String()))

	streamReadSplits, err := connectorClient.ReadSplits(context.Background(), req)
	if err != nil {
		return fmt.Errorf("list splits: %w", err)
	}

	var responses []*api_service_protos.TReadSplitsResponse

	for {
		resp, err := streamReadSplits.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}

			return fmt.Errorf("stream list splits: %w", err)
		}

		if !utils.IsSuccess(resp.Error) {
			return utils.NewSTDErrorFromAPIError(resp.Error)
		}

		responses = append(responses, resp)
	}

	if err := dumpReadResponses(logger, format, responses); err != nil {
		return fmt.Errorf("dump read responses: %w", err)
	}

	return nil
}

func dumpReadResponses(
	logger log.Logger,
	format api_service_protos.TReadSplitsRequest_EFormat,
	responses []*api_service_protos.TReadSplitsResponse,
) error {
	if format == api_service_protos.TReadSplitsRequest_ARROW_IPC_STREAMING {
		for _, resp := range responses {
			buf := bytes.NewBuffer(resp.GetArrowIpcStreaming())

			reader, err := ipc.NewReader(buf)
			if err != nil {
				return fmt.Errorf("new reader: %w", err)
			}

			for reader.Next() {
				record := reader.Record()
				logger.Debug("schema", log.String("schema", record.Schema().String()))

				for i, column := range record.Columns() {
					logger.Debug("column", log.Int("id", i), log.String("data", column.String()))
				}
			}

			reader.Release()
		}
	}

	return fmt.Errorf("unknown format: %v", format)
}

var Cmd = &cobra.Command{
	Use:   "client",
	Short: "client for Connector testing and debugging purposes",
	Run: func(cmd *cobra.Command, args []string) {
		if err := runClient(cmd, args); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}
