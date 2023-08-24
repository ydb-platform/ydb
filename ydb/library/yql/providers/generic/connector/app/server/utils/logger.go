package utils

import (
	"fmt"
	"io"

	"github.com/ydb-platform/ydb/library/go/core/log"
	"github.com/ydb-platform/ydb/library/go/core/log/zap"
	api_common "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/api/common"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
	"go.uber.org/zap/zapcore"
)

// TODO: it's better to do this in GRPC middleware
func AnnotateLogger(logger log.Logger, method string, dsi *api_common.TDataSourceInstance) log.Logger {
	logger = log.With(logger, log.String("method", method))

	if dsi != nil {
		logger = log.With(logger,
			log.String("data_source_kind", api_common.EDataSourceKind_name[int32(dsi.Kind)]),
			log.String("host", dsi.Endpoint.Host),
			log.UInt32("port", dsi.Endpoint.Port),
			log.String("database", dsi.Database),
			log.Bool("use_tls", dsi.UseTls),
			// TODO: can we print just a login without a password?
		)
	}

	return logger
}

func LogCloserError(logger log.Logger, closer io.Closer, msg string) {
	if err := closer.Close(); err != nil {
		logger.Error(msg, log.Error(err))
	}
}

func NewDevelopmentLogger() (log.Logger, error) {
	cfg := zap.NewDeployConfig()
	cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	cfg.Encoding = "console"

	zapLogger, err := cfg.Build()
	if err != nil {
		return nil, fmt.Errorf("new logger: %w", err)
	}

	return &zap.Logger{L: zapLogger}, nil
}

func DumpReadSplitsResponse(logger log.Logger, resp *api_service_protos.TReadSplitsResponse) {
	if columnSet := resp.GetColumnSet(); columnSet != nil {
		for i := range columnSet.Data {
			data := columnSet.Data[i]
			meta := columnSet.Meta[i]

			logger.Debug("response", log.Int("column_id", i), log.String("meta", meta.String()), log.String("data", data.String()))
		}

		return
	}

	if dump := resp.GetArrowIpcStreaming(); dump != nil {
		logger.Debug("response", log.Int("arrow_blob_length", len(dump)))
	}
}
