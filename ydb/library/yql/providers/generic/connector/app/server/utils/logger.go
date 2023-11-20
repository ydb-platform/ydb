package utils

import (
	"fmt"
	"io"
	"testing"

	"github.com/ydb-platform/ydb/library/go/core/log"
	"github.com/ydb-platform/ydb/library/go/core/log/zap"
	api_common "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/api/common"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/config"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
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

func NewLoggerFromConfig(cfg *config.TLoggerConfig) (log.Logger, error) {
	if cfg == nil {
		return NewDefaultLogger()
	}

	loggerCfg := zap.NewDeployConfig()
	loggerCfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	loggerCfg.Encoding = "console"
	loggerCfg.Level.SetLevel(convertToZapLogLevel(cfg.GetLogLevel()))

	zapLogger, err := loggerCfg.Build()
	if err != nil {
		return nil, fmt.Errorf("new logger: %w", err)
	}

	return &zap.Logger{L: zapLogger}, nil
}

func NewDefaultLogger() (log.Logger, error) {
	return NewLoggerFromConfig(&config.TLoggerConfig{LogLevel: config.ELogLevel_TRACE})
}

func NewTestLogger(t *testing.T) log.Logger { return &zap.Logger{L: zaptest.NewLogger(t)} }

func DumpReadSplitsResponse(logger log.Logger, resp *api_service_protos.TReadSplitsResponse) {
	switch t := resp.GetPayload().(type) {
	case *api_service_protos.TReadSplitsResponse_ArrowIpcStreaming:
		if dump := resp.GetArrowIpcStreaming(); dump != nil {
			logger.Debug("response", log.Int("arrow_blob_length", len(dump)))
		}
	case *api_service_protos.TReadSplitsResponse_ColumnSet:
		for i := range t.ColumnSet.Data {
			data := t.ColumnSet.Data[i]
			meta := t.ColumnSet.Meta[i]

			logger.Debug("response", log.Int("column_id", i), log.String("meta", meta.String()), log.String("data", data.String()))
		}
	default:
		panic(fmt.Sprintf("unexpected message type %v", t))
	}
}

func SelectToFields(slct *api_service_protos.TSelect) []log.Field {
	result := []log.Field{
		log.Any("from", slct.From),
		log.Any("what", slct.What),
		log.Any("where", slct.Where),
	}

	return result
}

type QueryLoggerFactory struct {
	enableQueryLogging bool
}

func NewQueryLoggerFactory(cfg *config.TLoggerConfig) QueryLoggerFactory {
	enabled := cfg.GetEnableSqlQueryLogging()

	return QueryLoggerFactory{enableQueryLogging: enabled}
}

func (f *QueryLoggerFactory) Make(logger log.Logger) QueryLogger {
	return QueryLogger{Logger: logger, enabled: f.enableQueryLogging}
}

type QueryLogger struct {
	log.Logger
	enabled bool
}

func (ql *QueryLogger) Dump(query string, args ...any) {
	if !ql.enabled {
		return
	}

	logFields := []log.Field{log.String("query", query)}
	if len(args) > 0 {
		logFields = append(logFields, log.Any("args", args))
	}

	ql.Debug("execute SQL query", logFields...)
}

func convertToZapLogLevel(lvl config.ELogLevel) zapcore.Level {
	switch lvl {
	case config.ELogLevel_TRACE, config.ELogLevel_DEBUG:
		return zapcore.DebugLevel
	case config.ELogLevel_INFO:
		return zapcore.InfoLevel
	case config.ELogLevel_WARN:
		return zapcore.WarnLevel
	case config.ELogLevel_ERROR:
		return zapcore.ErrorLevel
	case config.ELogLevel_FATAL:
		return zapcore.FatalLevel
	}

	return zapcore.InvalidLevel
}
