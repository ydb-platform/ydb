package clickhouse

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ydb-platform/ydb/library/go/core/log"
	api_common "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/api/common"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
)

var _ utils.Connection = (*Connection)(nil)

type Connection struct {
	*sql.DB
	logger utils.QueryLogger
}

type rows struct {
	*sql.Rows
}

func (r rows) MakeAcceptors() ([]any, error) {
	columns, err := r.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("column types: %w", err)
	}

	typeNames := make([]string, 0, len(columns))
	for _, column := range columns {
		typeNames = append(typeNames, column.DatabaseTypeName())
	}

	return acceptorsFromSQLTypes(typeNames)
}

func (c Connection) Query(ctx context.Context, query string, args ...any) (utils.Rows, error) {
	c.logger.Dump(query, args...)

	out, err := c.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query context: %w", err)
	}

	if err := out.Err(); err != nil {
		defer func() {
			if err := out.Close(); err != nil {
				c.logger.Error("close rows", log.Error(err))
			}
		}()

		return nil, fmt.Errorf("rows err: %w", err)
	}

	return rows{Rows: out}, nil
}

var _ utils.ConnectionManager[*Connection] = (*connectionManager)(nil)

type connectionManager struct {
	utils.ConnectionManagerBase
	// TODO: cache of connections, remove unused connections with TTL
}

func (c *connectionManager) Make(
	ctx context.Context,
	logger log.Logger,
	dsi *api_common.TDataSourceInstance,
) (*Connection, error) {
	if dsi.GetCredentials().GetBasic() == nil {
		return nil, fmt.Errorf("currently only basic auth is supported")
	}

	var protocol clickhouse.Protocol

	switch dsi.Protocol {
	case api_common.EProtocol_NATIVE:
		protocol = clickhouse.Native
	case api_common.EProtocol_HTTP:
		protocol = clickhouse.HTTP
	default:
		return nil, fmt.Errorf("can not run ClickHouse connection with protocol '%v'", dsi.Protocol)
	}

	opts := &clickhouse.Options{
		Addr: []string{utils.EndpointToString(dsi.GetEndpoint())},
		Auth: clickhouse.Auth{
			Database: dsi.Database,
			Username: dsi.Credentials.GetBasic().Username,
			Password: dsi.Credentials.GetBasic().Password,
		},
		Debug: true,
		Debugf: func(format string, v ...any) {
			logger.Debugf(format, v...)
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		Protocol: protocol,
	}

	if dsi.UseTls {
		opts.TLS = &tls.Config{
			InsecureSkipVerify: false,
		}
	}

	conn := clickhouse.OpenDB(opts)
	if err := conn.Ping(); err != nil {
		return nil, fmt.Errorf("conn ping: %w", err)
	}

	const (
		maxIdleConns    = 5
		maxOpenConns    = 10
		connMaxLifetime = time.Hour
	)

	conn.SetMaxIdleConns(maxIdleConns)
	conn.SetMaxOpenConns(maxOpenConns)
	conn.SetConnMaxLifetime(connMaxLifetime)

	queryLogger := c.QueryLoggerFactory.Make(logger)

	return &Connection{DB: conn, logger: queryLogger}, nil
}

func (c *connectionManager) Release(logger log.Logger, conn *Connection) {
	utils.LogCloserError(logger, conn, "close clickhouse connection")
}

func NewConnectionManager(cfg utils.ConnectionManagerBase) utils.ConnectionManager[*Connection] {
	return &connectionManager{ConnectionManagerBase: cfg}
}
