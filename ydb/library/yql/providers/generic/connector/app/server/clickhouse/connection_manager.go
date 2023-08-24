package clickhouse

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"net"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ydb-platform/ydb/library/go/core/log"
	api_common "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/api/common"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/rdbms"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
)

var _ rdbms.Connection = (*connection)(nil)

type connection struct {
	*sql.DB
}

func (c connection) Query(ctx context.Context, query string, args ...any) (rdbms.Rows, error) {
	return c.DB.QueryContext(ctx, query, args...)
}

var _ rdbms.ConnectionManager = (*connectionManager)(nil)

type connectionManager struct {
	// TODO: cache of connections, remove unused connections with TTL
}

func (c *connectionManager) Make(
	ctx context.Context,
	logger log.Logger,
	dsi *api_common.TDataSourceInstance,
) (rdbms.Connection, error) {
	if dsi.GetCredentials().GetBasic() == nil {
		return nil, fmt.Errorf("currently only basic auth is supported")
	}

	opts := &clickhouse.Options{
		Addr: []string{utils.EndpointToString(dsi.GetEndpoint())},
		Auth: clickhouse.Auth{
			Database: dsi.Database,
			Username: dsi.Credentials.GetBasic().Username,
			Password: dsi.Credentials.GetBasic().Password,
		},
		DialContext: func(ctx context.Context, addr string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", addr)
		},
		Debug: true,
		Debugf: func(format string, v ...any) {
			logger.Debugf(format, v...)
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		Protocol: clickhouse.HTTP,
	}

	if dsi.UseTls {
		opts.TLS = &tls.Config{
			InsecureSkipVerify: false,
		}
	}

	// FIXME: uncomment after YQ-2286
	// conn, err := clickhouse.Open(opts)
	// if err != nil {
	// 	return nil, fmt.Errorf("open connection: %w", err)
	// }

	conn := clickhouse.OpenDB(opts)

	if err := conn.Ping(); err != nil {
		return nil, fmt.Errorf("conn ping: %w", err)
	}

	const (
		maxIdleConns = 5
		maxOpenConns = 10
	)

	conn.SetMaxIdleConns(maxIdleConns)
	conn.SetMaxOpenConns(maxOpenConns)
	conn.SetConnMaxLifetime(time.Hour)

	return &connection{DB: conn}, nil
}

func (c *connectionManager) Release(logger log.Logger, conn rdbms.Connection) {
	utils.LogCloserError(logger, conn, "close clickhouse connection")
}

func NewConnectionManager() rdbms.ConnectionManager {
	return &connectionManager{}
}
