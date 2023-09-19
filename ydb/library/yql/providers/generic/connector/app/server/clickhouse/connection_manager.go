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
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
)

var _ utils.Connection = (*Connection)(nil)

type Connection struct {
	*sql.DB
}

func (c Connection) Query(ctx context.Context, query string, args ...any) (utils.Rows, error) {
	return c.DB.QueryContext(ctx, query, args...)
}

var _ utils.ConnectionManager[*Connection] = (*connectionManager)(nil)

type connectionManager struct {
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

	if dsi.Protocol != api_common.EProtocol_HTTP {
		// FIXME: fix NATIVE protocol in https://st.yandex-team.ru/YQ-2286
		return nil, fmt.Errorf("can not run ClickHouse connection with protocol '%v'", dsi.Protocol)
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

	return &Connection{DB: conn}, nil
}

func (c *connectionManager) Release(logger log.Logger, conn *Connection) {
	utils.LogCloserError(logger, conn, "close clickhouse connection")
}

func NewConnectionManager() utils.ConnectionManager[*Connection] {
	return &connectionManager{}
}
