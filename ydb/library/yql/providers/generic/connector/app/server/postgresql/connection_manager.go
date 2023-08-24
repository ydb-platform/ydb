package postgresql

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/ydb-platform/ydb/library/go/core/log"
	api_common "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/api/common"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/rdbms"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
)

var _ rdbms.Connection = (*connection)(nil)

type rows struct {
	pgx.Rows
}

func (r rows) Close() error {
	r.Rows.Close()
	return nil
}

type connection struct {
	*pgx.Conn
}

func (c connection) Close() error {
	return c.Conn.Close(context.TODO())
}

func (c connection) Query(ctx context.Context, query string, args ...any) (rdbms.Rows, error) {
	out, err := c.Conn.Query(ctx, query, args...)
	return rows{Rows: out}, err
}

var _ rdbms.ConnectionManager = (*connectionManager)(nil)

type connectionManager struct {
	// TODO: cache of connections, remove unused connections with TTL
}

func (c *connectionManager) Make(
	ctx context.Context,
	_ log.Logger,
	dsi *api_common.TDataSourceInstance,
) (rdbms.Connection, error) {
	connStr := fmt.Sprintf("dbname=%s user=%s password=%s host=%s port=%d",
		dsi.Database,
		dsi.Credentials.GetBasic().Username,
		dsi.Credentials.GetBasic().Password,
		dsi.GetEndpoint().GetHost(),
		dsi.GetEndpoint().GetPort(),
	)

	if dsi.UseTls {
		connStr += " sslmode=verify-full"
	} else {
		connStr += " sslmode=disable"
	}

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("open connection: %w", err)
	}

	return &connection{Conn: conn}, nil
}

func (c *connectionManager) Release(logger log.Logger, conn rdbms.Connection) {
	utils.LogCloserError(logger, conn, "close connection to PostgreSQL")
}

func NewConnectionManager() rdbms.ConnectionManager {
	return &connectionManager{}
}
