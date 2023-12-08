package postgresql

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb/library/go/core/log"
	api_common "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/api/common"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
)

var _ utils.Connection = (*Connection)(nil)

type rows struct {
	pgx.Rows
}

func (r rows) Close() error {
	r.Rows.Close()

	return nil
}

func (r rows) MakeTransformer(ydbTypes []*Ydb.Type) (utils.Transformer, error) {
	fields := r.FieldDescriptions()

	oids := make([]uint32, 0, len(fields))
	for _, field := range fields {
		oids = append(oids, field.DataTypeOID)
	}

	return transformerFromOIDs(oids, ydbTypes)
}

type Connection struct {
	*pgx.Conn
	logger utils.QueryLogger
}

func (c Connection) Close() error {
	return c.Conn.Close(context.TODO())
}

func (c Connection) Query(ctx context.Context, query string, args ...any) (utils.Rows, error) {
	c.logger.Dump(query, args...)

	out, err := c.Conn.Query(ctx, query, args...)

	return rows{Rows: out}, err
}

var _ utils.ConnectionManager = (*connectionManager)(nil)

type connectionManager struct {
	utils.ConnectionManagerBase
	// TODO: cache of connections, remove unused connections with TTL
}

func (c *connectionManager) Make(
	ctx context.Context,
	logger log.Logger,
	dsi *api_common.TDataSourceInstance,
) (utils.Connection, error) {
	if dsi.GetCredentials().GetBasic() == nil {
		return nil, fmt.Errorf("currently only basic auth is supported")
	}

	if dsi.Protocol != api_common.EProtocol_NATIVE {
		return nil, fmt.Errorf("can not create PostgreSQL connection with protocol '%v'", dsi.Protocol)
	}

	if socketType, _ := pgconn.NetworkAddress(dsi.GetEndpoint().GetHost(), uint16(dsi.GetEndpoint().GetPort())); socketType != "tcp" {
		return nil, fmt.Errorf("can not create PostgreSQL connection with socket type '%s'", socketType)
	}

	connStr := "dbname=DBNAME user=USER password=PASSWORD host=HOST port=5432"
	if dsi.UseTls {
		connStr += " sslmode=verify-full"
	} else {
		connStr += " sslmode=disable"
	}

	connCfg, err := pgx.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("parse connection config template: %w", err)
	}

	connCfg.Database = dsi.Database
	connCfg.Host = dsi.GetEndpoint().GetHost()
	connCfg.Port = uint16(dsi.GetEndpoint().GetPort())
	connCfg.User = dsi.Credentials.GetBasic().GetUsername()
	connCfg.Password = dsi.Credentials.GetBasic().GetPassword()

	if dsi.UseTls {
		connCfg.TLSConfig.ServerName = dsi.GetEndpoint().GetHost()
	}

	conn, err := pgx.ConnectConfig(ctx, connCfg)
	if err != nil {
		return nil, fmt.Errorf("open connection: %w", err)
	}

	// set schema (public by default)
	if _, err = conn.Exec(ctx, fmt.Sprintf("set search_path=%s", NewSQLFormatter().SanitiseIdentifier(dsi.GetPgOptions().GetSchema()))); err != nil {
		return nil, fmt.Errorf("exec: %w", err)
	}

	queryLogger := c.QueryLoggerFactory.Make(logger)

	return &Connection{conn, queryLogger}, nil
}

func (c *connectionManager) Release(logger log.Logger, conn utils.Connection) {
	utils.LogCloserError(logger, conn, "close posgresql connection")
}

func NewConnectionManager(cfg utils.ConnectionManagerBase) utils.ConnectionManager {
	return &connectionManager{ConnectionManagerBase: cfg}
}
