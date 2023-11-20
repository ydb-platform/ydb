// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package clickhouse

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync/atomic"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	ldriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

var globalConnID int64

type stdConnOpener struct {
	err error
	opt *Options
}

func (o *stdConnOpener) Driver() driver.Driver {
	return &stdDriver{}
}

func (o *stdConnOpener) Connect(ctx context.Context) (_ driver.Conn, err error) {
	if o.err != nil {
		return nil, o.err
	}
	var (
		conn     stdConnect
		connID   = int(atomic.AddInt64(&globalConnID, 1))
		dialFunc func(ctx context.Context, addr string, num int, opt *Options) (stdConnect, error)
	)

	switch o.opt.Protocol {
	case HTTP:
		dialFunc = func(ctx context.Context, addr string, num int, opt *Options) (stdConnect, error) {
			return dialHttp(ctx, addr, num, opt)
		}
	default:
		dialFunc = func(ctx context.Context, addr string, num int, opt *Options) (stdConnect, error) {
			return dial(ctx, addr, num, opt)
		}
	}

	if o.opt.Addr == nil || len(o.opt.Addr) == 0 {
		return nil, ErrAcquireConnNoAddress
	}

	for i := range o.opt.Addr {
		var num int
		switch o.opt.ConnOpenStrategy {
		case ConnOpenInOrder:
			num = i
		case ConnOpenRoundRobin:
			num = (int(connID) + i) % len(o.opt.Addr)
		}
		if conn, err = dialFunc(ctx, o.opt.Addr[num], connID, o.opt); err == nil {
			return &stdDriver{
				conn: conn,
			}, nil
		}
	}
	return nil, err
}

func init() {
	sql.Register("clickhouse", &stdDriver{})
}

func Connector(opt *Options) driver.Connector {
	if opt == nil {
		opt = &Options{}
	}

	o := opt.setDefaults()
	return &stdConnOpener{
		opt: o,
	}
}

func OpenDB(opt *Options) *sql.DB {
	if opt == nil {
		opt = &Options{}
	}
	var settings []string
	if opt.MaxIdleConns > 0 {
		settings = append(settings, "SetMaxIdleConns")
	}
	if opt.MaxOpenConns > 0 {
		settings = append(settings, "SetMaxOpenConns")
	}
	if opt.ConnMaxLifetime > 0 {
		settings = append(settings, "SetConnMaxLifetime")
	}
	if len(settings) != 0 {
		return sql.OpenDB(&stdConnOpener{
			err: fmt.Errorf("cannot connect. invalid settings. use %s (see https://pkg.go.dev/database/sql)", strings.Join(settings, ",")),
		})
	}
	o := opt.setDefaults()
	return sql.OpenDB(&stdConnOpener{
		opt: o,
	})
}

type stdConnect interface {
	isBad() bool
	close() error
	query(ctx context.Context, release func(*connect, error), query string, args ...interface{}) (*rows, error)
	exec(ctx context.Context, query string, args ...interface{}) error
	ping(ctx context.Context) (err error)
	prepareBatch(ctx context.Context, query string, release func(*connect, error)) (ldriver.Batch, error)
	asyncInsert(ctx context.Context, query string, wait bool) error
}

type stdDriver struct {
	conn   stdConnect
	commit func() error
}

func (std *stdDriver) Open(dsn string) (_ driver.Conn, err error) {
	var opt Options
	if err := opt.fromDSN(dsn); err != nil {
		return nil, err
	}
	o := opt.setDefaults()
	return (&stdConnOpener{opt: o}).Connect(context.Background())
}

func (std *stdDriver) ResetSession(ctx context.Context) error {
	if std.conn.isBad() {
		return driver.ErrBadConn
	}
	return nil
}

func (std *stdDriver) Ping(ctx context.Context) error { return std.conn.ping(ctx) }

func (std *stdDriver) Begin() (driver.Tx, error) { return std, nil }

func (std *stdDriver) Commit() error {
	if std.commit == nil {
		return nil
	}
	defer func() {
		std.commit = nil
	}()
	return std.commit()
}

func (std *stdDriver) Rollback() error {
	std.commit = nil
	std.conn.close()
	return nil
}

func (std *stdDriver) CheckNamedValue(nv *driver.NamedValue) error { return nil }

func (std *stdDriver) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if options := queryOptions(ctx); options.async.ok {
		if len(args) != 0 {
			return nil, errors.New("clickhouse: you can't use parameters in an asynchronous insert")
		}
		return driver.RowsAffected(0), std.conn.asyncInsert(ctx, query, options.async.wait)
	}
	if err := std.conn.exec(ctx, query, rebind(args)...); err != nil {
		return nil, err
	}
	return driver.RowsAffected(0), nil
}

func (std *stdDriver) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	r, err := std.conn.query(ctx, func(*connect, error) {}, query, rebind(args)...)
	if err != nil {
		return nil, err
	}
	return &stdRows{
		rows: r,
	}, nil
}

func (std *stdDriver) Prepare(query string) (driver.Stmt, error) {
	return std.PrepareContext(context.Background(), query)
}

func (std *stdDriver) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	batch, err := std.conn.prepareBatch(ctx, query, func(*connect, error) {})
	if err != nil {
		return nil, err
	}
	std.commit = batch.Send
	return &stdBatch{
		batch: batch,
	}, nil
}

func (std *stdDriver) Close() error { return std.conn.close() }

type stdBatch struct {
	batch ldriver.Batch
}

func (s *stdBatch) NumInput() int { return -1 }
func (s *stdBatch) Exec(args []driver.Value) (driver.Result, error) {
	values := make([]interface{}, 0, len(args))
	for _, v := range args {
		values = append(values, v)
	}
	if err := s.batch.Append(values...); err != nil {
		return nil, err
	}
	return driver.RowsAffected(0), nil
}

func (s *stdBatch) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	values := make([]driver.Value, 0, len(args))
	for _, v := range args {
		values = append(values, v.Value)
	}
	return s.Exec(values)
}

func (s *stdBatch) Query(args []driver.Value) (driver.Rows, error) {
	return nil, errors.New("only Exec method supported in batch mode")
}

func (s *stdBatch) Close() error { return nil }

type stdRows struct {
	rows *rows
}

func (r *stdRows) Columns() []string {
	return r.rows.Columns()
}

func (r *stdRows) ColumnTypeScanType(idx int) reflect.Type {
	return r.rows.block.Columns[idx].ScanType()
}

func (r *stdRows) ColumnTypeDatabaseTypeName(idx int) string {
	return string(r.rows.block.Columns[idx].Type())
}

func (r *stdRows) ColumnTypeNullable(idx int) (nullable, ok bool) {
	_, ok = r.rows.block.Columns[idx].(*column.Nullable)
	return ok, true
}

func (r *stdRows) ColumnTypePrecisionScale(idx int) (precision, scale int64, ok bool) {
	switch col := r.rows.block.Columns[idx].(type) {
	case *column.Decimal:
		return col.Precision(), col.Scale(), true
	case interface{ Base() column.Interface }:
		switch col := col.Base().(type) {
		case *column.Decimal:
			return col.Precision(), col.Scale(), true
		}
	}
	return 0, 0, false
}

func (r *stdRows) Next(dest []driver.Value) error {
	if len(r.rows.block.Columns) != len(dest) {
		return &OpError{
			Op:  "Next",
			Err: fmt.Errorf("expected %d destination arguments in Next, not %d", len(r.rows.block.Columns), len(dest)),
		}
	}
	if r.rows.Next() {
		for i := range dest {
			nullable, ok := r.ColumnTypeNullable(i)
			switch value := r.rows.block.Columns[i].Row(r.rows.row-1, nullable && ok).(type) {
			case driver.Valuer:
				v, err := value.Value()
				if err != nil {
					return err
				}
				dest[i] = v
			default:
				dest[i] = value
			}
		}
		return nil
	}
	if err := r.rows.Err(); err != nil {
		return err
	}
	return io.EOF
}

func (r *stdRows) HasNextResultSet() bool {
	return r.rows.totals != nil
}

func (r *stdRows) NextResultSet() error {
	switch {
	case r.rows.totals != nil:
		r.rows.block = r.rows.totals
		r.rows.totals = nil
	default:
		return io.EOF
	}
	return nil
}

func (r *stdRows) Close() error {
	return r.rows.Close()
}
