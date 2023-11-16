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
	"errors"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"io"
	"io/ioutil"
	"regexp"
	"strings"
)

// \x60 represents a backtick
var httpInsertRe = regexp.MustCompile(`(?i)^INSERT INTO\s+\x60?([\w.^\(]+)\x60?\s*(\([^\)]*\))?`)

// release is ignored, because http used by std with empty release function
func (h *httpConnect) prepareBatch(ctx context.Context, query string, release func(*connect, error)) (driver.Batch, error) {
	matches := httpInsertRe.FindStringSubmatch(query)
	if len(matches) < 3 {
		return nil, errors.New("cannot get table name from query")
	}
	tableName := matches[1]
	var rColumns []string
	if matches[2] != "" {
		colMatch := strings.TrimSuffix(strings.TrimPrefix(matches[2], "("), ")")
		rColumns = strings.Split(colMatch, ",")
		for i := range rColumns {
			rColumns[i] = strings.TrimSpace(rColumns[i])
		}
	}
	query = "INSERT INTO " + tableName + " FORMAT Native"
	queryTableSchema := "DESCRIBE TABLE " + tableName
	r, err := h.query(ctx, release, queryTableSchema)
	if err != nil {
		return nil, err
	}

	block := &proto.Block{}

	// get Table columns and types
	columns := make(map[string]string)
	var colNames []string
	for r.Next() {
		var (
			colName string
			colType string
			ignore  string
		)

		if err = r.Scan(&colName, &colType, &ignore, &ignore, &ignore, &ignore, &ignore); err != nil {
			return nil, err
		}
		colNames = append(colNames, colName)
		columns[colName] = colType
	}

	switch len(rColumns) {
	case 0:
		for _, colName := range colNames {
			if err = block.AddColumn(colName, column.Type(columns[colName])); err != nil {
				return nil, err
			}
		}
	default:
		// user has requested specific columns so only include these
		for _, colName := range rColumns {
			if colType, ok := columns[colName]; ok {
				if err = block.AddColumn(colName, column.Type(colType)); err != nil {
					return nil, err
				}
			} else {
				return nil, fmt.Errorf("column %s is not present in the table %s", colName, tableName)
			}
		}
	}

	return &httpBatch{
		ctx:       ctx,
		conn:      h,
		structMap: &structMap{},
		block:     block,
		query:     query,
	}, nil
}

type httpBatch struct {
	query     string
	err       error
	ctx       context.Context
	conn      *httpConnect
	structMap *structMap
	sent      bool
	block     *proto.Block
}

// Flush TODO: noop on http currently - requires streaming to be implemented
func (b *httpBatch) Flush() error {
	return nil
}

func (b *httpBatch) Abort() error {
	defer func() {
		b.sent = true
	}()
	if b.sent {
		return ErrBatchAlreadySent
	}
	return nil
}

func (b *httpBatch) Append(v ...interface{}) error {
	if b.sent {
		return ErrBatchAlreadySent
	}
	if err := b.block.Append(v...); err != nil {
		return err
	}
	return nil
}

func (b *httpBatch) AppendStruct(v interface{}) error {
	values, err := b.structMap.Map("AppendStruct", b.block.ColumnsNames(), v, false)
	if err != nil {
		return err
	}
	return b.Append(values...)
}

func (b *httpBatch) Column(idx int) driver.BatchColumn {
	if len(b.block.Columns) <= idx {
		return &batchColumn{
			err: &OpError{
				Op:  "batch.Column",
				Err: fmt.Errorf("invalid column index %d", idx),
			},
		}
	}
	return &batchColumn{
		batch:  b,
		column: b.block.Columns[idx],
		release: func(err error) {
			b.err = err
		},
	}
}

func (b *httpBatch) IsSent() bool {
	return b.sent
}

func (b *httpBatch) Send() (err error) {
	defer func() {
		b.sent = true
	}()
	if b.sent {
		return ErrBatchAlreadySent
	}
	if b.err != nil {
		return b.err
	}
	options := queryOptions(b.ctx)

	headers := make(map[string]string)

	r, pw := io.Pipe()
	crw := b.conn.compressionPool.Get()
	w := crw.reset(pw)

	defer b.conn.compressionPool.Put(crw)

	switch b.conn.compression {
	case CompressionGZIP, CompressionDeflate, CompressionBrotli:
		headers["Content-Encoding"] = b.conn.compression.String()
	case CompressionZSTD, CompressionLZ4:
		options.settings["decompress"] = "1"
	}

	go func() {
		var err error = nil
		defer pw.CloseWithError(err)
		defer w.Close()
		b.conn.buffer.Reset()
		if b.block.Rows() != 0 {
			if err = b.conn.writeData(b.block); err != nil {
				return
			}
		}
		if err = b.conn.writeData(&proto.Block{}); err != nil {
			return
		}
		if _, err = w.Write(b.conn.buffer.Buf); err != nil {
			return
		}
	}()

	options.settings["query"] = b.query
	headers["Content-Type"] = "application/octet-stream"
	for k, v := range b.conn.headers {
		headers[k] = v
	}
	res, err := b.conn.sendQuery(b.ctx, r, &options, headers)

	if res != nil {
		defer res.Body.Close()
		// we don't care about result, so just discard it to reuse connection
		_, _ = io.Copy(ioutil.Discard, res.Body)
	}

	return err
}

var _ driver.Batch = (*httpBatch)(nil)
