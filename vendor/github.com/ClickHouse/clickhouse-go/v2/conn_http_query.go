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
	"bytes"
	"context"
	"errors"
	"io"
	"strings"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

// release is ignored, because http used by std with empty release function
func (h *httpConnect) query(ctx context.Context, release func(*connect, error), query string, args ...interface{}) (*rows, error) {
	query, err := bind(h.location, query, args...)
	if err != nil {
		return nil, err
	}
	options := queryOptions(ctx)
	headers := make(map[string]string)
	switch h.compression {
	case CompressionZSTD, CompressionLZ4:
		options.settings["compress"] = "1"
	case CompressionGZIP, CompressionDeflate, CompressionBrotli:
		// request encoding
		headers["Accept-Encoding"] = h.compression.String()
	}

	for k, v := range h.headers {
		headers[k] = v
	}

	res, err := h.sendQuery(ctx, strings.NewReader(query), &options, headers)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	// detect compression from http Content-Encoding header - note user will need to have set enable_http_compression
	// for CH to respond with compressed data - we don't set this automatically as they might not have permissions
	var body []byte
	//adding Accept-Encoding:gzip on your request means response won’t be automatically decompressed per https://github.com/golang/go/blob/master/src/net/http/transport.go#L182-L190

	rw := h.compressionPool.Get()
	body, err = rw.read(res)
	bufferSize := h.blockBufferSize
	if options.blockBufferSize > 0 {
		// allow block buffer sze to be overridden per query
		bufferSize = options.blockBufferSize
	}
	var (
		errCh  = make(chan error)
		stream = make(chan *proto.Block, bufferSize)
	)

	if len(body) == 0 {
		// queries with no results can get an empty body
		go func() {
			close(stream)
			close(errCh)
		}()
		return &rows{
			err:       nil,
			stream:    stream,
			errors:    errCh,
			block:     &proto.Block{},
			columns:   []string{},
			structMap: &structMap{},
		}, nil
	}
	if err != nil {
		return nil, err
	}
	h.compressionPool.Put(rw)
	reader := chproto.NewReader(bytes.NewReader(body))
	block, err := h.readData(reader)
	if err != nil {
		return nil, err
	}

	go func() {
		for {
			block, err := h.readData(reader)
			if err != nil {
				// ch-go wraps EOF errors
				if !errors.Is(err, io.EOF) {
					errCh <- err
				}
				break
			}
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				break
			case stream <- block:
			}
		}
		close(stream)
		close(errCh)
	}()

	return &rows{
		block:     block,
		stream:    stream,
		errors:    errCh,
		columns:   block.ColumnsNames(),
		structMap: &structMap{},
	}, nil
}
