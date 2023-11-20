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
	"crypto/tls"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/resources"
	"github.com/pkg/errors"
	"log"
	"net"
	"os"
	"time"

	"github.com/ClickHouse/ch-go/compress"
	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

func dial(ctx context.Context, addr string, num int, opt *Options) (*connect, error) {
	var (
		err    error
		conn   net.Conn
		debugf = func(format string, v ...interface{}) {}
	)
	switch {
	case opt.DialContext != nil:
		conn, err = opt.DialContext(ctx, addr)
	default:
		switch {
		case opt.TLS != nil:
			conn, err = tls.DialWithDialer(&net.Dialer{Timeout: opt.DialTimeout}, "tcp", addr, opt.TLS)
		default:
			conn, err = net.DialTimeout("tcp", addr, opt.DialTimeout)
		}
	}
	if err != nil {
		return nil, err
	}
	if opt.Debug {
		if opt.Debugf != nil {
			debugf = opt.Debugf
		} else {
			debugf = log.New(os.Stdout, fmt.Sprintf("[clickhouse][conn=%d][%s]", num, conn.RemoteAddr()), 0).Printf
		}
	}
	compression := CompressionNone
	if opt.Compression != nil {
		switch opt.Compression.Method {
		case CompressionLZ4, CompressionZSTD, CompressionNone:
			compression = opt.Compression.Method
		default:
			return nil, fmt.Errorf("unsupported compression method for native protocol")
		}
	}

	var (
		connect = &connect{
			opt:             opt,
			conn:            conn,
			debugf:          debugf,
			buffer:          new(chproto.Buffer),
			reader:          chproto.NewReader(conn),
			revision:        proto.ClientTCPProtocolVersion,
			structMap:       &structMap{},
			compression:     compression,
			connectedAt:     time.Now(),
			compressor:      compress.NewWriter(),
			readTimeout:     opt.ReadTimeout,
			blockBufferSize: opt.BlockBufferSize,
		}
	)
	if err := connect.handshake(opt.Auth.Database, opt.Auth.Username, opt.Auth.Password); err != nil {
		return nil, err
	}

	// warn only on the first connection in the pool
	if num == 1 && !resources.ClientMeta.IsSupportedClickHouseVersion(connect.server.Version) {
		// send to debugger and console
		fmt.Printf("WARNING: version %v of ClickHouse is not supported by this client\n", connect.server.Version)
		debugf("[handshake] WARNING: version %v of ClickHouse is not supported by this client - client supports %v", connect.server.Version, resources.ClientMeta.SupportedVersions())
	}
	return connect, nil
}

// https://github.com/ClickHouse/ClickHouse/blob/master/src/Client/Connection.cpp
type connect struct {
	opt             *Options
	conn            net.Conn
	debugf          func(format string, v ...interface{})
	server          ServerVersion
	closed          bool
	buffer          *chproto.Buffer
	reader          *chproto.Reader
	released        bool
	revision        uint64
	structMap       *structMap
	compression     CompressionMethod
	connectedAt     time.Time
	compressor      *compress.Writer
	readTimeout     time.Duration
	blockBufferSize uint8
}

func (c *connect) settings(querySettings Settings) []proto.Setting {
	settings := make([]proto.Setting, 0, len(c.opt.Settings)+len(querySettings))
	for k, v := range c.opt.Settings {
		settings = append(settings, proto.Setting{
			Key:   k,
			Value: v,
		})
	}
	for k, v := range querySettings {
		settings = append(settings, proto.Setting{
			Key:   k,
			Value: v,
		})
	}
	return settings
}

func (c *connect) isBad() bool {
	switch {
	case c.closed:
		return true
	}
	if err := c.connCheck(); err != nil {
		return true
	}
	return false
}

func (c *connect) close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	c.buffer = nil
	c.reader = nil
	if err := c.conn.Close(); err != nil {
		return err
	}
	return nil
}

func (c *connect) progress() (*Progress, error) {
	var progress proto.Progress
	if err := progress.Decode(c.reader, c.revision); err != nil {
		return nil, err
	}
	c.debugf("[progress] %s", &progress)
	return &progress, nil
}

func (c *connect) exception() error {
	var e Exception
	if err := e.Decode(c.reader); err != nil {
		return err
	}
	c.debugf("[exception] %s", e.Error())
	return &e
}

func (c *connect) sendData(block *proto.Block, name string) error {
	c.debugf("[send data] compression=%t", c.compression)
	c.buffer.PutByte(proto.ClientData)
	c.buffer.PutString(name)
	// Saving offset of compressible data.
	start := len(c.buffer.Buf)
	if err := block.Encode(c.buffer, c.revision); err != nil {
		return err
	}
	if c.compression != CompressionNone {
		// Performing compression. Note: only blocks are compressed.
		data := c.buffer.Buf[start:]
		if err := c.compressor.Compress(compress.Method(c.compression), data); err != nil {
			return errors.Wrap(err, "compress")
		}
		c.buffer.Buf = append(c.buffer.Buf[:start], c.compressor.Data...)
	}

	if err := c.flush(); err != nil {
		return err
	}
	defer func() {
		c.buffer.Reset()
	}()
	return nil
}

func (c *connect) readData(packet byte, compressible bool) (*proto.Block, error) {
	if _, err := c.reader.Str(); err != nil {
		return nil, err
	}
	if compressible && c.compression != CompressionNone {
		c.reader.EnableCompression()
		defer c.reader.DisableCompression()
	}
	block := proto.Block{Timezone: c.server.Timezone}
	if err := block.Decode(c.reader, c.revision); err != nil {
		return nil, err
	}
	block.Packet = packet
	c.debugf("[read data] compression=%t. block: columns=%d, rows=%d", c.compression, len(block.Columns), block.Rows())
	return &block, nil
}

func (c *connect) flush() error {
	if len(c.buffer.Buf) == 0 {
		// Nothing to flush.
		return nil
	}
	n, err := c.conn.Write(c.buffer.Buf)
	if err != nil {
		return errors.Wrap(err, "write")
	}
	if n != len(c.buffer.Buf) {
		return errors.New("wrote less than expected")
	}

	c.buffer.Reset()
	return nil
}
