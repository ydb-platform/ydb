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

package column

import (
	"encoding"
	"fmt"
	"github.com/ClickHouse/ch-go/proto"
	"reflect"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
)

type FixedString struct {
	name string
	col  proto.ColFixedStr
}

func (col *FixedString) Reset() {
	col.col.Reset()
}

func (col *FixedString) Name() string {
	return col.name
}

func (col *FixedString) parse(t Type) (*FixedString, error) {
	if _, err := fmt.Sscanf(string(t), "FixedString(%d)", &col.col.Size); err != nil {
		return nil, err
	}
	return col, nil
}

func (col *FixedString) Type() Type {
	return Type(fmt.Sprintf("FixedString(%d)", col.col.Size))
}

func (col *FixedString) ScanType() reflect.Type {
	return scanTypeString
}

func (col *FixedString) Rows() int {
	return col.col.Rows()
}

func (col *FixedString) Row(i int, ptr bool) interface{} {
	value := col.row(i)
	if ptr {
		return &value
	}
	return value
}

func (col *FixedString) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *string:
		*d = col.row(row)
	case **string:
		*d = new(string)
		**d = col.row(row)
	case encoding.BinaryUnmarshaler:
		return d.UnmarshalBinary(col.rowBytes(row))
	default:
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "FixedString",
		}
	}
	return nil
}

func (col *FixedString) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	case []string:
		nulls = make([]uint8, len(v))
		for _, v := range v {
			if v == "" {
				col.col.Append(make([]byte, col.col.Size))
			} else {
				col.col.Append(binary.Str2Bytes(v))
			}
		}
	case []*string:
		nulls = make([]uint8, len(v))
		for i, v := range v {
			if v == nil {
				nulls[i] = 1
			}
			switch {
			case v == nil:
				col.col.Append(make([]byte, col.col.Size))
			default:
				if *v == "" {
					col.col.Append(make([]byte, col.col.Size))
				} else {
					col.col.Append(binary.Str2Bytes(*v))
				}
			}
		}
	case encoding.BinaryMarshaler:
		data, err := v.MarshalBinary()
		if err != nil {
			return nil, err
		}
		col.col.Append(data)
		nulls = make([]uint8, len(data)/col.col.Size)
	default:
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "FixedString",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *FixedString) AppendRow(v interface{}) (err error) {
	data := make([]byte, col.col.Size)
	switch v := v.(type) {
	case string:
		if v != "" {
			data = binary.Str2Bytes(v)
		}
	case *string:
		if v != nil {
			if *v != "" {
				data = binary.Str2Bytes(*v)
			}
		}
	case nil:
	case encoding.BinaryMarshaler:
		if data, err = v.MarshalBinary(); err != nil {
			return err
		}
	default:
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "FixedString",
			From: fmt.Sprintf("%T", v),
		}
	}
	col.col.Append(data)
	return nil
}

func (col *FixedString) Decode(reader *proto.Reader, rows int) error {
	return col.col.DecodeColumn(reader, rows)
}

func (col *FixedString) Encode(buffer *proto.Buffer) {
	col.col.EncodeColumn(buffer)
}

func (col *FixedString) row(i int) string {
	v := col.col.Row(i)
	return string(v)
}

func (col *FixedString) rowBytes(i int) []byte {
	return col.col.Row(i)
}

var _ Interface = (*FixedString)(nil)
