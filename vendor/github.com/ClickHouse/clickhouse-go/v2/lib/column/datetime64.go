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
	"database/sql"
	"fmt"
	"github.com/ClickHouse/ch-go/proto"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/timezone"
)

var (
	minDateTime64, _ = time.Parse("2006-01-02 15:04:05", "1925-01-01 00:00:00")
	maxDateTime64, _ = time.Parse("2006-01-02 15:04:05", "2283-11-11 00:00:00")
)

const (
	defaultDateTime64FormatNoZone   = "2006-01-02 15:04:05.999999999"
	defaultDateTime64FormatWithZone = "2006-01-02 15:04:05.999999999 -07:00"
)

type DateTime64 struct {
	chType   Type
	timezone *time.Location
	name     string
	col      proto.ColDateTime64
}

func (col *DateTime64) Reset() {
	col.col.Reset()
}

func (col *DateTime64) Name() string {
	return col.name
}

func (col *DateTime64) parse(t Type, tz *time.Location) (_ Interface, err error) {
	col.chType = t
	switch params := strings.Split(t.params(), ","); len(params) {
	case 2:
		precision, err := strconv.ParseInt(params[0], 10, 8)
		if err != nil {
			return nil, err
		}
		p := byte(precision)
		col.col.WithPrecision(proto.Precision(p))
		timezone, err := timezone.Load(params[1][2 : len(params[1])-1])
		if err != nil {
			return nil, err
		}
		col.col.WithLocation(timezone)
	case 1:
		precision, err := strconv.ParseInt(params[0], 10, 8)
		if err != nil {
			return nil, err
		}
		p := byte(precision)
		col.col.WithPrecision(proto.Precision(p))
		col.col.WithLocation(tz)
	default:
		return nil, &UnsupportedColumnTypeError{
			t: t,
		}
	}
	return col, nil
}

func (col *DateTime64) Type() Type {
	return col.chType
}

func (col *DateTime64) ScanType() reflect.Type {
	return scanTypeTime
}

func (col *DateTime64) Rows() int {
	return col.col.Rows()
}

func (col *DateTime64) Row(i int, ptr bool) interface{} {
	value := col.row(i)
	if ptr {
		return &value
	}
	return value
}

func (col *DateTime64) ScanRow(dest interface{}, row int) error {
	switch d := dest.(type) {
	case *time.Time:
		*d = col.row(row)
	case **time.Time:
		*d = new(time.Time)
		**d = col.row(row)
	case *sql.NullTime:
		d.Scan(col.row(row))
	default:
		if scan, ok := dest.(sql.Scanner); ok {
			return scan.Scan(col.row(row))
		}
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "Datetime64",
		}
	}
	return nil
}

func (col *DateTime64) Append(v interface{}) (nulls []uint8, err error) {
	switch v := v.(type) {
	// we assume int64 is in milliseconds and don't currently scale to the precision - no tests to indicate intended
	// historical behaviour
	case []int64:
		nulls = make([]uint8, len(v))
		for i := range v {
			col.col.Append(time.UnixMilli(v[i]))
		}
	case []*int64:
		nulls = make([]uint8, len(v))
		for i := range v {
			switch {
			case v != nil:
				col.col.Append(time.UnixMilli(*v[i]))
			default:
				col.col.Append(time.UnixMilli(0))
				nulls[i] = 1
			}
		}
	case []time.Time:
		nulls = make([]uint8, len(v))
		for i := range v {
			if err := dateOverflow(minDateTime64, maxDateTime64, v[i], "2006-01-02 15:04:05"); err != nil {
				return nil, err
			}
			col.col.Append(v[i])
		}
	case []*time.Time:
		nulls = make([]uint8, len(v))
		for i := range v {
			switch {
			case v[i] != nil:
				if err := dateOverflow(minDateTime64, maxDateTime64, *v[i], "2006-01-02 15:04:05"); err != nil {
					return nil, err
				}
				col.col.Append(*v[i])
			default:
				col.col.Append(time.Time{})
				nulls[i] = 1
			}
		}
	case []string:
		nulls = make([]uint8, len(v))
		for i := range v {
			value, err := col.parseDateTime(v[i])
			if err != nil {
				return nil, err
			}
			col.col.Append(value)
		}
	case []sql.NullTime:
		nulls = make([]uint8, len(v))
		for i := range v {
			col.AppendRow(v[i])
		}
	case []*sql.NullTime:
		nulls = make([]uint8, len(v))
		for i := range v {
			if v[i] == nil {
				nulls[i] = 1
			}
			col.AppendRow(v[i])
		}
	default:
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "Datetime64",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *DateTime64) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case int64:
		col.col.Append(time.UnixMilli(v))
	case *int64:
		switch {
		case v != nil:
			col.col.Append(time.UnixMilli(*v))
		default:
			col.col.Append(time.Time{})
		}
	case time.Time:
		if err := dateOverflow(minDateTime64, maxDateTime64, v, "2006-01-02 15:04:05"); err != nil {
			return err
		}
		col.col.Append(v)
	case *time.Time:
		switch {
		case v != nil:
			if err := dateOverflow(minDateTime64, maxDateTime64, *v, "2006-01-02 15:04:05"); err != nil {
				return err
			}
			col.col.Append(*v)
		default:
			col.col.Append(time.Time{})
		}
	case sql.NullTime:
		switch v.Valid {
		case true:
			col.col.Append(v.Time)
		default:
			col.col.Append(time.Time{})
		}
	case *sql.NullTime:
		switch v.Valid {
		case true:
			col.col.Append(v.Time)
		default:
			col.col.Append(time.Time{})
		}
	case string:
		datetime, err := col.parseDateTime(v)
		if err != nil {
			return err
		}
		col.col.Append(datetime)
	case nil:
		col.col.Append(time.Time{})
	default:
		s, ok := v.(fmt.Stringer)
		if ok {
			return col.AppendRow(s.String())
		}
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "Datetime64",
			From: fmt.Sprintf("%T", v),
		}
	}
	return nil
}

func (col *DateTime64) Decode(reader *proto.Reader, rows int) error {
	return col.col.DecodeColumn(reader, rows)
}

func (col *DateTime64) Encode(buffer *proto.Buffer) {
	col.col.EncodeColumn(buffer)
}

func (col *DateTime64) row(i int) time.Time {
	time := col.col.Row(i)
	if col.timezone != nil {
		time = time.In(col.timezone)
	}
	return time
}

func (col *DateTime64) timeToInt64(t time.Time) int64 {
	var timestamp int64
	if !t.IsZero() {
		timestamp = t.UnixNano()
	}
	return timestamp / int64(math.Pow10(9-int(col.col.Precision)))
}

func (col *DateTime64) parseDateTime(value string) (tv time.Time, err error) {
	if tv, err = time.Parse(defaultDateTime64FormatWithZone, value); err == nil {
		return tv, nil
	}
	if tv, err = time.Parse(defaultDateTime64FormatNoZone, value); err == nil {
		return time.Date(
			tv.Year(), tv.Month(), tv.Day(), tv.Hour(), tv.Minute(), tv.Second(), tv.Nanosecond(), time.Local,
		), nil
	}
	return time.Time{}, err
}

var _ Interface = (*DateTime64)(nil)
