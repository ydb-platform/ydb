package column

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-go/lib/binary"
	"reflect"
	"strconv"
	"time"
)

type Tuple struct {
	base
	columns []Column
}

func (tuple *Tuple) Read(decoder *binary.Decoder, isNull bool) (interface{}, error) {
	return nil, fmt.Errorf("do not use Read method for Tuple(T) column")
}

func (tuple *Tuple) ReadTuple(decoder *binary.Decoder, rows int) ([]interface{}, error) {
	var values = make([][]interface{}, rows)

	for _, c := range tuple.columns {

		switch column := c.(type) {
		case *Array:
			cols, err := column.ReadArray(decoder, rows)
			if err != nil {
				return nil, err
			}
			for i := 0; i < rows; i++ {
				values[i] = append(values[i], cols[i])
			}

		case *Nullable:
			cols, err := column.ReadNull(decoder, rows)
			if err != nil {
				return nil, err
			}
			for i := 0; i < rows; i++ {
				values[i] = append(values[i], cols[i])
			}

		case *Tuple:
			cols, err := column.ReadTuple(decoder, rows)
			if err != nil {
				return nil, err
			}
			for i := 0; i < rows; i++ {
				values[i] = append(values[i], cols[i])
			}

		default:
			for i := 0; i < rows; i++ {
				value, err := c.Read(decoder, false)
				if err != nil {
					return nil, err
				}
				values[i] = append(values[i], value)
			}
		}
	}

	var ret = make([]interface{}, rows)
	for i := range values {
		ret[i] = values[i]
	}

	return ret, nil
}

func (tuple *Tuple) Write(encoder *binary.Encoder, v interface{}) (err error) {
	return fmt.Errorf("unsupported Tuple(T) type [%T]", v)
}

func parseTuple(name, chType string, timezone *time.Location) (Column, error) {
	var columnType = chType

	chType = chType[6 : len(chType)-1]
	var types []string
	var last, diff int
	for i, b := range chType + "," {
		if b == '(' {
			diff++
		} else if b == ')' {
			diff--
		} else if b == ',' && diff == 0 {
			types = append(types, chType[last:i])
			last = i + 2
		}
	}

	var columns = make([]Column, 0, len(types))
	for i, chType := range types {
		column, err := Factory(name+"."+strconv.Itoa(i+1), chType, timezone)
		if err != nil {
			return nil, fmt.Errorf("%s: %v", chType, err)
		}
		columns = append(columns, column)
	}

	return &Tuple{
		base: base{
			name:    name,
			chType:  columnType,
			valueOf: reflect.ValueOf([]interface{}{}),
		},
		columns: columns,
	}, nil
}
