package column

import (
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/lib/binary"
)

type columnDecoder func() (interface{}, error)

var unsupportedArrayTypeErrTemp = "unsupported Array type '%s'"

// If you add Nullable type, that can be used in Array(Nullable(T)) add this type to ../codegen/nullable_appender/main.go in structure values.Types.
// Run code generation.
//go:generate go run ../codegen/nullable_appender -package $GOPACKAGE -file nullable_appender.go
type Array struct {
	base
	depth    int
	column   Column
	nullable bool
}

func (array *Array) Read(decoder *binary.Decoder, isNull bool) (interface{}, error) {
	return nil, fmt.Errorf("do not use Read method for Array(T) column")
}

func (array *Array) WriteNull(nulls, encoder *binary.Encoder, v interface{}) error {
	if array.nullable {
		column, ok := array.column.(*Nullable)
		if !ok {
			return fmt.Errorf("cannot convert to nullable type")
		}
		return column.WriteNull(nulls, encoder, v)
	}
	return fmt.Errorf("write null to not nullable array")
}

func (array *Array) Write(encoder *binary.Encoder, v interface{}) error {
	return array.column.Write(encoder, v)
}

func (array *Array) ReadArray(decoder *binary.Decoder, rows int) (_ []interface{}, err error) {
	var (
		offsets = make([][]uint64, array.depth)
		values  = make([]interface{}, rows)
	)

	// Read offsets
	lastOffset := uint64(rows)
	for i := 0; i < array.depth; i++ {
		offset := make([]uint64, lastOffset)
		for j := uint64(0); j < lastOffset; j++ {
			if offset[j], err = decoder.UInt64(); err != nil {
				return nil, err
			}
		}
		offsets[i] = offset
		lastOffset = 0
		if len(offset) > 0 {
			lastOffset = offset[len(offset)-1]
		}
	}

	var cd columnDecoder

	switch column := array.column.(type) {
	case *Nullable:
		nullRows, err := column.ReadNull(decoder, int(lastOffset))
		if err != nil {
			return nil, err
		}
		cd = func(rows []interface{}) columnDecoder {
			i := 0
			return func() (interface{}, error) {
				if i > len(rows) {
					return nil, errors.New("not enough rows to return while parsing Null column")
				}
				ret := rows[i]
				i++
				return ret, nil
			}
		}(nullRows)
	case *Tuple:
		tupleRows, err := column.ReadTuple(decoder, int(lastOffset))
		if err != nil {
			return nil, err
		}
		// closure to return fully assembled tuple values as if they
		// were decoded one at a time
		cd = func(rows []interface{}) columnDecoder {
			i := 0
			return func() (interface{}, error) {
				if i > len(rows) {
					return nil, errors.New("not enough rows to return while parsing Tuple column")
				}
				ret := rows[i]
				i++
				return ret, nil
			}
		}(tupleRows)
	default:
		cd = func(decoder *binary.Decoder) columnDecoder {
			return func() (interface{}, error) { return array.column.Read(decoder, array.nullable) }
		}(decoder)
	}

	// Read values
	for i := 0; i < rows; i++ {
		if values[i], err = array.read(cd, offsets, uint64(i), 0); err != nil {
			return nil, err
		}
	}
	return values, nil
}

func (array *Array) read(readColumn columnDecoder, offsets [][]uint64, index uint64, level int) (interface{}, error) {
	end := offsets[level][index]
	start := uint64(0)
	if index > 0 {
		start = offsets[level][index-1]
	}

	scanT := array.column.ScanType()
	slice := reflect.MakeSlice(array.arrayType(level), 0, int(end-start))
	for i := start; i < end; i++ {
		var (
			value interface{}
			err   error
		)
		if level == array.depth-1 {
			value, err = readColumn()
		} else {
			value, err = array.read(readColumn, offsets, i, level+1)
		}
		if err != nil {
			return nil, err
		}
		if array.nullable && level == array.depth-1 {
			f, ok := nullableAppender[scanT.String()]
			if !ok {
				return nil, fmt.Errorf(unsupportedArrayTypeErrTemp, scanT.String())
			}

			cSlice, err := f(value, slice)
			if err != nil {
				return nil, err
			}

			slice = cSlice
		} else {
			slice = reflect.Append(slice, reflect.ValueOf(value))
		}

	}
	return slice.Interface(), nil
}

func (array *Array) arrayType(level int) reflect.Type {
	t := array.column.ScanType()
	for i := 0; i < array.depth-level; i++ {
		t = reflect.SliceOf(t)
	}
	return t
}

func (array *Array) Depth() int {
	return array.depth
}

func parseArray(name, chType string, timezone *time.Location) (*Array, error) {
	if len(chType) < 11 {
		return nil, fmt.Errorf("invalid Array column type: %s", chType)
	}
	var (
		depth      int
		columnType = chType
	)

loop:
	for _, str := range strings.Split(chType, "Array(") {
		switch {
		case len(str) == 0:
			depth++
		default:
			chType = str[:len(str)-depth]
			break loop
		}
	}
	column, err := Factory(name, chType, timezone)
	if err != nil {
		return nil, fmt.Errorf("Array(T): %v", err)
	}

	var scanType interface{}
	switch t := column.ScanType(); t {
	case arrayBaseTypes[int8(0)]:
		scanType = []int8{}
	case arrayBaseTypes[int16(0)]:
		scanType = []int16{}
	case arrayBaseTypes[int32(0)]:
		scanType = []int32{}
	case arrayBaseTypes[int64(0)]:
		scanType = []int64{}
	case arrayBaseTypes[uint8(0)]:
		scanType = []uint8{}
	case arrayBaseTypes[uint16(0)]:
		scanType = []uint16{}
	case arrayBaseTypes[uint32(0)]:
		scanType = []uint32{}
	case arrayBaseTypes[uint64(0)]:
		scanType = []uint64{}
	case arrayBaseTypes[float32(0)]:
		scanType = []float32{}
	case arrayBaseTypes[float64(0)]:
		scanType = []float64{}
	case arrayBaseTypes[string("")]:
		scanType = []string{}
	case arrayBaseTypes[time.Time{}]:
		scanType = []time.Time{}
	case arrayBaseTypes[IPv4{}], arrayBaseTypes[IPv6{}]:
		scanType = []net.IP{}
	case reflect.ValueOf([]interface{}{}).Type():
		scanType = [][]interface{}{}

	//nullable
	case arrayBaseTypes[ptrInt8T]:
		scanType = []*int8{}
	case arrayBaseTypes[ptrInt16T]:
		scanType = []*int16{}
	case arrayBaseTypes[ptrInt32T]:
		scanType = []*int32{}
	case arrayBaseTypes[ptrInt64T]:
		scanType = []*int64{}
	case arrayBaseTypes[ptrUInt8T]:
		scanType = []*uint8{}
	case arrayBaseTypes[ptrUInt16T]:
		scanType = []*uint16{}
	case arrayBaseTypes[ptrUInt32T]:
		scanType = []*uint32{}
	case arrayBaseTypes[ptrUInt64T]:
		scanType = []*uint64{}
	case arrayBaseTypes[ptrFloat32]:
		scanType = []*float32{}
	case arrayBaseTypes[ptrFloat64]:
		scanType = []*float64{}
	case arrayBaseTypes[ptrString]:
		scanType = []*string{}
	case arrayBaseTypes[ptrTime]:
		scanType = []*time.Time{}
	case arrayBaseTypes[ptrIPv4], arrayBaseTypes[ptrIPv6]:
		scanType = []*net.IP{}
	default:
		return nil, fmt.Errorf(unsupportedArrayTypeErrTemp, column.ScanType().Name())
	}
	return &Array{
		base: base{
			name:    name,
			chType:  columnType,
			valueOf: reflect.ValueOf(scanType),
		},
		depth:    depth,
		column:   column,
		nullable: strings.HasPrefix(column.CHType(), "Nullable"),
	}, nil
}
