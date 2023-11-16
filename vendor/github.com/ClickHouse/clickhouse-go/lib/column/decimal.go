package column

import (
	b "encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"

	"github.com/ClickHouse/clickhouse-go/lib/binary"
)

// Table of powers of 10 for fast casting from floating types to decimal type
// representations.
var factors10 = []float64{
	1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13,
	1e14, 1e15, 1e16, 1e17, 1e18,
}

// Decimal represents Decimal(P, S) ClickHouse. Decimal is represented as
// integral. Also floating-point types are supported for query parameters.
//
// Since there is no support for int128 in Golang, decimals with precision 19
// through 38 are represented as 16 little-endian bytes.
type Decimal struct {
	base
	nobits    int // its domain is {32, 64}
	precision int
	scale     int
}

func (d *Decimal) Read(decoder *binary.Decoder, isNull bool) (interface{}, error) {
	switch d.nobits {
	case 32:
		return decoder.Int32()
	case 64:
		return decoder.Int64()
	case 128:
		return decoder.Decimal128()
	default:
		return nil, errors.New("unachievable execution path")
	}
}

func (d *Decimal) Write(encoder *binary.Encoder, v interface{}) error {
	switch d.nobits {
	case 32:
		return d.write32(encoder, v)
	case 64:
		return d.write64(encoder, v)
	case 128:
		return d.write128(encoder, v)
	default:
		return errors.New("unachievable execution path")
	}
}

func (d *Decimal) float2int32(floating float64) int32 {
	fixed := int32(floating * factors10[d.scale])
	return fixed
}

func (d *Decimal) float2int64(floating float64) int64 {
	fixed := int64(floating * factors10[d.scale])
	return fixed
}

func (d *Decimal) write32(encoder *binary.Encoder, v interface{}) error {
	switch v := v.(type) {
	case int8:
		return encoder.Int32(int32(v))
	case int16:
		return encoder.Int32(int32(v))
	case int32:
		return encoder.Int32(int32(v))
	case int64:
		if v > math.MaxInt32 || v < math.MinInt32 {
			return errors.New("overflow when narrowing type conversion from int64 to int32")
		}
		return encoder.Int32(int32(v))

	case uint8:
		return encoder.Int32(int32(v))
	case uint16:
		return encoder.Int32(int32(v))
	case uint32:
		if v > math.MaxInt32 {
			return errors.New("overflow when narrowing type conversion from uint32 to int32")
		}
		return encoder.Int32(int32(v))
	case uint64:
		if v > math.MaxInt32 {
			return errors.New("overflow when narrowing type conversion from uint64 to int32")
		}
		return encoder.Int32(int32(v))

	case float32:
		fixed := d.float2int32(float64(v))
		return encoder.Int32(fixed)
	case float64:
		fixed := d.float2int32(float64(v))
		return encoder.Int32(fixed)

	// this relies on Nullable never sending nil values through
	case *int8:
		return encoder.Int32(int32(*v))
	case *int16:
		return encoder.Int32(int32(*v))
	case *int32:
		return encoder.Int32(int32(*v))
	case *int64:
		if *v > math.MaxInt32 || *v < math.MinInt32 {
			return errors.New("overflow when narrowing type conversion from int64 to int32")
		}
		return encoder.Int32(int32(*v))

	case *uint8:
		return encoder.Int32(int32(*v))
	case *uint16:
		return encoder.Int32(int32(*v))
	case *uint32:
		if *v > math.MaxInt32 {
			return errors.New("overflow when narrowing type conversion from uint34 to int32")
		}
		return encoder.Int32(int32(*v))
	case *uint64:
		if *v > math.MaxInt32 {
			return errors.New("overflow when narrowing type conversion from uint64 to int32")
		}
		return encoder.Int32(int32(*v))

	case *float32:
		fixed := d.float2int32(float64(*v))
		return encoder.Int32(fixed)
	case *float64:
		fixed := d.float2int32(float64(*v))
		return encoder.Int32(fixed)
	}

	return &ErrUnexpectedType{
		T:      v,
		Column: d,
	}
}

func (d *Decimal) write64(encoder *binary.Encoder, v interface{}) error {
	switch v := v.(type) {
	case int:
		return encoder.Int64(int64(v))
	case int8:
		return encoder.Int64(int64(v))
	case int16:
		return encoder.Int64(int64(v))
	case int32:
		return encoder.Int64(int64(v))
	case int64:
		return encoder.Int64(int64(v))

	case uint8:
		return encoder.Int64(int64(v))
	case uint16:
		return encoder.Int64(int64(v))
	case uint32:
		return encoder.Int64(int64(v))
	case uint64:
		if v > math.MaxInt64 {
			return errors.New("overflow when narrowing type conversion from uint64 to int64")
		}
		return encoder.Int64(int64(v))

	case float32:
		fixed := d.float2int64(float64(v))
		return encoder.Int64(fixed)
	case float64:
		fixed := d.float2int64(float64(v))
		return encoder.Int64(fixed)

	// this relies on Nullable never sending nil values through
	case *int:
		return encoder.Int64(int64(*v))
	case *int8:
		return encoder.Int64(int64(*v))
	case *int16:
		return encoder.Int64(int64(*v))
	case *int32:
		return encoder.Int64(int64(*v))
	case *int64:
		return encoder.Int64(int64(*v))

	case *uint8:
		return encoder.Int64(int64(*v))
	case *uint16:
		return encoder.Int64(int64(*v))
	case *uint32:
		return encoder.Int64(int64(*v))
	case *uint64:
		if *v > math.MaxInt64 {
			return errors.New("overflow when narrowing type conversion from uint64 to int64")
		}
		return encoder.Int64(int64(*v))

	case *float32:
		fixed := d.float2int64(float64(*v))
		return encoder.Int64(fixed)
	case *float64:
		fixed := d.float2int64(float64(*v))
		return encoder.Int64(fixed)
	}

	return &ErrUnexpectedType{
		T:      v,
		Column: d,
	}
}

// Turns an int64 into 16 little-endian bytes.
func int64ToDecimal128(v int64) []byte {
	bytes := make([]byte, 16)
	b.LittleEndian.PutUint64(bytes[:8], uint64(v))
	sign := 0
	if v < 0 {
		sign = -1
	}
	b.LittleEndian.PutUint64(bytes[8:], uint64(sign))
	return bytes
}

// Turns a uint64 into 16 little-endian bytes.
func uint64ToDecimal128(v uint64) []byte {
	bytes := make([]byte, 16)
	b.LittleEndian.PutUint64(bytes[:8], uint64(v))
	return bytes
}

func (d *Decimal) write128(encoder *binary.Encoder, v interface{}) error {
	switch v := v.(type) {
	case int:
		return encoder.Decimal128(int64ToDecimal128(int64(v)))
	case int8:
		return encoder.Decimal128(int64ToDecimal128(int64(v)))
	case int16:
		return encoder.Decimal128(int64ToDecimal128(int64(v)))
	case int32:
		return encoder.Decimal128(int64ToDecimal128(int64(v)))
	case int64:
		return encoder.Decimal128(int64ToDecimal128(v))

	case uint8:
		return encoder.Decimal128(uint64ToDecimal128(uint64(v)))
	case uint16:
		return encoder.Decimal128(uint64ToDecimal128(uint64(v)))
	case uint32:
		return encoder.Decimal128(uint64ToDecimal128(uint64(v)))
	case uint64:
		return encoder.Decimal128(uint64ToDecimal128(v))

	case float32:
		fixed := d.float2int64(float64(v))
		return encoder.Decimal128(int64ToDecimal128(fixed))
	case float64:
		fixed := d.float2int64(float64(v))
		return encoder.Decimal128(int64ToDecimal128(fixed))

	case []byte:
		if len(v) != 16 {
			return errors.New("expected 16 bytes")
		}
		return encoder.Decimal128(v)

	// this relies on Nullable never sending nil values through
	case *int:
		return encoder.Decimal128(int64ToDecimal128(int64(*v)))
	case *int8:
		return encoder.Decimal128(int64ToDecimal128(int64(*v)))
	case *int16:
		return encoder.Decimal128(int64ToDecimal128(int64(*v)))
	case *int32:
		return encoder.Decimal128(int64ToDecimal128(int64(*v)))
	case *int64:
		return encoder.Decimal128(int64ToDecimal128(*v))

	case *uint8:
		return encoder.Decimal128(uint64ToDecimal128(uint64(*v)))
	case *uint16:
		return encoder.Decimal128(uint64ToDecimal128(uint64(*v)))
	case *uint32:
		return encoder.Decimal128(uint64ToDecimal128(uint64(*v)))
	case *uint64:
		return encoder.Decimal128(uint64ToDecimal128(*v))

	case *float32:
		fixed := d.float2int64(float64(*v))
		return encoder.Decimal128(int64ToDecimal128(fixed))
	case *float64:
		fixed := d.float2int64(float64(*v))
		return encoder.Decimal128(int64ToDecimal128(fixed))

	case *[]byte:
		if len(*v) != 16 {
			return errors.New("expected 16 bytes")
		}
		return encoder.Decimal128(*v)
	}

	return &ErrUnexpectedType{
		T:      v,
		Column: d,
	}
}

func parseDecimal(name, chType string) (Column, error) {
	switch {
	case len(chType) < 12:
		fallthrough
	case !strings.HasPrefix(chType, "Decimal"):
		fallthrough
	case chType[7] != '(':
		fallthrough
	case chType[len(chType)-1] != ')':
		return nil, fmt.Errorf("invalid Decimal format: '%s'", chType)
	}

	var params = strings.Split(chType[8:len(chType)-1], ",")

	if len(params) != 2 {
		return nil, fmt.Errorf("invalid Decimal format: '%s'", chType)
	}

	params[0] = strings.TrimSpace(params[0])
	params[1] = strings.TrimSpace(params[1])

	var err error
	var decimal = &Decimal{
		base: base{
			name:   name,
			chType: chType,
		},
	}

	if decimal.precision, err = strconv.Atoi(params[0]); err != nil {
		return nil, fmt.Errorf("'%s' is not Decimal type: %s", chType, err)
	} else if decimal.precision < 1 {
		return nil, errors.New("wrong precision of Decimal type")
	}

	if decimal.scale, err = strconv.Atoi(params[1]); err != nil {
		return nil, fmt.Errorf("'%s' is not Decimal type: %s", chType, err)
	} else if decimal.scale < 0 || decimal.scale > decimal.precision {
		return nil, errors.New("wrong scale of Decimal type")
	}

	switch {
	case decimal.precision <= 9:
		decimal.nobits = 32
		decimal.valueOf = columnBaseTypes[int32(0)]
	case decimal.precision <= 18:
		decimal.nobits = 64
		decimal.valueOf = columnBaseTypes[int64(0)]
	case decimal.precision <= 38:
		decimal.nobits = 128
		decimal.valueOf = reflect.ValueOf([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	default:
		return nil, errors.New("precision of Decimal exceeds max bound")
	}

	return decimal, nil
}

func (d *Decimal) GetPrecision() int {
	return d.precision
}

func (d *Decimal) GetScale() int {
	return d.scale
}
