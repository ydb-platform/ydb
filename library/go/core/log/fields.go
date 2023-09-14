package log

import (
	"context"
	"fmt"
	"time"
)

const (
	// DefaultErrorFieldName is the default field name used for errors
	DefaultErrorFieldName = "error"
)

// FieldType is a type of data Field can represent
type FieldType int

const (
	// FieldTypeNil is for a pure nil
	FieldTypeNil FieldType = iota
	// FieldTypeString is for a string
	FieldTypeString
	// FieldTypeBinary is for a binary array
	FieldTypeBinary
	// FieldTypeBoolean is for boolean
	FieldTypeBoolean
	// FieldTypeSigned is for signed integers
	FieldTypeSigned
	// FieldTypeUnsigned is for unsigned integers
	FieldTypeUnsigned
	// FieldTypeFloat is for float
	FieldTypeFloat
	// FieldTypeTime is for time.Time
	FieldTypeTime
	// FieldTypeDuration is for time.Duration
	FieldTypeDuration
	// FieldTypeError is for an error
	FieldTypeError
	// FieldTypeArray is for an array of any type
	FieldTypeArray
	// FieldTypeAny is for any type
	FieldTypeAny
	// FieldTypeReflect is for unknown types
	FieldTypeReflect
	// FieldTypeByteString is for a bytes that can be represented as UTF-8 string
	FieldTypeByteString
	// FieldTypeContext wraps context for lazy context fields evaluation if possible
	FieldTypeContext
)

// Field stores one structured logging field
type Field struct {
	key      string
	ftype    FieldType
	string   string
	signed   int64
	unsigned uint64
	float    float64
	iface    interface{}
}

// Key returns field key
func (f Field) Key() string {
	return f.key
}

// Type returns field type
func (f Field) Type() FieldType {
	return f.ftype
}

// String returns field string
func (f Field) String() string {
	return f.string
}

// Binary constructs field of []byte
func (f Field) Binary() []byte {
	if f.iface == nil {
		return nil
	}
	return f.iface.([]byte)
}

// Bool returns field bool
func (f Field) Bool() bool {
	return f.Signed() != 0
}

// Signed returns field int64
func (f Field) Signed() int64 {
	return f.signed
}

// Unsigned returns field uint64
func (f Field) Unsigned() uint64 {
	return f.unsigned
}

// Float returns field float64
func (f Field) Float() float64 {
	return f.float
}

// Time returns field time.Time
func (f Field) Time() time.Time {
	return time.Unix(0, f.signed)
}

// Duration returns field time.Duration
func (f Field) Duration() time.Duration {
	return time.Nanosecond * time.Duration(f.signed)
}

// Error constructs field of error type
func (f Field) Error() error {
	if f.iface == nil {
		return nil
	}
	return f.iface.(error)
}

// Interface returns field interface
func (f Field) Interface() interface{} {
	return f.iface
}

// Any returns contained data as interface{}
// nolint: gocyclo
func (f Field) Any() interface{} {
	switch f.Type() {
	case FieldTypeNil:
		return nil
	case FieldTypeString:
		return f.String()
	case FieldTypeBinary:
		return f.Interface()
	case FieldTypeBoolean:
		return f.Bool()
	case FieldTypeSigned:
		return f.Signed()
	case FieldTypeUnsigned:
		return f.Unsigned()
	case FieldTypeFloat:
		return f.Float()
	case FieldTypeTime:
		return f.Time()
	case FieldTypeDuration:
		return f.Duration()
	case FieldTypeError:
		return f.Error()
	case FieldTypeArray:
		return f.Interface()
	case FieldTypeAny:
		return f.Interface()
	case FieldTypeReflect:
		return f.Interface()
	case FieldTypeByteString:
		return f.Interface()
	case FieldTypeContext:
		return f.Interface()
	default:
		// For when new field type is not added to this func
		panic(fmt.Sprintf("unknown field type: %d", f.Type()))
	}
}

// Nil constructs field of nil type
func Nil(key string) Field {
	return Field{key: key, ftype: FieldTypeNil}
}

// String constructs field of string type
func String(key, value string) Field {
	return Field{key: key, ftype: FieldTypeString, string: value}
}

// Sprintf constructs field of string type with formatting
func Sprintf(key, format string, args ...interface{}) Field {
	return Field{key: key, ftype: FieldTypeString, string: fmt.Sprintf(format, args...)}
}

// Strings constructs Field from []string
func Strings(key string, value []string) Field {
	return Array(key, value)
}

// Binary constructs field of []byte type
func Binary(key string, value []byte) Field {
	return Field{key: key, ftype: FieldTypeBinary, iface: value}
}

// Bool constructs field of bool type
func Bool(key string, value bool) Field {
	field := Field{key: key, ftype: FieldTypeBoolean}
	if value {
		field.signed = 1
	} else {
		field.signed = 0
	}

	return field
}

// Bools constructs Field from []bool
func Bools(key string, value []bool) Field {
	return Array(key, value)
}

// Int constructs Field from int
func Int(key string, value int) Field {
	return Int64(key, int64(value))
}

// Ints constructs Field from []int
func Ints(key string, value []int) Field {
	return Array(key, value)
}

// Int8 constructs Field from int8
func Int8(key string, value int8) Field {
	return Int64(key, int64(value))
}

// Int8s constructs Field from []int8
func Int8s(key string, value []int8) Field {
	return Array(key, value)
}

// Int16 constructs Field from int16
func Int16(key string, value int16) Field {
	return Int64(key, int64(value))
}

// Int16s constructs Field from []int16
func Int16s(key string, value []int16) Field {
	return Array(key, value)
}

// Int32 constructs Field from int32
func Int32(key string, value int32) Field {
	return Int64(key, int64(value))
}

// Int32s constructs Field from []int32
func Int32s(key string, value []int32) Field {
	return Array(key, value)
}

// Int64 constructs Field from int64
func Int64(key string, value int64) Field {
	return Field{key: key, ftype: FieldTypeSigned, signed: value}
}

// Int64s constructs Field from []int64
func Int64s(key string, value []int64) Field {
	return Array(key, value)
}

// UInt constructs Field from uint
func UInt(key string, value uint) Field {
	return UInt64(key, uint64(value))
}

// UInts constructs Field from []uint
func UInts(key string, value []uint) Field {
	return Array(key, value)
}

// UInt8 constructs Field from uint8
func UInt8(key string, value uint8) Field {
	return UInt64(key, uint64(value))
}

// UInt8s constructs Field from []uint8
func UInt8s(key string, value []uint8) Field {
	return Array(key, value)
}

// UInt16 constructs Field from uint16
func UInt16(key string, value uint16) Field {
	return UInt64(key, uint64(value))
}

// UInt16s constructs Field from []uint16
func UInt16s(key string, value []uint16) Field {
	return Array(key, value)
}

// UInt32 constructs Field from uint32
func UInt32(key string, value uint32) Field {
	return UInt64(key, uint64(value))
}

// UInt32s constructs Field from []uint32
func UInt32s(key string, value []uint32) Field {
	return Array(key, value)
}

// UInt64 constructs Field from uint64
func UInt64(key string, value uint64) Field {
	return Field{key: key, ftype: FieldTypeUnsigned, unsigned: value}
}

// UInt64s constructs Field from []uint64
func UInt64s(key string, value []uint64) Field {
	return Array(key, value)
}

// Float32 constructs Field from float32
func Float32(key string, value float32) Field {
	return Float64(key, float64(value))
}

// Float32s constructs Field from []float32
func Float32s(key string, value []float32) Field {
	return Array(key, value)
}

// Float64 constructs Field from float64
func Float64(key string, value float64) Field {
	return Field{key: key, ftype: FieldTypeFloat, float: value}
}

// Float64s constructs Field from []float64
func Float64s(key string, value []float64) Field {
	return Array(key, value)
}

// Time constructs field of time.Time type
func Time(key string, value time.Time) Field {
	return Field{key: key, ftype: FieldTypeTime, signed: value.UnixNano()}
}

// Times constructs Field from []time.Time
func Times(key string, value []time.Time) Field {
	return Array(key, value)
}

// Duration constructs field of time.Duration type
func Duration(key string, value time.Duration) Field {
	return Field{key: key, ftype: FieldTypeDuration, signed: value.Nanoseconds()}
}

// Durations constructs Field from []time.Duration
func Durations(key string, value []time.Duration) Field {
	return Array(key, value)
}

// NamedError constructs field of error type
func NamedError(key string, value error) Field {
	return Field{key: key, ftype: FieldTypeError, iface: value}
}

// Error constructs field of error type with default field name
func Error(value error) Field {
	return NamedError(DefaultErrorFieldName, value)
}

// Errors constructs Field from []error
func Errors(key string, value []error) Field {
	return Array(key, value)
}

// Array constructs field of array type
func Array(key string, value interface{}) Field {
	return Field{key: key, ftype: FieldTypeArray, iface: value}
}

// Reflect constructs field of unknown type
func Reflect(key string, value interface{}) Field {
	return Field{key: key, ftype: FieldTypeReflect, iface: value}
}

// ByteString constructs field of bytes that could represent UTF-8 string
func ByteString(key string, value []byte) Field {
	return Field{key: key, ftype: FieldTypeByteString, iface: value}
}

// Context constructs field for lazy context fields evaluation if possible
func Context(ctx context.Context) Field {
	return Field{ftype: FieldTypeContext, iface: ctx}
}

// Any tries to deduce interface{} underlying type and constructs Field from it.
// Use of this function is ok only for the sole purpose of not repeating its entire code
// or parts of it in user's code (when you need to log interface{} types with unknown content).
// Otherwise please use specialized functions.
// nolint: gocyclo
func Any(key string, value interface{}) Field {
	switch val := value.(type) {
	case bool:
		return Bool(key, val)
	case float64:
		return Float64(key, val)
	case float32:
		return Float32(key, val)
	case int:
		return Int(key, val)
	case []int:
		return Ints(key, val)
	case int64:
		return Int64(key, val)
	case []int64:
		return Int64s(key, val)
	case int32:
		return Int32(key, val)
	case []int32:
		return Int32s(key, val)
	case int16:
		return Int16(key, val)
	case []int16:
		return Int16s(key, val)
	case int8:
		return Int8(key, val)
	case []int8:
		return Int8s(key, val)
	case string:
		return String(key, val)
	case []string:
		return Strings(key, val)
	case uint:
		return UInt(key, val)
	case []uint:
		return UInts(key, val)
	case uint64:
		return UInt64(key, val)
	case []uint64:
		return UInt64s(key, val)
	case uint32:
		return UInt32(key, val)
	case []uint32:
		return UInt32s(key, val)
	case uint16:
		return UInt16(key, val)
	case []uint16:
		return UInt16s(key, val)
	case uint8:
		return UInt8(key, val)
	case []byte:
		return Binary(key, val)
	case time.Time:
		return Time(key, val)
	case []time.Time:
		return Times(key, val)
	case time.Duration:
		return Duration(key, val)
	case []time.Duration:
		return Durations(key, val)
	case error:
		return NamedError(key, val)
	case []error:
		return Errors(key, val)
	case context.Context:
		return Context(val)
	default:
		return Field{key: key, ftype: FieldTypeAny, iface: value}
	}
}
