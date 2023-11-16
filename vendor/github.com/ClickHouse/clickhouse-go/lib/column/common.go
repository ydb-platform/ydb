package column

import (
	"fmt"
	"net"
	"reflect"
	"time"
)

type ErrUnexpectedType struct {
	Column Column
	T      interface{}
}

func (err *ErrUnexpectedType) Error() string {
	return fmt.Sprintf("%s: unexpected type %T", err.Column, err.T)
}

var columnBaseTypes = map[interface{}]reflect.Value{
	int8(0):     reflect.ValueOf(int8(0)),
	int16(0):    reflect.ValueOf(int16(0)),
	int32(0):    reflect.ValueOf(int32(0)),
	int64(0):    reflect.ValueOf(int64(0)),
	uint8(0):    reflect.ValueOf(uint8(0)),
	uint16(0):   reflect.ValueOf(uint16(0)),
	uint32(0):   reflect.ValueOf(uint32(0)),
	uint64(0):   reflect.ValueOf(uint64(0)),
	float32(0):  reflect.ValueOf(float32(0)),
	float64(0):  reflect.ValueOf(float64(0)),
	string(""):  reflect.ValueOf(string("")),
	time.Time{}: reflect.ValueOf(time.Time{}),
	IPv4{}:      reflect.ValueOf(net.IPv4zero),
	IPv6{}:      reflect.ValueOf(net.IPv6unspecified),
}

type ptrTo uint8

const (
	ptrInt8T ptrTo = iota
	ptrInt16T
	ptrInt32T
	ptrInt64T
	ptrUInt8T
	ptrUInt16T
	ptrUInt32T
	ptrUInt64T
	ptrFloat32
	ptrFloat64
	ptrString
	ptrTime
	ptrIPv4
	ptrIPv6
)

var arrayBaseTypes = map[interface{}]reflect.Type{
	int8(0):     reflect.ValueOf(int8(0)).Type(),
	int16(0):    reflect.ValueOf(int16(0)).Type(),
	int32(0):    reflect.ValueOf(int32(0)).Type(),
	int64(0):    reflect.ValueOf(int64(0)).Type(),
	uint8(0):    reflect.ValueOf(uint8(0)).Type(),
	uint16(0):   reflect.ValueOf(uint16(0)).Type(),
	uint32(0):   reflect.ValueOf(uint32(0)).Type(),
	uint64(0):   reflect.ValueOf(uint64(0)).Type(),
	float32(0):  reflect.ValueOf(float32(0)).Type(),
	float64(0):  reflect.ValueOf(float64(0)).Type(),
	string(""):  reflect.ValueOf(string("")).Type(),
	time.Time{}: reflect.ValueOf(time.Time{}).Type(),
	IPv4{}:      reflect.ValueOf(net.IPv4zero).Type(),
	IPv6{}:      reflect.ValueOf(net.IPv6unspecified).Type(),

	// nullable
	ptrInt8T:   reflect.PtrTo(reflect.ValueOf(int8(0)).Type()),
	ptrInt16T:  reflect.PtrTo(reflect.ValueOf(int16(0)).Type()),
	ptrInt32T:  reflect.PtrTo(reflect.ValueOf(int32(0)).Type()),
	ptrInt64T:  reflect.PtrTo(reflect.ValueOf(int64(0)).Type()),
	ptrUInt8T:  reflect.PtrTo(reflect.ValueOf(uint8(0)).Type()),
	ptrUInt16T: reflect.PtrTo(reflect.ValueOf(uint16(0)).Type()),
	ptrUInt32T: reflect.PtrTo(reflect.ValueOf(uint32(0)).Type()),
	ptrUInt64T: reflect.PtrTo(reflect.ValueOf(uint64(0)).Type()),
	ptrFloat32: reflect.PtrTo(reflect.ValueOf(float32(0)).Type()),
	ptrFloat64: reflect.PtrTo(reflect.ValueOf(float64(0)).Type()),
	ptrString:  reflect.PtrTo(reflect.ValueOf(string("")).Type()),
	ptrTime:    reflect.PtrTo(reflect.ValueOf(time.Time{}).Type()),
	ptrIPv4:    reflect.PtrTo(reflect.ValueOf(net.IPv4zero).Type()),
	ptrIPv6:    reflect.PtrTo(reflect.ValueOf(net.IPv6unspecified).Type()),
}

type base struct {
	name, chType string
	valueOf      reflect.Value
}

func (base *base) Name() string {
	return base.name
}

func (base *base) CHType() string {
	return base.chType
}

func (base *base) ScanType() reflect.Type {
	return base.valueOf.Type()
}

func (base *base) defaultValue() interface{} {
	return base.valueOf.Interface()
}

func (base *base) String() string {
	return fmt.Sprintf("%s (%s)", base.name, base.chType)
}

func (base *base) Depth() int {
	return 0
}
