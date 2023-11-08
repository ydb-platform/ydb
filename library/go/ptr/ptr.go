package ptr

import "time"

// Int returns pointer to provided value
func Int(v int) *int { return &v }

// Int8 returns pointer to provided value
func Int8(v int8) *int8 { return &v }

// Int16 returns pointer to provided value
func Int16(v int16) *int16 { return &v }

// Int32 returns pointer to provided value
func Int32(v int32) *int32 { return &v }

// Int64 returns pointer to provided value
func Int64(v int64) *int64 { return &v }

// Uint returns pointer to provided value
func Uint(v uint) *uint { return &v }

// Uint8 returns pointer to provided value
func Uint8(v uint8) *uint8 { return &v }

// Uint16 returns pointer to provided value
func Uint16(v uint16) *uint16 { return &v }

// Uint32 returns pointer to provided value
func Uint32(v uint32) *uint32 { return &v }

// Uint64 returns pointer to provided value
func Uint64(v uint64) *uint64 { return &v }

// Float32 returns pointer to provided value
func Float32(v float32) *float32 { return &v }

// Float64 returns pointer to provided value
func Float64(v float64) *float64 { return &v }

// Bool returns pointer to provided value
func Bool(v bool) *bool { return &v }

// String returns pointer to provided value
func String(v string) *string { return &v }

// Byte returns pointer to provided value
func Byte(v byte) *byte { return &v }

// Rune returns pointer to provided value
func Rune(v rune) *rune { return &v }

// Complex64 returns pointer to provided value
func Complex64(v complex64) *complex64 { return &v }

// Complex128 returns pointer to provided value
func Complex128(v complex128) *complex128 { return &v }

// Time returns pointer to provided value
func Time(v time.Time) *time.Time { return &v }

// Duration returns pointer to provided value
func Duration(v time.Duration) *time.Duration { return &v }

// T returns pointer to provided value
func T[T any](v T) *T { return &v }

// From returns value from pointer
func From[T any](v *T) T {
	if v == nil {
		return *new(T)
	}

	return *v
}
