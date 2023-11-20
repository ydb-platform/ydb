package json_test

import (
	"bytes"
	stdjson "encoding/json"

	"github.com/goccy/go-json"
)

func intptr(v int) *int                       { return &v }
func intptr3(v int) ***int                    { vv := &v; vvv := &vv; return &vvv }
func int8ptr(v int8) *int8                    { return &v }
func int8ptr3(v int8) ***int8                 { vv := &v; vvv := &vv; return &vvv }
func int16ptr(v int16) *int16                 { return &v }
func int16ptr3(v int16) ***int16              { vv := &v; vvv := &vv; return &vvv }
func int32ptr(v int32) *int32                 { return &v }
func int32ptr3(v int32) ***int32              { vv := &v; vvv := &vv; return &vvv }
func int64ptr(v int64) *int64                 { return &v }
func int64ptr3(v int64) ***int64              { vv := &v; vvv := &vv; return &vvv }
func uptr(v uint) *uint                       { return &v }
func uintptr3(v uint) ***uint                 { vv := &v; vvv := &vv; return &vvv }
func uint8ptr(v uint8) *uint8                 { return &v }
func uint8ptr3(v uint8) ***uint8              { vv := &v; vvv := &vv; return &vvv }
func uint16ptr(v uint16) *uint16              { return &v }
func uint16ptr3(v uint16) ***uint16           { vv := &v; vvv := &vv; return &vvv }
func uint32ptr(v uint32) *uint32              { return &v }
func uint32ptr3(v uint32) ***uint32           { vv := &v; vvv := &vv; return &vvv }
func uint64ptr(v uint64) *uint64              { return &v }
func uint64ptr3(v uint64) ***uint64           { vv := &v; vvv := &vv; return &vvv }
func float32ptr(v float32) *float32           { return &v }
func float32ptr3(v float32) ***float32        { vv := &v; vvv := &vv; return &vvv }
func float64ptr(v float64) *float64           { return &v }
func float64ptr3(v float64) ***float64        { vv := &v; vvv := &vv; return &vvv }
func stringptr(v string) *string              { return &v }
func stringptr3(v string) ***string           { vv := &v; vvv := &vv; return &vvv }
func boolptr(v bool) *bool                    { return &v }
func boolptr3(v bool) ***bool                 { vv := &v; vvv := &vv; return &vvv }
func bytesptr(v []byte) *[]byte               { return &v }
func bytesptr3(v []byte) ***[]byte            { vv := &v; vvv := &vv; return &vvv }
func numberptr(v json.Number) *json.Number    { return &v }
func numberptr3(v json.Number) ***json.Number { vv := &v; vvv := &vv; return &vvv }
func sliceptr(v []int) *[]int                 { return &v }
func arrayptr(v [2]int) *[2]int               { return &v }
func mapptr(v map[string]int) *map[string]int { return &v }

func encodeByEncodingJSON(data interface{}, indent, escape bool) string {
	var buf bytes.Buffer
	enc := stdjson.NewEncoder(&buf)
	enc.SetEscapeHTML(escape)
	if indent {
		enc.SetIndent("", "  ")
	}
	enc.Encode(data)
	return buf.String()
}
