package json_test

import (
	"bytes"
	"context"
	"encoding"
	stdjson "encoding/json"
	"errors"
	"fmt"
	"image"
	"math"
	"math/big"
	"net"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/goccy/go-json"
)

func Test_Decoder(t *testing.T) {
	t.Run("int", func(t *testing.T) {
		var v int
		assertErr(t, json.Unmarshal([]byte(`-1`), &v))
		assertEq(t, "int", int(-1), v)
	})
	t.Run("int8", func(t *testing.T) {
		var v int8
		assertErr(t, json.Unmarshal([]byte(`-2`), &v))
		assertEq(t, "int8", int8(-2), v)
	})
	t.Run("int16", func(t *testing.T) {
		var v int16
		assertErr(t, json.Unmarshal([]byte(`-3`), &v))
		assertEq(t, "int16", int16(-3), v)
	})
	t.Run("int32", func(t *testing.T) {
		var v int32
		assertErr(t, json.Unmarshal([]byte(`-4`), &v))
		assertEq(t, "int32", int32(-4), v)
	})
	t.Run("int64", func(t *testing.T) {
		var v int64
		assertErr(t, json.Unmarshal([]byte(`-5`), &v))
		assertEq(t, "int64", int64(-5), v)
	})
	t.Run("uint", func(t *testing.T) {
		var v uint
		assertErr(t, json.Unmarshal([]byte(`1`), &v))
		assertEq(t, "uint", uint(1), v)
	})
	t.Run("uint8", func(t *testing.T) {
		var v uint8
		assertErr(t, json.Unmarshal([]byte(`2`), &v))
		assertEq(t, "uint8", uint8(2), v)
	})
	t.Run("uint16", func(t *testing.T) {
		var v uint16
		assertErr(t, json.Unmarshal([]byte(`3`), &v))
		assertEq(t, "uint16", uint16(3), v)
	})
	t.Run("uint32", func(t *testing.T) {
		var v uint32
		assertErr(t, json.Unmarshal([]byte(`4`), &v))
		assertEq(t, "uint32", uint32(4), v)
	})
	t.Run("uint64", func(t *testing.T) {
		var v uint64
		assertErr(t, json.Unmarshal([]byte(`5`), &v))
		assertEq(t, "uint64", uint64(5), v)
	})
	t.Run("bool", func(t *testing.T) {
		t.Run("true", func(t *testing.T) {
			var v bool
			assertErr(t, json.Unmarshal([]byte(`true`), &v))
			assertEq(t, "bool", true, v)
		})
		t.Run("false", func(t *testing.T) {
			v := true
			assertErr(t, json.Unmarshal([]byte(`false`), &v))
			assertEq(t, "bool", false, v)
		})
	})
	t.Run("string", func(t *testing.T) {
		var v string
		assertErr(t, json.Unmarshal([]byte(`"hello"`), &v))
		assertEq(t, "string", "hello", v)
	})
	t.Run("float32", func(t *testing.T) {
		var v float32
		assertErr(t, json.Unmarshal([]byte(`3.14`), &v))
		assertEq(t, "float32", float32(3.14), v)
	})
	t.Run("float64", func(t *testing.T) {
		var v float64
		assertErr(t, json.Unmarshal([]byte(`3.14`), &v))
		assertEq(t, "float64", float64(3.14), v)
	})
	t.Run("slice", func(t *testing.T) {
		var v []int
		assertErr(t, json.Unmarshal([]byte(` [ 1 , 2 , 3 , 4 ] `), &v))
		assertEq(t, "slice", fmt.Sprint([]int{1, 2, 3, 4}), fmt.Sprint(v))
	})
	t.Run("slice_reuse_data", func(t *testing.T) {
		v := make([]int, 0, 10)
		assertErr(t, json.Unmarshal([]byte(` [ 1 , 2 , 3 , 4 ] `), &v))
		assertEq(t, "slice", fmt.Sprint([]int{1, 2, 3, 4}), fmt.Sprint(v))
		assertEq(t, "cap", 10, cap(v))
	})
	t.Run("array", func(t *testing.T) {
		var v [4]int
		assertErr(t, json.Unmarshal([]byte(` [ 1 , 2 , 3 , 4 ] `), &v))
		assertEq(t, "array", fmt.Sprint([4]int{1, 2, 3, 4}), fmt.Sprint(v))
	})
	t.Run("map", func(t *testing.T) {
		var v map[string]int
		assertErr(t, json.Unmarshal([]byte(` { "a": 1, "b": 2, "c": 3, "d": 4 } `), &v))
		assertEq(t, "map.a", v["a"], 1)
		assertEq(t, "map.b", v["b"], 2)
		assertEq(t, "map.c", v["c"], 3)
		assertEq(t, "map.d", v["d"], 4)
		t.Run("nested map", func(t *testing.T) {
			// https://github.com/goccy/go-json/issues/8
			content := `
{
  "a": {
    "nestedA": "value of nested a"
  },
  "b": {
    "nestedB": "value of nested b"
  },
  "c": {
    "nestedC": "value of nested c"
  }
}`
			var v map[string]interface{}
			assertErr(t, json.Unmarshal([]byte(content), &v))
			assertEq(t, "length", 3, len(v))
		})
	})
	t.Run("struct", func(t *testing.T) {
		type T struct {
			AA int    `json:"aa"`
			BB string `json:"bb"`
			CC bool   `json:"cc"`
		}
		var v struct {
			A int    `json:"abcd"`
			B string `json:"str"`
			C bool
			D *T
			E func()
		}
		content := []byte(`
{
  "abcd": 123,
  "str" : "hello",
  "c"   : true,
  "d"   : {
    "aa": 2,
    "bb": "world",
    "cc": true
  },
  "e"   : null
}`)
		assertErr(t, json.Unmarshal(content, &v))
		assertEq(t, "struct.A", 123, v.A)
		assertEq(t, "struct.B", "hello", v.B)
		assertEq(t, "struct.C", true, v.C)
		assertEq(t, "struct.D.AA", 2, v.D.AA)
		assertEq(t, "struct.D.BB", "world", v.D.BB)
		assertEq(t, "struct.D.CC", true, v.D.CC)
		assertEq(t, "struct.E", true, v.E == nil)
		t.Run("struct.field null", func(t *testing.T) {
			var v struct {
				A string
				B []string
				C []int
				D map[string]interface{}
				E [2]string
				F interface{}
				G func()
			}
			assertErr(t, json.Unmarshal([]byte(`{"a":null,"b":null,"c":null,"d":null,"e":null,"f":null,"g":null}`), &v))
			assertEq(t, "string", v.A, "")
			assertNeq(t, "[]string", v.B, nil)
			assertEq(t, "[]string", len(v.B), 0)
			assertNeq(t, "[]int", v.C, nil)
			assertEq(t, "[]int", len(v.C), 0)
			assertNeq(t, "map", v.D, nil)
			assertEq(t, "map", len(v.D), 0)
			assertNeq(t, "array", v.E, nil)
			assertEq(t, "array", len(v.E), 2)
			assertEq(t, "interface{}", v.F, nil)
			assertEq(t, "nilfunc", true, v.G == nil)
		})
	})
	t.Run("interface", func(t *testing.T) {
		t.Run("number", func(t *testing.T) {
			var v interface{}
			assertErr(t, json.Unmarshal([]byte(`10`), &v))
			assertEq(t, "interface.kind", "float64", reflect.TypeOf(v).Kind().String())
			assertEq(t, "interface", `10`, fmt.Sprint(v))
		})
		t.Run("string", func(t *testing.T) {
			var v interface{}
			assertErr(t, json.Unmarshal([]byte(`"hello"`), &v))
			assertEq(t, "interface.kind", "string", reflect.TypeOf(v).Kind().String())
			assertEq(t, "interface", `hello`, fmt.Sprint(v))
		})
		t.Run("escaped string", func(t *testing.T) {
			var v interface{}
			assertErr(t, json.Unmarshal([]byte(`"he\"llo"`), &v))
			assertEq(t, "interface.kind", "string", reflect.TypeOf(v).Kind().String())
			assertEq(t, "interface", `he"llo`, fmt.Sprint(v))
		})
		t.Run("bool", func(t *testing.T) {
			var v interface{}
			assertErr(t, json.Unmarshal([]byte(`true`), &v))
			assertEq(t, "interface.kind", "bool", reflect.TypeOf(v).Kind().String())
			assertEq(t, "interface", `true`, fmt.Sprint(v))
		})
		t.Run("slice", func(t *testing.T) {
			var v interface{}
			assertErr(t, json.Unmarshal([]byte(`[1,2,3,4]`), &v))
			assertEq(t, "interface.kind", "slice", reflect.TypeOf(v).Kind().String())
			assertEq(t, "interface", `[1 2 3 4]`, fmt.Sprint(v))
		})
		t.Run("map", func(t *testing.T) {
			var v interface{}
			assertErr(t, json.Unmarshal([]byte(`{"a": 1, "b": "c"}`), &v))
			assertEq(t, "interface.kind", "map", reflect.TypeOf(v).Kind().String())
			m := v.(map[string]interface{})
			assertEq(t, "interface", `1`, fmt.Sprint(m["a"]))
			assertEq(t, "interface", `c`, fmt.Sprint(m["b"]))
		})
		t.Run("null", func(t *testing.T) {
			var v interface{}
			v = 1
			assertErr(t, json.Unmarshal([]byte(`null`), &v))
			assertEq(t, "interface", nil, v)
		})
	})
	t.Run("func", func(t *testing.T) {
		var v func()
		assertErr(t, json.Unmarshal([]byte(`null`), &v))
		assertEq(t, "nilfunc", true, v == nil)
	})
}

func TestIssue98(t *testing.T) {
	data := "[\"\\"
	var v interface{}
	if err := json.Unmarshal([]byte(data), &v); err == nil {
		t.Fatal("expected error")
	}
}

func Test_Decoder_UseNumber(t *testing.T) {
	dec := json.NewDecoder(strings.NewReader(`{"a": 3.14}`))
	dec.UseNumber()
	var v map[string]interface{}
	assertErr(t, dec.Decode(&v))
	assertEq(t, "json.Number", "json.Number", fmt.Sprintf("%T", v["a"]))
}

func Test_Decoder_DisallowUnknownFields(t *testing.T) {
	dec := json.NewDecoder(strings.NewReader(`{"x": 1}`))
	dec.DisallowUnknownFields()
	var v struct {
		x int
	}
	err := dec.Decode(&v)
	if err == nil {
		t.Fatal("expected unknown field error")
	}
	if err.Error() != `json: unknown field "x"` {
		t.Fatal("expected unknown field error")
	}
}

func Test_Decoder_EmptyObjectWithSpace(t *testing.T) {
	dec := json.NewDecoder(strings.NewReader(`{"obj":{ }}`))
	var v struct {
		Obj map[string]int `json:"obj"`
	}
	assertErr(t, dec.Decode(&v))
}

type unmarshalJSON struct {
	v int
}

func (u *unmarshalJSON) UnmarshalJSON(b []byte) error {
	var v int
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	u.v = v
	return nil
}

func Test_UnmarshalJSON(t *testing.T) {
	t.Run("*struct", func(t *testing.T) {
		var v unmarshalJSON
		assertErr(t, json.Unmarshal([]byte(`10`), &v))
		assertEq(t, "unmarshal", 10, v.v)
	})
}

type unmarshalText struct {
	v int
}

func (u *unmarshalText) UnmarshalText(b []byte) error {
	var v int
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	u.v = v
	return nil
}

func Test_UnmarshalText(t *testing.T) {
	t.Run("*struct", func(t *testing.T) {
		var v unmarshalText
		assertErr(t, json.Unmarshal([]byte(`"11"`), &v))
		assertEq(t, "unmarshal", v.v, 11)
	})
}

func Test_InvalidUnmarshalError(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		var v *struct{}
		err := fmt.Sprint(json.Unmarshal([]byte(`{}`), v))
		assertEq(t, "invalid unmarshal error", "json: Unmarshal(nil *struct {})", err)
	})
	t.Run("non pointer", func(t *testing.T) {
		var v int
		err := fmt.Sprint(json.Unmarshal([]byte(`{}`), v))
		assertEq(t, "invalid unmarshal error", "json: Unmarshal(non-pointer int)", err)
	})
}

func Test_Token(t *testing.T) {
	dec := json.NewDecoder(strings.NewReader(`{"a": 1, "b": true, "c": [1, "two", null]}`))
	cnt := 0
	for {
		if _, err := dec.Token(); err != nil {
			break
		}
		cnt++
	}
	if cnt != 12 {
		t.Fatal("failed to parse token")
	}
}

func Test_DecodeStream(t *testing.T) {
	const stream = `
	[
		{"Name": "Ed", "Text": "Knock knock."},
		{"Name": "Sam", "Text": "Who's there?"},
		{"Name": "Ed", "Text": "Go fmt."},
		{"Name": "Sam", "Text": "Go fmt who?"},
		{"Name": "Ed", "Text": "Go fmt yourself!"}
	]
`
	type Message struct {
		Name, Text string
	}
	dec := json.NewDecoder(strings.NewReader(stream))

	tk, err := dec.Token()
	assertErr(t, err)
	assertEq(t, "[", fmt.Sprint(tk), "[")

	elem := 0
	// while the array contains values
	for dec.More() {
		var m Message
		// decode an array value (Message)
		assertErr(t, dec.Decode(&m))
		if m.Name == "" || m.Text == "" {
			t.Fatal("failed to assign value to struct field")
		}
		elem++
	}
	assertEq(t, "decode count", elem, 5)

	tk, err = dec.Token()
	assertErr(t, err)
	assertEq(t, "]", fmt.Sprint(tk), "]")
}

type T struct {
	X string
	Y int
	Z int `json:"-"`
}

type U struct {
	Alphabet string `json:"alpha"`
}

type V struct {
	F1 interface{}
	F2 int32
	F3 json.Number
	F4 *VOuter
}

type VOuter struct {
	V V
}

type W struct {
	S SS
}

type P struct {
	PP PP
}

type PP struct {
	T  T
	Ts []T
}

type SS string

func (*SS) UnmarshalJSON(data []byte) error {
	return &json.UnmarshalTypeError{Value: "number", Type: reflect.TypeOf(SS(""))}
}

// ifaceNumAsFloat64/ifaceNumAsNumber are used to test unmarshaling with and
// without UseNumber
var ifaceNumAsFloat64 = map[string]interface{}{
	"k1": float64(1),
	"k2": "s",
	"k3": []interface{}{float64(1), float64(2.0), float64(3e-3)},
	"k4": map[string]interface{}{"kk1": "s", "kk2": float64(2)},
}

var ifaceNumAsNumber = map[string]interface{}{
	"k1": json.Number("1"),
	"k2": "s",
	"k3": []interface{}{json.Number("1"), json.Number("2.0"), json.Number("3e-3")},
	"k4": map[string]interface{}{"kk1": "s", "kk2": json.Number("2")},
}

type tx struct {
	x int
}

type u8 uint8

// A type that can unmarshal itself.

type unmarshaler struct {
	T bool
}

func (u *unmarshaler) UnmarshalJSON(b []byte) error {
	*u = unmarshaler{true} // All we need to see that UnmarshalJSON is called.
	return nil
}

type ustruct struct {
	M unmarshaler
}

var _ encoding.TextUnmarshaler = (*unmarshalerText)(nil)

type ustructText struct {
	M unmarshalerText
}

// u8marshal is an integer type that can marshal/unmarshal itself.
type u8marshal uint8

func (u8 u8marshal) MarshalText() ([]byte, error) {
	return []byte(fmt.Sprintf("u%d", u8)), nil
}

var errMissingU8Prefix = errors.New("missing 'u' prefix")

func (u8 *u8marshal) UnmarshalText(b []byte) error {
	if !bytes.HasPrefix(b, []byte{'u'}) {
		return errMissingU8Prefix
	}
	n, err := strconv.Atoi(string(b[1:]))
	if err != nil {
		return err
	}
	*u8 = u8marshal(n)
	return nil
}

var _ encoding.TextUnmarshaler = (*u8marshal)(nil)

var (
	umtrue   = unmarshaler{true}
	umslice  = []unmarshaler{{true}}
	umstruct = ustruct{unmarshaler{true}}

	umtrueXY   = unmarshalerText{"x", "y"}
	umsliceXY  = []unmarshalerText{{"x", "y"}}
	umstructXY = ustructText{unmarshalerText{"x", "y"}}

	ummapXY = map[unmarshalerText]bool{{"x", "y"}: true}
)

// Test data structures for anonymous fields.

type Point struct {
	Z int
}

type Top struct {
	Level0 int
	Embed0
	*Embed0a
	*Embed0b `json:"e,omitempty"` // treated as named
	Embed0c  `json:"-"`           // ignored
	Loop
	Embed0p // has Point with X, Y, used
	Embed0q // has Point with Z, used
	embed   // contains exported field
}

type Embed0 struct {
	Level1a int // overridden by Embed0a's Level1a with json tag
	Level1b int // used because Embed0a's Level1b is renamed
	Level1c int // used because Embed0a's Level1c is ignored
	Level1d int // annihilated by Embed0a's Level1d
	Level1e int `json:"x"` // annihilated by Embed0a.Level1e
}

type Embed0a struct {
	Level1a int `json:"Level1a,omitempty"`
	Level1b int `json:"LEVEL1B,omitempty"`
	Level1c int `json:"-"`
	Level1d int // annihilated by Embed0's Level1d
	Level1f int `json:"x"` // annihilated by Embed0's Level1e
}

type Embed0b Embed0

type Embed0c Embed0

type Embed0p struct {
	image.Point
}

type Embed0q struct {
	Point
}

type embed struct {
	Q int
}

type Loop struct {
	Loop1 int `json:",omitempty"`
	Loop2 int `json:",omitempty"`
	*Loop
}

// From reflect test:
// The X in S6 and S7 annihilate, but they also block the X in S8.S9.
type S5 struct {
	S6
	S7
	S8
}

type S6 struct {
	X int
}

type S7 S6

type S8 struct {
	S9
}

type S9 struct {
	X int
	Y int
}

// From reflect test:
// The X in S11.S6 and S12.S6 annihilate, but they also block the X in S13.S8.S9.
type S10 struct {
	S11
	S12
	S13
}

type S11 struct {
	S6
}

type S12 struct {
	S6
}

type S13 struct {
	S8
}

type Ambig struct {
	// Given "hello", the first match should win.
	First  int `json:"HELLO"`
	Second int `json:"Hello"`
}

type XYZ struct {
	X interface{}
	Y interface{}
	Z interface{}
}

type unexportedWithMethods struct{}

func (unexportedWithMethods) F() {}

type byteWithMarshalJSON byte

func (b byteWithMarshalJSON) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"Z%.2x"`, byte(b))), nil
}

func (b *byteWithMarshalJSON) UnmarshalJSON(data []byte) error {
	if len(data) != 5 || data[0] != '"' || data[1] != 'Z' || data[4] != '"' {
		return fmt.Errorf("bad quoted string")
	}
	i, err := strconv.ParseInt(string(data[2:4]), 16, 8)
	if err != nil {
		return fmt.Errorf("bad hex")
	}
	*b = byteWithMarshalJSON(i)
	return nil
}

type byteWithPtrMarshalJSON byte

func (b *byteWithPtrMarshalJSON) MarshalJSON() ([]byte, error) {
	return byteWithMarshalJSON(*b).MarshalJSON()
}

func (b *byteWithPtrMarshalJSON) UnmarshalJSON(data []byte) error {
	return (*byteWithMarshalJSON)(b).UnmarshalJSON(data)
}

type byteWithMarshalText byte

func (b byteWithMarshalText) MarshalText() ([]byte, error) {
	return []byte(fmt.Sprintf(`Z%.2x`, byte(b))), nil
}

func (b *byteWithMarshalText) UnmarshalText(data []byte) error {
	if len(data) != 3 || data[0] != 'Z' {
		return fmt.Errorf("bad quoted string")
	}
	i, err := strconv.ParseInt(string(data[1:3]), 16, 8)
	if err != nil {
		return fmt.Errorf("bad hex")
	}
	*b = byteWithMarshalText(i)
	return nil
}

type byteWithPtrMarshalText byte

func (b *byteWithPtrMarshalText) MarshalText() ([]byte, error) {
	return byteWithMarshalText(*b).MarshalText()
}

func (b *byteWithPtrMarshalText) UnmarshalText(data []byte) error {
	return (*byteWithMarshalText)(b).UnmarshalText(data)
}

type intWithMarshalJSON int

func (b intWithMarshalJSON) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"Z%.2x"`, int(b))), nil
}

func (b *intWithMarshalJSON) UnmarshalJSON(data []byte) error {
	if len(data) != 5 || data[0] != '"' || data[1] != 'Z' || data[4] != '"' {
		return fmt.Errorf("bad quoted string")
	}
	i, err := strconv.ParseInt(string(data[2:4]), 16, 8)
	if err != nil {
		return fmt.Errorf("bad hex")
	}
	*b = intWithMarshalJSON(i)
	return nil
}

type intWithPtrMarshalJSON int

func (b *intWithPtrMarshalJSON) MarshalJSON() ([]byte, error) {
	return intWithMarshalJSON(*b).MarshalJSON()
}

func (b *intWithPtrMarshalJSON) UnmarshalJSON(data []byte) error {
	return (*intWithMarshalJSON)(b).UnmarshalJSON(data)
}

type intWithMarshalText int

func (b intWithMarshalText) MarshalText() ([]byte, error) {
	return []byte(fmt.Sprintf(`Z%.2x`, int(b))), nil
}

func (b *intWithMarshalText) UnmarshalText(data []byte) error {
	if len(data) != 3 || data[0] != 'Z' {
		return fmt.Errorf("bad quoted string")
	}
	i, err := strconv.ParseInt(string(data[1:3]), 16, 8)
	if err != nil {
		return fmt.Errorf("bad hex")
	}
	*b = intWithMarshalText(i)
	return nil
}

type intWithPtrMarshalText int

func (b *intWithPtrMarshalText) MarshalText() ([]byte, error) {
	return intWithMarshalText(*b).MarshalText()
}

func (b *intWithPtrMarshalText) UnmarshalText(data []byte) error {
	return (*intWithMarshalText)(b).UnmarshalText(data)
}

type mapStringToStringData struct {
	Data map[string]string `json:"data"`
}

type unmarshalTest struct {
	in                    string
	ptr                   interface{} // new(type)
	out                   interface{}
	err                   error
	useNumber             bool
	golden                bool
	disallowUnknownFields bool
}

type B struct {
	B bool `json:",string"`
}

type DoublePtr struct {
	I **int
	J **int
}

var unmarshalTests = []unmarshalTest{
	// basic types
	{in: `true`, ptr: new(bool), out: true},                                                                                                                       // 0
	{in: `1`, ptr: new(int), out: 1},                                                                                                                              // 1
	{in: `1.2`, ptr: new(float64), out: 1.2},                                                                                                                      // 2
	{in: `-5`, ptr: new(int16), out: int16(-5)},                                                                                                                   // 3
	{in: `2`, ptr: new(json.Number), out: json.Number("2"), useNumber: true},                                                                                      // 4
	{in: `2`, ptr: new(json.Number), out: json.Number("2")},                                                                                                       // 5
	{in: `2`, ptr: new(interface{}), out: float64(2.0)},                                                                                                           // 6
	{in: `2`, ptr: new(interface{}), out: json.Number("2"), useNumber: true},                                                                                      // 7
	{in: `"a\u1234"`, ptr: new(string), out: "a\u1234"},                                                                                                           // 8
	{in: `"http:\/\/"`, ptr: new(string), out: "http://"},                                                                                                         // 9
	{in: `"g-clef: \uD834\uDD1E"`, ptr: new(string), out: "g-clef: \U0001D11E"},                                                                                   // 10
	{in: `"invalid: \uD834x\uDD1E"`, ptr: new(string), out: "invalid: \uFFFDx\uFFFD"},                                                                             // 11
	{in: "null", ptr: new(interface{}), out: nil},                                                                                                                 // 12
	{in: `{"X": [1,2,3], "Y": 4}`, ptr: new(T), out: T{Y: 4}, err: &json.UnmarshalTypeError{"array", reflect.TypeOf(""), 7, "T", "X"}},                            // 13
	{in: `{"X": 23}`, ptr: new(T), out: T{}, err: &json.UnmarshalTypeError{"number", reflect.TypeOf(""), 8, "T", "X"}}, {in: `{"x": 1}`, ptr: new(tx), out: tx{}}, // 14
	{in: `{"x": 1}`, ptr: new(tx), out: tx{}}, // 15, 16
	{in: `{"x": 1}`, ptr: new(tx), err: fmt.Errorf("json: unknown field \"x\""), disallowUnknownFields: true},                           // 17
	{in: `{"S": 23}`, ptr: new(W), out: W{}, err: &json.UnmarshalTypeError{"number", reflect.TypeOf(SS("")), 0, "W", "S"}},              // 18
	{in: `{"F1":1,"F2":2,"F3":3}`, ptr: new(V), out: V{F1: float64(1), F2: int32(2), F3: json.Number("3")}},                             // 19
	{in: `{"F1":1,"F2":2,"F3":3}`, ptr: new(V), out: V{F1: json.Number("1"), F2: int32(2), F3: json.Number("3")}, useNumber: true},      // 20
	{in: `{"k1":1,"k2":"s","k3":[1,2.0,3e-3],"k4":{"kk1":"s","kk2":2}}`, ptr: new(interface{}), out: ifaceNumAsFloat64},                 // 21
	{in: `{"k1":1,"k2":"s","k3":[1,2.0,3e-3],"k4":{"kk1":"s","kk2":2}}`, ptr: new(interface{}), out: ifaceNumAsNumber, useNumber: true}, // 22

	// raw values with whitespace
	{in: "\n true ", ptr: new(bool), out: true},                  // 23
	{in: "\t 1 ", ptr: new(int), out: 1},                         // 24
	{in: "\r 1.2 ", ptr: new(float64), out: 1.2},                 // 25
	{in: "\t -5 \n", ptr: new(int16), out: int16(-5)},            // 26
	{in: "\t \"a\\u1234\" \n", ptr: new(string), out: "a\u1234"}, // 27

	// Z has a "-" tag.
	{in: `{"Y": 1, "Z": 2}`, ptr: new(T), out: T{Y: 1}},                                                              // 28
	{in: `{"Y": 1, "Z": 2}`, ptr: new(T), err: fmt.Errorf("json: unknown field \"Z\""), disallowUnknownFields: true}, // 29

	{in: `{"alpha": "abc", "alphabet": "xyz"}`, ptr: new(U), out: U{Alphabet: "abc"}},                                                          // 30
	{in: `{"alpha": "abc", "alphabet": "xyz"}`, ptr: new(U), err: fmt.Errorf("json: unknown field \"alphabet\""), disallowUnknownFields: true}, // 31
	{in: `{"alpha": "abc"}`, ptr: new(U), out: U{Alphabet: "abc"}},                                                                             // 32
	{in: `{"alphabet": "xyz"}`, ptr: new(U), out: U{}},                                                                                         // 33
	{in: `{"alphabet": "xyz"}`, ptr: new(U), err: fmt.Errorf("json: unknown field \"alphabet\""), disallowUnknownFields: true},                 // 34

	// syntax errors
	{in: `{"X": "foo", "Y"}`, err: json.NewSyntaxError("invalid character '}' after object key", 17)},                                              // 35
	{in: `[1, 2, 3+]`, err: json.NewSyntaxError("invalid character '+' after array element", 9)},                                                   // 36
	{in: `{"X":12x}`, err: json.NewSyntaxError("invalid character 'x' after object key:value pair", 8), useNumber: true},                           // 37
	{in: `[2, 3`, err: json.NewSyntaxError("unexpected end of JSON input", 5)},                                                                     // 38
	{in: `{"F3": -}`, ptr: new(V), out: V{F3: json.Number("-")}, err: json.NewSyntaxError("strconv.ParseFloat: parsing \"-\": invalid syntax", 9)}, // 39

	// raw value errors
	{in: "\x01 42", err: json.NewSyntaxError("invalid character '\\x01' looking for beginning of value", 1)},         // 40
	{in: " 42 \x01", err: json.NewSyntaxError("invalid character '\\x01' after top-level value", 5)},                 // 41
	{in: "\x01 true", err: json.NewSyntaxError("invalid character '\\x01' looking for beginning of value", 1)},       // 42
	{in: " false \x01", err: json.NewSyntaxError("invalid character '\\x01' after top-level value", 8)},              // 43
	{in: "\x01 1.2", err: json.NewSyntaxError("invalid character '\\x01' looking for beginning of value", 1)},        // 44
	{in: " 3.4 \x01", err: json.NewSyntaxError("invalid character '\\x01' after top-level value", 6)},                // 45
	{in: "\x01 \"string\"", err: json.NewSyntaxError("invalid character '\\x01' looking for beginning of value", 1)}, // 46
	{in: " \"string\" \x01", err: json.NewSyntaxError("invalid character '\\x01' after top-level value", 11)},        // 47

	// array tests
	{in: `[1, 2, 3]`, ptr: new([3]int), out: [3]int{1, 2, 3}},                                           // 48
	{in: `[1, 2, 3]`, ptr: new([1]int), out: [1]int{1}},                                                 // 49
	{in: `[1, 2, 3]`, ptr: new([5]int), out: [5]int{1, 2, 3, 0, 0}},                                     // 50
	{in: `[1, 2, 3]`, ptr: new(MustNotUnmarshalJSON), err: errors.New("MustNotUnmarshalJSON was used")}, // 51

	// empty array to interface test
	{in: `[]`, ptr: new([]interface{}), out: []interface{}{}},                                                //52
	{in: `null`, ptr: new([]interface{}), out: []interface{}(nil)},                                           //53
	{in: `{"T":[]}`, ptr: new(map[string]interface{}), out: map[string]interface{}{"T": []interface{}{}}},    //54
	{in: `{"T":null}`, ptr: new(map[string]interface{}), out: map[string]interface{}{"T": interface{}(nil)}}, // 55

	// composite tests
	{in: allValueIndent, ptr: new(All), out: allValue},      // 56
	{in: allValueCompact, ptr: new(All), out: allValue},     // 57
	{in: allValueIndent, ptr: new(*All), out: &allValue},    // 58
	{in: allValueCompact, ptr: new(*All), out: &allValue},   // 59
	{in: pallValueIndent, ptr: new(All), out: pallValue},    // 60
	{in: pallValueCompact, ptr: new(All), out: pallValue},   // 61
	{in: pallValueIndent, ptr: new(*All), out: &pallValue},  // 62
	{in: pallValueCompact, ptr: new(*All), out: &pallValue}, // 63

	// unmarshal interface test
	{in: `{"T":false}`, ptr: new(unmarshaler), out: umtrue},        // use "false" so test will fail if custom unmarshaler is not called
	{in: `{"T":false}`, ptr: new(*unmarshaler), out: &umtrue},      // 65
	{in: `[{"T":false}]`, ptr: new([]unmarshaler), out: umslice},   // 66
	{in: `[{"T":false}]`, ptr: new(*[]unmarshaler), out: &umslice}, // 67
	{in: `{"M":{"T":"x:y"}}`, ptr: new(ustruct), out: umstruct},    // 68

	// UnmarshalText interface test
	{in: `"x:y"`, ptr: new(unmarshalerText), out: umtrueXY},        // 69
	{in: `"x:y"`, ptr: new(*unmarshalerText), out: &umtrueXY},      // 70
	{in: `["x:y"]`, ptr: new([]unmarshalerText), out: umsliceXY},   // 71
	{in: `["x:y"]`, ptr: new(*[]unmarshalerText), out: &umsliceXY}, // 72
	{in: `{"M":"x:y"}`, ptr: new(ustructText), out: umstructXY},    // 73
	// integer-keyed map test
	{
		in:  `{"-1":"a","0":"b","1":"c"}`, // 74
		ptr: new(map[int]string),
		out: map[int]string{-1: "a", 0: "b", 1: "c"},
	},
	{
		in:  `{"0":"a","10":"c","9":"b"}`, // 75
		ptr: new(map[u8]string),
		out: map[u8]string{0: "a", 9: "b", 10: "c"},
	},
	{
		in:  `{"-9223372036854775808":"min","9223372036854775807":"max"}`, // 76
		ptr: new(map[int64]string),
		out: map[int64]string{math.MinInt64: "min", math.MaxInt64: "max"},
	},
	{
		in:  `{"18446744073709551615":"max"}`, // 77
		ptr: new(map[uint64]string),
		out: map[uint64]string{math.MaxUint64: "max"},
	},
	{
		in:  `{"0":false,"10":true}`, // 78
		ptr: new(map[uintptr]bool),
		out: map[uintptr]bool{0: false, 10: true},
	},
	// Check that MarshalText and UnmarshalText take precedence
	// over default integer handling in map keys.
	{
		in:  `{"u2":4}`, // 79
		ptr: new(map[u8marshal]int),
		out: map[u8marshal]int{2: 4},
	},
	{
		in:  `{"2":4}`, // 80
		ptr: new(map[u8marshal]int),
		err: errMissingU8Prefix,
	},
	// integer-keyed map errors
	{
		in:  `{"abc":"abc"}`, // 81
		ptr: new(map[int]string),
		err: &json.UnmarshalTypeError{Value: "number a", Type: reflect.TypeOf(0), Offset: 2},
	},
	{
		in:  `{"256":"abc"}`, // 82
		ptr: new(map[uint8]string),
		err: &json.UnmarshalTypeError{Value: "number 256", Type: reflect.TypeOf(uint8(0)), Offset: 2},
	},
	{
		in:  `{"128":"abc"}`, // 83
		ptr: new(map[int8]string),
		err: &json.UnmarshalTypeError{Value: "number 128", Type: reflect.TypeOf(int8(0)), Offset: 2},
	},
	{
		in:  `{"-1":"abc"}`, // 84
		ptr: new(map[uint8]string),
		err: &json.UnmarshalTypeError{Value: "number -", Type: reflect.TypeOf(uint8(0)), Offset: 2},
	},
	{
		in:  `{"F":{"a":2,"3":4}}`, // 85
		ptr: new(map[string]map[int]int),
		err: &json.UnmarshalTypeError{Value: "number a", Type: reflect.TypeOf(int(0)), Offset: 7},
	},
	{
		in:  `{"F":{"a":2,"3":4}}`, // 86
		ptr: new(map[string]map[uint]int),
		err: &json.UnmarshalTypeError{Value: "number a", Type: reflect.TypeOf(uint(0)), Offset: 7},
	},
	// Map keys can be encoding.TextUnmarshalers.
	{in: `{"x:y":true}`, ptr: new(map[unmarshalerText]bool), out: ummapXY}, // 87
	// If multiple values for the same key exists, only the most recent value is used.
	{in: `{"x:y":false,"x:y":true}`, ptr: new(map[unmarshalerText]bool), out: ummapXY}, // 88
	{ // 89
		in: `{
							"Level0": 1,
							"Level1b": 2,
							"Level1c": 3,
							"x": 4,
							"Level1a": 5,
							"LEVEL1B": 6,
							"e": {
								"Level1a": 8,
								"Level1b": 9,
								"Level1c": 10,
								"Level1d": 11,
								"x": 12
							},
							"Loop1": 13,
							"Loop2": 14,
							"X": 15,
							"Y": 16,
							"Z": 17,
							"Q": 18
						}`,
		ptr: new(Top),
		out: Top{
			Level0: 1,
			Embed0: Embed0{
				Level1b: 2,
				Level1c: 3,
			},
			Embed0a: &Embed0a{
				Level1a: 5,
				Level1b: 6,
			},
			Embed0b: &Embed0b{
				Level1a: 8,
				Level1b: 9,
				Level1c: 10,
				Level1d: 11,
				Level1e: 12,
			},
			Loop: Loop{
				Loop1: 13,
				Loop2: 14,
			},
			Embed0p: Embed0p{
				Point: image.Point{X: 15, Y: 16},
			},
			Embed0q: Embed0q{
				Point: Point{Z: 17},
			},
			embed: embed{
				Q: 18,
			},
		},
	},
	{
		in:  `{"hello": 1}`, // 90
		ptr: new(Ambig),
		out: Ambig{First: 1},
	},
	{
		in:  `{"X": 1,"Y":2}`, // 91
		ptr: new(S5),
		out: S5{S8: S8{S9: S9{Y: 2}}},
	},
	{
		in:                    `{"X": 1,"Y":2}`, // 92
		ptr:                   new(S5),
		err:                   fmt.Errorf("json: unknown field \"X\""),
		disallowUnknownFields: true,
	},
	{
		in:  `{"X": 1,"Y":2}`, // 93
		ptr: new(S10),
		out: S10{S13: S13{S8: S8{S9: S9{Y: 2}}}},
	},
	{
		in:                    `{"X": 1,"Y":2}`, // 94
		ptr:                   new(S10),
		err:                   fmt.Errorf("json: unknown field \"X\""),
		disallowUnknownFields: true,
	},
	{
		in:  `{"I": 0, "I": null, "J": null}`, // 95
		ptr: new(DoublePtr),
		out: DoublePtr{I: nil, J: nil},
	},
	{
		in:  "\"hello\\ud800world\"", // 96
		ptr: new(string),
		out: "hello\ufffdworld",
	},
	{
		in:  "\"hello\\ud800\\ud800world\"", // 97
		ptr: new(string),
		out: "hello\ufffd\ufffdworld",
	},
	{
		in:  "\"hello\\ud800\\ud800world\"", // 98
		ptr: new(string),
		out: "hello\ufffd\ufffdworld",
	},
	// Used to be issue 8305, but time.Time implements encoding.TextUnmarshaler so this works now.
	{
		in:  `{"2009-11-10T23:00:00Z": "hello world"}`, // 99
		ptr: new(map[time.Time]string),
		out: map[time.Time]string{time.Date(2009, 11, 10, 23, 0, 0, 0, time.UTC): "hello world"},
	},
	// issue 8305
	{
		in:  `{"2009-11-10T23:00:00Z": "hello world"}`, // 100
		ptr: new(map[Point]string),
		err: &json.UnmarshalTypeError{Value: "object", Type: reflect.TypeOf(Point{}), Offset: 0},
	},
	{
		in:  `{"asdf": "hello world"}`, // 101
		ptr: new(map[unmarshaler]string),
		err: &json.UnmarshalTypeError{Value: "object", Type: reflect.TypeOf(unmarshaler{}), Offset: 1},
	},
	// related to issue 13783.
	// Go 1.7 changed marshaling a slice of typed byte to use the methods on the byte type,
	// similar to marshaling a slice of typed int.
	// These tests check that, assuming the byte type also has valid decoding methods,
	// either the old base64 string encoding or the new per-element encoding can be
	// successfully unmarshaled. The custom unmarshalers were accessible in earlier
	// versions of Go, even though the custom marshaler was not.
	{
		in:  `"AQID"`, // 102
		ptr: new([]byteWithMarshalJSON),
		out: []byteWithMarshalJSON{1, 2, 3},
	},
	{
		in:     `["Z01","Z02","Z03"]`, // 103
		ptr:    new([]byteWithMarshalJSON),
		out:    []byteWithMarshalJSON{1, 2, 3},
		golden: true,
	},
	{
		in:  `"AQID"`, // 104
		ptr: new([]byteWithMarshalText),
		out: []byteWithMarshalText{1, 2, 3},
	},
	{
		in:     `["Z01","Z02","Z03"]`, // 105
		ptr:    new([]byteWithMarshalText),
		out:    []byteWithMarshalText{1, 2, 3},
		golden: true,
	},
	{
		in:  `"AQID"`, // 106
		ptr: new([]byteWithPtrMarshalJSON),
		out: []byteWithPtrMarshalJSON{1, 2, 3},
	},
	{
		in:     `["Z01","Z02","Z03"]`, // 107
		ptr:    new([]byteWithPtrMarshalJSON),
		out:    []byteWithPtrMarshalJSON{1, 2, 3},
		golden: true,
	},
	{
		in:  `"AQID"`, // 108
		ptr: new([]byteWithPtrMarshalText),
		out: []byteWithPtrMarshalText{1, 2, 3},
	},
	{
		in:     `["Z01","Z02","Z03"]`, // 109
		ptr:    new([]byteWithPtrMarshalText),
		out:    []byteWithPtrMarshalText{1, 2, 3},
		golden: true,
	},

	// ints work with the marshaler but not the base64 []byte case
	{
		in:     `["Z01","Z02","Z03"]`, // 110
		ptr:    new([]intWithMarshalJSON),
		out:    []intWithMarshalJSON{1, 2, 3},
		golden: true,
	},
	{
		in:     `["Z01","Z02","Z03"]`, // 111
		ptr:    new([]intWithMarshalText),
		out:    []intWithMarshalText{1, 2, 3},
		golden: true,
	},
	{
		in:     `["Z01","Z02","Z03"]`, // 112
		ptr:    new([]intWithPtrMarshalJSON),
		out:    []intWithPtrMarshalJSON{1, 2, 3},
		golden: true,
	},
	{
		in:     `["Z01","Z02","Z03"]`, // 113
		ptr:    new([]intWithPtrMarshalText),
		out:    []intWithPtrMarshalText{1, 2, 3},
		golden: true,
	},

	{in: `0.000001`, ptr: new(float64), out: 0.000001, golden: true},                               // 114
	{in: `1e-07`, ptr: new(float64), out: 1e-7, golden: true},                                      // 115
	{in: `100000000000000000000`, ptr: new(float64), out: 100000000000000000000.0, golden: true},   // 116
	{in: `1e+21`, ptr: new(float64), out: 1e21, golden: true},                                      // 117
	{in: `-0.000001`, ptr: new(float64), out: -0.000001, golden: true},                             // 118
	{in: `-1e-07`, ptr: new(float64), out: -1e-7, golden: true},                                    // 119
	{in: `-100000000000000000000`, ptr: new(float64), out: -100000000000000000000.0, golden: true}, // 120
	{in: `-1e+21`, ptr: new(float64), out: -1e21, golden: true},                                    // 121
	{in: `999999999999999900000`, ptr: new(float64), out: 999999999999999900000.0, golden: true},   // 122
	{in: `9007199254740992`, ptr: new(float64), out: 9007199254740992.0, golden: true},             // 123
	{in: `9007199254740993`, ptr: new(float64), out: 9007199254740992.0, golden: false},            // 124
	{
		in:  `{"V": {"F2": "hello"}}`, // 125
		ptr: new(VOuter),
		err: &json.UnmarshalTypeError{
			Value:  `number "`,
			Struct: "V",
			Field:  "F2",
			Type:   reflect.TypeOf(int32(0)),
			Offset: 20,
		},
	},
	{
		in:  `{"V": {"F4": {}, "F2": "hello"}}`, // 126
		ptr: new(VOuter),
		err: &json.UnmarshalTypeError{
			Value:  `number "`,
			Struct: "V",
			Field:  "F2",
			Type:   reflect.TypeOf(int32(0)),
			Offset: 30,
		},
	},
	// issue 15146.
	// invalid inputs in wrongStringTests below.
	{in: `{"B":"true"}`, ptr: new(B), out: B{true}, golden: true},                                                               // 127
	{in: `{"B":"false"}`, ptr: new(B), out: B{false}, golden: true},                                                             // 128
	{in: `{"B": "maybe"}`, ptr: new(B), err: errors.New(`json: bool unexpected end of JSON input`)},                             // 129
	{in: `{"B": "tru"}`, ptr: new(B), err: errors.New(`json: invalid character as true`)},                                       // 130
	{in: `{"B": "False"}`, ptr: new(B), err: errors.New(`json: bool unexpected end of JSON input`)},                             // 131
	{in: `{"B": "null"}`, ptr: new(B), out: B{false}},                                                                           // 132
	{in: `{"B": "nul"}`, ptr: new(B), err: errors.New(`json: invalid character as null`)},                                       // 133
	{in: `{"B": [2, 3]}`, ptr: new(B), err: errors.New(`json: cannot unmarshal array into Go struct field B.B of type string`)}, // 134
	// additional tests for disallowUnknownFields
	{ // 135
		in: `{
						"Level0": 1,
						"Level1b": 2,
						"Level1c": 3,
						"x": 4,
						"Level1a": 5,
						"LEVEL1B": 6,
						"e": {
							"Level1a": 8,
							"Level1b": 9,
							"Level1c": 10,
							"Level1d": 11,
							"x": 12
						},
						"Loop1": 13,
						"Loop2": 14,
						"X": 15,
						"Y": 16,
						"Z": 17,
						"Q": 18,
						"extra": true
					}`,
		ptr:                   new(Top),
		err:                   fmt.Errorf("json: unknown field \"extra\""),
		disallowUnknownFields: true,
	},
	{ // 136
		in: `{
						"Level0": 1,
						"Level1b": 2,
						"Level1c": 3,
						"x": 4,
						"Level1a": 5,
						"LEVEL1B": 6,
						"e": {
							"Level1a": 8,
							"Level1b": 9,
							"Level1c": 10,
							"Level1d": 11,
							"x": 12,
							"extra": null
						},
						"Loop1": 13,
						"Loop2": 14,
						"X": 15,
						"Y": 16,
						"Z": 17,
						"Q": 18
					}`,
		ptr:                   new(Top),
		err:                   fmt.Errorf("json: unknown field \"extra\""),
		disallowUnknownFields: true,
	},
	// issue 26444
	// UnmarshalTypeError without field & struct values
	{
		in:  `{"data":{"test1": "bob", "test2": 123}}`, // 137
		ptr: new(mapStringToStringData),
		err: &json.UnmarshalTypeError{Value: "number", Type: reflect.TypeOf(""), Offset: 37, Struct: "mapStringToStringData", Field: "Data"},
	},
	{
		in:  `{"data":{"test1": 123, "test2": "bob"}}`, // 138
		ptr: new(mapStringToStringData),
		err: &json.UnmarshalTypeError{Value: "number", Type: reflect.TypeOf(""), Offset: 21, Struct: "mapStringToStringData", Field: "Data"},
	},

	// trying to decode JSON arrays or objects via TextUnmarshaler
	{
		in:  `[1, 2, 3]`, // 139
		ptr: new(MustNotUnmarshalText),
		err: &json.UnmarshalTypeError{Value: "array", Type: reflect.TypeOf(&MustNotUnmarshalText{}), Offset: 1},
	},
	{
		in:  `{"foo": "bar"}`, // 140
		ptr: new(MustNotUnmarshalText),
		err: &json.UnmarshalTypeError{Value: "object", Type: reflect.TypeOf(&MustNotUnmarshalText{}), Offset: 1},
	},
	// #22369
	{
		in:  `{"PP": {"T": {"Y": "bad-type"}}}`, // 141
		ptr: new(P),
		err: &json.UnmarshalTypeError{
			Value:  `number "`,
			Struct: "T",
			Field:  "Y",
			Type:   reflect.TypeOf(int(0)),
			Offset: 29,
		},
	},
	{
		in:  `{"Ts": [{"Y": 1}, {"Y": 2}, {"Y": "bad-type"}]}`, // 142
		ptr: new(PP),
		err: &json.UnmarshalTypeError{
			Value:  `number "`,
			Struct: "T",
			Field:  "Y",
			Type:   reflect.TypeOf(int(0)),
			Offset: 29,
		},
	},
	// #14702
	{
		in:  `invalid`, // 143
		ptr: new(json.Number),
		err: json.NewSyntaxError(
			`invalid character 'i' looking for beginning of value`,
			1,
		),
	},
	{
		in:  `"invalid"`, // 144
		ptr: new(json.Number),
		err: fmt.Errorf(`strconv.ParseFloat: parsing "invalid": invalid syntax`),
	},
	{
		in:  `{"A":"invalid"}`, // 145
		ptr: new(struct{ A json.Number }),
		err: fmt.Errorf(`strconv.ParseFloat: parsing "invalid": invalid syntax`),
	},
	{
		in: `{"A":"invalid"}`, // 146
		ptr: new(struct {
			A json.Number `json:",string"`
		}),
		err: fmt.Errorf(`json: json.Number unexpected end of JSON input`),
	},
	{
		in:  `{"A":"invalid"}`, // 147
		ptr: new(map[string]json.Number),
		err: fmt.Errorf(`strconv.ParseFloat: parsing "invalid": invalid syntax`),
	},
	// invalid UTF-8 is coerced to valid UTF-8.
	{
		in:  "\"hello\xffworld\"", // 148
		ptr: new(string),
		out: "hello\ufffdworld",
	},
	{
		in:  "\"hello\xc2\xc2world\"", // 149
		ptr: new(string),
		out: "hello\ufffd\ufffdworld",
	},
	{
		in:  "\"hello\xc2\xffworld\"", // 150
		ptr: new(string),
		out: "hello\ufffd\ufffdworld",
	},
	{
		in:  "\"hello\xed\xa0\x80\xed\xb0\x80world\"", // 151
		ptr: new(string),
		out: "hello\ufffd\ufffd\ufffd\ufffd\ufffd\ufffdworld",
	},
	{in: "-128", ptr: new(int8), out: int8(-128)},
	{in: "127", ptr: new(int8), out: int8(127)},
	{in: "-32768", ptr: new(int16), out: int16(-32768)},
	{in: "32767", ptr: new(int16), out: int16(32767)},
	{in: "-2147483648", ptr: new(int32), out: int32(-2147483648)},
	{in: "2147483647", ptr: new(int32), out: int32(2147483647)},
}

type All struct {
	Bool    bool
	Int     int
	Int8    int8
	Int16   int16
	Int32   int32
	Int64   int64
	Uint    uint
	Uint8   uint8
	Uint16  uint16
	Uint32  uint32
	Uint64  uint64
	Uintptr uintptr
	Float32 float32
	Float64 float64

	Foo  string `json:"bar"`
	Foo2 string `json:"bar2,dummyopt"`

	IntStr     int64   `json:",string"`
	UintptrStr uintptr `json:",string"`

	PBool    *bool
	PInt     *int
	PInt8    *int8
	PInt16   *int16
	PInt32   *int32
	PInt64   *int64
	PUint    *uint
	PUint8   *uint8
	PUint16  *uint16
	PUint32  *uint32
	PUint64  *uint64
	PUintptr *uintptr
	PFloat32 *float32
	PFloat64 *float64

	String  string
	PString *string

	Map   map[string]Small
	MapP  map[string]*Small
	PMap  *map[string]Small
	PMapP *map[string]*Small

	EmptyMap map[string]Small
	NilMap   map[string]Small

	Slice   []Small
	SliceP  []*Small
	PSlice  *[]Small
	PSliceP *[]*Small

	EmptySlice []Small
	NilSlice   []Small

	StringSlice []string
	ByteSlice   []byte

	Small   Small
	PSmall  *Small
	PPSmall **Small

	Interface  interface{}
	PInterface *interface{}

	unexported int
}

type Small struct {
	Tag string
}

var allValue = All{
	Bool:       true,
	Int:        2,
	Int8:       3,
	Int16:      4,
	Int32:      5,
	Int64:      6,
	Uint:       7,
	Uint8:      8,
	Uint16:     9,
	Uint32:     10,
	Uint64:     11,
	Uintptr:    12,
	Float32:    14.1,
	Float64:    15.1,
	Foo:        "foo",
	Foo2:       "foo2",
	IntStr:     42,
	UintptrStr: 44,
	String:     "16",
	Map: map[string]Small{
		"17": {Tag: "tag17"},
		"18": {Tag: "tag18"},
	},
	MapP: map[string]*Small{
		"19": {Tag: "tag19"},
		"20": nil,
	},
	EmptyMap:    map[string]Small{},
	Slice:       []Small{{Tag: "tag20"}, {Tag: "tag21"}},
	SliceP:      []*Small{{Tag: "tag22"}, nil, {Tag: "tag23"}},
	EmptySlice:  []Small{},
	StringSlice: []string{"str24", "str25", "str26"},
	ByteSlice:   []byte{27, 28, 29},
	Small:       Small{Tag: "tag30"},
	PSmall:      &Small{Tag: "tag31"},
	Interface:   5.2,
}

var pallValue = All{
	PBool:      &allValue.Bool,
	PInt:       &allValue.Int,
	PInt8:      &allValue.Int8,
	PInt16:     &allValue.Int16,
	PInt32:     &allValue.Int32,
	PInt64:     &allValue.Int64,
	PUint:      &allValue.Uint,
	PUint8:     &allValue.Uint8,
	PUint16:    &allValue.Uint16,
	PUint32:    &allValue.Uint32,
	PUint64:    &allValue.Uint64,
	PUintptr:   &allValue.Uintptr,
	PFloat32:   &allValue.Float32,
	PFloat64:   &allValue.Float64,
	PString:    &allValue.String,
	PMap:       &allValue.Map,
	PMapP:      &allValue.MapP,
	PSlice:     &allValue.Slice,
	PSliceP:    &allValue.SliceP,
	PPSmall:    &allValue.PSmall,
	PInterface: &allValue.Interface,
}

var allValueIndent = `{
	"Bool": true,
	"Int": 2,
	"Int8": 3,
	"Int16": 4,
	"Int32": 5,
	"Int64": 6,
	"Uint": 7,
	"Uint8": 8,
	"Uint16": 9,
	"Uint32": 10,
	"Uint64": 11,
	"Uintptr": 12,
	"Float32": 14.1,
	"Float64": 15.1,
	"bar": "foo",
	"bar2": "foo2",
	"IntStr": "42",
	"UintptrStr": "44",
	"PBool": null,
	"PInt": null,
	"PInt8": null,
	"PInt16": null,
	"PInt32": null,
	"PInt64": null,
	"PUint": null,
	"PUint8": null,
	"PUint16": null,
	"PUint32": null,
	"PUint64": null,
	"PUintptr": null,
	"PFloat32": null,
	"PFloat64": null,
	"String": "16",
	"PString": null,
	"Map": {
		"17": {
			"Tag": "tag17"
		},
		"18": {
			"Tag": "tag18"
		}
	},
	"MapP": {
		"19": {
			"Tag": "tag19"
		},
		"20": null
	},
	"PMap": null,
	"PMapP": null,
	"EmptyMap": {},
	"NilMap": null,
	"Slice": [
		{
			"Tag": "tag20"
		},
		{
			"Tag": "tag21"
		}
	],
	"SliceP": [
		{
			"Tag": "tag22"
		},
		null,
		{
			"Tag": "tag23"
		}
	],
	"PSlice": null,
	"PSliceP": null,
	"EmptySlice": [],
	"NilSlice": null,
	"StringSlice": [
		"str24",
		"str25",
		"str26"
	],
	"ByteSlice": "Gxwd",
	"Small": {
		"Tag": "tag30"
	},
	"PSmall": {
		"Tag": "tag31"
	},
	"PPSmall": null,
	"Interface": 5.2,
	"PInterface": null
}`

var allValueCompact = strings.Map(noSpace, allValueIndent)

var pallValueIndent = `{
	"Bool": false,
	"Int": 0,
	"Int8": 0,
	"Int16": 0,
	"Int32": 0,
	"Int64": 0,
	"Uint": 0,
	"Uint8": 0,
	"Uint16": 0,
	"Uint32": 0,
	"Uint64": 0,
	"Uintptr": 0,
	"Float32": 0,
	"Float64": 0,
	"bar": "",
	"bar2": "",
        "IntStr": "0",
	"UintptrStr": "0",
	"PBool": true,
	"PInt": 2,
	"PInt8": 3,
	"PInt16": 4,
	"PInt32": 5,
	"PInt64": 6,
	"PUint": 7,
	"PUint8": 8,
	"PUint16": 9,
	"PUint32": 10,
	"PUint64": 11,
	"PUintptr": 12,
	"PFloat32": 14.1,
	"PFloat64": 15.1,
	"String": "",
	"PString": "16",
	"Map": null,
	"MapP": null,
	"PMap": {
		"17": {
			"Tag": "tag17"
		},
		"18": {
			"Tag": "tag18"
		}
	},
	"PMapP": {
		"19": {
			"Tag": "tag19"
		},
		"20": null
	},
	"EmptyMap": null,
	"NilMap": null,
	"Slice": null,
	"SliceP": null,
	"PSlice": [
		{
			"Tag": "tag20"
		},
		{
			"Tag": "tag21"
		}
	],
	"PSliceP": [
		{
			"Tag": "tag22"
		},
		null,
		{
			"Tag": "tag23"
		}
	],
	"EmptySlice": null,
	"NilSlice": null,
	"StringSlice": null,
	"ByteSlice": null,
	"Small": {
		"Tag": ""
	},
	"PSmall": null,
	"PPSmall": {
		"Tag": "tag31"
	},
	"Interface": null,
	"PInterface": 5.2
}`

var pallValueCompact = strings.Map(noSpace, pallValueIndent)

type NullTest struct {
	Bool      bool
	Int       int
	Int8      int8
	Int16     int16
	Int32     int32
	Int64     int64
	Uint      uint
	Uint8     uint8
	Uint16    uint16
	Uint32    uint32
	Uint64    uint64
	Float32   float32
	Float64   float64
	String    string
	PBool     *bool
	Map       map[string]string
	Slice     []string
	Interface interface{}

	PRaw    *json.RawMessage
	PTime   *time.Time
	PBigInt *big.Int
	PText   *MustNotUnmarshalText
	PBuffer *bytes.Buffer // has methods, just not relevant ones
	PStruct *struct{}

	Raw    json.RawMessage
	Time   time.Time
	BigInt big.Int
	Text   MustNotUnmarshalText
	Buffer bytes.Buffer
	Struct struct{}
}

type MustNotUnmarshalJSON struct{}

func (x MustNotUnmarshalJSON) UnmarshalJSON(data []byte) error {
	return errors.New("MustNotUnmarshalJSON was used")
}

type MustNotUnmarshalText struct{}

func (x MustNotUnmarshalText) UnmarshalText(text []byte) error {
	return errors.New("MustNotUnmarshalText was used")
}

func isSpace(c byte) bool {
	return c <= ' ' && (c == ' ' || c == '\t' || c == '\r' || c == '\n')
}

func noSpace(c rune) rune {
	if isSpace(byte(c)) { //only used for ascii
		return -1
	}
	return c
}

var badUTF8 = []struct {
	in, out string
}{
	{"hello\xffworld", `"hello\ufffdworld"`},
	{"", `""`},
	{"\xff", `"\ufffd"`},
	{"\xff\xff", `"\ufffd\ufffd"`},
	{"a\xffb", `"a\ufffdb"`},
	{"\xe6\x97\xa5\xe6\x9c\xac\xff\xaa\x9e", `"日本\ufffd\ufffd\ufffd"`},
}

func TestMarshalAllValue(t *testing.T) {
	b, err := json.Marshal(allValue)
	if err != nil {
		t.Fatalf("Marshal allValue: %v", err)
	}
	if string(b) != allValueCompact {
		t.Errorf("Marshal allValueCompact")
		diff(t, b, []byte(allValueCompact))
		return
	}

	b, err = json.Marshal(pallValue)
	if err != nil {
		t.Fatalf("Marshal pallValue: %v", err)
	}
	if string(b) != pallValueCompact {
		t.Errorf("Marshal pallValueCompact")
		diff(t, b, []byte(pallValueCompact))
		return
	}
}

func TestMarshalBadUTF8(t *testing.T) {
	for _, tt := range badUTF8 {
		b, err := json.Marshal(tt.in)
		if string(b) != tt.out || err != nil {
			t.Errorf("Marshal(%q) = %#q, %v, want %#q, nil", tt.in, b, err, tt.out)
		}
	}
}

func TestMarshalNumberZeroVal(t *testing.T) {
	var n json.Number
	out, err := json.Marshal(n)
	if err != nil {
		t.Fatal(err)
	}
	outStr := string(out)
	if outStr != "0" {
		t.Fatalf("Invalid zero val for Number: %q", outStr)
	}
}

func TestMarshalEmbeds(t *testing.T) {
	top := &Top{
		Level0: 1,
		Embed0: Embed0{
			Level1b: 2,
			Level1c: 3,
		},
		Embed0a: &Embed0a{
			Level1a: 5,
			Level1b: 6,
		},
		Embed0b: &Embed0b{
			Level1a: 8,
			Level1b: 9,
			Level1c: 10,
			Level1d: 11,
			Level1e: 12,
		},
		Loop: Loop{
			Loop1: 13,
			Loop2: 14,
		},
		Embed0p: Embed0p{
			Point: image.Point{X: 15, Y: 16},
		},
		Embed0q: Embed0q{
			Point: Point{Z: 17},
		},
		embed: embed{
			Q: 18,
		},
	}
	b, err := json.Marshal(top)
	if err != nil {
		t.Fatal(err)
	}
	want := "{\"Level0\":1,\"Level1b\":2,\"Level1c\":3,\"Level1a\":5,\"LEVEL1B\":6,\"e\":{\"Level1a\":8,\"Level1b\":9,\"Level1c\":10,\"Level1d\":11,\"x\":12},\"Loop1\":13,\"Loop2\":14,\"X\":15,\"Y\":16,\"Z\":17,\"Q\":18}"
	if string(b) != want {
		t.Errorf("Wrong marshal result.\n got: %q\nwant: %q", b, want)
	}
}

func equalError(a, b error) bool {
	if a == nil {
		return b == nil
	}
	if b == nil {
		return a == nil
	}
	return a.Error() == b.Error()
}

func TestUnmarshal(t *testing.T) {
	for i, tt := range unmarshalTests {
		t.Run(fmt.Sprintf("%d_%q", i, tt.in), func(t *testing.T) {
			in := []byte(tt.in)
			if tt.ptr == nil {
				return
			}

			typ := reflect.TypeOf(tt.ptr)
			if typ.Kind() != reflect.Ptr {
				t.Errorf("#%d: unmarshalTest.ptr %T is not a pointer type", i, tt.ptr)
				return
			}
			typ = typ.Elem()

			// v = new(right-type)
			v := reflect.New(typ)

			if !reflect.DeepEqual(tt.ptr, v.Interface()) {
				// There's no reason for ptr to point to non-zero data,
				// as we decode into new(right-type), so the data is
				// discarded.
				// This can easily mean tests that silently don't test
				// what they should. To test decoding into existing
				// data, see TestPrefilled.
				t.Errorf("#%d: unmarshalTest.ptr %#v is not a pointer to a zero value", i, tt.ptr)
				return
			}

			dec := json.NewDecoder(bytes.NewReader(in))
			if tt.useNumber {
				dec.UseNumber()
			}
			if tt.disallowUnknownFields {
				dec.DisallowUnknownFields()
			}
			if err := dec.Decode(v.Interface()); !equalError(err, tt.err) {
				t.Errorf("#%d: %v, want %v", i, err, tt.err)
				return
			} else if err != nil {
				return
			}
			if !reflect.DeepEqual(v.Elem().Interface(), tt.out) {
				t.Errorf("#%d: mismatch\nhave: %#+v\nwant: %#+v", i, v.Elem().Interface(), tt.out)
				data, _ := json.Marshal(v.Elem().Interface())
				println(string(data))
				data, _ = json.Marshal(tt.out)
				println(string(data))
				return
			}

			// Check round trip also decodes correctly.
			if tt.err == nil {
				enc, err := json.Marshal(v.Interface())
				if err != nil {
					t.Errorf("#%d: error re-marshaling: %v", i, err)
					return
				}
				if tt.golden && !bytes.Equal(enc, in) {
					t.Errorf("#%d: remarshal mismatch:\nhave: %s\nwant: %s", i, enc, in)
				}
				vv := reflect.New(reflect.TypeOf(tt.ptr).Elem())
				dec = json.NewDecoder(bytes.NewReader(enc))
				if tt.useNumber {
					dec.UseNumber()
				}
				if err := dec.Decode(vv.Interface()); err != nil {
					t.Errorf("#%d: error re-unmarshaling %#q: %v", i, enc, err)
					return
				}
				if !reflect.DeepEqual(v.Elem().Interface(), vv.Elem().Interface()) {
					t.Errorf("#%d: mismatch\nhave: %#+v\nwant: %#+v", i, v.Elem().Interface(), vv.Elem().Interface())
					t.Errorf("     In: %q", strings.Map(noSpace, string(in)))
					t.Errorf("Marshal: %q", strings.Map(noSpace, string(enc)))
					return
				}
			}
		})
	}
}

func TestUnmarshalMarshal(t *testing.T) {
	initBig()
	var v interface{}
	if err := json.Unmarshal(jsonBig, &v); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if !bytes.Equal(jsonBig, b) {
		t.Errorf("Marshal jsonBig")
		diff(t, b, jsonBig)
		return
	}
}

var numberTests = []struct {
	in       string
	i        int64
	intErr   string
	f        float64
	floatErr string
}{
	{in: "-1.23e1", intErr: "strconv.ParseInt: parsing \"-1.23e1\": invalid syntax", f: -1.23e1},
	{in: "-12", i: -12, f: -12.0},
	{in: "1e1000", intErr: "strconv.ParseInt: parsing \"1e1000\": invalid syntax", floatErr: "strconv.ParseFloat: parsing \"1e1000\": value out of range"},
}

// Independent of Decode, basic coverage of the accessors in Number
func TestNumberAccessors(t *testing.T) {
	for _, tt := range numberTests {
		n := json.Number(tt.in)
		if s := n.String(); s != tt.in {
			t.Errorf("Number(%q).String() is %q", tt.in, s)
		}
		if i, err := n.Int64(); err == nil && tt.intErr == "" && i != tt.i {
			t.Errorf("Number(%q).Int64() is %d", tt.in, i)
		} else if (err == nil && tt.intErr != "") || (err != nil && err.Error() != tt.intErr) {
			t.Errorf("Number(%q).Int64() wanted error %q but got: %v", tt.in, tt.intErr, err)
		}
		if f, err := n.Float64(); err == nil && tt.floatErr == "" && f != tt.f {
			t.Errorf("Number(%q).Float64() is %g", tt.in, f)
		} else if (err == nil && tt.floatErr != "") || (err != nil && err.Error() != tt.floatErr) {
			t.Errorf("Number(%q).Float64() wanted error %q but got: %v", tt.in, tt.floatErr, err)
		}
	}
}

func TestLargeByteSlice(t *testing.T) {
	s0 := make([]byte, 2000)
	for i := range s0 {
		s0[i] = byte(i)
	}
	b, err := json.Marshal(s0)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var s1 []byte
	if err := json.Unmarshal(b, &s1); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !bytes.Equal(s0, s1) {
		t.Errorf("Marshal large byte slice")
		diff(t, s0, s1)
	}
}

type Xint struct {
	X int
}

func TestUnmarshalInterface(t *testing.T) {
	var xint Xint
	var i interface{} = &xint
	if err := json.Unmarshal([]byte(`{"X":1}`), &i); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if xint.X != 1 {
		t.Fatalf("Did not write to xint")
	}
}

func TestUnmarshalPtrPtr(t *testing.T) {
	var xint Xint
	pxint := &xint
	if err := json.Unmarshal([]byte(`{"X":1}`), &pxint); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if xint.X != 1 {
		t.Fatalf("Did not write to xint")
	}
}

func TestEscape(t *testing.T) {
	const input = `"foobar"<html>` + " [\u2028 \u2029]"
	const expected = `"\"foobar\"\u003chtml\u003e [\u2028 \u2029]"`
	b, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}
	if s := string(b); s != expected {
		t.Errorf("Encoding of [%s]:\n got [%s]\nwant [%s]", input, s, expected)
	}
}

// WrongString is a struct that's misusing the ,string modifier.
type WrongString struct {
	Message string `json:"result,string"`
}

type wrongStringTest struct {
	in, err string
}

var wrongStringTests = []wrongStringTest{
	{`{"result":"x"}`, `invalid character 'x' looking for beginning of value`},
	{`{"result":"foo"}`, `invalid character 'f' looking for beginning of value`},
	{`{"result":"123"}`, `json: cannot unmarshal number into Go struct field WrongString.Message of type string`},
	{`{"result":123}`, `json: cannot unmarshal number into Go struct field WrongString.Message of type string`},
	{`{"result":"\""}`, `json: string unexpected end of JSON input`},
	{`{"result":"\"foo"}`, `json: string unexpected end of JSON input`},
}

// If people misuse the ,string modifier, the error message should be
// helpful, telling the user that they're doing it wrong.
func TestErrorMessageFromMisusedString(t *testing.T) {
	for n, tt := range wrongStringTests {
		r := strings.NewReader(tt.in)
		var s WrongString
		err := json.NewDecoder(r).Decode(&s)
		got := fmt.Sprintf("%v", err)
		if got != tt.err {
			t.Errorf("%d. got err = %q, want %q", n, got, tt.err)
		}
	}
}

func TestRefUnmarshal(t *testing.T) {
	type S struct {
		// Ref is defined in encode_test.go.
		R0 Ref
		R1 *Ref
		R2 RefText
		R3 *RefText
	}
	want := S{
		R0: 12,
		R1: new(Ref),
		R2: 13,
		R3: new(RefText),
	}
	*want.R1 = 12
	*want.R3 = 13

	var got S
	if err := json.Unmarshal([]byte(`{"R0":"ref","R1":"ref","R2":"ref","R3":"ref"}`), &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

// Test that the empty string doesn't panic decoding when ,string is specified
// Issue 3450
func TestEmptyString(t *testing.T) {
	type T2 struct {
		Number1 int `json:",string"`
		Number2 int `json:",string"`
	}
	data := `{"Number1":"1", "Number2":""}`
	dec := json.NewDecoder(strings.NewReader(data))
	var t2 T2
	err := dec.Decode(&t2)
	if err == nil {
		t.Fatal("Decode: did not return error")
	}
	if t2.Number1 != 1 {
		t.Fatal("Decode: did not set Number1")
	}
}

// Test that a null for ,string is not replaced with the previous quoted string (issue 7046).
// It should also not be an error (issue 2540, issue 8587).
func TestNullString(t *testing.T) {
	type T struct {
		A int  `json:",string"`
		B int  `json:",string"`
		C *int `json:",string"`
	}
	data := []byte(`{"A": "1", "B": null, "C": null}`)
	var s T
	s.B = 1
	s.C = new(int)
	*s.C = 2
	err := json.Unmarshal(data, &s)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if s.B != 1 || s.C != nil {
		t.Fatalf("after Unmarshal, s.B=%d, s.C=%p, want 1, nil", s.B, s.C)
	}
}

func intp(x int) *int {
	p := new(int)
	*p = x
	return p
}

func intpp(x *int) **int {
	pp := new(*int)
	*pp = x
	return pp
}

var interfaceSetTests = []struct {
	pre  interface{}
	json string
	post interface{}
}{
	{"foo", `"bar"`, "bar"},
	{"foo", `2`, 2.0},
	{"foo", `true`, true},
	{"foo", `null`, nil},
	{nil, `null`, nil},
	{new(int), `null`, nil},
	{(*int)(nil), `null`, nil},
	//{new(*int), `null`, new(*int)},
	{(**int)(nil), `null`, nil},
	{intp(1), `null`, nil},
	//{intpp(nil), `null`, intpp(nil)},
	//{intpp(intp(1)), `null`, intpp(nil)},
}

func TestInterfaceSet(t *testing.T) {
	for idx, tt := range interfaceSetTests {
		b := struct{ X interface{} }{tt.pre}
		blob := `{"X":` + tt.json + `}`
		if err := json.Unmarshal([]byte(blob), &b); err != nil {
			t.Errorf("Unmarshal %#q: %v", blob, err)
			continue
		}
		if !reflect.DeepEqual(b.X, tt.post) {
			t.Errorf("%d: Unmarshal %#q into %#v: X=%#v, want %#v", idx, blob, tt.pre, b.X, tt.post)
		}
	}
}

// JSON null values should be ignored for primitives and string values instead of resulting in an error.
// Issue 2540
func TestUnmarshalNulls(t *testing.T) {
	// Unmarshal docs:
	// The JSON null value unmarshals into an interface, map, pointer, or slice
	// by setting that Go value to nil. Because null is often used in JSON to mean
	// ``not present,'' unmarshaling a JSON null into any other Go type has no effect
	// on the value and produces no error.

	jsonData := []byte(`{
				"Bool"    : null,
				"Int"     : null,
				"Int8"    : null,
				"Int16"   : null,
				"Int32"   : null,
				"Int64"   : null,
				"Uint"    : null,
				"Uint8"   : null,
				"Uint16"  : null,
				"Uint32"  : null,
				"Uint64"  : null,
				"Float32" : null,
				"Float64" : null,
				"String"  : null,
				"PBool": null,
				"Map": null,
				"Slice": null,
				"Interface": null,
				"PRaw": null,
				"PTime": null,
				"PBigInt": null,
				"PText": null,
				"PBuffer": null,
				"PStruct": null,
				"Raw": null,
				"Time": null,
				"BigInt": null,
				"Text": null,
				"Buffer": null,
				"Struct": null
			}`)
	nulls := NullTest{
		Bool:      true,
		Int:       2,
		Int8:      3,
		Int16:     4,
		Int32:     5,
		Int64:     6,
		Uint:      7,
		Uint8:     8,
		Uint16:    9,
		Uint32:    10,
		Uint64:    11,
		Float32:   12.1,
		Float64:   13.1,
		String:    "14",
		PBool:     new(bool),
		Map:       map[string]string{},
		Slice:     []string{},
		Interface: new(MustNotUnmarshalJSON),
		PRaw:      new(json.RawMessage),
		PTime:     new(time.Time),
		PBigInt:   new(big.Int),
		PText:     new(MustNotUnmarshalText),
		PStruct:   new(struct{}),
		PBuffer:   new(bytes.Buffer),
		Raw:       json.RawMessage("123"),
		Time:      time.Unix(123456789, 0),
		BigInt:    *big.NewInt(123),
	}

	before := nulls.Time.String()

	err := json.Unmarshal(jsonData, &nulls)
	if err != nil {
		t.Errorf("Unmarshal of null values failed: %v", err)
	}
	if !nulls.Bool || nulls.Int != 2 || nulls.Int8 != 3 || nulls.Int16 != 4 || nulls.Int32 != 5 || nulls.Int64 != 6 ||
		nulls.Uint != 7 || nulls.Uint8 != 8 || nulls.Uint16 != 9 || nulls.Uint32 != 10 || nulls.Uint64 != 11 ||
		nulls.Float32 != 12.1 || nulls.Float64 != 13.1 || nulls.String != "14" {
		t.Errorf("Unmarshal of null values affected primitives")
	}

	if nulls.PBool != nil {
		t.Errorf("Unmarshal of null did not clear nulls.PBool")
	}
	if nulls.Map != nil {
		t.Errorf("Unmarshal of null did not clear nulls.Map")
	}
	if nulls.Slice != nil {
		t.Errorf("Unmarshal of null did not clear nulls.Slice")
	}
	if nulls.Interface != nil {
		t.Errorf("Unmarshal of null did not clear nulls.Interface")
	}
	if nulls.PRaw != nil {
		t.Errorf("Unmarshal of null did not clear nulls.PRaw")
	}
	if nulls.PTime != nil {
		t.Errorf("Unmarshal of null did not clear nulls.PTime")
	}
	if nulls.PBigInt != nil {
		t.Errorf("Unmarshal of null did not clear nulls.PBigInt")
	}
	if nulls.PText != nil {
		t.Errorf("Unmarshal of null did not clear nulls.PText")
	}
	if nulls.PBuffer != nil {
		t.Errorf("Unmarshal of null did not clear nulls.PBuffer")
	}
	if nulls.PStruct != nil {
		t.Errorf("Unmarshal of null did not clear nulls.PStruct")
	}

	if string(nulls.Raw) != "null" {
		t.Errorf("Unmarshal of RawMessage null did not record null: %v", string(nulls.Raw))
	}
	if nulls.Time.String() != before {
		t.Errorf("Unmarshal of time.Time null set time to %v", nulls.Time.String())
	}
	if nulls.BigInt.String() != "123" {
		t.Errorf("Unmarshal of big.Int null set int to %v", nulls.BigInt.String())
	}
}

func TestStringKind(t *testing.T) {
	type stringKind string

	var m1, m2 map[stringKind]int
	m1 = map[stringKind]int{
		"foo": 42,
	}

	data, err := json.Marshal(m1)
	if err != nil {
		t.Errorf("Unexpected error marshaling: %v", err)
	}

	err = json.Unmarshal(data, &m2)
	if err != nil {
		t.Errorf("Unexpected error unmarshaling: %v", err)
	}

	if !reflect.DeepEqual(m1, m2) {
		t.Error("Items should be equal after encoding and then decoding")
	}
}

// Custom types with []byte as underlying type could not be marshaled
// and then unmarshaled.
// Issue 8962.
func TestByteKind(t *testing.T) {
	type byteKind []byte

	a := byteKind("hello")

	data, err := json.Marshal(a)
	if err != nil {
		t.Error(err)
	}
	var b byteKind
	err = json.Unmarshal(data, &b)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Errorf("expected %v == %v", a, b)
	}
}

// The fix for issue 8962 introduced a regression.
// Issue 12921.
func TestSliceOfCustomByte(t *testing.T) {
	type Uint8 uint8

	a := []Uint8("hello")

	data, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	var b []Uint8
	err = json.Unmarshal(data, &b)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("expected %v == %v", a, b)
	}
}

var decodeTypeErrorTests = []struct {
	dest interface{}
	src  string
}{
	{new(string), `{"user": "name"}`}, // issue 4628.
	{new(error), `{}`},                // issue 4222
	{new(error), `[]`},
	{new(error), `""`},
	{new(error), `123`},
	{new(error), `true`},
}

func TestUnmarshalTypeError(t *testing.T) {
	for _, item := range decodeTypeErrorTests {
		err := json.Unmarshal([]byte(item.src), item.dest)
		if _, ok := err.(*json.UnmarshalTypeError); !ok {
			t.Errorf("expected type error for Unmarshal(%q, type %T): got %T",
				item.src, item.dest, err)
		}
	}
}

var unmarshalSyntaxTests = []string{
	"tru",
	"fals",
	"nul",
	"123e",
	`"hello`,
	`[1,2,3`,
	`{"key":1`,
	`{"key":1,`,
}

func TestUnmarshalSyntax(t *testing.T) {
	var x interface{}
	for _, src := range unmarshalSyntaxTests {
		err := json.Unmarshal([]byte(src), &x)
		if _, ok := err.(*json.SyntaxError); !ok {
			t.Errorf("expected syntax error for Unmarshal(%q): got %T", src, err)
		}
	}
}

// Test handling of unexported fields that should be ignored.
// Issue 4660
type unexportedFields struct {
	Name string
	m    map[string]interface{} `json:"-"`
	m2   map[string]interface{} `json:"abcd"`

	s []int `json:"-"`
}

func TestUnmarshalUnexported(t *testing.T) {
	input := `{"Name": "Bob", "m": {"x": 123}, "m2": {"y": 456}, "abcd": {"z": 789}, "s": [2, 3]}`
	want := &unexportedFields{Name: "Bob"}

	out := &unexportedFields{}
	err := json.Unmarshal([]byte(input), out)
	if err != nil {
		t.Errorf("got error %v, expected nil", err)
	}
	if !reflect.DeepEqual(out, want) {
		t.Errorf("got %q, want %q", out, want)
	}
}

// Time3339 is a time.Time which encodes to and from JSON
// as an RFC 3339 time in UTC.
type Time3339 time.Time

func (t *Time3339) UnmarshalJSON(b []byte) error {
	if len(b) < 2 || b[0] != '"' || b[len(b)-1] != '"' {
		return fmt.Errorf("types: failed to unmarshal non-string value %q as an RFC 3339 time", b)
	}
	tm, err := time.Parse(time.RFC3339, string(b[1:len(b)-1]))
	if err != nil {
		return err
	}
	*t = Time3339(tm)
	return nil
}

func TestUnmarshalJSONLiteralError(t *testing.T) {
	var t3 Time3339
	err := json.Unmarshal([]byte(`"0000-00-00T00:00:00Z"`), &t3)
	if err == nil {
		t.Fatalf("expected error; got time %v", time.Time(t3))
	}
	if !strings.Contains(err.Error(), "range") {
		t.Errorf("got err = %v; want out of range error", err)
	}
}

// Test that extra object elements in an array do not result in a
// "data changing underfoot" error.
// Issue 3717
func TestSkipArrayObjects(t *testing.T) {
	data := `[{}]`
	var dest [0]interface{}

	err := json.Unmarshal([]byte(data), &dest)
	if err != nil {
		t.Errorf("got error %q, want nil", err)
	}
}

// Test semantics of pre-filled data, such as struct fields, map elements,
// slices, and arrays.
// Issues 4900 and 8837, among others.
func TestPrefilled(t *testing.T) {
	// Values here change, cannot reuse table across runs.
	var prefillTests = []struct {
		in  string
		ptr interface{}
		out interface{}
	}{
		{
			in:  `{"X": 1, "Y": 2}`,
			ptr: &XYZ{X: float32(3), Y: int16(4), Z: 1.5},
			out: &XYZ{X: float64(1), Y: float64(2), Z: 1.5},
		},
		{
			in:  `{"X": 1, "Y": 2}`,
			ptr: &map[string]interface{}{"X": float32(3), "Y": int16(4), "Z": 1.5},
			out: &map[string]interface{}{"X": float64(1), "Y": float64(2), "Z": 1.5},
		},
		{
			in:  `[2]`,
			ptr: &[]int{1},
			out: &[]int{2},
		},
		{
			in:  `[2, 3]`,
			ptr: &[]int{1},
			out: &[]int{2, 3},
		},
		{
			in:  `[2, 3]`,
			ptr: &[...]int{1},
			out: &[...]int{2},
		},
		{
			in:  `[3]`,
			ptr: &[...]int{1, 2},
			out: &[...]int{3, 0},
		},
	}

	for _, tt := range prefillTests {
		ptrstr := fmt.Sprintf("%v", tt.ptr)
		err := json.Unmarshal([]byte(tt.in), tt.ptr) // tt.ptr edited here
		if err != nil {
			t.Errorf("Unmarshal: %v", err)
		}
		if !reflect.DeepEqual(tt.ptr, tt.out) {
			t.Errorf("Unmarshal(%#q, %s): have %v, want %v", tt.in, ptrstr, tt.ptr, tt.out)
		}
	}
}

var invalidUnmarshalTests = []struct {
	v    interface{}
	want string
}{
	{nil, "json: Unmarshal(nil)"},
	{struct{}{}, "json: Unmarshal(non-pointer struct {})"},
	{(*int)(nil), "json: Unmarshal(nil *int)"},
}

func TestInvalidUnmarshal(t *testing.T) {
	buf := []byte(`{"a":"1"}`)
	for _, tt := range invalidUnmarshalTests {
		err := json.Unmarshal(buf, tt.v)
		if err == nil {
			t.Errorf("Unmarshal expecting error, got nil")
			continue
		}
		if got := err.Error(); got != tt.want {
			t.Errorf("Unmarshal = %q; want %q", got, tt.want)
		}
	}
}

var invalidUnmarshalTextTests = []struct {
	v    interface{}
	want string
}{
	{nil, "json: Unmarshal(nil)"},
	{struct{}{}, "json: Unmarshal(non-pointer struct {})"},
	{(*int)(nil), "json: Unmarshal(nil *int)"},
	{new(net.IP), "json: cannot unmarshal number into Go value of type *net.IP"},
}

func TestInvalidUnmarshalText(t *testing.T) {
	buf := []byte(`123`)
	for _, tt := range invalidUnmarshalTextTests {
		err := json.Unmarshal(buf, tt.v)
		if err == nil {
			t.Errorf("Unmarshal expecting error, got nil")
			continue
		}
		if got := err.Error(); got != tt.want {
			t.Errorf("Unmarshal = %q; want %q", got, tt.want)
		}
	}
}

// Test that string option is ignored for invalid types.
// Issue 9812.
func TestInvalidStringOption(t *testing.T) {
	num := 0
	item := struct {
		T time.Time         `json:",string"`
		M map[string]string `json:",string"`
		S []string          `json:",string"`
		A [1]string         `json:",string"`
		I interface{}       `json:",string"`
		P *int              `json:",string"`
	}{M: make(map[string]string), S: make([]string, 0), I: num, P: &num}

	data, err := json.Marshal(item)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	err = json.Unmarshal(data, &item)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
}

// Test unmarshal behavior with regards to embedded unexported structs.
//
// (Issue 21357) If the embedded struct is a pointer and is unallocated,
// this returns an error because unmarshal cannot set the field.
//
// (Issue 24152) If the embedded struct is given an explicit name,
// ensure that the normal unmarshal logic does not panic in reflect.
//
// (Issue 28145) If the embedded struct is given an explicit name and has
// exported methods, don't cause a panic trying to get its value.
func TestUnmarshalEmbeddedUnexported(t *testing.T) {
	type (
		embed1 struct{ Q int }
		embed2 struct{ Q int }
		embed3 struct {
			Q int64 `json:",string"`
		}
		S1 struct {
			*embed1
			R int
		}
		S2 struct {
			*embed1
			Q int
		}
		S3 struct {
			embed1
			R int
		}
		S4 struct {
			*embed1
			embed2
		}
		S5 struct {
			*embed3
			R int
		}
		S6 struct {
			embed1 `json:"embed1"`
		}
		S7 struct {
			embed1 `json:"embed1"`
			embed2
		}
		S8 struct {
			embed1 `json:"embed1"`
			embed2 `json:"embed2"`
			Q      int
		}
		S9 struct {
			unexportedWithMethods `json:"embed"`
		}
	)

	tests := []struct {
		in  string
		ptr interface{}
		out interface{}
		err error
	}{{
		// Error since we cannot set S1.embed1, but still able to set S1.R.
		in:  `{"R":2,"Q":1}`,
		ptr: new(S1),
		out: &S1{R: 2},
		err: fmt.Errorf("json: cannot set embedded pointer to unexported struct: json_test.embed1"),
	}, {
		// The top level Q field takes precedence.
		in:  `{"Q":1}`,
		ptr: new(S2),
		out: &S2{Q: 1},
	}, {
		// No issue with non-pointer variant.
		in:  `{"R":2,"Q":1}`,
		ptr: new(S3),
		out: &S3{embed1: embed1{Q: 1}, R: 2},
	}, {
		// No error since both embedded structs have field R, which annihilate each other.
		// Thus, no attempt is made at setting S4.embed1.
		in:  `{"R":2}`,
		ptr: new(S4),
		out: new(S4),
	}, {
		// Error since we cannot set S5.embed1, but still able to set S5.R.
		in:  `{"R":2,"Q":1}`,
		ptr: new(S5),
		out: &S5{R: 2},
		err: fmt.Errorf("json: cannot set embedded pointer to unexported struct: json_test.embed3"),
	}, {
		// Issue 24152, ensure decodeState.indirect does not panic.
		in:  `{"embed1": {"Q": 1}}`,
		ptr: new(S6),
		out: &S6{embed1{1}},
	}, {
		// Issue 24153, check that we can still set forwarded fields even in
		// the presence of a name conflict.
		//
		// This relies on obscure behavior of reflect where it is possible
		// to set a forwarded exported field on an unexported embedded struct
		// even though there is a name conflict, even when it would have been
		// impossible to do so according to Go visibility rules.
		// Go forbids this because it is ambiguous whether S7.Q refers to
		// S7.embed1.Q or S7.embed2.Q. Since embed1 and embed2 are unexported,
		// it should be impossible for an external package to set either Q.
		//
		// It is probably okay for a future reflect change to break this.
		in:  `{"embed1": {"Q": 1}, "Q": 2}`,
		ptr: new(S7),
		out: &S7{embed1{1}, embed2{2}},
	}, {
		// Issue 24153, similar to the S7 case.
		in:  `{"embed1": {"Q": 1}, "embed2": {"Q": 2}, "Q": 3}`,
		ptr: new(S8),
		out: &S8{embed1{1}, embed2{2}, 3},
	}, {
		// Issue 228145, similar to the cases above.
		in:  `{"embed": {}}`,
		ptr: new(S9),
		out: &S9{},
	}}

	for i, tt := range tests {
		err := json.Unmarshal([]byte(tt.in), tt.ptr)
		if !equalError(err, tt.err) {
			t.Errorf("#%d: %v, want %v", i, err, tt.err)
		}
		if !reflect.DeepEqual(tt.ptr, tt.out) {
			t.Errorf("#%d: mismatch\ngot:  %#+v\nwant: %#+v", i, tt.ptr, tt.out)
		}
	}
}

func TestUnmarshalErrorAfterMultipleJSON(t *testing.T) {
	tests := []struct {
		in  string
		err error
	}{{
		in:  `1 false null :`,
		err: json.NewSyntaxError("invalid character '\x00' looking for beginning of value", 14),
	}, {
		in:  `1 [] [,]`,
		err: json.NewSyntaxError("invalid character ',' looking for beginning of value", 6),
	}, {
		in:  `1 [] [true:]`,
		err: json.NewSyntaxError("json: slice unexpected end of JSON input", 10),
	}, {
		in:  `1  {}    {"x"=}`,
		err: json.NewSyntaxError("expected colon after object key", 13),
	}, {
		in:  `falsetruenul#`,
		err: json.NewSyntaxError("json: invalid character # as null", 12),
	}}
	for i, tt := range tests {
		dec := json.NewDecoder(strings.NewReader(tt.in))
		var err error
		for {
			var v interface{}
			if err = dec.Decode(&v); err != nil {
				break
			}
		}
		if !reflect.DeepEqual(err, tt.err) {
			t.Errorf("#%d: got %#v, want %#v", i, err, tt.err)
		}
	}
}

type unmarshalPanic struct{}

func (unmarshalPanic) UnmarshalJSON([]byte) error { panic(0xdead) }

func TestUnmarshalPanic(t *testing.T) {
	defer func() {
		if got := recover(); !reflect.DeepEqual(got, 0xdead) {
			t.Errorf("panic() = (%T)(%v), want 0xdead", got, got)
		}
	}()
	json.Unmarshal([]byte("{}"), &unmarshalPanic{})
	t.Fatalf("Unmarshal should have panicked")
}

// The decoder used to hang if decoding into an interface pointing to its own address.
// See golang.org/issues/31740.
func TestUnmarshalRecursivePointer(t *testing.T) {
	var v interface{}
	v = &v
	data := []byte(`{"a": "b"}`)

	if err := json.Unmarshal(data, v); err != nil {
		t.Fatal(err)
	}
}

type textUnmarshalerString string

func (m *textUnmarshalerString) UnmarshalText(text []byte) error {
	*m = textUnmarshalerString(strings.ToLower(string(text)))
	return nil
}

// Test unmarshal to a map, where the map key is a user defined type.
// See golang.org/issues/34437.
func TestUnmarshalMapWithTextUnmarshalerStringKey(t *testing.T) {
	var p map[textUnmarshalerString]string
	if err := json.Unmarshal([]byte(`{"FOO": "1"}`), &p); err != nil {
		t.Fatalf("Unmarshal unexpected error: %v", err)
	}

	if _, ok := p["foo"]; !ok {
		t.Errorf(`Key "foo" does not exist in map: %v`, p)
	}
}

func TestUnmarshalRescanLiteralMangledUnquote(t *testing.T) {
	// See golang.org/issues/38105.
	var p map[textUnmarshalerString]string
	if err := json.Unmarshal([]byte(`{"开源":"12345开源"}`), &p); err != nil {
		t.Fatalf("Unmarshal unexpected error: %v", err)
	}
	if _, ok := p["开源"]; !ok {
		t.Errorf(`Key "开源" does not exist in map: %v`, p)
	}

	// See golang.org/issues/38126.
	type T struct {
		F1 string `json:"F1,string"`
	}
	t1 := T{"aaa\tbbb"}

	b, err := json.Marshal(t1)
	if err != nil {
		t.Fatalf("Marshal unexpected error: %v", err)
	}
	var t2 T
	if err := json.Unmarshal(b, &t2); err != nil {
		t.Fatalf("Unmarshal unexpected error: %v", err)
	}
	if t1 != t2 {
		t.Errorf("Marshal and Unmarshal roundtrip mismatch: want %q got %q", t1, t2)
	}

	// See golang.org/issues/39555.
	input := map[textUnmarshalerString]string{"FOO": "", `"`: ""}

	encoded, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("Marshal unexpected error: %v", err)
	}
	var got map[textUnmarshalerString]string
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatalf("Unmarshal unexpected error: %v", err)
	}
	want := map[textUnmarshalerString]string{"foo": "", `"`: ""}
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("Unexpected roundtrip result:\nwant: %q\ngot:  %q", want, got)
	}
}

func TestUnmarshalMaxDepth(t *testing.T) {
	testcases := []struct {
		name        string
		data        string
		errMaxDepth bool
	}{
		{
			name:        "ArrayUnderMaxNestingDepth",
			data:        `{"a":` + strings.Repeat(`[`, 10000-1) + strings.Repeat(`]`, 10000-1) + `}`,
			errMaxDepth: false,
		},
		{
			name:        "ArrayOverMaxNestingDepth",
			data:        `{"a":` + strings.Repeat(`[`, 10000) + strings.Repeat(`]`, 10000) + `}`,
			errMaxDepth: true,
		},
		{
			name:        "ArrayOverStackDepth",
			data:        `{"a":` + strings.Repeat(`[`, 3000000) + strings.Repeat(`]`, 3000000) + `}`,
			errMaxDepth: true,
		},
		{
			name:        "ObjectUnderMaxNestingDepth",
			data:        `{"a":` + strings.Repeat(`{"a":`, 10000-1) + `0` + strings.Repeat(`}`, 10000-1) + `}`,
			errMaxDepth: false,
		},
		{
			name:        "ObjectOverMaxNestingDepth",
			data:        `{"a":` + strings.Repeat(`{"a":`, 10000) + `0` + strings.Repeat(`}`, 10000) + `}`,
			errMaxDepth: true,
		},
		{
			name:        "ObjectOverStackDepth",
			data:        `{"a":` + strings.Repeat(`{"a":`, 3000000) + `0` + strings.Repeat(`}`, 3000000) + `}`,
			errMaxDepth: true,
		},
	}

	targets := []struct {
		name     string
		newValue func() interface{}
	}{
		{
			name: "unstructured",
			newValue: func() interface{} {
				var v interface{}
				return &v
			},
		},
		{
			name: "typed named field",
			newValue: func() interface{} {
				v := struct {
					A interface{} `json:"a"`
				}{}
				return &v
			},
		},
		{
			name: "typed missing field",
			newValue: func() interface{} {
				v := struct {
					B interface{} `json:"b"`
				}{}
				return &v
			},
		},
		{
			name: "custom unmarshaler",
			newValue: func() interface{} {
				v := unmarshaler{}
				return &v
			},
		},
	}

	for _, tc := range testcases {
		for _, target := range targets {
			t.Run(target.name+"-"+tc.name, func(t *testing.T) {
				t.Run("unmarshal", func(t *testing.T) {
					err := json.Unmarshal([]byte(tc.data), target.newValue())
					if !tc.errMaxDepth {
						if err != nil {
							t.Errorf("unexpected error: %v", err)
						}
					} else {
						if err == nil {
							t.Errorf("expected error containing 'exceeded max depth', got none")
						} else if !strings.Contains(err.Error(), "exceeded max depth") {
							t.Errorf("expected error containing 'exceeded max depth', got: %v", err)
						}
					}
				})
				t.Run("stream", func(t *testing.T) {
					err := json.NewDecoder(strings.NewReader(tc.data)).Decode(target.newValue())
					if !tc.errMaxDepth {
						if err != nil {
							t.Errorf("unexpected error: %v", err)
						}
					} else {
						if err == nil {
							t.Errorf("expected error containing 'exceeded max depth', got none")
						} else if !strings.Contains(err.Error(), "exceeded max depth") {
							t.Errorf("expected error containing 'exceeded max depth', got: %v", err)
						}
					}
				})
			})
		}
	}
}

func TestDecodeSlice(t *testing.T) {
	type B struct{ Int int32 }
	type A struct{ B *B }
	type X struct{ A []*A }

	w1 := &X{}
	w2 := &X{}

	if err := json.Unmarshal([]byte(`{"a": [ {"b":{"int": 42} } ] }`), w1); err != nil {
		t.Fatal(err)
	}
	w1addr := uintptr(unsafe.Pointer(w1.A[0].B))

	if err := json.Unmarshal([]byte(`{"a": [ {"b":{"int": 112} } ] }`), w2); err != nil {
		t.Fatal(err)
	}
	if uintptr(unsafe.Pointer(w1.A[0].B)) != w1addr {
		t.Fatal("wrong addr")
	}
	w2addr := uintptr(unsafe.Pointer(w2.A[0].B))
	if w1addr == w2addr {
		t.Fatal("invaid address")
	}
}

func TestDecodeMultipleUnmarshal(t *testing.T) {
	data := []byte(`[{"AA":{"X":[{"a": "A"},{"b": "B"}],"Y":"y","Z":"z"},"BB":"bb"},{"AA":{"X":[],"Y":"y","Z":"z"},"BB":"bb"}]`)
	var a []json.RawMessage
	if err := json.Unmarshal(data, &a); err != nil {
		t.Fatal(err)
	}
	if len(a) != 2 {
		t.Fatalf("failed to decode: got %v", a)
	}
	t.Run("first", func(t *testing.T) {
		data := a[0]
		var v map[string]json.RawMessage
		if err := json.Unmarshal(data, &v); err != nil {
			t.Fatal(err)
		}
		if string(v["AA"]) != `{"X":[{"a": "A"},{"b": "B"}],"Y":"y","Z":"z"}` {
			t.Fatalf("failed to decode. got %q", v["AA"])
		}
		var aa map[string]json.RawMessage
		if err := json.Unmarshal(v["AA"], &aa); err != nil {
			t.Fatal(err)
		}
		if string(aa["X"]) != `[{"a": "A"},{"b": "B"}]` {
			t.Fatalf("failed to decode. got %q", v["X"])
		}
		var x []json.RawMessage
		if err := json.Unmarshal(aa["X"], &x); err != nil {
			t.Fatal(err)
		}
		if len(x) != 2 {
			t.Fatalf("failed to decode: %v", x)
		}
		if string(x[0]) != `{"a": "A"}` {
			t.Fatal("failed to decode")
		}
		if string(x[1]) != `{"b": "B"}` {
			t.Fatal("failed to decode")
		}
	})
	t.Run("second", func(t *testing.T) {
		data := a[1]
		var v map[string]json.RawMessage
		if err := json.Unmarshal(data, &v); err != nil {
			t.Fatal(err)
		}
		if string(v["AA"]) != `{"X":[],"Y":"y","Z":"z"}` {
			t.Fatalf("failed to decode. got %q", v["AA"])
		}
		var aa map[string]json.RawMessage
		if err := json.Unmarshal(v["AA"], &aa); err != nil {
			t.Fatal(err)
		}
		if string(aa["X"]) != `[]` {
			t.Fatalf("failed to decode. got %q", v["X"])
		}
		var x []json.RawMessage
		if err := json.Unmarshal(aa["X"], &x); err != nil {
			t.Fatal(err)
		}
		if len(x) != 0 {
			t.Fatalf("failed to decode: %v", x)
		}
	})
}

func TestMultipleDecodeWithRawMessage(t *testing.T) {
	original := []byte(`{
		"Body": {
			"List": [
				{
					"Returns": [
						{
							"Value": "10",
							"nodeType": "Literal"
						}
					],
					"nodeKind": "Return",
					"nodeType": "Statement"
				}
			],
			"nodeKind": "Block",
			"nodeType": "Statement"
		},
		"nodeType": "Function"
	}`)

	var a map[string]json.RawMessage
	if err := json.Unmarshal(original, &a); err != nil {
		t.Fatal(err)
	}
	var b map[string]json.RawMessage
	if err := json.Unmarshal(a["Body"], &b); err != nil {
		t.Fatal(err)
	}
	var c []json.RawMessage
	if err := json.Unmarshal(b["List"], &c); err != nil {
		t.Fatal(err)
	}
	var d map[string]json.RawMessage
	if err := json.Unmarshal(c[0], &d); err != nil {
		t.Fatal(err)
	}
	var e []json.RawMessage
	if err := json.Unmarshal(d["Returns"], &e); err != nil {
		t.Fatal(err)
	}
	var f map[string]json.RawMessage
	if err := json.Unmarshal(e[0], &f); err != nil {
		t.Fatal(err)
	}
}

type intUnmarshaler int

func (u *intUnmarshaler) UnmarshalJSON(b []byte) error {
	if *u != 0 && *u != 10 {
		return fmt.Errorf("failed to decode of slice with int unmarshaler")
	}
	*u = 10
	return nil
}

type arrayUnmarshaler [5]int

func (u *arrayUnmarshaler) UnmarshalJSON(b []byte) error {
	if (*u)[0] != 0 && (*u)[0] != 10 {
		return fmt.Errorf("failed to decode of slice with array unmarshaler")
	}
	(*u)[0] = 10
	return nil
}

type mapUnmarshaler map[string]int

func (u *mapUnmarshaler) UnmarshalJSON(b []byte) error {
	if len(*u) != 0 && len(*u) != 1 {
		return fmt.Errorf("failed to decode of slice with map unmarshaler")
	}
	*u = map[string]int{"a": 10}
	return nil
}

type structUnmarshaler struct {
	A        int
	notFirst bool
}

func (u *structUnmarshaler) UnmarshalJSON(b []byte) error {
	if !u.notFirst && u.A != 0 {
		return fmt.Errorf("failed to decode of slice with struct unmarshaler")
	}
	u.A = 10
	u.notFirst = true
	return nil
}

func TestSliceElemUnmarshaler(t *testing.T) {
	t.Run("int", func(t *testing.T) {
		var v []intUnmarshaler
		if err := json.Unmarshal([]byte(`[1,2,3,4,5]`), &v); err != nil {
			t.Fatal(err)
		}
		if len(v) != 5 {
			t.Fatalf("failed to decode of slice with int unmarshaler: %v", v)
		}
		if v[0] != 10 {
			t.Fatalf("failed to decode of slice with int unmarshaler: %v", v)
		}
		if err := json.Unmarshal([]byte(`[6]`), &v); err != nil {
			t.Fatal(err)
		}
		if len(v) != 1 {
			t.Fatalf("failed to decode of slice with int unmarshaler: %v", v)
		}
		if v[0] != 10 {
			t.Fatalf("failed to decode of slice with int unmarshaler: %v", v)
		}
	})
	t.Run("slice", func(t *testing.T) {
		var v []json.RawMessage
		if err := json.Unmarshal([]byte(`[1,2,3,4,5]`), &v); err != nil {
			t.Fatal(err)
		}
		if len(v) != 5 {
			t.Fatalf("failed to decode of slice with slice unmarshaler: %v", v)
		}
		if len(v[0]) != 1 {
			t.Fatalf("failed to decode of slice with slice unmarshaler: %v", v)
		}
		if err := json.Unmarshal([]byte(`[6]`), &v); err != nil {
			t.Fatal(err)
		}
		if len(v) != 1 {
			t.Fatalf("failed to decode of slice with slice unmarshaler: %v", v)
		}
		if len(v[0]) != 1 {
			t.Fatalf("failed to decode of slice with slice unmarshaler: %v", v)
		}
	})
	t.Run("array", func(t *testing.T) {
		var v []arrayUnmarshaler
		if err := json.Unmarshal([]byte(`[1,2,3,4,5]`), &v); err != nil {
			t.Fatal(err)
		}
		if len(v) != 5 {
			t.Fatalf("failed to decode of slice with array unmarshaler: %v", v)
		}
		if v[0][0] != 10 {
			t.Fatalf("failed to decode of slice with array unmarshaler: %v", v)
		}
		if err := json.Unmarshal([]byte(`[6]`), &v); err != nil {
			t.Fatal(err)
		}
		if len(v) != 1 {
			t.Fatalf("failed to decode of slice with array unmarshaler: %v", v)
		}
		if v[0][0] != 10 {
			t.Fatalf("failed to decode of slice with array unmarshaler: %v", v)
		}
	})
	t.Run("map", func(t *testing.T) {
		var v []mapUnmarshaler
		if err := json.Unmarshal([]byte(`[{"a":1},{"b":2},{"c":3},{"d":4},{"e":5}]`), &v); err != nil {
			t.Fatal(err)
		}
		if len(v) != 5 {
			t.Fatalf("failed to decode of slice with map unmarshaler: %v", v)
		}
		if v[0]["a"] != 10 {
			t.Fatalf("failed to decode of slice with map unmarshaler: %v", v)
		}
		if err := json.Unmarshal([]byte(`[6]`), &v); err != nil {
			t.Fatal(err)
		}
		if len(v) != 1 {
			t.Fatalf("failed to decode of slice with map unmarshaler: %v", v)
		}
		if v[0]["a"] != 10 {
			t.Fatalf("failed to decode of slice with map unmarshaler: %v", v)
		}
	})
	t.Run("struct", func(t *testing.T) {
		var v []structUnmarshaler
		if err := json.Unmarshal([]byte(`[1,2,3,4,5]`), &v); err != nil {
			t.Fatal(err)
		}
		if len(v) != 5 {
			t.Fatalf("failed to decode of slice with struct unmarshaler: %v", v)
		}
		if v[0].A != 10 {
			t.Fatalf("failed to decode of slice with struct unmarshaler: %v", v)
		}
		if err := json.Unmarshal([]byte(`[6]`), &v); err != nil {
			t.Fatal(err)
		}
		if len(v) != 1 {
			t.Fatalf("failed to decode of slice with struct unmarshaler: %v", v)
		}
		if v[0].A != 10 {
			t.Fatalf("failed to decode of slice with struct unmarshaler: %v", v)
		}
	})
}

type keepRefTest struct {
	A int
	B string
}

func (t *keepRefTest) UnmarshalJSON(data []byte) error {
	v := []interface{}{&t.A, &t.B}
	return json.Unmarshal(data, &v)
}

func TestKeepReferenceSlice(t *testing.T) {
	var v keepRefTest
	if err := json.Unmarshal([]byte(`[54,"hello"]`), &v); err != nil {
		t.Fatal(err)
	}
	if v.A != 54 {
		t.Fatal("failed to keep reference for slice")
	}
	if v.B != "hello" {
		t.Fatal("failed to keep reference for slice")
	}
}

func TestInvalidTopLevelValue(t *testing.T) {
	t.Run("invalid end of buffer", func(t *testing.T) {
		var v struct{}
		if err := stdjson.Unmarshal([]byte(`{}0`), &v); err == nil {
			t.Fatal("expected error")
		}
		if err := json.Unmarshal([]byte(`{}0`), &v); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("invalid object", func(t *testing.T) {
		var v interface{}
		if err := stdjson.Unmarshal([]byte(`{"a":4}{"a"5}`), &v); err == nil {
			t.Fatal("expected error")
		}
		if err := json.Unmarshal([]byte(`{"a":4}{"a"5}`), &v); err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestInvalidNumber(t *testing.T) {
	t.Run("invalid length of number", func(t *testing.T) {
		invalidNum := strings.Repeat("1", 30)
		t.Run("int", func(t *testing.T) {
			var v int64
			stdErr := stdjson.Unmarshal([]byte(invalidNum), &v)
			if stdErr == nil {
				t.Fatal("expected error")
			}
			err := json.Unmarshal([]byte(invalidNum), &v)
			if err == nil {
				t.Fatal("expected error")
			}
			if stdErr.Error() != err.Error() {
				t.Fatalf("unexpected error message. expected: %q but got %q", stdErr.Error(), err.Error())
			}
		})
		t.Run("uint", func(t *testing.T) {
			var v uint64
			stdErr := stdjson.Unmarshal([]byte(invalidNum), &v)
			if stdErr == nil {
				t.Fatal("expected error")
			}
			err := json.Unmarshal([]byte(invalidNum), &v)
			if err == nil {
				t.Fatal("expected error")
			}
			if stdErr.Error() != err.Error() {
				t.Fatalf("unexpected error message. expected: %q but got %q", stdErr.Error(), err.Error())
			}
		})

	})
	t.Run("invalid number of zero", func(t *testing.T) {
		t.Run("int", func(t *testing.T) {
			invalidNum := strings.Repeat("0", 10)
			var v int64
			stdErr := stdjson.Unmarshal([]byte(invalidNum), &v)
			if stdErr == nil {
				t.Fatal("expected error")
			}
			err := json.Unmarshal([]byte(invalidNum), &v)
			if err == nil {
				t.Fatal("expected error")
			}
			if stdErr.Error() != err.Error() {
				t.Fatalf("unexpected error message. expected: %q but got %q", stdErr.Error(), err.Error())
			}
		})
		t.Run("uint", func(t *testing.T) {
			invalidNum := strings.Repeat("0", 10)
			var v uint64
			stdErr := stdjson.Unmarshal([]byte(invalidNum), &v)
			if stdErr == nil {
				t.Fatal("expected error")
			}
			err := json.Unmarshal([]byte(invalidNum), &v)
			if err == nil {
				t.Fatal("expected error")
			}
			if stdErr.Error() != err.Error() {
				t.Fatalf("unexpected error message. expected: %q but got %q", stdErr.Error(), err.Error())
			}
		})
	})
	t.Run("invalid number", func(t *testing.T) {
		t.Run("int", func(t *testing.T) {
			t.Run("-0", func(t *testing.T) {
				var v int64
				if err := stdjson.Unmarshal([]byte(`-0`), &v); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal([]byte(`-0`), &v); err != nil {
					t.Fatal(err)
				}
			})
			t.Run("+0", func(t *testing.T) {
				var v int64
				if err := stdjson.Unmarshal([]byte(`+0`), &v); err == nil {
					t.Error("expected error")
				}
				if err := json.Unmarshal([]byte(`+0`), &v); err == nil {
					t.Error("expected error")
				}
			})
		})
		t.Run("uint", func(t *testing.T) {
			t.Run("-0", func(t *testing.T) {
				var v uint64
				if err := stdjson.Unmarshal([]byte(`-0`), &v); err == nil {
					t.Error("expected error")
				}
				if err := json.Unmarshal([]byte(`-0`), &v); err == nil {
					t.Error("expected error")
				}
			})
			t.Run("+0", func(t *testing.T) {
				var v uint64
				if err := stdjson.Unmarshal([]byte(`+0`), &v); err == nil {
					t.Error("expected error")
				}
				if err := json.Unmarshal([]byte(`+0`), &v); err == nil {
					t.Error("expected error")
				}
			})
		})
		t.Run("float", func(t *testing.T) {
			t.Run("0.0", func(t *testing.T) {
				var f float64
				if err := stdjson.Unmarshal([]byte(`0.0`), &f); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal([]byte(`0.0`), &f); err != nil {
					t.Fatal(err)
				}
			})
			t.Run("0.000000000", func(t *testing.T) {
				var f float64
				if err := stdjson.Unmarshal([]byte(`0.000000000`), &f); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal([]byte(`0.000000000`), &f); err != nil {
					t.Fatal(err)
				}
			})
			t.Run("repeat zero a lot with float value", func(t *testing.T) {
				var f float64
				if err := stdjson.Unmarshal([]byte("0."+strings.Repeat("0", 30)), &f); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal([]byte("0."+strings.Repeat("0", 30)), &f); err != nil {
					t.Fatal(err)
				}
			})
		})
	})
}

type someInterface interface {
	DoesNotMatter()
}

func TestDecodeUnknownInterface(t *testing.T) {
	t.Run("unmarshal", func(t *testing.T) {
		var v map[string]someInterface
		if err := json.Unmarshal([]byte(`{"a":null,"b":null}`), &v); err != nil {
			t.Fatal(err)
		}
		if len(v) != 2 {
			t.Fatalf("failed to decode: %v", v)
		}
		if a, exists := v["a"]; a != nil || !exists {
			t.Fatalf("failed to decode: %v", v)
		}
		if b, exists := v["b"]; b != nil || !exists {
			t.Fatalf("failed to decode: %v", v)
		}
	})
	t.Run("stream", func(t *testing.T) {
		var v map[string]someInterface
		if err := json.NewDecoder(strings.NewReader(`{"a":null,"b":null}`)).Decode(&v); err != nil {
			t.Fatal(err)
		}
		if len(v) != 2 {
			t.Fatalf("failed to decode: %v", v)
		}
		if a, exists := v["a"]; a != nil || !exists {
			t.Fatalf("failed to decode: %v", v)
		}
		if b, exists := v["b"]; b != nil || !exists {
			t.Fatalf("failed to decode: %v", v)
		}
	})
}

func TestDecodeByteSliceNull(t *testing.T) {
	t.Run("unmarshal", func(t *testing.T) {
		var v1 []byte
		if err := stdjson.Unmarshal([]byte(`null`), &v1); err != nil {
			t.Fatal(err)
		}
		var v2 []byte
		if err := json.Unmarshal([]byte(`null`), &v2); err != nil {
			t.Fatal(err)
		}
		if v1 == nil && v2 != nil || len(v1) != len(v2) {
			t.Fatalf("failed to decode null to []byte. expected:%#v but got %#v", v1, v2)
		}
	})
	t.Run("stream", func(t *testing.T) {
		var v1 []byte
		if err := stdjson.NewDecoder(strings.NewReader(`null`)).Decode(&v1); err != nil {
			t.Fatal(err)
		}
		var v2 []byte
		if err := json.NewDecoder(strings.NewReader(`null`)).Decode(&v2); err != nil {
			t.Fatal(err)
		}
		if v1 == nil && v2 != nil || len(v1) != len(v2) {
			t.Fatalf("failed to decode null to []byte. expected:%#v but got %#v", v1, v2)
		}
	})
}

func TestDecodeBackSlash(t *testing.T) {
	t.Run("unmarshal", func(t *testing.T) {
		t.Run("string", func(t *testing.T) {
			var v1 map[string]stdjson.RawMessage
			if err := stdjson.Unmarshal([]byte(`{"c":"\\"}`), &v1); err != nil {
				t.Fatal(err)
			}
			var v2 map[string]json.RawMessage
			if err := json.Unmarshal([]byte(`{"c":"\\"}`), &v2); err != nil {
				t.Fatal(err)
			}
			if len(v1) != len(v2) || !bytes.Equal(v1["c"], v2["c"]) {
				t.Fatalf("failed to decode backslash: expected %#v but got %#v", v1, v2)
			}
		})
		t.Run("array", func(t *testing.T) {
			var v1 map[string]stdjson.RawMessage
			if err := stdjson.Unmarshal([]byte(`{"c":["\\"]}`), &v1); err != nil {
				t.Fatal(err)
			}
			var v2 map[string]json.RawMessage
			if err := json.Unmarshal([]byte(`{"c":["\\"]}`), &v2); err != nil {
				t.Fatal(err)
			}
			if len(v1) != len(v2) || !bytes.Equal(v1["c"], v2["c"]) {
				t.Fatalf("failed to decode backslash: expected %#v but got %#v", v1, v2)
			}
		})
		t.Run("object", func(t *testing.T) {
			var v1 map[string]stdjson.RawMessage
			if err := stdjson.Unmarshal([]byte(`{"c":{"\\":"\\"}}`), &v1); err != nil {
				t.Fatal(err)
			}
			var v2 map[string]json.RawMessage
			if err := json.Unmarshal([]byte(`{"c":{"\\":"\\"}}`), &v2); err != nil {
				t.Fatal(err)
			}
			if len(v1) != len(v2) || !bytes.Equal(v1["c"], v2["c"]) {
				t.Fatalf("failed to decode backslash: expected %#v but got %#v", v1, v2)
			}
		})
	})
	t.Run("stream", func(t *testing.T) {
		t.Run("string", func(t *testing.T) {
			var v1 map[string]stdjson.RawMessage
			if err := stdjson.NewDecoder(strings.NewReader(`{"c":"\\"}`)).Decode(&v1); err != nil {
				t.Fatal(err)
			}
			var v2 map[string]json.RawMessage
			if err := json.NewDecoder(strings.NewReader(`{"c":"\\"}`)).Decode(&v2); err != nil {
				t.Fatal(err)
			}
			if len(v1) != len(v2) || !bytes.Equal(v1["c"], v2["c"]) {
				t.Fatalf("failed to decode backslash: expected %#v but got %#v", v1, v2)
			}
		})
		t.Run("array", func(t *testing.T) {
			var v1 map[string]stdjson.RawMessage
			if err := stdjson.NewDecoder(strings.NewReader(`{"c":["\\"]}`)).Decode(&v1); err != nil {
				t.Fatal(err)
			}
			var v2 map[string]json.RawMessage
			if err := json.NewDecoder(strings.NewReader(`{"c":["\\"]}`)).Decode(&v2); err != nil {
				t.Fatal(err)
			}
			if len(v1) != len(v2) || !bytes.Equal(v1["c"], v2["c"]) {
				t.Fatalf("failed to decode backslash: expected %#v but got %#v", v1, v2)
			}
		})
		t.Run("object", func(t *testing.T) {
			var v1 map[string]stdjson.RawMessage
			if err := stdjson.NewDecoder(strings.NewReader(`{"c":{"\\":"\\"}}`)).Decode(&v1); err != nil {
				t.Fatal(err)
			}
			var v2 map[string]json.RawMessage
			if err := json.NewDecoder(strings.NewReader(`{"c":{"\\":"\\"}}`)).Decode(&v2); err != nil {
				t.Fatal(err)
			}
			if len(v1) != len(v2) || !bytes.Equal(v1["c"], v2["c"]) {
				t.Fatalf("failed to decode backslash: expected %#v but got %#v", v1, v2)
			}
		})
	})
}

func TestIssue218(t *testing.T) {
	type A struct {
		X int
	}
	type B struct {
		Y int
	}
	type S struct {
		A *A `json:"a,omitempty"`
		B *B `json:"b,omitempty"`
	}
	tests := []struct {
		name     string
		given    []S
		expected []S
	}{
		{
			name: "A should be correct",
			given: []S{{
				A: &A{
					X: 1,
				},
			}},
			expected: []S{{
				A: &A{
					X: 1,
				},
			}},
		},
		{
			name: "B should be correct",
			given: []S{{
				B: &B{
					Y: 2,
				},
			}},
			expected: []S{{
				B: &B{
					Y: 2,
				},
			}},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := json.NewEncoder(&buf).Encode(test.given); err != nil {
				t.Fatal(err)
			}
			var actual []S
			if err := json.NewDecoder(bytes.NewReader(buf.Bytes())).Decode(&actual); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(test.expected, actual) {
				t.Fatalf("mismatch value: expected %v but got %v", test.expected, actual)
			}
		})
	}
}

func TestDecodeEscapedCharField(t *testing.T) {
	b := []byte(`{"\u6D88\u606F":"\u6D88\u606F"}`)
	t.Run("unmarshal", func(t *testing.T) {
		v := struct {
			Msg string `json:"消息"`
		}{}
		if err := json.Unmarshal(b, &v); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal([]byte(v.Msg), []byte("消息")) {
			t.Fatal("failed to decode unicode char")
		}
	})
	t.Run("stream", func(t *testing.T) {
		v := struct {
			Msg string `json:"消息"`
		}{}
		if err := json.NewDecoder(bytes.NewBuffer(b)).Decode(&v); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal([]byte(v.Msg), []byte("消息")) {
			t.Fatal("failed to decode unicode char")
		}
	})
}

type unmarshalContextKey struct{}

type unmarshalContextStructType struct {
	v int
}

func (t *unmarshalContextStructType) UnmarshalJSON(ctx context.Context, b []byte) error {
	v := ctx.Value(unmarshalContextKey{})
	s, ok := v.(string)
	if !ok {
		return fmt.Errorf("failed to propagate parent context.Context")
	}
	if s != "hello" {
		return fmt.Errorf("failed to propagate parent context.Context")
	}
	t.v = 100
	return nil
}

func TestDecodeContextOption(t *testing.T) {
	src := []byte("10")
	buf := bytes.NewBuffer(src)

	t.Run("UnmarshalContext", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), unmarshalContextKey{}, "hello")
		var v unmarshalContextStructType
		if err := json.UnmarshalContext(ctx, src, &v); err != nil {
			t.Fatal(err)
		}
		if v.v != 100 {
			t.Fatal("failed to decode with context")
		}
	})
	t.Run("DecodeContext", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), unmarshalContextKey{}, "hello")
		var v unmarshalContextStructType
		if err := json.NewDecoder(buf).DecodeContext(ctx, &v); err != nil {
			t.Fatal(err)
		}
		if v.v != 100 {
			t.Fatal("failed to decode with context")
		}
	})
}

func TestIssue251(t *testing.T) {
	array := [3]int{1, 2, 3}
	err := stdjson.Unmarshal([]byte("[ ]"), &array)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(array)

	array = [3]int{1, 2, 3}
	err = json.Unmarshal([]byte("[ ]"), &array)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(array)

	array = [3]int{1, 2, 3}
	err = json.NewDecoder(strings.NewReader(`[ ]`)).Decode(&array)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(array)
}

func TestDecodeBinaryTypeWithEscapedChar(t *testing.T) {
	type T struct {
		Msg []byte `json:"msg"`
	}
	content := []byte(`{"msg":"aGVsbG8K\n"}`)
	t.Run("unmarshal", func(t *testing.T) {
		var expected T
		if err := stdjson.Unmarshal(content, &expected); err != nil {
			t.Fatal(err)
		}
		var got T
		if err := json.Unmarshal(content, &got); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(expected.Msg, got.Msg) {
			t.Fatalf("failed to decode binary type with escaped char. expected %q but got %q", expected.Msg, got.Msg)
		}
	})
	t.Run("stream", func(t *testing.T) {
		var expected T
		if err := stdjson.NewDecoder(bytes.NewBuffer(content)).Decode(&expected); err != nil {
			t.Fatal(err)
		}
		var got T
		if err := json.NewDecoder(bytes.NewBuffer(content)).Decode(&got); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(expected.Msg, got.Msg) {
			t.Fatalf("failed to decode binary type with escaped char. expected %q but got %q", expected.Msg, got.Msg)
		}
	})
}

func TestIssue282(t *testing.T) {
	var J = []byte(`{
  "a": {},
  "b": {},
  "c": {},
  "d": {},
  "e": {},
  "f": {},
  "g": {},
  "h": {
    "m": "1"
  },
  "i": {}
}`)

	type T4 struct {
		F0 string
		F1 string
		F2 string
		F3 string
		F4 string
		F5 string
		F6 int
	}
	type T3 struct {
		F0 string
		F1 T4
	}
	type T2 struct {
		F0 string `json:"m"`
		F1 T3
	}
	type T0 map[string]T2

	// T2 size is 136 bytes. This is indirect type.
	var v T0
	if err := json.Unmarshal(J, &v); err != nil {
		t.Fatal(err)
	}
	if v["h"].F0 != "1" {
		t.Fatalf("failed to assign map value")
	}
}

func TestDecodeStructFieldMap(t *testing.T) {
	type Foo struct {
		Bar map[float64]float64 `json:"bar,omitempty"`
	}
	var v Foo
	if err := json.Unmarshal([]byte(`{"name":"test"}`), &v); err != nil {
		t.Fatal(err)
	}
	if v.Bar != nil {
		t.Fatalf("failed to decode v.Bar = %+v", v.Bar)
	}
}

type issue303 struct {
	Count int
	Type  string
	Value interface{}
}

func (t *issue303) UnmarshalJSON(b []byte) error {
	type tmpType issue303

	wrapped := struct {
		Value json.RawMessage
		tmpType
	}{}
	if err := json.Unmarshal(b, &wrapped); err != nil {
		return err
	}
	*t = issue303(wrapped.tmpType)

	switch wrapped.Type {
	case "string":
		var str string
		if err := json.Unmarshal(wrapped.Value, &str); err != nil {
			return err
		}
		t.Value = str
	}
	return nil
}

func TestIssue303(t *testing.T) {
	var v issue303
	if err := json.Unmarshal([]byte(`{"Count":7,"Type":"string","Value":"hello"}`), &v); err != nil {
		t.Fatal(err)
	}
	if v.Count != 7 || v.Type != "string" || v.Value != "hello" {
		t.Fatalf("failed to decode. count = %d type = %s value = %v", v.Count, v.Type, v.Value)
	}
}

func TestIssue327(t *testing.T) {
	var v struct {
		Date time.Time `json:"date"`
	}
	dec := json.NewDecoder(strings.NewReader(`{"date": "2021-11-23T13:47:30+01:00"})`))
	if err := dec.DecodeContext(context.Background(), &v); err != nil {
		t.Fatal(err)
	}
	expected := "2021-11-23T13:47:30+01:00"
	if got := v.Date.Format(time.RFC3339); got != expected {
		t.Fatalf("failed to decode. expected %q but got %q", expected, got)
	}
}

func TestIssue337(t *testing.T) {
	in := strings.Repeat(" ", 510) + "{}"
	var m map[string]string
	if err := json.NewDecoder(strings.NewReader(in)).Decode(&m); err != nil {
		t.Fatal("unexpected error:", err)
	}
	if len(m) != 0 {
		t.Fatal("unexpected result", m)
	}
}

func Benchmark306(b *testing.B) {
	type T0 struct {
		Str string
	}
	in := []byte(`{"Str":"` + strings.Repeat(`abcd\"`, 10000) + `"}`)
	b.Run("stdjson", func(b *testing.B) {
		var x T0
		for i := 0; i < b.N; i++ {
			stdjson.Unmarshal(in, &x)
		}
	})
	b.Run("go-json", func(b *testing.B) {
		var x T0
		for i := 0; i < b.N; i++ {
			json.Unmarshal(in, &x)
		}
	})
}

func TestIssue348(t *testing.T) {
	in := strings.Repeat("["+strings.Repeat(",1000", 500)[1:]+"]", 2)
	dec := json.NewDecoder(strings.NewReader(in))
	for dec.More() {
		var foo interface{}
		if err := dec.Decode(&foo); err != nil {
			t.Error(err)
		}
	}
}

type issue342 string

func (t *issue342) UnmarshalJSON(b []byte) error {
	panic("unreachable")
}

func TestIssue342(t *testing.T) {
	var v map[issue342]int
	in := []byte(`{"a":1}`)
	if err := json.Unmarshal(in, &v); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	expected := 1
	if got := v["a"]; got != expected {
		t.Errorf("unexpected result: got(%v) != expected(%v)", got, expected)
	}
}

func TestIssue360(t *testing.T) {
	var uints []uint8
	err := json.Unmarshal([]byte(`[0, 1, 2]`), &uints)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(uints) != 3 || !(uints[0] == 0 && uints[1] == 1 && uints[2] == 2) {
		t.Errorf("unexpected result: %v", uints)
	}
}

func TestIssue359(t *testing.T) {
	var a interface{} = 1
	var b interface{} = &a
	var c interface{} = &b
	v, err := json.Marshal(c)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if string(v) != "1" {
		t.Errorf("unexpected result: %v", string(v))
	}
}

func TestIssue364(t *testing.T) {
	var v struct {
		Description string `json:"description"`
	}
	err := json.Unmarshal([]byte(`{"description":"\uD83D\uDE87 Toledo is a metro station"}`), &v)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if v.Description != "🚇 Toledo is a metro station" {
		t.Errorf("unexpected result: %v", v.Description)
	}
}

func TestIssue362(t *testing.T) {
	type AliasedPrimitive int
	type Combiner struct {
		SomeField int
		AliasedPrimitive
	}
	originalCombiner := Combiner{AliasedPrimitive: 7}
	b, err := json.Marshal(originalCombiner)
	assertErr(t, err)
	newCombiner := Combiner{}
	err = json.Unmarshal(b, &newCombiner)
	assertErr(t, err)
	assertEq(t, "TestEmbeddedPrimitiveAlias", originalCombiner, newCombiner)
}

func TestIssue335(t *testing.T) {
	var v []string
	in := []byte(`["\u","A"]`)
	err := json.Unmarshal(in, &v)
	if err == nil {
		t.Errorf("unexpected success")
	}
}

func TestIssue372(t *testing.T) {
	type A int
	type T struct {
		_ int
		*A
	}
	var v T
	err := json.Unmarshal([]byte(`{"A":7}`), &v)
	assertErr(t, err)

	got := *v.A
	expected := A(7)
	if got != expected {
		t.Errorf("unexpected result: %v != %v", got, expected)
	}
}

type issue384 struct{}

func (t *issue384) UnmarshalJSON(b []byte) error {
	return nil
}

func TestIssue384(t *testing.T) {
	testcases := []string{
		`{"data": "` + strings.Repeat("-", 500) + `\""}`,
		`["` + strings.Repeat("-", 508) + `\""]`,
	}
	for _, tc := range testcases {
		dec := json.NewDecoder(strings.NewReader(tc))
		var v issue384
		if err := dec.Decode(&v); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}
}

func TestIssue408(t *testing.T) {
	type T struct {
		Arr [2]int32 `json:"arr"`
	}
	var v T
	if err := json.Unmarshal([]byte(`{"arr": [1,2]}`), &v); err != nil {
		t.Fatal(err)
	}
}

func TestIssue416(t *testing.T) {
	b := []byte(`{"Сообщение":"Текст"}`)

	type T struct {
		Msg string `json:"Сообщение"`
	}
	var x T
	err := json.Unmarshal(b, &x)
	assertErr(t, err)
	assertEq(t, "unexpected result", "Текст", x.Msg)
}

func TestIssue429(t *testing.T) {
	var x struct {
		N int32
	}
	for _, b := range []string{
		`{"\u"`,
		`{"\u0"`,
		`{"\u00"`,
	} {
		if err := json.Unmarshal([]byte(b), &x); err == nil {
			t.Errorf("unexpected success")
		}
	}
}
