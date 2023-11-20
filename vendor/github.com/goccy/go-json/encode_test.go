package json_test

import (
	"bytes"
	"context"
	"encoding"
	stdjson "encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/big"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/goccy/go-json"
)

type recursiveT struct {
	A *recursiveT `json:"a,omitempty"`
	B *recursiveU `json:"b,omitempty"`
	C *recursiveU `json:"c,omitempty"`
	D string      `json:"d,omitempty"`
}

type recursiveU struct {
	T *recursiveT `json:"t,omitempty"`
}

func Test_Marshal(t *testing.T) {
	t.Run("int", func(t *testing.T) {
		bytes, err := json.Marshal(-10)
		assertErr(t, err)
		assertEq(t, "int", `-10`, string(bytes))
	})
	t.Run("int8", func(t *testing.T) {
		bytes, err := json.Marshal(int8(-11))
		assertErr(t, err)
		assertEq(t, "int8", `-11`, string(bytes))
	})
	t.Run("int16", func(t *testing.T) {
		bytes, err := json.Marshal(int16(-12))
		assertErr(t, err)
		assertEq(t, "int16", `-12`, string(bytes))
	})
	t.Run("int32", func(t *testing.T) {
		bytes, err := json.Marshal(int32(-13))
		assertErr(t, err)
		assertEq(t, "int32", `-13`, string(bytes))
	})
	t.Run("int64", func(t *testing.T) {
		bytes, err := json.Marshal(int64(-14))
		assertErr(t, err)
		assertEq(t, "int64", `-14`, string(bytes))
	})
	t.Run("uint", func(t *testing.T) {
		bytes, err := json.Marshal(uint(10))
		assertErr(t, err)
		assertEq(t, "uint", `10`, string(bytes))
	})
	t.Run("uint8", func(t *testing.T) {
		bytes, err := json.Marshal(uint8(11))
		assertErr(t, err)
		assertEq(t, "uint8", `11`, string(bytes))
	})
	t.Run("uint16", func(t *testing.T) {
		bytes, err := json.Marshal(uint16(12))
		assertErr(t, err)
		assertEq(t, "uint16", `12`, string(bytes))
	})
	t.Run("uint32", func(t *testing.T) {
		bytes, err := json.Marshal(uint32(13))
		assertErr(t, err)
		assertEq(t, "uint32", `13`, string(bytes))
	})
	t.Run("uint64", func(t *testing.T) {
		bytes, err := json.Marshal(uint64(14))
		assertErr(t, err)
		assertEq(t, "uint64", `14`, string(bytes))
	})
	t.Run("float32", func(t *testing.T) {
		bytes, err := json.Marshal(float32(3.14))
		assertErr(t, err)
		assertEq(t, "float32", `3.14`, string(bytes))
	})
	t.Run("float64", func(t *testing.T) {
		bytes, err := json.Marshal(float64(3.14))
		assertErr(t, err)
		assertEq(t, "float64", `3.14`, string(bytes))
	})
	t.Run("bool", func(t *testing.T) {
		bytes, err := json.Marshal(true)
		assertErr(t, err)
		assertEq(t, "bool", `true`, string(bytes))
	})
	t.Run("string", func(t *testing.T) {
		bytes, err := json.Marshal("hello world")
		assertErr(t, err)
		assertEq(t, "string", `"hello world"`, string(bytes))
	})
	t.Run("struct", func(t *testing.T) {
		bytes, err := json.Marshal(struct {
			A int    `json:"a"`
			B uint   `json:"b"`
			C string `json:"c"`
			D int    `json:"-"`  // ignore field
			a int    `json:"aa"` // private field
		}{
			A: -1,
			B: 1,
			C: "hello world",
		})
		assertErr(t, err)
		assertEq(t, "struct", `{"a":-1,"b":1,"c":"hello world"}`, string(bytes))
		t.Run("null", func(t *testing.T) {
			type T struct {
				A *struct{} `json:"a"`
			}
			var v T
			bytes, err := json.Marshal(&v)
			assertErr(t, err)
			assertEq(t, "struct", `{"a":null}`, string(bytes))
		})
		t.Run("recursive", func(t *testing.T) {
			bytes, err := json.Marshal(recursiveT{
				A: &recursiveT{
					B: &recursiveU{
						T: &recursiveT{
							D: "hello",
						},
					},
					C: &recursiveU{
						T: &recursiveT{
							D: "world",
						},
					},
				},
			})
			assertErr(t, err)
			assertEq(t, "recursive", `{"a":{"b":{"t":{"d":"hello"}},"c":{"t":{"d":"world"}}}}`, string(bytes))
		})
		t.Run("embedded", func(t *testing.T) {
			type T struct {
				A string `json:"a"`
			}
			type U struct {
				*T
				B string `json:"b"`
			}
			type T2 struct {
				A string `json:"a,omitempty"`
			}
			type U2 struct {
				*T2
				B string `json:"b,omitempty"`
			}
			t.Run("exists field", func(t *testing.T) {
				bytes, err := json.Marshal(&U{
					T: &T{
						A: "aaa",
					},
					B: "bbb",
				})
				assertErr(t, err)
				assertEq(t, "embedded", `{"a":"aaa","b":"bbb"}`, string(bytes))
				t.Run("omitempty", func(t *testing.T) {
					bytes, err := json.Marshal(&U2{
						T2: &T2{
							A: "aaa",
						},
						B: "bbb",
					})
					assertErr(t, err)
					assertEq(t, "embedded", `{"a":"aaa","b":"bbb"}`, string(bytes))
				})
			})
			t.Run("none field", func(t *testing.T) {
				bytes, err := json.Marshal(&U{
					B: "bbb",
				})
				assertErr(t, err)
				assertEq(t, "embedded", `{"b":"bbb"}`, string(bytes))
				t.Run("omitempty", func(t *testing.T) {
					bytes, err := json.Marshal(&U2{
						B: "bbb",
					})
					assertErr(t, err)
					assertEq(t, "embedded", `{"b":"bbb"}`, string(bytes))
				})
			})
		})

		t.Run("embedded with tag", func(t *testing.T) {
			type T struct {
				A string `json:"a"`
			}
			type U struct {
				*T `json:"t"`
				B  string `json:"b"`
			}
			type T2 struct {
				A string `json:"a,omitempty"`
			}
			type U2 struct {
				*T2 `json:"t,omitempty"`
				B   string `json:"b,omitempty"`
			}
			t.Run("exists field", func(t *testing.T) {
				bytes, err := json.Marshal(&U{
					T: &T{
						A: "aaa",
					},
					B: "bbb",
				})
				assertErr(t, err)
				assertEq(t, "embedded", `{"t":{"a":"aaa"},"b":"bbb"}`, string(bytes))
				t.Run("omitempty", func(t *testing.T) {
					bytes, err := json.Marshal(&U2{
						T2: &T2{
							A: "aaa",
						},
						B: "bbb",
					})
					assertErr(t, err)
					assertEq(t, "embedded", `{"t":{"a":"aaa"},"b":"bbb"}`, string(bytes))
				})
			})

			t.Run("none field", func(t *testing.T) {
				bytes, err := json.Marshal(&U{
					B: "bbb",
				})
				assertErr(t, err)
				assertEq(t, "embedded", `{"t":null,"b":"bbb"}`, string(bytes))
				t.Run("omitempty", func(t *testing.T) {
					bytes, err := json.Marshal(&U2{
						B: "bbb",
					})
					assertErr(t, err)
					assertEq(t, "embedded", `{"b":"bbb"}`, string(bytes))
				})
			})
		})

		t.Run("omitempty", func(t *testing.T) {
			type T struct {
				A int                    `json:",omitempty"`
				B int8                   `json:",omitempty"`
				C int16                  `json:",omitempty"`
				D int32                  `json:",omitempty"`
				E int64                  `json:",omitempty"`
				F uint                   `json:",omitempty"`
				G uint8                  `json:",omitempty"`
				H uint16                 `json:",omitempty"`
				I uint32                 `json:",omitempty"`
				J uint64                 `json:",omitempty"`
				K float32                `json:",omitempty"`
				L float64                `json:",omitempty"`
				O string                 `json:",omitempty"`
				P bool                   `json:",omitempty"`
				Q []int                  `json:",omitempty"`
				R map[string]interface{} `json:",omitempty"`
				S *struct{}              `json:",omitempty"`
				T int                    `json:"t,omitempty"`
			}
			var v T
			v.T = 1
			bytes, err := json.Marshal(&v)
			assertErr(t, err)
			assertEq(t, "struct", `{"t":1}`, string(bytes))
			t.Run("int", func(t *testing.T) {
				var v struct {
					A int `json:"a,omitempty"`
					B int `json:"b"`
				}
				v.B = 1
				bytes, err := json.Marshal(&v)
				assertErr(t, err)
				assertEq(t, "int", `{"b":1}`, string(bytes))
			})
			t.Run("int8", func(t *testing.T) {
				var v struct {
					A int  `json:"a,omitempty"`
					B int8 `json:"b"`
				}
				v.B = 1
				bytes, err := json.Marshal(&v)
				assertErr(t, err)
				assertEq(t, "int8", `{"b":1}`, string(bytes))
			})
			t.Run("int16", func(t *testing.T) {
				var v struct {
					A int   `json:"a,omitempty"`
					B int16 `json:"b"`
				}
				v.B = 1
				bytes, err := json.Marshal(&v)
				assertErr(t, err)
				assertEq(t, "int16", `{"b":1}`, string(bytes))
			})
			t.Run("int32", func(t *testing.T) {
				var v struct {
					A int   `json:"a,omitempty"`
					B int32 `json:"b"`
				}
				v.B = 1
				bytes, err := json.Marshal(&v)
				assertErr(t, err)
				assertEq(t, "int32", `{"b":1}`, string(bytes))
			})
			t.Run("int64", func(t *testing.T) {
				var v struct {
					A int   `json:"a,omitempty"`
					B int64 `json:"b"`
				}
				v.B = 1
				bytes, err := json.Marshal(&v)
				assertErr(t, err)
				assertEq(t, "int64", `{"b":1}`, string(bytes))
			})
			t.Run("string", func(t *testing.T) {
				var v struct {
					A int    `json:"a,omitempty"`
					B string `json:"b"`
				}
				v.B = "b"
				bytes, err := json.Marshal(&v)
				assertErr(t, err)
				assertEq(t, "string", `{"b":"b"}`, string(bytes))
			})
			t.Run("float32", func(t *testing.T) {
				var v struct {
					A int     `json:"a,omitempty"`
					B float32 `json:"b"`
				}
				v.B = 1.1
				bytes, err := json.Marshal(&v)
				assertErr(t, err)
				assertEq(t, "float32", `{"b":1.1}`, string(bytes))
			})
			t.Run("float64", func(t *testing.T) {
				var v struct {
					A int     `json:"a,omitempty"`
					B float64 `json:"b"`
				}
				v.B = 3.14
				bytes, err := json.Marshal(&v)
				assertErr(t, err)
				assertEq(t, "float64", `{"b":3.14}`, string(bytes))
			})
			t.Run("slice", func(t *testing.T) {
				var v struct {
					A int   `json:"a,omitempty"`
					B []int `json:"b"`
				}
				v.B = []int{1, 2, 3}
				bytes, err := json.Marshal(&v)
				assertErr(t, err)
				assertEq(t, "slice", `{"b":[1,2,3]}`, string(bytes))
			})
			t.Run("array", func(t *testing.T) {
				var v struct {
					A int    `json:"a,omitempty"`
					B [2]int `json:"b"`
				}
				v.B = [2]int{1, 2}
				bytes, err := json.Marshal(&v)
				assertErr(t, err)
				assertEq(t, "array", `{"b":[1,2]}`, string(bytes))
			})
			t.Run("map", func(t *testing.T) {
				v := new(struct {
					A int                    `json:"a,omitempty"`
					B map[string]interface{} `json:"b"`
				})
				v.B = map[string]interface{}{"c": 1}
				bytes, err := json.Marshal(v)
				assertErr(t, err)
				assertEq(t, "array", `{"b":{"c":1}}`, string(bytes))
			})
		})
		t.Run("head_omitempty", func(t *testing.T) {
			type T struct {
				A *struct{} `json:"a,omitempty"`
			}
			var v T
			bytes, err := json.Marshal(&v)
			assertErr(t, err)
			assertEq(t, "struct", `{}`, string(bytes))
		})
		t.Run("pointer_head_omitempty", func(t *testing.T) {
			type V struct{}
			type U struct {
				B *V `json:"b,omitempty"`
			}
			type T struct {
				A *U `json:"a"`
			}
			bytes, err := json.Marshal(&T{A: &U{}})
			assertErr(t, err)
			assertEq(t, "struct", `{"a":{}}`, string(bytes))
		})
		t.Run("head_int_omitempty", func(t *testing.T) {
			type T struct {
				A int `json:"a,omitempty"`
			}
			var v T
			bytes, err := json.Marshal(&v)
			assertErr(t, err)
			assertEq(t, "struct", `{}`, string(bytes))
		})
	})
	t.Run("slice", func(t *testing.T) {
		t.Run("[]int", func(t *testing.T) {
			bytes, err := json.Marshal([]int{1, 2, 3, 4})
			assertErr(t, err)
			assertEq(t, "[]int", `[1,2,3,4]`, string(bytes))
		})
		t.Run("[]interface{}", func(t *testing.T) {
			bytes, err := json.Marshal([]interface{}{1, 2.1, "hello"})
			assertErr(t, err)
			assertEq(t, "[]interface{}", `[1,2.1,"hello"]`, string(bytes))
		})
	})

	t.Run("array", func(t *testing.T) {
		bytes, err := json.Marshal([4]int{1, 2, 3, 4})
		assertErr(t, err)
		assertEq(t, "array", `[1,2,3,4]`, string(bytes))
	})
	t.Run("map", func(t *testing.T) {
		t.Run("map[string]int", func(t *testing.T) {
			v := map[string]int{
				"a": 1,
				"b": 2,
				"c": 3,
				"d": 4,
			}
			bytes, err := json.Marshal(v)
			assertErr(t, err)
			assertEq(t, "map", `{"a":1,"b":2,"c":3,"d":4}`, string(bytes))
			b, err := json.MarshalWithOption(v, json.UnorderedMap())
			assertErr(t, err)
			assertEq(t, "unordered map", len(`{"a":1,"b":2,"c":3,"d":4}`), len(string(b)))
		})
		t.Run("map[string]interface{}", func(t *testing.T) {
			type T struct {
				A int
			}
			v := map[string]interface{}{
				"a": 1,
				"b": 2.1,
				"c": &T{
					A: 10,
				},
				"d": 4,
			}
			bytes, err := json.Marshal(v)
			assertErr(t, err)
			assertEq(t, "map[string]interface{}", `{"a":1,"b":2.1,"c":{"A":10},"d":4}`, string(bytes))
		})
	})
}

type mustErrTypeForDebug struct{}

func (mustErrTypeForDebug) MarshalJSON() ([]byte, error) {
	panic("panic")
	return nil, fmt.Errorf("panic")
}

func TestDebugMode(t *testing.T) {
	defer func() {
		if err := recover(); err == nil {
			t.Fatal("expected error")
		}
	}()
	var buf bytes.Buffer
	json.MarshalWithOption(mustErrTypeForDebug{}, json.Debug(), json.DebugWith(&buf))
}

func TestIssue116(t *testing.T) {
	t.Run("first", func(t *testing.T) {
		type Boo struct{ B string }
		type Struct struct {
			A   int
			Boo *Boo
		}
		type Embedded struct {
			Struct
		}
		b, err := json.Marshal(Embedded{Struct: Struct{
			A:   1,
			Boo: &Boo{B: "foo"},
		}})
		if err != nil {
			t.Fatal(err)
		}
		expected := `{"A":1,"Boo":{"B":"foo"}}`
		actual := string(b)
		if actual != expected {
			t.Fatalf("expected %s but got %s", expected, actual)
		}
	})
	t.Run("second", func(t *testing.T) {
		type Boo struct{ B string }
		type Struct struct {
			A int
			B *Boo
		}
		type Embedded struct {
			Struct
		}
		b, err := json.Marshal(Embedded{Struct: Struct{
			A: 1,
			B: &Boo{B: "foo"},
		}})
		if err != nil {
			t.Fatal(err)
		}
		actual := string(b)
		expected := `{"A":1,"B":{"B":"foo"}}`
		if actual != expected {
			t.Fatalf("expected %s but got %s", expected, actual)
		}
	})
}

type marshalJSON struct{}

func (*marshalJSON) MarshalJSON() ([]byte, error) {
	return []byte(`1`), nil
}

func Test_MarshalJSON(t *testing.T) {
	t.Run("*struct", func(t *testing.T) {
		bytes, err := json.Marshal(&marshalJSON{})
		assertErr(t, err)
		assertEq(t, "MarshalJSON", "1", string(bytes))
	})
	t.Run("time", func(t *testing.T) {
		bytes, err := json.Marshal(time.Time{})
		assertErr(t, err)
		assertEq(t, "MarshalJSON", `"0001-01-01T00:00:00Z"`, string(bytes))
	})
}

func Test_MarshalIndent(t *testing.T) {
	prefix := "-"
	indent := "\t"
	t.Run("struct", func(t *testing.T) {
		v := struct {
			A int         `json:"a"`
			B uint        `json:"b"`
			C string      `json:"c"`
			D interface{} `json:"d"`
			X int         `json:"-"`  // ignore field
			a int         `json:"aa"` // private field
		}{
			A: -1,
			B: 1,
			C: "hello world",
			D: struct {
				E bool `json:"e"`
			}{
				E: true,
			},
		}
		expected, err := stdjson.MarshalIndent(v, prefix, indent)
		assertErr(t, err)
		got, err := json.MarshalIndent(v, prefix, indent)
		assertErr(t, err)
		assertEq(t, "struct", string(expected), string(got))
	})
	t.Run("slice", func(t *testing.T) {
		t.Run("[]int", func(t *testing.T) {
			bytes, err := json.MarshalIndent([]int{1, 2, 3, 4}, prefix, indent)
			assertErr(t, err)
			result := "[\n-\t1,\n-\t2,\n-\t3,\n-\t4\n-]"
			assertEq(t, "[]int", result, string(bytes))
		})
		t.Run("[]interface{}", func(t *testing.T) {
			bytes, err := json.MarshalIndent([]interface{}{1, 2.1, "hello"}, prefix, indent)
			assertErr(t, err)
			result := "[\n-\t1,\n-\t2.1,\n-\t\"hello\"\n-]"
			assertEq(t, "[]interface{}", result, string(bytes))
		})
	})

	t.Run("array", func(t *testing.T) {
		bytes, err := json.MarshalIndent([4]int{1, 2, 3, 4}, prefix, indent)
		assertErr(t, err)
		result := "[\n-\t1,\n-\t2,\n-\t3,\n-\t4\n-]"
		assertEq(t, "array", result, string(bytes))
	})
	t.Run("map", func(t *testing.T) {
		t.Run("map[string]int", func(t *testing.T) {
			bytes, err := json.MarshalIndent(map[string]int{
				"a": 1,
				"b": 2,
				"c": 3,
				"d": 4,
			}, prefix, indent)
			assertErr(t, err)
			result := "{\n-\t\"a\": 1,\n-\t\"b\": 2,\n-\t\"c\": 3,\n-\t\"d\": 4\n-}"
			assertEq(t, "map", result, string(bytes))
		})
		t.Run("map[string]interface{}", func(t *testing.T) {
			type T struct {
				E int
				F int
			}
			v := map[string]interface{}{
				"a": 1,
				"b": 2.1,
				"c": &T{
					E: 10,
					F: 11,
				},
				"d": 4,
			}
			bytes, err := json.MarshalIndent(v, prefix, indent)
			assertErr(t, err)
			result := "{\n-\t\"a\": 1,\n-\t\"b\": 2.1,\n-\t\"c\": {\n-\t\t\"E\": 10,\n-\t\t\"F\": 11\n-\t},\n-\t\"d\": 4\n-}"
			assertEq(t, "map[string]interface{}", result, string(bytes))
		})
	})
}

type StringTag struct {
	BoolStr    bool        `json:",string"`
	IntStr     int64       `json:",string"`
	UintptrStr uintptr     `json:",string"`
	StrStr     string      `json:",string"`
	NumberStr  json.Number `json:",string"`
}

func TestRoundtripStringTag(t *testing.T) {
	tests := []struct {
		name string
		in   StringTag
		want string // empty to just test that we roundtrip
	}{
		{
			name: "AllTypes",
			in: StringTag{
				BoolStr:    true,
				IntStr:     42,
				UintptrStr: 44,
				StrStr:     "xzbit",
				NumberStr:  "46",
			},
			want: `{
				"BoolStr": "true",
				"IntStr": "42",
				"UintptrStr": "44",
				"StrStr": "\"xzbit\"",
				"NumberStr": "46"
			}`,
		},
		{
			// See golang.org/issues/38173.
			name: "StringDoubleEscapes",
			in: StringTag{
				StrStr:    "\b\f\n\r\t\"\\",
				NumberStr: "0", // just to satisfy the roundtrip
			},
			want: `{
				"BoolStr": "false",
				"IntStr": "0",
				"UintptrStr": "0",
				"StrStr": "\"\\u0008\\u000c\\n\\r\\t\\\"\\\\\"",
				"NumberStr": "0"
			}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Indent with a tab prefix to make the multi-line string
			// literals in the table nicer to read.
			got, err := json.MarshalIndent(&test.in, "\t\t\t", "\t")
			if err != nil {
				t.Fatal(err)
			}
			if got := string(got); got != test.want {
				t.Fatalf(" got: %s\nwant: %s\n", got, test.want)
			}

			// Verify that it round-trips.
			var s2 StringTag
			if err := json.Unmarshal(got, &s2); err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if !reflect.DeepEqual(test.in, s2) {
				t.Fatalf("decode didn't match.\nsource: %#v\nEncoded as:\n%s\ndecode: %#v", test.in, string(got), s2)
			}
		})
	}
}

// byte slices are special even if they're renamed types.
type renamedByte byte
type renamedByteSlice []byte
type renamedRenamedByteSlice []renamedByte

func TestEncodeRenamedByteSlice(t *testing.T) {
	s := renamedByteSlice("abc")
	result, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	expect := `"YWJj"`
	if string(result) != expect {
		t.Errorf(" got %s want %s", result, expect)
	}
	r := renamedRenamedByteSlice("abc")
	result, err = json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != expect {
		t.Errorf(" got %s want %s", result, expect)
	}
}

func TestMarshalRawMessageValue(t *testing.T) {
	type (
		T1 struct {
			M json.RawMessage `json:",omitempty"`
		}
		T2 struct {
			M *json.RawMessage `json:",omitempty"`
		}
	)

	var (
		rawNil   = json.RawMessage(nil)
		rawEmpty = json.RawMessage([]byte{})
		rawText  = json.RawMessage([]byte(`"foo"`))
	)

	tests := []struct {
		in   interface{}
		want string
		ok   bool
	}{
		// Test with nil RawMessage.
		{rawNil, "null", true},
		{&rawNil, "null", true},
		{[]interface{}{rawNil}, "[null]", true},
		{&[]interface{}{rawNil}, "[null]", true},
		{[]interface{}{&rawNil}, "[null]", true},
		{&[]interface{}{&rawNil}, "[null]", true},
		{struct{ M json.RawMessage }{rawNil}, `{"M":null}`, true},
		{&struct{ M json.RawMessage }{rawNil}, `{"M":null}`, true},
		{struct{ M *json.RawMessage }{&rawNil}, `{"M":null}`, true},
		{&struct{ M *json.RawMessage }{&rawNil}, `{"M":null}`, true},
		{map[string]interface{}{"M": rawNil}, `{"M":null}`, true},
		{&map[string]interface{}{"M": rawNil}, `{"M":null}`, true},
		{map[string]interface{}{"M": &rawNil}, `{"M":null}`, true},
		{&map[string]interface{}{"M": &rawNil}, `{"M":null}`, true},
		{T1{rawNil}, "{}", true},
		{T2{&rawNil}, `{"M":null}`, true},
		{&T1{rawNil}, "{}", true},
		{&T2{&rawNil}, `{"M":null}`, true},

		// Test with empty, but non-nil, RawMessage.
		{rawEmpty, "", false},
		{&rawEmpty, "", false},
		{[]interface{}{rawEmpty}, "", false},
		{&[]interface{}{rawEmpty}, "", false},
		{[]interface{}{&rawEmpty}, "", false},
		{&[]interface{}{&rawEmpty}, "", false},
		{struct{ X json.RawMessage }{rawEmpty}, "", false},
		{&struct{ X json.RawMessage }{rawEmpty}, "", false},
		{struct{ X *json.RawMessage }{&rawEmpty}, "", false},
		{&struct{ X *json.RawMessage }{&rawEmpty}, "", false},
		{map[string]interface{}{"nil": rawEmpty}, "", false},
		{&map[string]interface{}{"nil": rawEmpty}, "", false},
		{map[string]interface{}{"nil": &rawEmpty}, "", false},
		{&map[string]interface{}{"nil": &rawEmpty}, "", false},

		{T1{rawEmpty}, "{}", true},
		{T2{&rawEmpty}, "", false},
		{&T1{rawEmpty}, "{}", true},
		{&T2{&rawEmpty}, "", false},

		// Test with RawMessage with some text.
		//
		// The tests below marked with Issue6458 used to generate "ImZvbyI=" instead "foo".
		// This behavior was intentionally changed in Go 1.8.
		// See https://golang.org/issues/14493#issuecomment-255857318
		{rawText, `"foo"`, true}, // Issue6458
		{&rawText, `"foo"`, true},
		{[]interface{}{rawText}, `["foo"]`, true},  // Issue6458
		{&[]interface{}{rawText}, `["foo"]`, true}, // Issue6458
		{[]interface{}{&rawText}, `["foo"]`, true},
		{&[]interface{}{&rawText}, `["foo"]`, true},
		{struct{ M json.RawMessage }{rawText}, `{"M":"foo"}`, true}, // Issue6458
		{&struct{ M json.RawMessage }{rawText}, `{"M":"foo"}`, true},
		{struct{ M *json.RawMessage }{&rawText}, `{"M":"foo"}`, true},
		{&struct{ M *json.RawMessage }{&rawText}, `{"M":"foo"}`, true},
		{map[string]interface{}{"M": rawText}, `{"M":"foo"}`, true},  // Issue6458
		{&map[string]interface{}{"M": rawText}, `{"M":"foo"}`, true}, // Issue6458
		{map[string]interface{}{"M": &rawText}, `{"M":"foo"}`, true},
		{&map[string]interface{}{"M": &rawText}, `{"M":"foo"}`, true},
		{T1{rawText}, `{"M":"foo"}`, true}, // Issue6458
		{T2{&rawText}, `{"M":"foo"}`, true},
		{&T1{rawText}, `{"M":"foo"}`, true},
		{&T2{&rawText}, `{"M":"foo"}`, true},
	}

	for i, tt := range tests {
		b, err := json.Marshal(tt.in)
		if ok := (err == nil); ok != tt.ok {
			if err != nil {
				t.Errorf("test %d, unexpected failure: %v", i, err)
			} else {
				t.Errorf("test %d, unexpected success", i)
			}
		}
		if got := string(b); got != tt.want {
			t.Errorf("test %d, Marshal(%#v) = %q, want %q", i, tt.in, got, tt.want)
		}
	}
}

type marshalerError struct{}

func (*marshalerError) MarshalJSON() ([]byte, error) {
	return nil, errors.New("unexpected error")
}

func Test_MarshalerError(t *testing.T) {
	var v marshalerError
	_, err := json.Marshal(&v)
	expect := `json: error calling MarshalJSON for type *json_test.marshalerError: unexpected error`
	assertEq(t, "marshaler error", expect, fmt.Sprint(err))
}

// Ref has Marshaler and Unmarshaler methods with pointer receiver.
type Ref int

func (*Ref) MarshalJSON() ([]byte, error) {
	return []byte(`"ref"`), nil
}

func (r *Ref) UnmarshalJSON([]byte) error {
	*r = 12
	return nil
}

// Val has Marshaler methods with value receiver.
type Val int

func (Val) MarshalJSON() ([]byte, error) {
	return []byte(`"val"`), nil
}

// RefText has Marshaler and Unmarshaler methods with pointer receiver.
type RefText int

func (*RefText) MarshalText() ([]byte, error) {
	return []byte(`"ref"`), nil
}

func (r *RefText) UnmarshalText([]byte) error {
	*r = 13
	return nil
}

// ValText has Marshaler methods with value receiver.
type ValText int

func (ValText) MarshalText() ([]byte, error) {
	return []byte(`"val"`), nil
}

func TestRefValMarshal(t *testing.T) {
	var s = struct {
		R0 Ref
		R1 *Ref
		R2 RefText
		R3 *RefText
		V0 Val
		V1 *Val
		V2 ValText
		V3 *ValText
	}{
		R0: 12,
		R1: new(Ref),
		R2: 14,
		R3: new(RefText),
		V0: 13,
		V1: new(Val),
		V2: 15,
		V3: new(ValText),
	}
	const want = `{"R0":"ref","R1":"ref","R2":"\"ref\"","R3":"\"ref\"","V0":"val","V1":"val","V2":"\"val\"","V3":"\"val\""}`
	b, err := json.Marshal(&s)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if got := string(b); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

// C implements Marshaler and returns unescaped JSON.
type C int

func (C) MarshalJSON() ([]byte, error) {
	return []byte(`"<&>"`), nil
}

// CText implements Marshaler and returns unescaped text.
type CText int

func (CText) MarshalText() ([]byte, error) {
	return []byte(`"<&>"`), nil
}

func TestMarshalerEscaping(t *testing.T) {
	var c C
	want := `"\u003c\u0026\u003e"`
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("Marshal(c): %v", err)
	}
	if got := string(b); got != want {
		t.Errorf("Marshal(c) = %#q, want %#q", got, want)
	}

	var ct CText
	want = `"\"\u003c\u0026\u003e\""`
	b, err = json.Marshal(ct)
	if err != nil {
		t.Fatalf("Marshal(ct): %v", err)
	}
	if got := string(b); got != want {
		t.Errorf("Marshal(ct) = %#q, want %#q", got, want)
	}
}

type marshalPanic struct{}

func (marshalPanic) MarshalJSON() ([]byte, error) { panic(0xdead) }

func TestMarshalPanic(t *testing.T) {
	defer func() {
		if got := recover(); !reflect.DeepEqual(got, 0xdead) {
			t.Errorf("panic() = (%T)(%v), want 0xdead", got, got)
		}
	}()
	json.Marshal(&marshalPanic{})
	t.Error("Marshal should have panicked")
}

func TestMarshalUncommonFieldNames(t *testing.T) {
	v := struct {
		A0, À, Aβ int
	}{}
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal("Marshal:", err)
	}
	want := `{"A0":0,"À":0,"Aβ":0}`
	got := string(b)
	if got != want {
		t.Fatalf("Marshal: got %s want %s", got, want)
	}
}

func TestMarshalerError(t *testing.T) {
	s := "test variable"
	st := reflect.TypeOf(s)
	errText := "json: test error"

	tests := []struct {
		err  *json.MarshalerError
		want string
	}{
		{
			json.NewMarshalerError(st, fmt.Errorf(errText), ""),
			"json: error calling MarshalJSON for type " + st.String() + ": " + errText,
		},
		{
			json.NewMarshalerError(st, fmt.Errorf(errText), "TestMarshalerError"),
			"json: error calling TestMarshalerError for type " + st.String() + ": " + errText,
		},
	}

	for i, tt := range tests {
		got := tt.err.Error()
		if got != tt.want {
			t.Errorf("MarshalerError test %d, got: %s, want: %s", i, got, tt.want)
		}
	}
}

type unmarshalerText struct {
	A, B string
}

// needed for re-marshaling tests
func (u unmarshalerText) MarshalText() ([]byte, error) {
	return []byte(u.A + ":" + u.B), nil
}

func (u *unmarshalerText) UnmarshalText(b []byte) error {
	pos := bytes.IndexByte(b, ':')
	if pos == -1 {
		return errors.New("missing separator")
	}
	u.A, u.B = string(b[:pos]), string(b[pos+1:])
	return nil
}

func TestTextMarshalerMapKeysAreSorted(t *testing.T) {
	data := map[unmarshalerText]int{
		{"x", "y"}: 1,
		{"y", "x"}: 2,
		{"a", "z"}: 3,
		{"z", "a"}: 4,
	}
	b, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("Failed to Marshal text.Marshaler: %v", err)
	}
	const want = `{"a:z":3,"x:y":1,"y:x":2,"z:a":4}`
	if string(b) != want {
		t.Errorf("Marshal map with text.Marshaler keys: got %#q, want %#q", b, want)
	}

	b, err = stdjson.Marshal(data)
	if err != nil {
		t.Fatalf("Failed to std Marshal text.Marshaler: %v", err)
	}
	if string(b) != want {
		t.Errorf("std Marshal map with text.Marshaler keys: got %#q, want %#q", b, want)
	}
}

// https://golang.org/issue/33675
func TestNilMarshalerTextMapKey(t *testing.T) {
	v := map[*unmarshalerText]int{
		(*unmarshalerText)(nil): 1,
		{"A", "B"}:              2,
	}
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("Failed to Marshal *text.Marshaler: %v", err)
	}
	const want = `{"":1,"A:B":2}`
	if string(b) != want {
		t.Errorf("Marshal map with *text.Marshaler keys: got %#q, want %#q", b, want)
	}
}

var re = regexp.MustCompile

// syntactic checks on form of marshaled floating point numbers.
var badFloatREs = []*regexp.Regexp{
	re(`p`),                     // no binary exponential notation
	re(`^\+`),                   // no leading + sign
	re(`^-?0[^.]`),              // no unnecessary leading zeros
	re(`^-?\.`),                 // leading zero required before decimal point
	re(`\.(e|$)`),               // no trailing decimal
	re(`\.[0-9]+0(e|$)`),        // no trailing zero in fraction
	re(`^-?(0|[0-9]{2,})\..*e`), // exponential notation must have normalized mantissa
	re(`e[0-9]`),                // positive exponent must be signed
	//re(`e[+-]0`),                // exponent must not have leading zeros
	re(`e-[1-6]$`),             // not tiny enough for exponential notation
	re(`e+(.|1.|20)$`),         // not big enough for exponential notation
	re(`^-?0\.0000000`),        // too tiny, should use exponential notation
	re(`^-?[0-9]{22}`),         // too big, should use exponential notation
	re(`[1-9][0-9]{16}[1-9]`),  // too many significant digits in integer
	re(`[1-9][0-9.]{17}[1-9]`), // too many significant digits in decimal
	// below here for float32 only
	re(`[1-9][0-9]{8}[1-9]`),  // too many significant digits in integer
	re(`[1-9][0-9.]{9}[1-9]`), // too many significant digits in decimal
}

func TestMarshalFloat(t *testing.T) {
	//t.Parallel()
	nfail := 0
	test := func(f float64, bits int) {
		vf := interface{}(f)
		if bits == 32 {
			f = float64(float32(f)) // round
			vf = float32(f)
		}
		bout, err := json.Marshal(vf)
		if err != nil {
			t.Errorf("Marshal(%T(%g)): %v", vf, vf, err)
			nfail++
			return
		}
		out := string(bout)

		// result must convert back to the same float
		g, err := strconv.ParseFloat(out, bits)
		if err != nil {
			t.Errorf("Marshal(%T(%g)) = %q, cannot parse back: %v", vf, vf, out, err)
			nfail++
			return
		}
		if f != g || fmt.Sprint(f) != fmt.Sprint(g) { // fmt.Sprint handles ±0
			t.Errorf("Marshal(%T(%g)) = %q (is %g, not %g)", vf, vf, out, float32(g), vf)
			nfail++
			return
		}

		bad := badFloatREs
		if bits == 64 {
			bad = bad[:len(bad)-2]
		}
		for _, re := range bad {
			if re.MatchString(out) {
				t.Errorf("Marshal(%T(%g)) = %q, must not match /%s/", vf, vf, out, re)
				nfail++
				return
			}
		}
	}

	var (
		bigger  = math.Inf(+1)
		smaller = math.Inf(-1)
	)

	var digits = "1.2345678901234567890123"
	for i := len(digits); i >= 2; i-- {
		if testing.Short() && i < len(digits)-4 {
			break
		}
		for exp := -30; exp <= 30; exp++ {
			for _, sign := range "+-" {
				for bits := 32; bits <= 64; bits += 32 {
					s := fmt.Sprintf("%c%se%d", sign, digits[:i], exp)
					f, err := strconv.ParseFloat(s, bits)
					if err != nil {
						log.Fatal(err)
					}
					next := math.Nextafter
					if bits == 32 {
						next = func(g, h float64) float64 {
							return float64(math.Nextafter32(float32(g), float32(h)))
						}
					}
					test(f, bits)
					test(next(f, bigger), bits)
					test(next(f, smaller), bits)
					if nfail > 50 {
						t.Fatalf("stopping test early")
					}
				}
			}
		}
	}
	test(0, 64)
	test(math.Copysign(0, -1), 64)
	test(0, 32)
	test(math.Copysign(0, -1), 32)
}

var encodeStringTests = []struct {
	in  string
	out string
}{
	{"\x00", `"\u0000"`},
	{"\x01", `"\u0001"`},
	{"\x02", `"\u0002"`},
	{"\x03", `"\u0003"`},
	{"\x04", `"\u0004"`},
	{"\x05", `"\u0005"`},
	{"\x06", `"\u0006"`},
	{"\x07", `"\u0007"`},
	{"\x08", `"\u0008"`},
	{"\x09", `"\t"`},
	{"\x0a", `"\n"`},
	{"\x0b", `"\u000b"`},
	{"\x0c", `"\u000c"`},
	{"\x0d", `"\r"`},
	{"\x0e", `"\u000e"`},
	{"\x0f", `"\u000f"`},
	{"\x10", `"\u0010"`},
	{"\x11", `"\u0011"`},
	{"\x12", `"\u0012"`},
	{"\x13", `"\u0013"`},
	{"\x14", `"\u0014"`},
	{"\x15", `"\u0015"`},
	{"\x16", `"\u0016"`},
	{"\x17", `"\u0017"`},
	{"\x18", `"\u0018"`},
	{"\x19", `"\u0019"`},
	{"\x1a", `"\u001a"`},
	{"\x1b", `"\u001b"`},
	{"\x1c", `"\u001c"`},
	{"\x1d", `"\u001d"`},
	{"\x1e", `"\u001e"`},
	{"\x1f", `"\u001f"`},
}

func TestEncodeString(t *testing.T) {
	for _, tt := range encodeStringTests {
		b, err := json.Marshal(tt.in)
		if err != nil {
			t.Errorf("Marshal(%q): %v", tt.in, err)
			continue
		}
		out := string(b)
		if out != tt.out {
			t.Errorf("Marshal(%q) = %#q, want %#q", tt.in, out, tt.out)
		}
	}
}

type jsonbyte byte

func (b jsonbyte) MarshalJSON() ([]byte, error) { return tenc(`{"JB":%d}`, b) }

type textbyte byte

func (b textbyte) MarshalText() ([]byte, error) { return tenc(`TB:%d`, b) }

type jsonint int

func (i jsonint) MarshalJSON() ([]byte, error) { return tenc(`{"JI":%d}`, i) }

type textint int

func (i textint) MarshalText() ([]byte, error) { return tenc(`TI:%d`, i) }

func tenc(format string, a ...interface{}) ([]byte, error) {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, format, a...)
	return buf.Bytes(), nil
}

// Issue 13783
func TestEncodeBytekind(t *testing.T) {
	testdata := []struct {
		data interface{}
		want string
	}{
		{byte(7), "7"},
		{jsonbyte(7), `{"JB":7}`},
		{textbyte(4), `"TB:4"`},
		{jsonint(5), `{"JI":5}`},
		{textint(1), `"TI:1"`},
		{[]byte{0, 1}, `"AAE="`},

		{[]jsonbyte{0, 1}, `[{"JB":0},{"JB":1}]`},
		{[][]jsonbyte{{0, 1}, {3}}, `[[{"JB":0},{"JB":1}],[{"JB":3}]]`},
		{[]textbyte{2, 3}, `["TB:2","TB:3"]`},

		{[]jsonint{5, 4}, `[{"JI":5},{"JI":4}]`},
		{[]textint{9, 3}, `["TI:9","TI:3"]`},
		{[]int{9, 3}, `[9,3]`},
	}
	for i, d := range testdata {
		js, err := json.Marshal(d.data)
		if err != nil {
			t.Error(err)
			continue
		}
		got, want := string(js), d.want
		if got != want {
			t.Errorf("%d: got %s, want %s", i, got, want)
		}
	}
}

// golang.org/issue/8582
func TestEncodePointerString(t *testing.T) {
	type stringPointer struct {
		N *int64 `json:"n,string"`
	}
	var n int64 = 42
	b, err := json.Marshal(stringPointer{N: &n})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if got, want := string(b), `{"n":"42"}`; got != want {
		t.Errorf("Marshal = %s, want %s", got, want)
	}
	var back stringPointer
	err = json.Unmarshal(b, &back)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if back.N == nil {
		t.Fatalf("Unmarshaled nil N field")
	}
	if *back.N != 42 {
		t.Fatalf("*N = %d; want 42", *back.N)
	}
}

type SamePointerNoCycle struct {
	Ptr1, Ptr2 *SamePointerNoCycle
}

var samePointerNoCycle = &SamePointerNoCycle{}

type PointerCycle struct {
	Ptr *PointerCycle
}

var pointerCycle = &PointerCycle{}

type PointerCycleIndirect struct {
	Ptrs []interface{}
}

var pointerCycleIndirect = &PointerCycleIndirect{}

func init() {
	ptr := &SamePointerNoCycle{}
	samePointerNoCycle.Ptr1 = ptr
	samePointerNoCycle.Ptr2 = ptr

	pointerCycle.Ptr = pointerCycle
	pointerCycleIndirect.Ptrs = []interface{}{pointerCycleIndirect}
}

func TestSamePointerNoCycle(t *testing.T) {
	if _, err := json.Marshal(samePointerNoCycle); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

var unsupportedValues = []interface{}{
	math.NaN(),
	math.Inf(-1),
	math.Inf(1),
	pointerCycle,
	pointerCycleIndirect,
}

func TestUnsupportedValues(t *testing.T) {
	for _, v := range unsupportedValues {
		if _, err := json.Marshal(v); err != nil {
			if _, ok := err.(*json.UnsupportedValueError); !ok {
				t.Errorf("for %v, got %T want UnsupportedValueError", v, err)
			}
		} else {
			t.Errorf("for %v, expected error", v)
		}
	}
}

func TestIssue10281(t *testing.T) {
	type Foo struct {
		N json.Number
	}
	x := Foo{json.Number(`invalid`)}

	b, err := json.Marshal(&x)
	if err == nil {
		t.Errorf("Marshal(&x) = %#q; want error", b)
	}
}

func TestHTMLEscape(t *testing.T) {
	var b, want bytes.Buffer
	m := `{"M":"<html>foo &` + "\xe2\x80\xa8 \xe2\x80\xa9" + `</html>"}`
	want.Write([]byte(`{"M":"\u003chtml\u003efoo \u0026\u2028 \u2029\u003c/html\u003e"}`))
	json.HTMLEscape(&b, []byte(m))
	if !bytes.Equal(b.Bytes(), want.Bytes()) {
		t.Errorf("HTMLEscape(&b, []byte(m)) = %s; want %s", b.Bytes(), want.Bytes())
	}
}

type BugA struct {
	S string
}

type BugB struct {
	BugA
	S string
}

type BugC struct {
	S string
}

// Legal Go: We never use the repeated embedded field (S).
type BugX struct {
	A int
	BugA
	BugB
}

// golang.org/issue/16042.
// Even if a nil interface value is passed in, as long as
// it implements Marshaler, it should be marshaled.
type nilJSONMarshaler string

func (nm *nilJSONMarshaler) MarshalJSON() ([]byte, error) {
	if nm == nil {
		return json.Marshal("0zenil0")
	}
	return json.Marshal("zenil:" + string(*nm))
}

// golang.org/issue/34235.
// Even if a nil interface value is passed in, as long as
// it implements encoding.TextMarshaler, it should be marshaled.
type nilTextMarshaler string

func (nm *nilTextMarshaler) MarshalText() ([]byte, error) {
	if nm == nil {
		return []byte("0zenil0"), nil
	}
	return []byte("zenil:" + string(*nm)), nil
}

// See golang.org/issue/16042 and golang.org/issue/34235.
func TestNilMarshal(t *testing.T) {
	testCases := []struct {
		v    interface{}
		want string
	}{
		{v: nil, want: `null`},
		{v: new(float64), want: `0`},
		{v: []interface{}(nil), want: `null`},
		{v: []string(nil), want: `null`},
		{v: map[string]string(nil), want: `null`},
		{v: []byte(nil), want: `null`},
		{v: struct{ M string }{"gopher"}, want: `{"M":"gopher"}`},
		{v: struct{ M json.Marshaler }{}, want: `{"M":null}`},
		{v: struct{ M json.Marshaler }{(*nilJSONMarshaler)(nil)}, want: `{"M":"0zenil0"}`},
		{v: struct{ M interface{} }{(*nilJSONMarshaler)(nil)}, want: `{"M":null}`},
		{v: struct{ M encoding.TextMarshaler }{}, want: `{"M":null}`},
		{v: struct{ M encoding.TextMarshaler }{(*nilTextMarshaler)(nil)}, want: `{"M":"0zenil0"}`},
		{v: struct{ M interface{} }{(*nilTextMarshaler)(nil)}, want: `{"M":null}`},
	}

	for i, tt := range testCases {
		out, err := json.Marshal(tt.v)
		if err != nil || string(out) != tt.want {
			t.Errorf("%d: Marshal(%#v) = %#q, %#v, want %#q, nil", i, tt.v, out, err, tt.want)
			continue
		}
	}
}

// Issue 5245.
func TestEmbeddedBug(t *testing.T) {
	v := BugB{
		BugA{"A"},
		"B",
	}
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal("Marshal:", err)
	}
	want := `{"S":"B"}`
	got := string(b)
	if got != want {
		t.Fatalf("Marshal: got %s want %s", got, want)
	}
	// Now check that the duplicate field, S, does not appear.
	x := BugX{
		A: 23,
	}
	b, err = json.Marshal(x)
	if err != nil {
		t.Fatal("Marshal:", err)
	}
	want = `{"A":23}`
	got = string(b)
	if got != want {
		t.Fatalf("Marshal: got %s want %s", got, want)
	}
}

type BugD struct { // Same as BugA after tagging.
	XXX string `json:"S"`
}

// BugD's tagged S field should dominate BugA's.
type BugY struct {
	BugA
	BugD
}

// Test that a field with a tag dominates untagged fields.
func TestTaggedFieldDominates(t *testing.T) {
	v := BugY{
		BugA{"BugA"},
		BugD{"BugD"},
	}
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal("Marshal:", err)
	}
	want := `{"S":"BugD"}`
	got := string(b)
	if got != want {
		t.Fatalf("Marshal: got %s want %s", got, want)
	}
}

// There are no tags here, so S should not appear.
type BugZ struct {
	BugA
	BugC
	BugY // Contains a tagged S field through BugD; should not dominate.
}

func TestDuplicatedFieldDisappears(t *testing.T) {
	v := BugZ{
		BugA{"BugA"},
		BugC{"BugC"},
		BugY{
			BugA{"nested BugA"},
			BugD{"nested BugD"},
		},
	}
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal("Marshal:", err)
	}
	want := `{}`
	got := string(b)
	if got != want {
		t.Fatalf("Marshal: got %s want %s", got, want)
	}
}

func TestAnonymousFields(t *testing.T) {
	tests := []struct {
		label     string             // Test name
		makeInput func() interface{} // Function to create input value
		want      string             // Expected JSON output
	}{{
		// Both S1 and S2 have a field named X. From the perspective of S,
		// it is ambiguous which one X refers to.
		// This should not serialize either field.
		label: "AmbiguousField",
		makeInput: func() interface{} {
			type (
				S1 struct{ x, X int }
				S2 struct{ x, X int }
				S  struct {
					S1
					S2
				}
			)
			return S{S1{1, 2}, S2{3, 4}}
		},
		want: `{}`,
	}, {
		label: "DominantField",
		// Both S1 and S2 have a field named X, but since S has an X field as
		// well, it takes precedence over S1.X and S2.X.
		makeInput: func() interface{} {
			type (
				S1 struct{ x, X int }
				S2 struct{ x, X int }
				S  struct {
					S1
					S2
					x, X int
				}
			)
			return S{S1{1, 2}, S2{3, 4}, 5, 6}
		},
		want: `{"X":6}`,
	}, {
		// Unexported embedded field of non-struct type should not be serialized.
		label: "UnexportedEmbeddedInt",
		makeInput: func() interface{} {
			type (
				myInt int
				S     struct{ myInt }
			)
			return S{5}
		},
		want: `{}`,
	}, {
		// Exported embedded field of non-struct type should be serialized.
		label: "ExportedEmbeddedInt",
		makeInput: func() interface{} {
			type (
				MyInt int
				S     struct{ MyInt }
			)
			return S{5}
		},
		want: `{"MyInt":5}`,
	}, {
		// Unexported embedded field of pointer to non-struct type
		// should not be serialized.
		label: "UnexportedEmbeddedIntPointer",
		makeInput: func() interface{} {
			type (
				myInt int
				S     struct{ *myInt }
			)
			s := S{new(myInt)}
			*s.myInt = 5
			return s
		},
		want: `{}`,
	}, {
		// Exported embedded field of pointer to non-struct type
		// should be serialized.
		label: "ExportedEmbeddedIntPointer",
		makeInput: func() interface{} {
			type (
				MyInt int
				S     struct{ *MyInt }
			)
			s := S{new(MyInt)}
			*s.MyInt = 5
			return s
		},
		want: `{"MyInt":5}`,
	}, {
		// Exported fields of embedded structs should have their
		// exported fields be serialized regardless of whether the struct types
		// themselves are exported.
		label: "EmbeddedStruct",
		makeInput: func() interface{} {
			type (
				s1 struct{ x, X int }
				S2 struct{ y, Y int }
				S  struct {
					s1
					S2
				}
			)
			return S{s1{1, 2}, S2{3, 4}}
		},
		want: `{"X":2,"Y":4}`,
	}, {
		// Exported fields of pointers to embedded structs should have their
		// exported fields be serialized regardless of whether the struct types
		// themselves are exported.
		label: "EmbeddedStructPointer",
		makeInput: func() interface{} {
			type (
				s1 struct{ x, X int }
				S2 struct{ y, Y int }
				S  struct {
					*s1
					*S2
				}
			)
			return S{&s1{1, 2}, &S2{3, 4}}
		},
		want: `{"X":2,"Y":4}`,
	}, {
		// Exported fields on embedded unexported structs at multiple levels
		// of nesting should still be serialized.
		label: "NestedStructAndInts",
		makeInput: func() interface{} {
			type (
				MyInt1 int
				MyInt2 int
				myInt  int
				s2     struct {
					MyInt2
					myInt
				}
				s1 struct {
					MyInt1
					myInt
					s2
				}
				S struct {
					s1
					myInt
				}
			)
			return S{s1{1, 2, s2{3, 4}}, 6}
		},
		want: `{"MyInt1":1,"MyInt2":3}`,
	}, {
		// If an anonymous struct pointer field is nil, we should ignore
		// the embedded fields behind it. Not properly doing so may
		// result in the wrong output or reflect panics.
		label: "EmbeddedFieldBehindNilPointer",
		makeInput: func() interface{} {
			type (
				S2 struct{ Field string }
				S  struct{ *S2 }
			)
			return S{}
		},
		want: `{}`,
	}}

	for i, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			b, err := json.Marshal(tt.makeInput())
			if err != nil {
				t.Fatalf("%d: Marshal() = %v, want nil error", i, err)
			}
			if string(b) != tt.want {
				t.Fatalf("%d: Marshal() = %q, want %q", i, b, tt.want)
			}
		})
	}
}

type Optionals struct {
	Sr string `json:"sr"`
	So string `json:"so,omitempty"`
	Sw string `json:"-"`

	Ir int `json:"omitempty"` // actually named omitempty, not an option
	Io int `json:"io,omitempty"`

	Slr []string `json:"slr,random"`
	Slo []string `json:"slo,omitempty"`

	Mr map[string]interface{} `json:"mr"`
	Mo map[string]interface{} `json:",omitempty"`

	Fr float64 `json:"fr"`
	Fo float64 `json:"fo,omitempty"`

	Br bool `json:"br"`
	Bo bool `json:"bo,omitempty"`

	Ur uint `json:"ur"`
	Uo uint `json:"uo,omitempty"`

	Str struct{} `json:"str"`
	Sto struct{} `json:"sto,omitempty"`
}

var optionalsExpected = `{
 "sr": "",
 "omitempty": 0,
 "slr": null,
 "mr": {},
 "fr": 0,
 "br": false,
 "ur": 0,
 "str": {},
 "sto": {}
}`

func TestOmitEmpty(t *testing.T) {
	var o Optionals
	o.Sw = "something"
	o.Mr = map[string]interface{}{}
	o.Mo = map[string]interface{}{}

	got, err := json.MarshalIndent(&o, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	if got := string(got); got != optionalsExpected {
		t.Errorf(" got: %s\nwant: %s\n", got, optionalsExpected)
	}
}

type testNullStr string

func (v *testNullStr) MarshalJSON() ([]byte, error) {
	if *v == "" {
		return []byte("null"), nil
	}

	return []byte(*v), nil
}

func TestIssue147(t *testing.T) {
	type T struct {
		Field1 string      `json:"field1"`
		Field2 testNullStr `json:"field2,omitempty"`
	}
	got, err := json.Marshal(T{
		Field1: "a",
		Field2: "b",
	})
	if err != nil {
		t.Fatal(err)
	}
	expect, _ := stdjson.Marshal(T{
		Field1: "a",
		Field2: "b",
	})
	if !bytes.Equal(expect, got) {
		t.Fatalf("expect %q but got %q", string(expect), string(got))
	}
}

type testIssue144 struct {
	name   string
	number int64
}

func (v *testIssue144) MarshalJSON() ([]byte, error) {
	if v.name != "" {
		return json.Marshal(v.name)
	}
	return json.Marshal(v.number)
}

func TestIssue144(t *testing.T) {
	type Embeded struct {
		Field *testIssue144 `json:"field,omitempty"`
	}
	type T struct {
		Embeded
	}
	{
		v := T{
			Embeded: Embeded{Field: &testIssue144{name: "hoge"}},
		}
		got, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		expect, _ := stdjson.Marshal(v)
		if !bytes.Equal(expect, got) {
			t.Fatalf("expect %q but got %q", string(expect), string(got))
		}
	}
	{
		v := &T{
			Embeded: Embeded{Field: &testIssue144{name: "hoge"}},
		}
		got, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		expect, _ := stdjson.Marshal(v)
		if !bytes.Equal(expect, got) {
			t.Fatalf("expect %q but got %q", string(expect), string(got))
		}
	}
}

func TestIssue118(t *testing.T) {
	type data struct {
		Columns []string   `json:"columns"`
		Rows1   [][]string `json:"rows1"`
		Rows2   [][]string `json:"rows2"`
	}
	v := data{Columns: []string{"1", "2", "3"}}
	got, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	expect, _ := stdjson.MarshalIndent(v, "", "  ")
	if !bytes.Equal(expect, got) {
		t.Fatalf("expect %q but got %q", string(expect), string(got))
	}
}

func TestIssue104(t *testing.T) {
	type A struct {
		Field1 string
		Field2 int
		Field3 float64
	}
	type T struct {
		Field A
	}
	got, err := json.Marshal(T{})
	if err != nil {
		t.Fatal(err)
	}
	expect, _ := stdjson.Marshal(T{})
	if !bytes.Equal(expect, got) {
		t.Fatalf("expect %q but got %q", string(expect), string(got))
	}
}

func TestIssue179(t *testing.T) {
	data := `
{
  "t": {
    "t1": false,
    "t2": 0,
    "t3": "",
    "t4": [],
    "t5": null,
    "t6": null
  }
}`
	type T struct {
		X struct {
			T1 bool        `json:"t1,omitempty"`
			T2 float64     `json:"t2,omitempty"`
			T3 string      `json:"t3,omitempty"`
			T4 []string    `json:"t4,omitempty"`
			T5 *struct{}   `json:"t5,omitempty"`
			T6 interface{} `json:"t6,omitempty"`
		} `json:"x"`
	}
	var v T
	if err := stdjson.Unmarshal([]byte(data), &v); err != nil {
		t.Fatal(err)
	}
	var v2 T
	if err := json.Unmarshal([]byte(data), &v2); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(v, v2) {
		t.Fatalf("failed to decode: expected %v got %v", v, v2)
	}
	b1, err := stdjson.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	b2, err := json.Marshal(v2)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b1, b2) {
		t.Fatalf("failed to equal encoded result: expected %q but got %q", b1, b2)
	}
}

func TestIssue180(t *testing.T) {
	v := struct {
		T struct {
			T1 bool        `json:"t1"`
			T2 float64     `json:"t2"`
			T3 string      `json:"t3"`
			T4 []string    `json:"t4"`
			T5 *struct{}   `json:"t5"`
			T6 interface{} `json:"t6"`
			T7 [][]string  `json:"t7"`
		} `json:"t"`
	}{
		T: struct {
			T1 bool        `json:"t1"`
			T2 float64     `json:"t2"`
			T3 string      `json:"t3"`
			T4 []string    `json:"t4"`
			T5 *struct{}   `json:"t5"`
			T6 interface{} `json:"t6"`
			T7 [][]string  `json:"t7"`
		}{
			T4: []string{},
			T7: [][]string{
				[]string{""},
				[]string{"hello", "world"},
				[]string{},
			},
		},
	}
	b1, err := stdjson.MarshalIndent(v, "", "\t")
	if err != nil {
		t.Fatal(err)
	}
	b2, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b1, b2) {
		t.Fatalf("failed to equal encoded result: expected %s but got %s", string(b1), string(b2))
	}
}

func TestIssue235(t *testing.T) {
	type TaskMessage struct {
		Type      string
		Payload   map[string]interface{}
		UniqueKey string
	}
	msg := TaskMessage{
		Payload: map[string]interface{}{
			"sent_at": map[string]interface{}{
				"Time":  "0001-01-01T00:00:00Z",
				"Valid": false,
			},
		},
	}
	if _, err := json.Marshal(msg); err != nil {
		t.Fatal(err)
	}
}

func TestEncodeMapKeyTypeInterface(t *testing.T) {
	if _, err := json.Marshal(map[interface{}]interface{}{"a": 1}); err == nil {
		t.Fatal("expected error")
	}
}

type marshalContextKey struct{}

type marshalContextStructType struct{}

func (t *marshalContextStructType) MarshalJSON(ctx context.Context) ([]byte, error) {
	v := ctx.Value(marshalContextKey{})
	s, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("failed to propagate parent context.Context")
	}
	if s != "hello" {
		return nil, fmt.Errorf("failed to propagate parent context.Context")
	}
	return []byte(`"success"`), nil
}

func TestEncodeContextOption(t *testing.T) {
	t.Run("MarshalContext", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), marshalContextKey{}, "hello")
		b, err := json.MarshalContext(ctx, &marshalContextStructType{})
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != `"success"` {
			t.Fatal("failed to encode with MarshalerContext")
		}
	})
	t.Run("EncodeContext", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), marshalContextKey{}, "hello")
		buf := bytes.NewBuffer([]byte{})
		if err := json.NewEncoder(buf).EncodeContext(ctx, &marshalContextStructType{}); err != nil {
			t.Fatal(err)
		}
		if buf.String() != "\"success\"\n" {
			t.Fatal("failed to encode with EncodeContext")
		}
	})
}

func TestInterfaceWithPointer(t *testing.T) {
	var (
		ivalue   int         = 10
		uvalue   uint        = 20
		svalue   string      = "value"
		bvalue   bool        = true
		fvalue   float32     = 3.14
		nvalue   json.Number = "1.23"
		structv              = struct{ A int }{A: 10}
		slice                = []int{1, 2, 3, 4}
		array                = [4]int{1, 2, 3, 4}
		mapvalue             = map[string]int{"a": 1}
	)
	data := map[string]interface{}{
		"ivalue":  ivalue,
		"uvalue":  uvalue,
		"svalue":  svalue,
		"bvalue":  bvalue,
		"fvalue":  fvalue,
		"nvalue":  nvalue,
		"struct":  structv,
		"slice":   slice,
		"array":   array,
		"map":     mapvalue,
		"pivalue": &ivalue,
		"puvalue": &uvalue,
		"psvalue": &svalue,
		"pbvalue": &bvalue,
		"pfvalue": &fvalue,
		"pnvalue": &nvalue,
		"pstruct": &structv,
		"pslice":  &slice,
		"parray":  &array,
		"pmap":    &mapvalue,
	}
	expected, err := stdjson.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	assertEq(t, "interface{}", string(expected), string(actual))
}

func TestIssue263(t *testing.T) {
	type Foo struct {
		A []string `json:"a"`
		B int      `json:"b"`
	}

	type MyStruct struct {
		Foo *Foo `json:"foo,omitempty"`
	}

	s := MyStruct{
		Foo: &Foo{
			A: []string{"ls -lah"},
			B: 0,
		},
	}
	expected, err := stdjson.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(expected, actual) {
		t.Fatalf("expected:[%s] but got:[%s]", string(expected), string(actual))
	}
}

func TestEmbeddedNotFirstField(t *testing.T) {
	type Embedded struct {
		Has bool `json:"has"`
	}
	type T struct {
		X        int `json:"is"`
		Embedded `json:"child"`
	}
	p := T{X: 10, Embedded: Embedded{Has: true}}
	expected, err := stdjson.Marshal(&p)
	if err != nil {
		t.Fatal(err)
	}
	got, err := json.Marshal(&p)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(expected, got) {
		t.Fatalf("failed to encode embedded structure. expected = %q but got %q", expected, got)
	}
}

type implementedMethodIface interface {
	M()
}

type implementedIfaceType struct {
	A int
	B string
}

func (implementedIfaceType) M() {}

func TestImplementedMethodInterfaceType(t *testing.T) {
	data := []implementedIfaceType{implementedIfaceType{}}
	expected, err := stdjson.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	got, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(expected, got) {
		t.Fatalf("failed to encode implemented method interface type. expected:[%q] but got:[%q]", expected, got)
	}
}

func TestEmptyStructInterface(t *testing.T) {
	expected, err := stdjson.Marshal([]interface{}{struct{}{}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := json.Marshal([]interface{}{struct{}{}})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(expected, got) {
		t.Fatalf("failed to encode empty struct interface. expected:[%q] but got:[%q]", expected, got)
	}
}

func TestIssue290(t *testing.T) {
	type Issue290 interface {
		A()
	}
	var a struct {
		A Issue290
	}
	expected, err := stdjson.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	got, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(expected, got) {
		t.Fatalf("failed to encode non empty interface. expected = %q but got %q", expected, got)
	}
}

func TestIssue299(t *testing.T) {
	t.Run("conflict second field", func(t *testing.T) {
		type Embedded struct {
			ID   string            `json:"id"`
			Name map[string]string `json:"name"`
		}
		type Container struct {
			Embedded
			Name string `json:"name"`
		}
		c := &Container{
			Embedded: Embedded{
				ID:   "1",
				Name: map[string]string{"en": "Hello", "es": "Hola"},
			},
			Name: "Hi",
		}
		expected, _ := stdjson.Marshal(c)
		got, err := json.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(expected, got) {
			t.Fatalf("expected %q but got %q", expected, got)
		}
	})
	t.Run("conflict map field", func(t *testing.T) {
		type Embedded struct {
			Name map[string]string `json:"name"`
		}
		type Container struct {
			Embedded
			Name string `json:"name"`
		}
		c := &Container{
			Embedded: Embedded{
				Name: map[string]string{"en": "Hello", "es": "Hola"},
			},
			Name: "Hi",
		}
		expected, _ := stdjson.Marshal(c)
		got, err := json.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(expected, got) {
			t.Fatalf("expected %q but got %q", expected, got)
		}
	})
	t.Run("conflict slice field", func(t *testing.T) {
		type Embedded struct {
			Name []string `json:"name"`
		}
		type Container struct {
			Embedded
			Name string `json:"name"`
		}
		c := &Container{
			Embedded: Embedded{
				Name: []string{"Hello"},
			},
			Name: "Hi",
		}
		expected, _ := stdjson.Marshal(c)
		got, err := json.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(expected, got) {
			t.Fatalf("expected %q but got %q", expected, got)
		}
	})
}

func TestRecursivePtrHead(t *testing.T) {
	type User struct {
		Account  *string `json:"account"`
		Password *string `json:"password"`
		Nickname *string `json:"nickname"`
		Address  *string `json:"address,omitempty"`
		Friends  []*User `json:"friends,omitempty"`
	}
	user1Account, user1Password, user1Nickname := "abcdef", "123456", "user1"
	user1 := &User{
		Account:  &user1Account,
		Password: &user1Password,
		Nickname: &user1Nickname,
		Address:  nil,
	}
	user2Account, user2Password, user2Nickname := "ghijkl", "123456", "user2"
	user2 := &User{
		Account:  &user2Account,
		Password: &user2Password,
		Nickname: &user2Nickname,
		Address:  nil,
	}
	user1.Friends = []*User{user2}
	expected, err := stdjson.Marshal(user1)
	if err != nil {
		t.Fatal(err)
	}
	got, err := json.Marshal(user1)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(expected, got) {
		t.Fatalf("failed to encode. expected %q but got %q", expected, got)
	}
}

func TestMarshalIndent(t *testing.T) {
	v := map[string]map[string]interface{}{
		"a": {
			"b": "1",
			"c": map[string]interface{}{
				"d": "1",
			},
		},
	}
	expected, err := stdjson.MarshalIndent(v, "", "    ")
	if err != nil {
		t.Fatal(err)
	}
	got, err := json.MarshalIndent(v, "", "    ")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(expected, got) {
		t.Fatalf("expected: %q but got %q", expected, got)
	}
}

type issue318Embedded struct {
	_ [64]byte
}

type issue318 struct {
	issue318Embedded `json:"-"`
	ID               issue318MarshalText `json:"id,omitempty"`
}

type issue318MarshalText struct {
	ID string
}

func (i issue318MarshalText) MarshalText() ([]byte, error) {
	return []byte(i.ID), nil
}

func TestIssue318(t *testing.T) {
	v := issue318{
		ID: issue318MarshalText{ID: "1"},
	}
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	expected := `{"id":"1"}`
	if string(b) != expected {
		t.Fatalf("failed to encode. expected %s but got %s", expected, string(b))
	}
}

type emptyStringMarshaler struct {
	Value stringMarshaler `json:"value,omitempty"`
}

type stringMarshaler string

func (s stringMarshaler) MarshalJSON() ([]byte, error) {
	return []byte(`"` + s + `"`), nil
}

func TestEmptyStringMarshaler(t *testing.T) {
	value := emptyStringMarshaler{}
	expected, err := stdjson.Marshal(value)
	assertErr(t, err)
	got, err := json.Marshal(value)
	assertErr(t, err)
	assertEq(t, "struct", string(expected), string(got))
}

func TestIssue324(t *testing.T) {
	type T struct {
		FieldA *string  `json:"fieldA,omitempty"`
		FieldB *string  `json:"fieldB,omitempty"`
		FieldC *bool    `json:"fieldC"`
		FieldD []string `json:"fieldD,omitempty"`
	}
	v := &struct {
		Code string `json:"code"`
		*T
	}{
		T: &T{},
	}
	var sv = "Test Field"
	v.Code = "Test"
	v.T.FieldB = &sv
	expected, err := stdjson.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	got, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(expected, got) {
		t.Fatalf("failed to encode. expected %q but got %q", expected, got)
	}
}

func TestIssue339(t *testing.T) {
	type T1 struct {
		*big.Int
	}
	type T2 struct {
		T1 T1 `json:"T1"`
	}
	v := T2{T1{Int: big.NewInt(10000)}}
	b, err := json.Marshal(&v)
	assertErr(t, err)
	got := string(b)
	expected := `{"T1":10000}`
	if got != expected {
		t.Errorf("unexpected result: %v != %v", got, expected)
	}
}

func TestIssue376(t *testing.T) {
	type Container struct {
		V interface{} `json:"value"`
	}
	type MapOnly struct {
		Map map[string]int64 `json:"map"`
	}
	b, err := json.Marshal(Container{MapOnly{}})
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	expected := `{"value":{"map":null}}`
	if got != expected {
		t.Errorf("unexpected result: %v != %v", got, expected)
	}
}

type Issue370 struct {
	String string
	Valid  bool
}

func (i *Issue370) MarshalJSON() ([]byte, error) {
	if !i.Valid {
		return json.Marshal(nil)
	}
	return json.Marshal(i.String)
}

func TestIssue370(t *testing.T) {
	v := []struct {
		V Issue370
	}{
		{V: Issue370{String: "test", Valid: true}},
	}
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	expected := `[{"V":"test"}]`
	if got != expected {
		t.Errorf("unexpected result: %v != %v", got, expected)
	}
}

func TestIssue374(t *testing.T) {
	r := io.MultiReader(strings.NewReader(strings.Repeat(" ", 505)+`"\u`), strings.NewReader(`0000"`))
	var v interface{}
	if err := json.NewDecoder(r).Decode(&v); err != nil {
		t.Fatal(err)
	}
	got := v.(string)
	expected := "\u0000"
	if got != expected {
		t.Errorf("unexpected result: %q != %q", got, expected)
	}
}

func TestIssue381(t *testing.T) {
	var v struct {
		Field0  bool
		Field1  bool
		Field2  bool
		Field3  bool
		Field4  bool
		Field5  bool
		Field6  bool
		Field7  bool
		Field8  bool
		Field9  bool
		Field10 bool
		Field11 bool
		Field12 bool
		Field13 bool
		Field14 bool
		Field15 bool
		Field16 bool
		Field17 bool
		Field18 bool
		Field19 bool
		Field20 bool
		Field21 bool
		Field22 bool
		Field23 bool
		Field24 bool
		Field25 bool
		Field26 bool
		Field27 bool
		Field28 bool
		Field29 bool
		Field30 bool
		Field31 bool
		Field32 bool
		Field33 bool
		Field34 bool
		Field35 bool
		Field36 bool
		Field37 bool
		Field38 bool
		Field39 bool
		Field40 bool
		Field41 bool
		Field42 bool
		Field43 bool
		Field44 bool
		Field45 bool
		Field46 bool
		Field47 bool
		Field48 bool
		Field49 bool
		Field50 bool
		Field51 bool
		Field52 bool
		Field53 bool
		Field54 bool
		Field55 bool
		Field56 bool
		Field57 bool
		Field58 bool
		Field59 bool
		Field60 bool
		Field61 bool
		Field62 bool
		Field63 bool
		Field64 bool
		Field65 bool
		Field66 bool
		Field67 bool
		Field68 bool
		Field69 bool
		Field70 bool
		Field71 bool
		Field72 bool
		Field73 bool
		Field74 bool
		Field75 bool
		Field76 bool
		Field77 bool
		Field78 bool
		Field79 bool
		Field80 bool
		Field81 bool
		Field82 bool
		Field83 bool
		Field84 bool
		Field85 bool
		Field86 bool
		Field87 bool
		Field88 bool
		Field89 bool
		Field90 bool
		Field91 bool
		Field92 bool
		Field93 bool
		Field94 bool
		Field95 bool
		Field96 bool
		Field97 bool
		Field98 bool
		Field99 bool
	}

	// test encoder cache issue, not related to encoder
	b, err := json.Marshal(v)
	if err != nil {
		t.Errorf("failed to marshal %s", err.Error())
		t.FailNow()
	}

	std, err := stdjson.Marshal(v)
	if err != nil {
		t.Errorf("failed to marshal with encoding/json %s", err.Error())
		t.FailNow()
	}

	if !bytes.Equal(std, b) {
		t.Errorf("encoding result not equal to encoding/json")
		t.FailNow()
	}
}

func TestIssue386(t *testing.T) {
	raw := `{"date": null, "platform": "\u6f2b\u753b", "images": {"small": "https://lain.bgm.tv/pic/cover/s/d2/a1/80048_jp.jpg", "grid": "https://lain.bgm.tv/pic/cover/g/d2/a1/80048_jp.jpg", "large": "https://lain.bgm.tv/pic/cover/l/d2/a1/80048_jp.jpg", "medium": "https://lain.bgm.tv/pic/cover/m/d2/a1/80048_jp.jpg", "common": "https://lain.bgm.tv/pic/cover/c/d2/a1/80048_jp.jpg"}, "summary": "\u5929\u624d\u8a2d\u8a08\u58eb\u30fb\u5929\u5bae\uff08\u3042\u307e\u307f\u3084\uff09\u3092\u62b1\u3048\u308b\u6751\u96e8\u7dcf\u5408\u4f01\u753b\u306f\u3001\u771f\u6c34\u5efa\u8a2d\u3068\u63d0\u643a\u3057\u3066\u300c\u3055\u304d\u305f\u307e\u30a2\u30ea\u30fc\u30ca\u300d\u306e\u30b3\u30f3\u30da\u306b\u512a\u52dd\u3059\u308b\u3053\u3068\u306b\u8ced\u3051\u3066\u3044\u305f\u3002\u3057\u304b\u3057\u3001\u73fe\u77e5\u4e8b\u306e\u6d25\u5730\u7530\uff08\u3064\u3061\u3060\uff09\u306f\u5927\u65e5\u5efa\u8a2d\u306b\u512a\u52dd\u3055\u305b\u3088\u3046\u3068\u6697\u8e8d\u3059\u308b\u3002\u305d\u308c\u306f\u73fe\u77e5\u4e8b\u306e\u6d25\u5730\u7530\u3068\u526f\u77e5\u4e8b\u306e\u592a\u7530\uff08\u304a\u304a\u305f\uff09\u306e\u653f\u6cbb\u751f\u547d\u3092\u5de6\u53f3\u3059\u308b\u4e89\u3044\u3068\u306a\u308a\u2026\u2026!?\u3000\u305d\u3057\u3066\u516c\u5171\u4e8b\u696d\u306b\u6e26\u5dfb\u304f\u6df1\u3044\u95c7\u306b\u842c\u7530\u9280\u6b21\u90ce\uff08\u307e\u3093\u3060\u30fb\u304e\u3093\u3058\u308d\u3046\uff09\u306f\u2026\u2026!?", "name": "\u30df\u30ca\u30df\u306e\u5e1d\u738b (48)"}`
	var a struct {
		Date     *string `json:"date"`
		Platform *string `json:"platform"`
		Summary  string  `json:"summary"`
		Name     string  `json:"name"`
	}
	err := json.NewDecoder(strings.NewReader(raw)).Decode(&a)
	if err != nil {
		t.Error(err)
	}
}

type customMapKey string

func (b customMapKey) MarshalJSON() ([]byte, error) {
	return []byte("[]"), nil
}

func TestCustomMarshalForMapKey(t *testing.T) {
	m := map[customMapKey]string{customMapKey("skipcustom"): "test"}
	expected, err := stdjson.Marshal(m)
	assertErr(t, err)
	got, err := json.Marshal(m)
	assertErr(t, err)
	assertEq(t, "custom map key", string(expected), string(got))
}

func TestIssue391(t *testing.T) {
	type A struct {
		X string `json:"x,omitempty"`
	}
	type B struct {
		A
	}
	type C struct {
		X []int `json:"x,omitempty"`
	}
	for _, tc := range []struct {
		name string
		in   interface{}
		out  string
	}{
		{in: struct{ B }{}, out: "{}"},
		{in: struct {
			B
			Y string `json:"y"`
		}{}, out: `{"y":""}`},
		{in: struct {
			Y string `json:"y"`
			B
		}{}, out: `{"y":""}`},
		{in: struct{ C }{}, out: "{}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := json.Marshal(tc.in)
			assertErr(t, err)
			assertEq(t, "unexpected result", tc.out, string(b))
		})
	}
}

func TestIssue417(t *testing.T) {
	x := map[string]string{
		"b": "b",
		"a": "a",
	}
	b, err := json.MarshalIndentWithOption(x, "", " ", json.UnorderedMap())
	assertErr(t, err)

	var y map[string]string
	err = json.Unmarshal(b, &y)
	assertErr(t, err)

	assertEq(t, "key b", "b", y["b"])
	assertEq(t, "key a", "a", y["a"])
}

func TestIssue426(t *testing.T) {
	type I interface {
		Foo()
	}
	type A struct {
		I
		Val string
	}
	var s A
	s.Val = "456"

	b, _ := json.Marshal(s)
	assertEq(t, "unexpected result", `{"I":null,"Val":"456"}`, string(b))
}

func TestIssue441(t *testing.T) {
	type A struct {
		Y string `json:"y,omitempty"`
	}

	type B struct {
		X *int `json:"x,omitempty"`
		A
		Z int `json:"z,omitempty"`
	}

	b, err := json.Marshal(B{})
	assertErr(t, err)
	assertEq(t, "unexpected result", "{}", string(b))
}
