package json_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/goccy/go-json"
)

func TestCoverBytes(t *testing.T) {
	type structBytes struct {
		A []byte `json:"a"`
	}
	type structBytesOmitEmpty struct {
		A []byte `json:"a,omitempty"`
	}
	type structBytesBytes struct {
		A []byte `json:"a,bytes"`
	}

	type structBytesPtr struct {
		A *[]byte `json:"a"`
	}
	type structBytesPtrOmitEmpty struct {
		A *[]byte `json:"a,omitempty"`
	}
	type structBytesPtrBytes struct {
		A *[]byte `json:"a,bytes"`
	}

	tests := []struct {
		name string
		data interface{}
	}{
		{
			name: "Bytes",
			data: []byte("a"),
		},
		{
			name: "BytesPtr",
			data: []byte("a"),
		},
		{
			name: "BytesPtr3",
			data: bytesptr3([]byte("a")),
		},
		{
			name: "BytesPtrNil",
			data: (*[]byte)(nil),
		},
		{
			name: "BytesPtr3Nil",
			data: (***[]byte)(nil),
		},

		// HeadBytesZero
		{
			name: "HeadBytesZero",
			data: struct {
				A []byte `json:"a"`
			}{},
		},
		{
			name: "HeadBytesZeroOmitEmpty",
			data: struct {
				A []byte `json:"a,omitempty"`
			}{},
		},
		{
			name: "HeadBytesZeroString",
			data: struct {
				A []byte `json:"a,string"`
			}{},
		},

		// HeadBytes
		{
			name: "HeadString",
			data: struct {
				A []byte `json:"a"`
			}{A: []byte("foo")},
		},
		{
			name: "HeadBytesOmitEmpty",
			data: struct {
				A []byte `json:"a,omitempty"`
			}{A: []byte("foo")},
		},
		{
			name: "HeadBytesString",
			data: struct {
				A []byte `json:"a,string"`
			}{A: []byte("foo")},
		},

		// HeadBytesPtr
		{
			name: "HeadBytesPtr",
			data: struct {
				A *[]byte `json:"a"`
			}{A: bytesptr([]byte("foo"))},
		},
		{
			name: "HeadBytesPtrOmitEmpty",
			data: struct {
				A *[]byte `json:"a,omitempty"`
			}{A: bytesptr([]byte("foo"))},
		},
		{
			name: "HeadBytesPtrString",
			data: struct {
				A *[]byte `json:"a,string"`
			}{A: bytesptr([]byte("foo"))},
		},

		// HeadBytesPtrNil
		{
			name: "HeadBytesPtrNil",
			data: struct {
				A *[]byte `json:"a"`
			}{A: nil},
		},
		{
			name: "HeadBytesPtrNilOmitEmpty",
			data: struct {
				A *[]byte `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "HeadBytesPtrNilString",
			data: struct {
				A *[]byte `json:"a,string"`
			}{A: nil},
		},

		// PtrHeadBytesZero
		{
			name: "PtrHeadBytesZero",
			data: &struct {
				A []byte `json:"a"`
			}{},
		},
		{
			name: "PtrHeadBytesZeroOmitEmpty",
			data: &struct {
				A []byte `json:"a,omitempty"`
			}{},
		},
		{
			name: "PtrHeadBytesZeroString",
			data: &struct {
				A []byte `json:"a,string"`
			}{},
		},

		// PtrHeadBytes
		{
			name: "PtrHeadString",
			data: &struct {
				A []byte `json:"a"`
			}{A: []byte("foo")},
		},
		{
			name: "PtrHeadBytesOmitEmpty",
			data: &struct {
				A []byte `json:"a,omitempty"`
			}{A: []byte("foo")},
		},
		{
			name: "PtrHeadBytesString",
			data: &struct {
				A []byte `json:"a,string"`
			}{A: []byte("foo")},
		},

		// PtrHeadBytesPtr
		{
			name: "PtrHeadBytesPtr",
			data: &struct {
				A *[]byte `json:"a"`
			}{A: bytesptr([]byte("foo"))},
		},
		{
			name: "PtrHeadBytesPtrOmitEmpty",
			data: &struct {
				A *[]byte `json:"a,omitempty"`
			}{A: bytesptr([]byte("foo"))},
		},
		{
			name: "PtrHeadBytesPtrString",
			data: &struct {
				A *[]byte `json:"a,string"`
			}{A: bytesptr([]byte("foo"))},
		},

		// PtrHeadBytesPtrNil
		{
			name: "PtrHeadBytesPtrNil",
			data: &struct {
				A *[]byte `json:"a"`
			}{A: nil},
		},
		{
			name: "PtrHeadBytesPtrNilOmitEmpty",
			data: &struct {
				A *[]byte `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadBytesPtrNilString",
			data: &struct {
				A *[]byte `json:"a,string"`
			}{A: nil},
		},

		// PtrHeadBytesNil
		{
			name: "PtrHeadBytesNil",
			data: (*struct {
				A *[]byte `json:"a"`
			})(nil),
		},
		{
			name: "PtrHeadBytesNilOmitEmpty",
			data: (*struct {
				A *[]byte `json:"a,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadBytesNilString",
			data: (*struct {
				A *[]byte `json:"a,string"`
			})(nil),
		},

		// HeadBytesZeroMultiFields
		{
			name: "HeadBytesZeroMultiFields",
			data: struct {
				A []byte `json:"a"`
				B []byte `json:"b"`
				C []byte `json:"c"`
			}{},
		},
		{
			name: "HeadBytesZeroMultiFieldsOmitEmpty",
			data: struct {
				A []byte `json:"a,omitempty"`
				B []byte `json:"b,omitempty"`
				C []byte `json:"c,omitempty"`
			}{},
		},
		{
			name: "HeadBytesZeroMultiFieldsString",
			data: struct {
				A []byte `json:"a,string"`
				B []byte `json:"b,string"`
				C []byte `json:"c,string"`
			}{},
		},

		// HeadBytesMultiFields
		{
			name: "HeadBytesMultiFields",
			data: struct {
				A []byte `json:"a"`
				B []byte `json:"b"`
				C []byte `json:"c"`
			}{A: []byte("foo"), B: []byte("bar"), C: []byte([]byte("baz"))},
		},
		{
			name: "HeadBytesMultiFieldsOmitEmpty",
			data: struct {
				A []byte `json:"a,omitempty"`
				B []byte `json:"b,omitempty"`
				C []byte `json:"c,omitempty"`
			}{A: []byte("foo"), B: []byte("bar"), C: []byte([]byte("baz"))},
		},
		{
			name: "HeadBytesMultiFieldsString",
			data: struct {
				A []byte `json:"a,string"`
				B []byte `json:"b,string"`
				C []byte `json:"c,string"`
			}{A: []byte("foo"), B: []byte("bar"), C: []byte([]byte("baz"))},
		},

		// HeadBytesPtrMultiFields
		{
			name: "HeadBytesPtrMultiFields",
			data: struct {
				A *[]byte `json:"a"`
				B *[]byte `json:"b"`
				C *[]byte `json:"c"`
			}{A: bytesptr([]byte("foo")), B: bytesptr([]byte("bar")), C: bytesptr([]byte([]byte("baz")))},
		},
		{
			name: "HeadBytesPtrMultiFieldsOmitEmpty",
			data: struct {
				A *[]byte `json:"a,omitempty"`
				B *[]byte `json:"b,omitempty"`
				C *[]byte `json:"c,omitempty"`
			}{A: bytesptr([]byte("foo")), B: bytesptr([]byte("bar")), C: bytesptr([]byte([]byte("baz")))},
		},
		{
			name: "HeadBytesPtrMultiFieldsString",
			data: struct {
				A *[]byte `json:"a,string"`
				B *[]byte `json:"b,string"`
				C *[]byte `json:"c,string"`
			}{A: bytesptr([]byte("foo")), B: bytesptr([]byte("bar")), C: bytesptr([]byte([]byte("baz")))},
		},

		// HeadBytesPtrNilMultiFields
		{
			name: "HeadBytesPtrNilMultiFields",
			data: struct {
				A *[]byte `json:"a"`
				B *[]byte `json:"b"`
				C *[]byte `json:"c"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadBytesPtrNilMultiFieldsOmitEmpty",
			data: struct {
				A *[]byte `json:"a,omitempty"`
				B *[]byte `json:"b,omitempty"`
				C *[]byte `json:"c,omitempty"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadBytesPtrNilMultiFieldsString",
			data: struct {
				A *[]byte `json:"a,string"`
				B *[]byte `json:"b,string"`
				C *[]byte `json:"c,string"`
			}{A: nil, B: nil, C: nil},
		},

		// PtrHeadBytesZeroMultiFields
		{
			name: "PtrHeadBytesZeroMultiFields",
			data: &struct {
				A []byte `json:"a"`
				B []byte `json:"b"`
				C []byte `json:"c"`
			}{},
		},
		{
			name: "PtrHeadBytesZeroMultiFieldsOmitEmpty",
			data: &struct {
				A []byte `json:"a,omitempty"`
				B []byte `json:"b,omitempty"`
				C []byte `json:"c,omitempty"`
			}{},
		},
		{
			name: "PtrHeadBytesZeroMultiFieldsString",
			data: &struct {
				A []byte `json:"a,string"`
				B []byte `json:"b,string"`
				C []byte `json:"c,string"`
			}{},
		},

		// PtrHeadBytesMultiFields
		{
			name: "PtrHeadBytesMultiFields",
			data: &struct {
				A []byte `json:"a"`
				B []byte `json:"b"`
				C []byte `json:"c"`
			}{A: []byte("foo"), B: []byte("bar"), C: []byte("baz")},
		},
		{
			name: "PtrHeadBytesMultiFieldsOmitEmpty",
			data: &struct {
				A []byte `json:"a,omitempty"`
				B []byte `json:"b,omitempty"`
				C []byte `json:"c,omitempty"`
			}{A: []byte("foo"), B: []byte("bar"), C: []byte("baz")},
		},
		{
			name: "PtrHeadBytesMultiFieldsString",
			data: &struct {
				A []byte `json:"a,string"`
				B []byte `json:"b,string"`
				C []byte `json:"c,string"`
			}{A: []byte("foo"), B: []byte("bar"), C: []byte("baz")},
		},

		// PtrHeadBytesPtrMultiFields
		{
			name: "PtrHeadBytesPtrMultiFields",
			data: &struct {
				A *[]byte `json:"a"`
				B *[]byte `json:"b"`
				C *[]byte `json:"c"`
			}{A: bytesptr([]byte("foo")), B: bytesptr([]byte("bar")), C: bytesptr([]byte("baz"))},
		},
		{
			name: "PtrHeadBytesPtrMultiFieldsOmitEmpty",
			data: &struct {
				A *[]byte `json:"a,omitempty"`
				B *[]byte `json:"b,omitempty"`
				C *[]byte `json:"c,omitempty"`
			}{A: bytesptr([]byte("foo")), B: bytesptr([]byte("bar")), C: bytesptr([]byte("baz"))},
		},
		{
			name: "PtrHeadBytesPtrMultiFieldsString",
			data: &struct {
				A *[]byte `json:"a,string"`
				B *[]byte `json:"b,string"`
				C *[]byte `json:"c,string"`
			}{A: bytesptr([]byte("foo")), B: bytesptr([]byte("bar")), C: bytesptr([]byte("baz"))},
		},

		// PtrHeadBytesPtrNilMultiFields
		{
			name: "PtrHeadBytesPtrNilMultiFields",
			data: &struct {
				A *[]byte `json:"a"`
				B *[]byte `json:"b"`
				C *[]byte `json:"c"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "PtrHeadBytesPtrNilMultiFieldsOmitEmpty",
			data: &struct {
				A *[]byte `json:"a,omitempty"`
				B *[]byte `json:"b,omitempty"`
				C *[]byte `json:"c,omitempty"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "PtrHeadBytesPtrNilMultiFieldsString",
			data: &struct {
				A *[]byte `json:"a,string"`
				B *[]byte `json:"b,string"`
				C *[]byte `json:"c,string"`
			}{A: nil, B: nil, C: nil},
		},

		// PtrHeadBytesNilMultiFields
		{
			name: "PtrHeadBytesNilMultiFields",
			data: (*struct {
				A *[]byte `json:"a"`
				B *[]byte `json:"b"`
				C *[]byte `json:"c"`
			})(nil),
		},
		{
			name: "PtrHeadBytesNilMultiFieldsOmitEmpty",
			data: (*struct {
				A *[]byte `json:"a,omitempty"`
				B *[]byte `json:"b,omitempty"`
				C *[]byte `json:"c,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadBytesNilMultiFieldsString",
			data: (*struct {
				A *[]byte `json:"a,string"`
				B *[]byte `json:"b,string"`
				C *[]byte `json:"c,string"`
			})(nil),
		},

		// HeadBytesZeroNotRoot
		{
			name: "HeadBytesZeroNotRoot",
			data: struct {
				A struct {
					A []byte `json:"a"`
				}
			}{},
		},
		{
			name: "HeadBytesZeroNotRootOmitEmpty",
			data: struct {
				A struct {
					A []byte `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadBytesZeroNotRootString",
			data: struct {
				A struct {
					A []byte `json:"a,string"`
				}
			}{},
		},

		// HeadBytesNotRoot
		{
			name: "HeadBytesNotRoot",
			data: struct {
				A struct {
					A []byte `json:"a"`
				}
			}{A: struct {
				A []byte `json:"a"`
			}{A: []byte("foo")}},
		},
		{
			name: "HeadBytesNotRootOmitEmpty",
			data: struct {
				A struct {
					A []byte `json:"a,omitempty"`
				}
			}{A: struct {
				A []byte `json:"a,omitempty"`
			}{A: []byte("foo")}},
		},
		{
			name: "HeadBytesNotRootString",
			data: struct {
				A struct {
					A []byte `json:"a,string"`
				}
			}{A: struct {
				A []byte `json:"a,string"`
			}{A: []byte("foo")}},
		},

		// HeadBytesPtrNotRoot
		{
			name: "HeadBytesPtrNotRoot",
			data: struct {
				A struct {
					A *[]byte `json:"a"`
				}
			}{A: struct {
				A *[]byte `json:"a"`
			}{bytesptr([]byte("foo"))}},
		},
		{
			name: "HeadBytesPtrNotRootOmitEmpty",
			data: struct {
				A struct {
					A *[]byte `json:"a,omitempty"`
				}
			}{A: struct {
				A *[]byte `json:"a,omitempty"`
			}{bytesptr([]byte("foo"))}},
		},
		{
			name: "HeadBytesPtrNotRootString",
			data: struct {
				A struct {
					A *[]byte `json:"a,string"`
				}
			}{A: struct {
				A *[]byte `json:"a,string"`
			}{bytesptr([]byte("foo"))}},
		},

		// HeadBytesPtrNilNotRoot
		{
			name: "HeadBytesPtrNilNotRoot",
			data: struct {
				A struct {
					A *[]byte `json:"a"`
				}
			}{},
		},
		{
			name: "HeadBytesPtrNilNotRootOmitEmpty",
			data: struct {
				A struct {
					A *[]byte `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadBytesPtrNilNotRootString",
			data: struct {
				A struct {
					A *[]byte `json:"a,string"`
				}
			}{},
		},

		// PtrHeadBytesZeroNotRoot
		{
			name: "PtrHeadBytesZeroNotRoot",
			data: struct {
				A *struct {
					A []byte `json:"a"`
				}
			}{A: new(struct {
				A []byte `json:"a"`
			})},
		},
		{
			name: "PtrHeadBytesZeroNotRootOmitEmpty",
			data: struct {
				A *struct {
					A []byte `json:"a,omitempty"`
				}
			}{A: new(struct {
				A []byte `json:"a,omitempty"`
			})},
		},
		{
			name: "PtrHeadBytesZeroNotRootString",
			data: struct {
				A *struct {
					A []byte `json:"a,string"`
				}
			}{A: new(struct {
				A []byte `json:"a,string"`
			})},
		},

		// PtrHeadBytesNotRoot
		{
			name: "PtrHeadBytesNotRoot",
			data: struct {
				A *struct {
					A []byte `json:"a"`
				}
			}{A: &(struct {
				A []byte `json:"a"`
			}{A: []byte("foo")})},
		},
		{
			name: "PtrHeadBytesNotRootOmitEmpty",
			data: struct {
				A *struct {
					A []byte `json:"a,omitempty"`
				}
			}{A: &(struct {
				A []byte `json:"a,omitempty"`
			}{A: []byte("foo")})},
		},
		{
			name: "PtrHeadBytesNotRootString",
			data: struct {
				A *struct {
					A []byte `json:"a,string"`
				}
			}{A: &(struct {
				A []byte `json:"a,string"`
			}{A: []byte("foo")})},
		},

		// PtrHeadBytesPtrNotRoot
		{
			name: "PtrHeadBytesPtrNotRoot",
			data: struct {
				A *struct {
					A *[]byte `json:"a"`
				}
			}{A: &(struct {
				A *[]byte `json:"a"`
			}{A: bytesptr([]byte("foo"))})},
		},
		{
			name: "PtrHeadBytesPtrNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *[]byte `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *[]byte `json:"a,omitempty"`
			}{A: bytesptr([]byte("foo"))})},
		},
		{
			name: "PtrHeadBytesPtrNotRootString",
			data: struct {
				A *struct {
					A *[]byte `json:"a,string"`
				}
			}{A: &(struct {
				A *[]byte `json:"a,string"`
			}{A: bytesptr([]byte("foo"))})},
		},

		// PtrHeadBytesPtrNilNotRoot
		{
			name: "PtrHeadBytesPtrNilNotRoot",
			data: struct {
				A *struct {
					A *[]byte `json:"a"`
				}
			}{A: &(struct {
				A *[]byte `json:"a"`
			}{A: nil})},
		},
		{
			name: "PtrHeadBytesPtrNilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *[]byte `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *[]byte `json:"a,omitempty"`
			}{A: nil})},
		},
		{
			name: "PtrHeadBytesPtrNilNotRootString",
			data: struct {
				A *struct {
					A *[]byte `json:"a,string"`
				}
			}{A: &(struct {
				A *[]byte `json:"a,string"`
			}{A: nil})},
		},

		// PtrHeadBytesNilNotRoot
		{
			name: "PtrHeadBytesNilNotRoot",
			data: struct {
				A *struct {
					A *[]byte `json:"a"`
				}
			}{A: nil},
		},
		{
			name: "PtrHeadBytesNilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *[]byte `json:"a,omitempty"`
				} `json:",omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadBytesNilNotRootString",
			data: struct {
				A *struct {
					A *[]byte `json:"a,string"`
				} `json:",string"`
			}{A: nil},
		},

		// HeadBytesZeroMultiFieldsNotRoot
		{
			name: "HeadBytesZeroMultiFieldsNotRoot",
			data: struct {
				A struct {
					A []byte `json:"a"`
				}
				B struct {
					B []byte `json:"b"`
				}
			}{},
		},
		{
			name: "HeadBytesZeroMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A []byte `json:"a,omitempty"`
				}
				B struct {
					B []byte `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "HeadBytesZeroMultiFieldsNotRootString",
			data: struct {
				A struct {
					A []byte `json:"a,string"`
				}
				B struct {
					B []byte `json:"b,string"`
				}
			}{},
		},

		// HeadBytesMultiFieldsNotRoot
		{
			name: "HeadBytesMultiFieldsNotRoot",
			data: struct {
				A struct {
					A []byte `json:"a"`
				}
				B struct {
					B []byte `json:"b"`
				}
			}{A: struct {
				A []byte `json:"a"`
			}{A: []byte("foo")}, B: struct {
				B []byte `json:"b"`
			}{B: []byte("bar")}},
		},
		{
			name: "HeadBytesMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A []byte `json:"a,omitempty"`
				}
				B struct {
					B []byte `json:"b,omitempty"`
				}
			}{A: struct {
				A []byte `json:"a,omitempty"`
			}{A: []byte("foo")}, B: struct {
				B []byte `json:"b,omitempty"`
			}{B: []byte("bar")}},
		},
		{
			name: "HeadBytesMultiFieldsNotRootString",
			data: struct {
				A struct {
					A []byte `json:"a,string"`
				}
				B struct {
					B []byte `json:"b,string"`
				}
			}{A: struct {
				A []byte `json:"a,string"`
			}{A: []byte("foo")}, B: struct {
				B []byte `json:"b,string"`
			}{B: []byte("bar")}},
		},

		// HeadBytesPtrMultiFieldsNotRoot
		{
			name: "HeadBytesPtrMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *[]byte `json:"a"`
				}
				B struct {
					B *[]byte `json:"b"`
				}
			}{A: struct {
				A *[]byte `json:"a"`
			}{A: bytesptr([]byte("foo"))}, B: struct {
				B *[]byte `json:"b"`
			}{B: bytesptr([]byte("bar"))}},
		},
		{
			name: "HeadBytesPtrMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *[]byte `json:"a,omitempty"`
				}
				B struct {
					B *[]byte `json:"b,omitempty"`
				}
			}{A: struct {
				A *[]byte `json:"a,omitempty"`
			}{A: bytesptr([]byte("foo"))}, B: struct {
				B *[]byte `json:"b,omitempty"`
			}{B: bytesptr([]byte("bar"))}},
		},
		{
			name: "HeadBytesPtrMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *[]byte `json:"a,string"`
				}
				B struct {
					B *[]byte `json:"b,string"`
				}
			}{A: struct {
				A *[]byte `json:"a,string"`
			}{A: bytesptr([]byte("foo"))}, B: struct {
				B *[]byte `json:"b,string"`
			}{B: bytesptr([]byte("bar"))}},
		},

		// HeadBytesPtrNilMultiFieldsNotRoot
		{
			name: "HeadBytesPtrNilMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *[]byte `json:"a"`
				}
				B struct {
					B *[]byte `json:"b"`
				}
			}{A: struct {
				A *[]byte `json:"a"`
			}{A: nil}, B: struct {
				B *[]byte `json:"b"`
			}{B: nil}},
		},
		{
			name: "HeadBytesPtrNilMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *[]byte `json:"a,omitempty"`
				}
				B struct {
					B *[]byte `json:"b,omitempty"`
				}
			}{A: struct {
				A *[]byte `json:"a,omitempty"`
			}{A: nil}, B: struct {
				B *[]byte `json:"b,omitempty"`
			}{B: nil}},
		},
		{
			name: "HeadBytesPtrNilMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *[]byte `json:"a,string"`
				}
				B struct {
					B *[]byte `json:"b,string"`
				}
			}{A: struct {
				A *[]byte `json:"a,string"`
			}{A: nil}, B: struct {
				B *[]byte `json:"b,string"`
			}{B: nil}},
		},

		// PtrHeadBytesZeroMultiFieldsNotRoot
		{
			name: "PtrHeadBytesZeroMultiFieldsNotRoot",
			data: &struct {
				A struct {
					A []byte `json:"a"`
				}
				B struct {
					B []byte `json:"b"`
				}
			}{},
		},
		{
			name: "PtrHeadBytesZeroMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A []byte `json:"a,omitempty"`
				}
				B struct {
					B []byte `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "PtrHeadBytesZeroMultiFieldsNotRootString",
			data: &struct {
				A struct {
					A []byte `json:"a,string"`
				}
				B struct {
					B []byte `json:"b,string"`
				}
			}{},
		},

		// PtrHeadBytesMultiFieldsNotRoot
		{
			name: "PtrHeadBytesMultiFieldsNotRoot",
			data: &struct {
				A struct {
					A []byte `json:"a"`
				}
				B struct {
					B []byte `json:"b"`
				}
			}{A: struct {
				A []byte `json:"a"`
			}{A: []byte("foo")}, B: struct {
				B []byte `json:"b"`
			}{B: []byte("bar")}},
		},
		{
			name: "PtrHeadBytesMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A []byte `json:"a,omitempty"`
				}
				B struct {
					B []byte `json:"b,omitempty"`
				}
			}{A: struct {
				A []byte `json:"a,omitempty"`
			}{A: []byte("foo")}, B: struct {
				B []byte `json:"b,omitempty"`
			}{B: []byte("bar")}},
		},
		{
			name: "PtrHeadBytesMultiFieldsNotRootString",
			data: &struct {
				A struct {
					A []byte `json:"a,string"`
				}
				B struct {
					B []byte `json:"b,string"`
				}
			}{A: struct {
				A []byte `json:"a,string"`
			}{A: []byte("foo")}, B: struct {
				B []byte `json:"b,string"`
			}{B: []byte("bar")}},
		},

		// PtrHeadBytesPtrMultiFieldsNotRoot
		{
			name: "PtrHeadBytesPtrMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *[]byte `json:"a"`
				}
				B *struct {
					B *[]byte `json:"b"`
				}
			}{A: &(struct {
				A *[]byte `json:"a"`
			}{A: bytesptr([]byte("foo"))}), B: &(struct {
				B *[]byte `json:"b"`
			}{B: bytesptr([]byte("bar"))})},
		},
		{
			name: "PtrHeadBytesPtrMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *[]byte `json:"a,omitempty"`
				}
				B *struct {
					B *[]byte `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *[]byte `json:"a,omitempty"`
			}{A: bytesptr([]byte("foo"))}), B: &(struct {
				B *[]byte `json:"b,omitempty"`
			}{B: bytesptr([]byte("bar"))})},
		},
		{
			name: "PtrHeadBytesPtrMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *[]byte `json:"a,string"`
				}
				B *struct {
					B *[]byte `json:"b,string"`
				}
			}{A: &(struct {
				A *[]byte `json:"a,string"`
			}{A: bytesptr([]byte("foo"))}), B: &(struct {
				B *[]byte `json:"b,string"`
			}{B: bytesptr([]byte("bar"))})},
		},

		// PtrHeadBytesPtrNilMultiFieldsNotRoot
		{
			name: "PtrHeadBytesPtrNilMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *[]byte `json:"a"`
				}
				B *struct {
					B *[]byte `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadBytesPtrNilMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *[]byte `json:"a,omitempty"`
				}
				B *struct {
					B *[]byte `json:"b,omitempty"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadBytesPtrNilMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *[]byte `json:"a,string"`
				}
				B *struct {
					B *[]byte `json:"b,string"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadBytesNilMultiFieldsNotRoot
		{
			name: "PtrHeadBytesNilMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *[]byte `json:"a"`
				}
				B *struct {
					B *[]byte `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadBytesNilMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *[]byte `json:"a,omitempty"`
				}
				B *struct {
					B *[]byte `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadBytesNilMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *[]byte `json:"a,string"`
				}
				B *struct {
					B *[]byte `json:"b,string"`
				}
			})(nil),
		},

		// PtrHeadBytesDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadBytesDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A []byte `json:"a"`
					B []byte `json:"b"`
					C []byte `json:"c"`
				}
				B *struct {
					A []byte `json:"a"`
					B []byte `json:"b"`
					C []byte `json:"c"`
				}
			}{A: &(struct {
				A []byte `json:"a"`
				B []byte `json:"b"`
				C []byte `json:"c"`
			}{A: []byte("foo"), B: []byte("bar"), C: []byte("baz")}), B: &(struct {
				A []byte `json:"a"`
				B []byte `json:"b"`
				C []byte `json:"c"`
			}{A: []byte("foo"), B: []byte("bar"), C: []byte("baz")})},
		},
		{
			name: "PtrHeadBytesDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A []byte `json:"a,omitempty"`
					B []byte `json:"b,omitempty"`
				}
				B *struct {
					A []byte `json:"a,omitempty"`
					B []byte `json:"b,omitempty"`
				}
			}{A: &(struct {
				A []byte `json:"a,omitempty"`
				B []byte `json:"b,omitempty"`
			}{A: []byte("foo"), B: []byte("bar")}), B: &(struct {
				A []byte `json:"a,omitempty"`
				B []byte `json:"b,omitempty"`
			}{A: []byte("foo"), B: []byte("bar")})},
		},
		{
			name: "PtrHeadBytesDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A []byte `json:"a,string"`
					B []byte `json:"b,string"`
				}
				B *struct {
					A []byte `json:"a,string"`
					B []byte `json:"b,string"`
				}
			}{A: &(struct {
				A []byte `json:"a,string"`
				B []byte `json:"b,string"`
			}{A: []byte("foo"), B: []byte("bar")}), B: &(struct {
				A []byte `json:"a,string"`
				B []byte `json:"b,string"`
			}{A: []byte("foo"), B: []byte("bar")})},
		},

		// PtrHeadBytesNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadBytesNilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A []byte `json:"a"`
					B []byte `json:"b"`
				}
				B *struct {
					A []byte `json:"a"`
					B []byte `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadBytesNilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A []byte `json:"a,omitempty"`
					B []byte `json:"b,omitempty"`
				}
				B *struct {
					A []byte `json:"a,omitempty"`
					B []byte `json:"b,omitempty"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadBytesNilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A []byte `json:"a,string"`
					B []byte `json:"b,string"`
				}
				B *struct {
					A []byte `json:"a,string"`
					B []byte `json:"b,string"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadBytesNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadBytesNilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A []byte `json:"a"`
					B []byte `json:"b"`
				}
				B *struct {
					A []byte `json:"a"`
					B []byte `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadBytesNilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A []byte `json:"a,omitempty"`
					B []byte `json:"b,omitempty"`
				}
				B *struct {
					A []byte `json:"a,omitempty"`
					B []byte `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadBytesNilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A []byte `json:"a,string"`
					B []byte `json:"b,string"`
				}
				B *struct {
					A []byte `json:"a,string"`
					B []byte `json:"b,string"`
				}
			})(nil),
		},

		// PtrHeadBytesPtrDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadBytesPtrDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *[]byte `json:"a"`
					B *[]byte `json:"b"`
				}
				B *struct {
					A *[]byte `json:"a"`
					B *[]byte `json:"b"`
				}
			}{A: &(struct {
				A *[]byte `json:"a"`
				B *[]byte `json:"b"`
			}{A: bytesptr([]byte("foo")), B: bytesptr([]byte("bar"))}), B: &(struct {
				A *[]byte `json:"a"`
				B *[]byte `json:"b"`
			}{A: bytesptr([]byte("foo")), B: bytesptr([]byte("bar"))})},
		},
		{
			name: "PtrHeadBytesPtrDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *[]byte `json:"a,omitempty"`
					B *[]byte `json:"b,omitempty"`
				}
				B *struct {
					A *[]byte `json:"a,omitempty"`
					B *[]byte `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *[]byte `json:"a,omitempty"`
				B *[]byte `json:"b,omitempty"`
			}{A: bytesptr([]byte("foo")), B: bytesptr([]byte("bar"))}), B: &(struct {
				A *[]byte `json:"a,omitempty"`
				B *[]byte `json:"b,omitempty"`
			}{A: bytesptr([]byte("foo")), B: bytesptr([]byte("bar"))})},
		},
		{
			name: "PtrHeadBytesPtrDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *[]byte `json:"a,string"`
					B *[]byte `json:"b,string"`
				}
				B *struct {
					A *[]byte `json:"a,string"`
					B *[]byte `json:"b,string"`
				}
			}{A: &(struct {
				A *[]byte `json:"a,string"`
				B *[]byte `json:"b,string"`
			}{A: bytesptr([]byte("foo")), B: bytesptr([]byte("bar"))}), B: &(struct {
				A *[]byte `json:"a,string"`
				B *[]byte `json:"b,string"`
			}{A: bytesptr([]byte("foo")), B: bytesptr([]byte("bar"))})},
		},

		// PtrHeadBytesPtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadBytesPtrNilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *[]byte `json:"a"`
					B *[]byte `json:"b"`
				}
				B *struct {
					A *[]byte `json:"a"`
					B *[]byte `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadBytesPtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *[]byte `json:"a,omitempty"`
					B *[]byte `json:"b,omitempty"`
				}
				B *struct {
					A *[]byte `json:"a,omitempty"`
					B *[]byte `json:"b,omitempty"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadBytesPtrNilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *[]byte `json:"a,string"`
					B *[]byte `json:"b,string"`
				}
				B *struct {
					A *[]byte `json:"a,string"`
					B *[]byte `json:"b,string"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadBytesPtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadBytesPtrNilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *[]byte `json:"a"`
					B *[]byte `json:"b"`
				}
				B *struct {
					A *[]byte `json:"a"`
					B *[]byte `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadBytesPtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *[]byte `json:"a,omitempty"`
					B *[]byte `json:"b,omitempty"`
				}
				B *struct {
					A *[]byte `json:"a,omitempty"`
					B *[]byte `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadBytesPtrNilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *[]byte `json:"a,string"`
					B *[]byte `json:"b,string"`
				}
				B *struct {
					A *[]byte `json:"a,string"`
					B *[]byte `json:"b,string"`
				}
			})(nil),
		},

		// AnonymousHeadBytes
		{
			name: "AnonymousHeadString",
			data: struct {
				structBytes
				B []byte `json:"b"`
			}{
				structBytes: structBytes{A: []byte("foo")},
				B:           []byte("bar"),
			},
		},
		{
			name: "AnonymousHeadBytesOmitEmpty",
			data: struct {
				structBytesOmitEmpty
				B []byte `json:"b,omitempty"`
			}{
				structBytesOmitEmpty: structBytesOmitEmpty{A: []byte("foo")},
				B:                    []byte("bar"),
			},
		},
		{
			name: "AnonymousHeadBytesString",
			data: struct {
				structBytesBytes
				B []byte `json:"b,string"`
			}{
				structBytesBytes: structBytesBytes{A: []byte("foo")},
				B:                []byte("bar"),
			},
		},

		// PtrAnonymousHeadBytes
		{
			name: "PtrAnonymousHeadString",
			data: struct {
				*structBytes
				B []byte `json:"b"`
			}{
				structBytes: &structBytes{A: []byte("foo")},
				B:           []byte("bar"),
			},
		},
		{
			name: "PtrAnonymousHeadBytesOmitEmpty",
			data: struct {
				*structBytesOmitEmpty
				B []byte `json:"b,omitempty"`
			}{
				structBytesOmitEmpty: &structBytesOmitEmpty{A: []byte("foo")},
				B:                    []byte("bar"),
			},
		},
		{
			name: "PtrAnonymousHeadBytesString",
			data: struct {
				*structBytesBytes
				B []byte `json:"b,string"`
			}{
				structBytesBytes: &structBytesBytes{A: []byte("foo")},
				B:                []byte("bar"),
			},
		},

		// NilPtrAnonymousHeadBytes
		{
			name: "NilPtrAnonymousHeadString",
			data: struct {
				*structBytes
				B []byte `json:"b"`
			}{
				structBytes: nil,
				B:           []byte("baz"),
			},
		},
		{
			name: "NilPtrAnonymousHeadBytesOmitEmpty",
			data: struct {
				*structBytesOmitEmpty
				B []byte `json:"b,omitempty"`
			}{
				structBytesOmitEmpty: nil,
				B:                    []byte("baz"),
			},
		},
		{
			name: "NilPtrAnonymousHeadBytesString",
			data: struct {
				*structBytesBytes
				B []byte `json:"b,string"`
			}{
				structBytesBytes: nil,
				B:                []byte("baz"),
			},
		},

		// AnonymousHeadBytesPtr
		{
			name: "AnonymousHeadBytesPtr",
			data: struct {
				structBytesPtr
				B *[]byte `json:"b"`
			}{
				structBytesPtr: structBytesPtr{A: bytesptr([]byte("foo"))},
				B:              bytesptr([]byte("bar")),
			},
		},
		{
			name: "AnonymousHeadBytesPtrOmitEmpty",
			data: struct {
				structBytesPtrOmitEmpty
				B *[]byte `json:"b,omitempty"`
			}{
				structBytesPtrOmitEmpty: structBytesPtrOmitEmpty{A: bytesptr([]byte("foo"))},
				B:                       bytesptr([]byte("bar")),
			},
		},
		{
			name: "AnonymousHeadBytesPtrString",
			data: struct {
				structBytesPtrBytes
				B *[]byte `json:"b,string"`
			}{
				structBytesPtrBytes: structBytesPtrBytes{A: bytesptr([]byte("foo"))},
				B:                   bytesptr([]byte("bar")),
			},
		},

		// AnonymousHeadBytesPtrNil
		{
			name: "AnonymousHeadBytesPtrNil",
			data: struct {
				structBytesPtr
				B *[]byte `json:"b"`
			}{
				structBytesPtr: structBytesPtr{A: nil},
				B:              bytesptr([]byte("foo")),
			},
		},
		{
			name: "AnonymousHeadBytesPtrNilOmitEmpty",
			data: struct {
				structBytesPtrOmitEmpty
				B *[]byte `json:"b,omitempty"`
			}{
				structBytesPtrOmitEmpty: structBytesPtrOmitEmpty{A: nil},
				B:                       bytesptr([]byte("foo")),
			},
		},
		{
			name: "AnonymousHeadBytesPtrNilString",
			data: struct {
				structBytesPtrBytes
				B *[]byte `json:"b,string"`
			}{
				structBytesPtrBytes: structBytesPtrBytes{A: nil},
				B:                   bytesptr([]byte("foo")),
			},
		},

		// PtrAnonymousHeadBytesPtr
		{
			name: "PtrAnonymousHeadBytesPtr",
			data: struct {
				*structBytesPtr
				B *[]byte `json:"b"`
			}{
				structBytesPtr: &structBytesPtr{A: bytesptr([]byte("foo"))},
				B:              bytesptr([]byte("bar")),
			},
		},
		{
			name: "PtrAnonymousHeadBytesPtrOmitEmpty",
			data: struct {
				*structBytesPtrOmitEmpty
				B *[]byte `json:"b,omitempty"`
			}{
				structBytesPtrOmitEmpty: &structBytesPtrOmitEmpty{A: bytesptr([]byte("foo"))},
				B:                       bytesptr([]byte("bar")),
			},
		},
		{
			name: "PtrAnonymousHeadBytesPtrString",
			data: struct {
				*structBytesPtrBytes
				B *[]byte `json:"b,string"`
			}{
				structBytesPtrBytes: &structBytesPtrBytes{A: bytesptr([]byte("foo"))},
				B:                   bytesptr([]byte("bar")),
			},
		},

		// NilPtrAnonymousHeadBytesPtr
		{
			name: "NilPtrAnonymousHeadBytesPtr",
			data: struct {
				*structBytesPtr
				B *[]byte `json:"b"`
			}{
				structBytesPtr: nil,
				B:              bytesptr([]byte("foo")),
			},
		},
		{
			name: "NilPtrAnonymousHeadBytesPtrOmitEmpty",
			data: struct {
				*structBytesPtrOmitEmpty
				B *[]byte `json:"b,omitempty"`
			}{
				structBytesPtrOmitEmpty: nil,
				B:                       bytesptr([]byte("foo")),
			},
		},
		{
			name: "NilPtrAnonymousHeadBytesPtrString",
			data: struct {
				*structBytesPtrBytes
				B *[]byte `json:"b,string"`
			}{
				structBytesPtrBytes: nil,
				B:                   bytesptr([]byte("foo")),
			},
		},

		// AnonymousHeadBytesOnly
		{
			name: "AnonymousHeadBytesOnly",
			data: struct {
				structBytes
			}{
				structBytes: structBytes{A: []byte("foo")},
			},
		},
		{
			name: "AnonymousHeadBytesOnlyOmitEmpty",
			data: struct {
				structBytesOmitEmpty
			}{
				structBytesOmitEmpty: structBytesOmitEmpty{A: []byte("foo")},
			},
		},
		{
			name: "AnonymousHeadBytesOnlyString",
			data: struct {
				structBytesBytes
			}{
				structBytesBytes: structBytesBytes{A: []byte("foo")},
			},
		},

		// PtrAnonymousHeadBytesOnly
		{
			name: "PtrAnonymousHeadBytesOnly",
			data: struct {
				*structBytes
			}{
				structBytes: &structBytes{A: []byte("foo")},
			},
		},
		{
			name: "PtrAnonymousHeadBytesOnlyOmitEmpty",
			data: struct {
				*structBytesOmitEmpty
			}{
				structBytesOmitEmpty: &structBytesOmitEmpty{A: []byte("foo")},
			},
		},
		{
			name: "PtrAnonymousHeadBytesOnlyString",
			data: struct {
				*structBytesBytes
			}{
				structBytesBytes: &structBytesBytes{A: []byte("foo")},
			},
		},

		// NilPtrAnonymousHeadBytesOnly
		{
			name: "NilPtrAnonymousHeadBytesOnly",
			data: struct {
				*structBytes
			}{
				structBytes: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadBytesOnlyOmitEmpty",
			data: struct {
				*structBytesOmitEmpty
			}{
				structBytesOmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadBytesOnlyString",
			data: struct {
				*structBytesBytes
			}{
				structBytesBytes: nil,
			},
		},

		// AnonymousHeadBytesPtrOnly
		{
			name: "AnonymousHeadBytesPtrOnly",
			data: struct {
				structBytesPtr
			}{
				structBytesPtr: structBytesPtr{A: bytesptr([]byte("foo"))},
			},
		},
		{
			name: "AnonymousHeadBytesPtrOnlyOmitEmpty",
			data: struct {
				structBytesPtrOmitEmpty
			}{
				structBytesPtrOmitEmpty: structBytesPtrOmitEmpty{A: bytesptr([]byte("foo"))},
			},
		},
		{
			name: "AnonymousHeadBytesPtrOnlyString",
			data: struct {
				structBytesPtrBytes
			}{
				structBytesPtrBytes: structBytesPtrBytes{A: bytesptr([]byte("foo"))},
			},
		},

		// AnonymousHeadBytesPtrNilOnly
		{
			name: "AnonymousHeadBytesPtrNilOnly",
			data: struct {
				structBytesPtr
			}{
				structBytesPtr: structBytesPtr{A: nil},
			},
		},
		{
			name: "AnonymousHeadBytesPtrNilOnlyOmitEmpty",
			data: struct {
				structBytesPtrOmitEmpty
			}{
				structBytesPtrOmitEmpty: structBytesPtrOmitEmpty{A: nil},
			},
		},
		{
			name: "AnonymousHeadBytesPtrNilOnlyString",
			data: struct {
				structBytesPtrBytes
			}{
				structBytesPtrBytes: structBytesPtrBytes{A: nil},
			},
		},

		// PtrAnonymousHeadBytesPtrOnly
		{
			name: "PtrAnonymousHeadBytesPtrOnly",
			data: struct {
				*structBytesPtr
			}{
				structBytesPtr: &structBytesPtr{A: bytesptr([]byte("foo"))},
			},
		},
		{
			name: "PtrAnonymousHeadBytesPtrOnlyOmitEmpty",
			data: struct {
				*structBytesPtrOmitEmpty
			}{
				structBytesPtrOmitEmpty: &structBytesPtrOmitEmpty{A: bytesptr([]byte("foo"))},
			},
		},
		{
			name: "PtrAnonymousHeadBytesPtrOnlyString",
			data: struct {
				*structBytesPtrBytes
			}{
				structBytesPtrBytes: &structBytesPtrBytes{A: bytesptr([]byte("foo"))},
			},
		},

		// NilPtrAnonymousHeadBytesPtrOnly
		{
			name: "NilPtrAnonymousHeadBytesPtrOnly",
			data: struct {
				*structBytesPtr
			}{
				structBytesPtr: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadBytesPtrOnlyOmitEmpty",
			data: struct {
				*structBytesPtrOmitEmpty
			}{
				structBytesPtrOmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadBytesPtrOnlyString",
			data: struct {
				*structBytesPtrBytes
			}{
				structBytesPtrBytes: nil,
			},
		},
	}
	for _, test := range tests {
		for _, indent := range []bool{true, false} {
			for _, htmlEscape := range []bool{true, false} {
				t.Run(fmt.Sprintf("%s_indent_%t_escape_%t", test.name, indent, htmlEscape), func(t *testing.T) {
					var buf bytes.Buffer
					enc := json.NewEncoder(&buf)
					enc.SetEscapeHTML(htmlEscape)
					if indent {
						enc.SetIndent("", "  ")
					}
					if err := enc.Encode(test.data); err != nil {
						t.Fatalf("%s(htmlEscape:%v,indent:%v): %v: %s", test.name, htmlEscape, indent, test.data, err)
					}
					stdresult := encodeByEncodingJSON(test.data, indent, htmlEscape)
					if buf.String() != stdresult {
						t.Errorf("%s(htmlEscape:%v,indent:%v): doesn't compatible with encoding/json. expected %q but got %q", test.name, htmlEscape, indent, stdresult, buf.String())
					}
				})
			}
		}
	}
}
