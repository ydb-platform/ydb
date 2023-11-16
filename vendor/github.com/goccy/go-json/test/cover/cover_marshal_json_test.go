package json_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/goccy/go-json"
)

type coverMarshalJSON struct {
	A int
}

func (c coverMarshalJSON) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprint(c.A)), nil
}

type coverPtrMarshalJSON struct {
	B int
}

func (c *coverPtrMarshalJSON) MarshalJSON() ([]byte, error) {
	if c == nil {
		return []byte(`"NULL"`), nil
	}
	return []byte(fmt.Sprint(c.B)), nil
}

type coverPtrMarshalJSONString struct {
	dummy int
	C     string
}

func (c *coverPtrMarshalJSONString) MarshalJSON() ([]byte, error) {
	if c == nil {
		return []byte(`"NULL"`), nil
	}
	return []byte(c.C), nil
}

type coverFuncMarshalJSON func()

func (f coverFuncMarshalJSON) MarshalJSON() ([]byte, error) {
	if f == nil {
		return []byte(`null`), nil
	}
	f()
	return []byte(`"func"`), nil
}

func TestCoverMarshalJSON(t *testing.T) {
	type structMarshalJSON struct {
		A coverMarshalJSON `json:"a"`
	}
	type structMarshalJSONOmitEmpty struct {
		A coverMarshalJSON `json:"a,omitempty"`
	}
	type structMarshalJSONString struct {
		A coverMarshalJSON `json:"a,string"`
	}
	type structPtrMarshalJSON struct {
		A coverPtrMarshalJSON `json:"a"`
	}
	type structPtrMarshalJSONOmitEmpty struct {
		A coverPtrMarshalJSON `json:"a,omitempty"`
	}
	type structPtrMarshalJSONString struct {
		A coverPtrMarshalJSON `json:"a,string"`
	}

	type structMarshalJSONPtr struct {
		A *coverMarshalJSON `json:"a"`
	}
	type structMarshalJSONPtrOmitEmpty struct {
		A *coverMarshalJSON `json:"a,omitempty"`
	}
	type structMarshalJSONPtrString struct {
		A *coverMarshalJSON `json:"a,string"`
	}
	type structPtrMarshalJSONPtr struct {
		A *coverPtrMarshalJSON `json:"a"`
	}
	type structPtrMarshalJSONPtrOmitEmpty struct {
		A *coverPtrMarshalJSON `json:"a,omitempty"`
	}
	type structPtrMarshalJSONPtrString struct {
		A *coverPtrMarshalJSON `json:"a,string"`
	}

	tests := []struct {
		name string
		data interface{}
	}{
		{
			name: "FuncMarshalJSON",
			data: coverFuncMarshalJSON(func() {}),
		},
		{
			name: "StructFuncMarshalJSON",
			data: struct {
				A coverFuncMarshalJSON
			}{A: func() {}},
		},
		{
			name: "StructFuncMarshalJSONMultiFields",
			data: struct {
				A coverFuncMarshalJSON
				B coverFuncMarshalJSON
			}{A: func() {}, B: func() {}},
		},
		{
			name: "PtrStructFuncMarshalJSONMultiFields",
			data: &struct {
				A coverFuncMarshalJSON
				B coverFuncMarshalJSON
				C coverFuncMarshalJSON
			}{A: func() {}, B: nil, C: func() {}},
		},
		{
			name: "MarshalJSON",
			data: coverMarshalJSON{A: 1},
		},
		{
			name: "PtrMarshalJSON",
			data: &coverMarshalJSON{A: 1},
		},
		{
			name: "PtrMarshalJSON",
			data: coverPtrMarshalJSON{B: 1},
		},
		{
			name: "PtrPtrMarshalJSON",
			data: &coverPtrMarshalJSON{B: 1},
		},
		{
			name: "SliceMarshalJSON",
			data: []coverMarshalJSON{{A: 1}, {A: 2}},
		},
		{
			name: "SliceAddrMarshalJSON",
			data: []*coverMarshalJSON{{A: 1}, {A: 2}},
		},
		{
			name: "SlicePtrMarshalJSON",
			data: []coverPtrMarshalJSON{{B: 1}, {B: 2}},
		},
		{
			name: "SliceAddrPtrMarshalJSON",
			data: []*coverPtrMarshalJSON{{B: 1}, {B: 2}},
		},
		{
			name: "StructSliceMarshalJSON",
			data: struct {
				A []coverMarshalJSON
			}{A: []coverMarshalJSON{{A: 1}, {A: 2}}},
		},
		{
			name: "StructSliceAddrMarshalJSON",
			data: struct {
				A []*coverMarshalJSON
			}{A: []*coverMarshalJSON{{A: 1}, {A: 2}}},
		},
		{
			name: "StructSlicePtrMarshalJSON",
			data: struct {
				A []coverPtrMarshalJSON
			}{A: []coverPtrMarshalJSON{{B: 1}, {B: 2}}},
		},
		{
			name: "StructSliceAddrPtrMarshalJSON",
			data: struct {
				A []*coverPtrMarshalJSON
			}{A: []*coverPtrMarshalJSON{{B: 1}, {B: 2}}},
		},
		{
			name: "PtrStructSliceMarshalJSON",
			data: &struct {
				A []coverMarshalJSON
			}{A: []coverMarshalJSON{{A: 1}, {A: 2}}},
		},
		{
			name: "PtrStructSliceAddrMarshalJSON",
			data: &struct {
				A []*coverMarshalJSON
			}{A: []*coverMarshalJSON{{A: 1}, {A: 2}}},
		},
		{
			name: "PtrStructSlicePtrMarshalJSON",
			data: &struct {
				A []coverPtrMarshalJSON
			}{A: []coverPtrMarshalJSON{{B: 1}, {B: 2}}},
		},
		{
			name: "PtrStructSlicePtrMarshalJSONString",
			data: &struct {
				A []coverPtrMarshalJSONString
			}{A: []coverPtrMarshalJSONString{{C: "1"}, {C: "2"}}},
		},
		{
			name: "PtrStructSliceAddrPtrMarshalJSONString",
			data: &struct {
				A []*coverPtrMarshalJSONString
			}{A: []*coverPtrMarshalJSONString{{C: "1"}, {C: "2"}}},
		},
		{
			name: "PtrStructArrayPtrMarshalJSONString",
			data: &struct {
				A [2]coverPtrMarshalJSONString
			}{A: [2]coverPtrMarshalJSONString{{C: "1"}, {C: "2"}}},
		},
		{
			name: "PtrStructArrayAddrPtrMarshalJSONString",
			data: &struct {
				A [2]*coverPtrMarshalJSONString
			}{A: [2]*coverPtrMarshalJSONString{{C: "1"}, {C: "2"}}},
		},
		{
			name: "PtrStructMapPtrMarshalJSONString",
			data: &struct {
				A map[string]coverPtrMarshalJSONString
			}{A: map[string]coverPtrMarshalJSONString{"a": {C: "1"}, "b": {C: "2"}}},
		},
		{
			name: "PtrStructMapAddrPtrMarshalJSONString",
			data: &struct {
				A map[string]*coverPtrMarshalJSONString
			}{A: map[string]*coverPtrMarshalJSONString{"a": {C: "1"}, "b": {C: "2"}}},
		},

		// HeadMarshalJSONZero
		{
			name: "HeadMarshalJSONZero",
			data: struct {
				A coverMarshalJSON `json:"a"`
			}{},
		},
		{
			name: "HeadMarshalJSONZeroOmitEmpty",
			data: struct {
				A coverMarshalJSON `json:"a,omitempty"`
			}{},
		},
		{
			name: "HeadMarshalJSONZeroString",
			data: struct {
				A coverMarshalJSON `json:"a,string"`
			}{},
		},
		{
			name: "HeadPtrMarshalJSONZero",
			data: struct {
				A coverPtrMarshalJSON `json:"a"`
			}{},
		},
		{
			name: "HeadPtrMarshalJSONZeroOmitEmpty",
			data: struct {
				A coverPtrMarshalJSON `json:"a,omitempty"`
			}{},
		},
		{
			name: "HeadPtrMarshalJSONZeroString",
			data: struct {
				A coverPtrMarshalJSON `json:"a,string"`
			}{},
		},

		// HeadMarshalJSON
		{
			name: "HeadMarshalJSON",
			data: struct {
				A coverMarshalJSON `json:"a"`
			}{A: coverMarshalJSON{}},
		},
		{
			name: "HeadMarshalJSONOmitEmpty",
			data: struct {
				A coverMarshalJSON `json:"a,omitempty"`
			}{A: coverMarshalJSON{}},
		},
		{
			name: "HeadMarshalJSONString",
			data: struct {
				A coverMarshalJSON `json:"a,string"`
			}{A: coverMarshalJSON{}},
		},
		{
			name: "HeadPtrMarshalJSON",
			data: struct {
				A coverPtrMarshalJSON `json:"a"`
			}{A: coverPtrMarshalJSON{}},
		},
		{
			name: "HeadPtrMarshalJSONOmitEmpty",
			data: struct {
				A coverPtrMarshalJSON `json:"a,omitempty"`
			}{A: coverPtrMarshalJSON{}},
		},
		{
			name: "HeadPtrMarshalJSONString",
			data: struct {
				A coverPtrMarshalJSON `json:"a,string"`
			}{A: coverPtrMarshalJSON{}},
		},

		// HeadMarshalJSONPtr
		{
			name: "HeadMarshalJSONPtr",
			data: struct {
				A *coverMarshalJSON `json:"a"`
			}{A: &coverMarshalJSON{}},
		},
		{
			name: "HeadMarshalJSONPtrOmitEmpty",
			data: struct {
				A *coverMarshalJSON `json:"a,omitempty"`
			}{A: &coverMarshalJSON{}},
		},
		{
			name: "HeadMarshalJSONPtrString",
			data: struct {
				A *coverMarshalJSON `json:"a,string"`
			}{A: &coverMarshalJSON{}},
		},
		{
			name: "HeadPtrMarshalJSONPtr",
			data: struct {
				A *coverPtrMarshalJSON `json:"a"`
			}{A: &coverPtrMarshalJSON{}},
		},
		{
			name: "HeadPtrMarshalJSONPtrOmitEmpty",
			data: struct {
				A *coverPtrMarshalJSON `json:"a,omitempty"`
			}{A: &coverPtrMarshalJSON{}},
		},
		{
			name: "HeadPtrMarshalJSONPtrString",
			data: struct {
				A *coverPtrMarshalJSON `json:"a,string"`
			}{A: &coverPtrMarshalJSON{}},
		},

		// HeadMarshalJSONPtrNil
		{
			name: "HeadMarshalJSONPtrNil",
			data: struct {
				A *coverMarshalJSON `json:"a"`
			}{A: nil},
		},
		{
			name: "HeadMarshalJSONPtrNilOmitEmpty",
			data: struct {
				A *coverMarshalJSON `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "HeadMarshalJSONPtrNilString",
			data: struct {
				A *coverMarshalJSON `json:"a,string"`
			}{A: nil},
		},
		{
			name: "HeadPtrMarshalJSONPtrNil",
			data: struct {
				A *coverPtrMarshalJSON `json:"a"`
			}{A: nil},
		},
		{
			name: "HeadPtrMarshalJSONPtrNilOmitEmpty",
			data: struct {
				A *coverPtrMarshalJSON `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "HeadPtrMarshalJSONPtrNilString",
			data: struct {
				A *coverPtrMarshalJSON `json:"a,string"`
			}{A: nil},
		},

		// PtrHeadMarshalJSONZero
		{
			name: "PtrHeadMarshalJSONZero",
			data: &struct {
				A coverMarshalJSON `json:"a"`
			}{},
		},
		{
			name: "PtrHeadMarshalJSONZeroOmitEmpty",
			data: &struct {
				A coverMarshalJSON `json:"a,omitempty"`
			}{},
		},
		{
			name: "PtrHeadMarshalJSONZeroString",
			data: &struct {
				A coverMarshalJSON `json:"a,string"`
			}{},
		},
		{
			name: "PtrHeadPtrMarshalJSONZero",
			data: &struct {
				A coverPtrMarshalJSON `json:"a"`
			}{},
		},
		{
			name: "PtrHeadPtrMarshalJSONZeroOmitEmpty",
			data: &struct {
				A coverPtrMarshalJSON `json:"a,omitempty"`
			}{},
		},
		{
			name: "PtrHeadPtrMarshalJSONZeroString",
			data: &struct {
				A coverPtrMarshalJSON `json:"a,string"`
			}{},
		},

		// PtrHeadMarshalJSON
		{
			name: "PtrHeadMarshalJSON",
			data: &struct {
				A coverMarshalJSON `json:"a"`
			}{A: coverMarshalJSON{}},
		},
		{
			name: "PtrHeadMarshalJSONOmitEmpty",
			data: &struct {
				A coverMarshalJSON `json:"a,omitempty"`
			}{A: coverMarshalJSON{}},
		},
		{
			name: "PtrHeadMarshalJSONString",
			data: &struct {
				A coverMarshalJSON `json:"a,string"`
			}{A: coverMarshalJSON{}},
		},
		{
			name: "PtrHeadPtrMarshalJSON",
			data: &struct {
				A coverPtrMarshalJSON `json:"a"`
			}{A: coverPtrMarshalJSON{}},
		},
		{
			name: "PtrHeadPtrMarshalJSONOmitEmpty",
			data: &struct {
				A coverPtrMarshalJSON `json:"a,omitempty"`
			}{A: coverPtrMarshalJSON{}},
		},
		{
			name: "PtrHeadPtrMarshalJSONString",
			data: &struct {
				A coverPtrMarshalJSON `json:"a,string"`
			}{A: coverPtrMarshalJSON{}},
		},

		// PtrHeadMarshalJSONPtr
		{
			name: "PtrHeadMarshalJSONPtr",
			data: &struct {
				A *coverMarshalJSON `json:"a"`
			}{A: &coverMarshalJSON{}},
		},
		{
			name: "PtrHeadMarshalJSONPtrOmitEmpty",
			data: &struct {
				A *coverMarshalJSON `json:"a,omitempty"`
			}{A: &coverMarshalJSON{}},
		},
		{
			name: "PtrHeadMarshalJSONPtrString",
			data: &struct {
				A *coverMarshalJSON `json:"a,string"`
			}{A: &coverMarshalJSON{}},
		},
		{
			name: "PtrHeadPtrMarshalJSONPtr",
			data: &struct {
				A *coverPtrMarshalJSON `json:"a"`
			}{A: &coverPtrMarshalJSON{}},
		},
		{
			name: "PtrHeadPtrMarshalJSONPtrOmitEmpty",
			data: &struct {
				A *coverPtrMarshalJSON `json:"a,omitempty"`
			}{A: &coverPtrMarshalJSON{}},
		},
		{
			name: "PtrHeadPtrMarshalJSONPtrString",
			data: &struct {
				A *coverPtrMarshalJSON `json:"a,string"`
			}{A: &coverPtrMarshalJSON{}},
		},

		// PtrHeadMarshalJSONPtrNil
		{
			name: "PtrHeadMarshalJSONPtrNil",
			data: &struct {
				A *coverMarshalJSON `json:"a"`
			}{A: nil},
		},
		{
			name: "PtrHeadMarshalJSONPtrNilOmitEmpty",
			data: &struct {
				A *coverMarshalJSON `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadMarshalJSONPtrNilString",
			data: &struct {
				A *coverMarshalJSON `json:"a,string"`
			}{A: nil},
		},
		{
			name: "PtrHeadPtrMarshalJSONPtrNil",
			data: &struct {
				A *coverPtrMarshalJSON `json:"a"`
			}{A: nil},
		},
		{
			name: "PtrHeadPtrMarshalJSONPtrNilOmitEmpty",
			data: &struct {
				A *coverPtrMarshalJSON `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadPtrMarshalJSONPtrNilString",
			data: &struct {
				A *coverPtrMarshalJSON `json:"a,string"`
			}{A: nil},
		},

		// PtrHeadMarshalJSONNil
		{
			name: "PtrHeadMarshalJSONNil",
			data: (*struct {
				A *coverMarshalJSON `json:"a"`
			})(nil),
		},
		{
			name: "PtrHeadMarshalJSONNilOmitEmpty",
			data: (*struct {
				A *coverMarshalJSON `json:"a,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadMarshalJSONNilString",
			data: (*struct {
				A *coverMarshalJSON `json:"a,string"`
			})(nil),
		},
		{
			name: "PtrHeadPtrMarshalJSONNil",
			data: (*struct {
				A *coverPtrMarshalJSON `json:"a"`
			})(nil),
		},
		{
			name: "PtrHeadPtrMarshalJSONNilOmitEmpty",
			data: (*struct {
				A *coverPtrMarshalJSON `json:"a,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadPtrMarshalJSONNilString",
			data: (*struct {
				A *coverPtrMarshalJSON `json:"a,string"`
			})(nil),
		},

		// HeadMarshalJSONZeroMultiFields
		{
			name: "HeadMarshalJSONZeroMultiFields",
			data: struct {
				A coverMarshalJSON `json:"a"`
				B coverMarshalJSON `json:"b"`
				C coverMarshalJSON `json:"c"`
			}{},
		},
		{
			name: "HeadMarshalJSONZeroMultiFieldsOmitEmpty",
			data: struct {
				A coverMarshalJSON `json:"a,omitempty"`
				B coverMarshalJSON `json:"b,omitempty"`
				C coverMarshalJSON `json:"c,omitempty"`
			}{},
		},
		{
			name: "HeadMarshalJSONZeroMultiFields",
			data: struct {
				A coverMarshalJSON `json:"a,string"`
				B coverMarshalJSON `json:"b,string"`
				C coverMarshalJSON `json:"c,string"`
			}{},
		},
		{
			name: "HeadPtrMarshalJSONZeroMultiFields",
			data: struct {
				A coverPtrMarshalJSON `json:"a"`
				B coverPtrMarshalJSON `json:"b"`
				C coverPtrMarshalJSON `json:"c"`
			}{},
		},
		{
			name: "HeadPtrMarshalJSONZeroMultiFieldsOmitEmpty",
			data: struct {
				A coverPtrMarshalJSON `json:"a,omitempty"`
				B coverPtrMarshalJSON `json:"b,omitempty"`
				C coverPtrMarshalJSON `json:"c,omitempty"`
			}{},
		},
		{
			name: "HeadPtrMarshalJSONZeroMultiFields",
			data: struct {
				A coverPtrMarshalJSON `json:"a,string"`
				B coverPtrMarshalJSON `json:"b,string"`
				C coverPtrMarshalJSON `json:"c,string"`
			}{},
		},

		// HeadMarshalJSONMultiFields
		{
			name: "HeadMarshalJSONMultiFields",
			data: struct {
				A coverMarshalJSON `json:"a"`
				B coverMarshalJSON `json:"b"`
				C coverMarshalJSON `json:"c"`
			}{A: coverMarshalJSON{}, B: coverMarshalJSON{}, C: coverMarshalJSON{}},
		},
		{
			name: "HeadMarshalJSONMultiFieldsOmitEmpty",
			data: struct {
				A coverMarshalJSON `json:"a,omitempty"`
				B coverMarshalJSON `json:"b,omitempty"`
				C coverMarshalJSON `json:"c,omitempty"`
			}{A: coverMarshalJSON{}, B: coverMarshalJSON{}, C: coverMarshalJSON{}},
		},
		{
			name: "HeadMarshalJSONMultiFieldsString",
			data: struct {
				A coverMarshalJSON `json:"a,string"`
				B coverMarshalJSON `json:"b,string"`
				C coverMarshalJSON `json:"c,string"`
			}{A: coverMarshalJSON{}, B: coverMarshalJSON{}, C: coverMarshalJSON{}},
		},
		{
			name: "HeadPtrMarshalJSONMultiFields",
			data: struct {
				A coverPtrMarshalJSON `json:"a"`
				B coverPtrMarshalJSON `json:"b"`
				C coverPtrMarshalJSON `json:"c"`
			}{A: coverPtrMarshalJSON{}, B: coverPtrMarshalJSON{}, C: coverPtrMarshalJSON{}},
		},
		{
			name: "HeadPtrMarshalJSONMultiFieldsOmitEmpty",
			data: struct {
				A coverPtrMarshalJSON `json:"a,omitempty"`
				B coverPtrMarshalJSON `json:"b,omitempty"`
				C coverPtrMarshalJSON `json:"c,omitempty"`
			}{A: coverPtrMarshalJSON{}, B: coverPtrMarshalJSON{}, C: coverPtrMarshalJSON{}},
		},
		{
			name: "HeadPtrMarshalJSONMultiFieldsString",
			data: struct {
				A coverPtrMarshalJSON `json:"a,string"`
				B coverPtrMarshalJSON `json:"b,string"`
				C coverPtrMarshalJSON `json:"c,string"`
			}{A: coverPtrMarshalJSON{}, B: coverPtrMarshalJSON{}, C: coverPtrMarshalJSON{}},
		},

		// HeadMarshalJSONPtrMultiFields
		{
			name: "HeadMarshalJSONPtrMultiFields",
			data: struct {
				A *coverMarshalJSON `json:"a"`
				B *coverMarshalJSON `json:"b"`
				C *coverMarshalJSON `json:"c"`
			}{A: &coverMarshalJSON{}, B: &coverMarshalJSON{}, C: &coverMarshalJSON{}},
		},
		{
			name: "HeadMarshalJSONPtrMultiFieldsOmitEmpty",
			data: struct {
				A *coverMarshalJSON `json:"a,omitempty"`
				B *coverMarshalJSON `json:"b,omitempty"`
				C *coverMarshalJSON `json:"c,omitempty"`
			}{A: &coverMarshalJSON{}, B: &coverMarshalJSON{}, C: &coverMarshalJSON{}},
		},
		{
			name: "HeadMarshalJSONPtrMultiFieldsString",
			data: struct {
				A *coverMarshalJSON `json:"a,string"`
				B *coverMarshalJSON `json:"b,string"`
				C *coverMarshalJSON `json:"c,string"`
			}{A: &coverMarshalJSON{}, B: &coverMarshalJSON{}, C: &coverMarshalJSON{}},
		},
		{
			name: "HeadPtrMarshalJSONPtrMultiFields",
			data: struct {
				A *coverPtrMarshalJSON `json:"a"`
				B *coverPtrMarshalJSON `json:"b"`
				C *coverPtrMarshalJSON `json:"c"`
			}{A: &coverPtrMarshalJSON{}, B: &coverPtrMarshalJSON{}, C: &coverPtrMarshalJSON{}},
		},
		{
			name: "HeadPtrMarshalJSONPtrMultiFieldsOmitEmpty",
			data: struct {
				A *coverPtrMarshalJSON `json:"a,omitempty"`
				B *coverPtrMarshalJSON `json:"b,omitempty"`
				C *coverPtrMarshalJSON `json:"c,omitempty"`
			}{A: &coverPtrMarshalJSON{}, B: &coverPtrMarshalJSON{}, C: &coverPtrMarshalJSON{}},
		},
		{
			name: "HeadPtrMarshalJSONPtrMultiFieldsString",
			data: struct {
				A *coverPtrMarshalJSON `json:"a,string"`
				B *coverPtrMarshalJSON `json:"b,string"`
				C *coverPtrMarshalJSON `json:"c,string"`
			}{A: &coverPtrMarshalJSON{}, B: &coverPtrMarshalJSON{}, C: &coverPtrMarshalJSON{}},
		},

		// HeadMarshalJSONPtrNilMultiFields
		{
			name: "HeadMarshalJSONPtrNilMultiFields",
			data: struct {
				A *coverMarshalJSON `json:"a"`
				B *coverMarshalJSON `json:"b"`
				C *coverMarshalJSON `json:"c"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadMarshalJSONPtrNilMultiFieldsOmitEmpty",
			data: struct {
				A *coverMarshalJSON `json:"a,omitempty"`
				B *coverMarshalJSON `json:"b,omitempty"`
				C *coverMarshalJSON `json:"c,omitempty"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadMarshalJSONPtrNilMultiFieldsString",
			data: struct {
				A *coverMarshalJSON `json:"a,string"`
				B *coverMarshalJSON `json:"b,string"`
				C *coverMarshalJSON `json:"c,string"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadPtrMarshalJSONPtrNilMultiFields",
			data: struct {
				A *coverPtrMarshalJSON `json:"a"`
				B *coverPtrMarshalJSON `json:"b"`
				C *coverPtrMarshalJSON `json:"c"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadPtrMarshalJSONPtrNilMultiFieldsOmitEmpty",
			data: struct {
				A *coverPtrMarshalJSON `json:"a,omitempty"`
				B *coverPtrMarshalJSON `json:"b,omitempty"`
				C *coverPtrMarshalJSON `json:"c,omitempty"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadPtrMarshalJSONPtrNilMultiFieldsString",
			data: struct {
				A *coverPtrMarshalJSON `json:"a,string"`
				B *coverPtrMarshalJSON `json:"b,string"`
				C *coverPtrMarshalJSON `json:"c,string"`
			}{A: nil, B: nil, C: nil},
		},

		// PtrHeadMarshalJSONZeroMultiFields
		{
			name: "PtrHeadMarshalJSONZeroMultiFields",
			data: &struct {
				A coverMarshalJSON `json:"a"`
				B coverMarshalJSON `json:"b"`
			}{},
		},
		{
			name: "PtrHeadMarshalJSONZeroMultiFieldsOmitEmpty",
			data: &struct {
				A coverMarshalJSON `json:"a,omitempty"`
				B coverMarshalJSON `json:"b,omitempty"`
			}{},
		},
		{
			name: "PtrHeadMarshalJSONZeroMultiFieldsString",
			data: &struct {
				A coverMarshalJSON `json:"a,string"`
				B coverMarshalJSON `json:"b,string"`
			}{},
		},
		{
			name: "PtrHeadPtrMarshalJSONZeroMultiFields",
			data: &struct {
				A coverPtrMarshalJSON `json:"a"`
				B coverPtrMarshalJSON `json:"b"`
			}{},
		},
		{
			name: "PtrHeadPtrMarshalJSONZeroMultiFieldsOmitEmpty",
			data: &struct {
				A coverPtrMarshalJSON `json:"a,omitempty"`
				B coverPtrMarshalJSON `json:"b,omitempty"`
			}{},
		},
		{
			name: "PtrHeadPtrMarshalJSONZeroMultiFieldsString",
			data: &struct {
				A coverPtrMarshalJSON `json:"a,string"`
				B coverPtrMarshalJSON `json:"b,string"`
			}{},
		},

		// PtrHeadMarshalJSONMultiFields
		{
			name: "PtrHeadMarshalJSONMultiFields",
			data: &struct {
				A coverMarshalJSON `json:"a"`
				B coverMarshalJSON `json:"b"`
			}{A: coverMarshalJSON{}, B: coverMarshalJSON{}},
		},
		{
			name: "PtrHeadMarshalJSONMultiFieldsOmitEmpty",
			data: &struct {
				A coverMarshalJSON `json:"a,omitempty"`
				B coverMarshalJSON `json:"b,omitempty"`
			}{A: coverMarshalJSON{}, B: coverMarshalJSON{}},
		},
		{
			name: "PtrHeadMarshalJSONMultiFieldsString",
			data: &struct {
				A coverMarshalJSON `json:"a,string"`
				B coverMarshalJSON `json:"b,string"`
			}{A: coverMarshalJSON{}, B: coverMarshalJSON{}},
		},
		{
			name: "PtrHeadPtrMarshalJSONMultiFields",
			data: &struct {
				A coverPtrMarshalJSON `json:"a"`
				B coverPtrMarshalJSON `json:"b"`
			}{A: coverPtrMarshalJSON{}, B: coverPtrMarshalJSON{}},
		},
		{
			name: "PtrHeadPtrMarshalJSONMultiFieldsOmitEmpty",
			data: &struct {
				A coverPtrMarshalJSON `json:"a,omitempty"`
				B coverPtrMarshalJSON `json:"b,omitempty"`
			}{A: coverPtrMarshalJSON{}, B: coverPtrMarshalJSON{}},
		},
		{
			name: "PtrHeadPtrMarshalJSONMultiFieldsString",
			data: &struct {
				A coverPtrMarshalJSON `json:"a,string"`
				B coverPtrMarshalJSON `json:"b,string"`
			}{A: coverPtrMarshalJSON{}, B: coverPtrMarshalJSON{}},
		},

		// PtrHeadMarshalJSONPtrMultiFields
		{
			name: "PtrHeadMarshalJSONPtrMultiFields",
			data: &struct {
				A *coverMarshalJSON `json:"a"`
				B *coverMarshalJSON `json:"b"`
			}{A: &coverMarshalJSON{}, B: &coverMarshalJSON{}},
		},
		{
			name: "PtrHeadMarshalJSONPtrMultiFieldsOmitEmpty",
			data: &struct {
				A *coverMarshalJSON `json:"a,omitempty"`
				B *coverMarshalJSON `json:"b,omitempty"`
			}{A: &coverMarshalJSON{}, B: &coverMarshalJSON{}},
		},
		{
			name: "PtrHeadMarshalJSONPtrMultiFieldsString",
			data: &struct {
				A *coverMarshalJSON `json:"a,string"`
				B *coverMarshalJSON `json:"b,string"`
			}{A: &coverMarshalJSON{}, B: &coverMarshalJSON{}},
		},
		{
			name: "PtrHeadPtrMarshalJSONPtrMultiFields",
			data: &struct {
				A *coverPtrMarshalJSON `json:"a"`
				B *coverPtrMarshalJSON `json:"b"`
			}{A: &coverPtrMarshalJSON{}, B: &coverPtrMarshalJSON{}},
		},
		{
			name: "PtrHeadPtrMarshalJSONPtrMultiFieldsOmitEmpty",
			data: &struct {
				A *coverPtrMarshalJSON `json:"a,omitempty"`
				B *coverPtrMarshalJSON `json:"b,omitempty"`
			}{A: &coverPtrMarshalJSON{}, B: &coverPtrMarshalJSON{}},
		},
		{
			name: "PtrHeadPtrMarshalJSONPtrMultiFieldsString",
			data: &struct {
				A *coverPtrMarshalJSON `json:"a,string"`
				B *coverPtrMarshalJSON `json:"b,string"`
			}{A: &coverPtrMarshalJSON{}, B: &coverPtrMarshalJSON{}},
		},

		// PtrHeadMarshalJSONPtrNilMultiFields
		{
			name: "PtrHeadMarshalJSONPtrNilMultiFields",
			data: &struct {
				A *coverMarshalJSON `json:"a"`
				B *coverMarshalJSON `json:"b"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadMarshalJSONPtrNilMultiFieldsOmitEmpty",
			data: &struct {
				A *coverMarshalJSON `json:"a,omitempty"`
				B *coverMarshalJSON `json:"b,omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadMarshalJSONPtrNilMultiFieldsString",
			data: &struct {
				A *coverMarshalJSON `json:"a,string"`
				B *coverMarshalJSON `json:"b,string"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadPtrMarshalJSONPtrNilMultiFields",
			data: &struct {
				A *coverPtrMarshalJSON `json:"a"`
				B *coverPtrMarshalJSON `json:"b"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadPtrMarshalJSONPtrNilMultiFieldsOmitEmpty",
			data: &struct {
				A *coverPtrMarshalJSON `json:"a,omitempty"`
				B *coverPtrMarshalJSON `json:"b,omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadPtrMarshalJSONPtrNilMultiFieldsString",
			data: &struct {
				A *coverPtrMarshalJSON `json:"a,string"`
				B *coverPtrMarshalJSON `json:"b,string"`
			}{A: nil, B: nil},
		},

		// PtrHeadMarshalJSONNilMultiFields
		{
			name: "PtrHeadMarshalJSONNilMultiFields",
			data: (*struct {
				A coverMarshalJSON `json:"a"`
				B coverMarshalJSON `json:"b"`
			})(nil),
		},
		{
			name: "PtrHeadMarshalJSONNilMultiFieldsOmitEmpty",
			data: (*struct {
				A coverMarshalJSON `json:"a,omitempty"`
				B coverMarshalJSON `json:"b,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadMarshalJSONNilMultiFieldsString",
			data: (*struct {
				A coverMarshalJSON `json:"a,string"`
				B coverMarshalJSON `json:"b,string"`
			})(nil),
		},
		{
			name: "PtrHeadPtrMarshalJSONNilMultiFields",
			data: (*struct {
				A coverPtrMarshalJSON `json:"a"`
				B coverPtrMarshalJSON `json:"b"`
			})(nil),
		},
		{
			name: "PtrHeadPtrMarshalJSONNilMultiFieldsOmitEmpty",
			data: (*struct {
				A coverPtrMarshalJSON `json:"a,omitempty"`
				B coverPtrMarshalJSON `json:"b,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadPtrMarshalJSONNilMultiFieldsString",
			data: (*struct {
				A coverPtrMarshalJSON `json:"a,string"`
				B coverPtrMarshalJSON `json:"b,string"`
			})(nil),
		},

		// PtrHeadMarshalJSONNilMultiFields
		{
			name: "PtrHeadMarshalJSONNilMultiFields",
			data: (*struct {
				A *coverMarshalJSON `json:"a"`
				B *coverMarshalJSON `json:"b"`
			})(nil),
		},
		{
			name: "PtrHeadMarshalJSONNilMultiFieldsOmitEmpty",
			data: (*struct {
				A *coverMarshalJSON `json:"a,omitempty"`
				B *coverMarshalJSON `json:"b,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadMarshalJSONNilMultiFieldsString",
			data: (*struct {
				A *coverMarshalJSON `json:"a,string"`
				B *coverMarshalJSON `json:"b,string"`
			})(nil),
		},
		{
			name: "PtrHeadPtrMarshalJSONNilMultiFields",
			data: (*struct {
				A *coverPtrMarshalJSON `json:"a"`
				B *coverPtrMarshalJSON `json:"b"`
			})(nil),
		},
		{
			name: "PtrHeadPtrMarshalJSONNilMultiFieldsOmitEmpty",
			data: (*struct {
				A *coverPtrMarshalJSON `json:"a,omitempty"`
				B *coverPtrMarshalJSON `json:"b,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadPtrMarshalJSONNilMultiFieldsString",
			data: (*struct {
				A *coverPtrMarshalJSON `json:"a,string"`
				B *coverPtrMarshalJSON `json:"b,string"`
			})(nil),
		},

		// HeadMarshalJSONZeroNotRoot
		{
			name: "HeadMarshalJSONZeroNotRoot",
			data: struct {
				A struct {
					A coverMarshalJSON `json:"a"`
				}
			}{},
		},
		{
			name: "HeadMarshalJSONZeroNotRootOmitEmpty",
			data: struct {
				A struct {
					A coverMarshalJSON `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadMarshalJSONZeroNotRootString",
			data: struct {
				A struct {
					A coverMarshalJSON `json:"a,string"`
				}
			}{},
		},
		{
			name: "HeadPtrMarshalJSONZeroNotRoot",
			data: struct {
				A struct {
					A coverPtrMarshalJSON `json:"a"`
				}
			}{},
		},
		{
			name: "HeadPtrMarshalJSONZeroNotRootOmitEmpty",
			data: struct {
				A struct {
					A coverPtrMarshalJSON `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadPtrMarshalJSONZeroNotRootString",
			data: struct {
				A struct {
					A coverPtrMarshalJSON `json:"a,string"`
				}
			}{},
		},

		// HeadMarshalJSONNotRoot
		{
			name: "HeadMarshalJSONNotRoot",
			data: struct {
				A struct {
					A coverMarshalJSON `json:"a"`
				}
			}{A: struct {
				A coverMarshalJSON `json:"a"`
			}{A: coverMarshalJSON{}}},
		},
		{
			name: "HeadMarshalJSONNotRootOmitEmpty",
			data: struct {
				A struct {
					A coverMarshalJSON `json:"a,omitempty"`
				}
			}{A: struct {
				A coverMarshalJSON `json:"a,omitempty"`
			}{A: coverMarshalJSON{}}},
		},
		{
			name: "HeadMarshalJSONNotRootString",
			data: struct {
				A struct {
					A coverMarshalJSON `json:"a,string"`
				}
			}{A: struct {
				A coverMarshalJSON `json:"a,string"`
			}{A: coverMarshalJSON{}}},
		},
		{
			name: "HeadMarshalJSONNotRoot",
			data: struct {
				A struct {
					A coverPtrMarshalJSON `json:"a"`
				}
			}{A: struct {
				A coverPtrMarshalJSON `json:"a"`
			}{A: coverPtrMarshalJSON{}}},
		},
		{
			name: "HeadMarshalJSONNotRootOmitEmpty",
			data: struct {
				A struct {
					A coverPtrMarshalJSON `json:"a,omitempty"`
				}
			}{A: struct {
				A coverPtrMarshalJSON `json:"a,omitempty"`
			}{A: coverPtrMarshalJSON{}}},
		},
		{
			name: "HeadMarshalJSONNotRootString",
			data: struct {
				A struct {
					A coverPtrMarshalJSON `json:"a,string"`
				}
			}{A: struct {
				A coverPtrMarshalJSON `json:"a,string"`
			}{A: coverPtrMarshalJSON{}}},
		},

		// HeadMarshalJSONPtrNotRoot
		{
			name: "HeadMarshalJSONPtrNotRoot",
			data: struct {
				A struct {
					A *coverMarshalJSON `json:"a"`
				}
			}{A: struct {
				A *coverMarshalJSON `json:"a"`
			}{&coverMarshalJSON{}}},
		},
		{
			name: "HeadMarshalJSONPtrNotRootOmitEmpty",
			data: struct {
				A struct {
					A *coverMarshalJSON `json:"a,omitempty"`
				}
			}{A: struct {
				A *coverMarshalJSON `json:"a,omitempty"`
			}{&coverMarshalJSON{}}},
		},
		{
			name: "HeadMarshalJSONPtrNotRootString",
			data: struct {
				A struct {
					A *coverMarshalJSON `json:"a,string"`
				}
			}{A: struct {
				A *coverMarshalJSON `json:"a,string"`
			}{&coverMarshalJSON{}}},
		},
		{
			name: "HeadPtrMarshalJSONPtrNotRoot",
			data: struct {
				A struct {
					A *coverPtrMarshalJSON `json:"a"`
				}
			}{A: struct {
				A *coverPtrMarshalJSON `json:"a"`
			}{&coverPtrMarshalJSON{}}},
		},
		{
			name: "HeadPtrMarshalJSONPtrNotRootOmitEmpty",
			data: struct {
				A struct {
					A *coverPtrMarshalJSON `json:"a,omitempty"`
				}
			}{A: struct {
				A *coverPtrMarshalJSON `json:"a,omitempty"`
			}{&coverPtrMarshalJSON{}}},
		},
		{
			name: "HeadPtrMarshalJSONPtrNotRootString",
			data: struct {
				A struct {
					A *coverPtrMarshalJSON `json:"a,string"`
				}
			}{A: struct {
				A *coverPtrMarshalJSON `json:"a,string"`
			}{&coverPtrMarshalJSON{}}},
		},

		// HeadMarshalJSONPtrNilNotRoot
		{
			name: "HeadMarshalJSONPtrNilNotRoot",
			data: struct {
				A struct {
					A *coverMarshalJSON `json:"a"`
				}
			}{},
		},
		{
			name: "HeadMarshalJSONPtrNilNotRootOmitEmpty",
			data: struct {
				A struct {
					A *coverMarshalJSON `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadMarshalJSONPtrNilNotRootString",
			data: struct {
				A struct {
					A *coverMarshalJSON `json:"a,string"`
				}
			}{},
		},
		{
			name: "HeadPtrMarshalJSONPtrNilNotRoot",
			data: struct {
				A struct {
					A *coverPtrMarshalJSON `json:"a"`
				}
			}{},
		},
		{
			name: "HeadPtrMarshalJSONPtrNilNotRootOmitEmpty",
			data: struct {
				A struct {
					A *coverPtrMarshalJSON `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadPtrMarshalJSONPtrNilNotRootString",
			data: struct {
				A struct {
					A *coverPtrMarshalJSON `json:"a,string"`
				}
			}{},
		},

		// PtrHeadMarshalJSONZeroNotRoot
		{
			name: "PtrHeadMarshalJSONZeroNotRoot",
			data: struct {
				A *struct {
					A coverMarshalJSON `json:"a"`
				}
			}{A: new(struct {
				A coverMarshalJSON `json:"a"`
			})},
		},
		{
			name: "PtrHeadMarshalJSONZeroNotRootOmitEmpty",
			data: struct {
				A *struct {
					A coverMarshalJSON `json:"a,omitempty"`
				}
			}{A: new(struct {
				A coverMarshalJSON `json:"a,omitempty"`
			})},
		},
		{
			name: "PtrHeadMarshalJSONZeroNotRootString",
			data: struct {
				A *struct {
					A coverMarshalJSON `json:"a,string"`
				}
			}{A: new(struct {
				A coverMarshalJSON `json:"a,string"`
			})},
		},
		{
			name: "PtrHeadPtrMarshalJSONZeroNotRoot",
			data: struct {
				A *struct {
					A coverPtrMarshalJSON `json:"a"`
				}
			}{A: new(struct {
				A coverPtrMarshalJSON `json:"a"`
			})},
		},
		{
			name: "PtrHeadPtrMarshalJSONZeroNotRootOmitEmpty",
			data: struct {
				A *struct {
					A coverPtrMarshalJSON `json:"a,omitempty"`
				}
			}{A: new(struct {
				A coverPtrMarshalJSON `json:"a,omitempty"`
			})},
		},
		{
			name: "PtrHeadPtrMarshalJSONZeroNotRootString",
			data: struct {
				A *struct {
					A coverPtrMarshalJSON `json:"a,string"`
				}
			}{A: new(struct {
				A coverPtrMarshalJSON `json:"a,string"`
			})},
		},

		// PtrHeadMarshalJSONNotRoot
		{
			name: "PtrHeadMarshalJSONNotRoot",
			data: struct {
				A *struct {
					A coverMarshalJSON `json:"a"`
				}
			}{A: &(struct {
				A coverMarshalJSON `json:"a"`
			}{A: coverMarshalJSON{}})},
		},
		{
			name: "PtrHeadMarshalJSONNotRootOmitEmpty",
			data: struct {
				A *struct {
					A coverMarshalJSON `json:"a,omitempty"`
				}
			}{A: &(struct {
				A coverMarshalJSON `json:"a,omitempty"`
			}{A: coverMarshalJSON{}})},
		},
		{
			name: "PtrHeadMarshalJSONNotRootString",
			data: struct {
				A *struct {
					A coverMarshalJSON `json:"a,string"`
				}
			}{A: &(struct {
				A coverMarshalJSON `json:"a,string"`
			}{A: coverMarshalJSON{}})},
		},
		{
			name: "PtrHeadPtrMarshalJSONNotRoot",
			data: struct {
				A *struct {
					A coverPtrMarshalJSON `json:"a"`
				}
			}{A: &(struct {
				A coverPtrMarshalJSON `json:"a"`
			}{A: coverPtrMarshalJSON{}})},
		},
		{
			name: "PtrHeadPtrMarshalJSONNotRootOmitEmpty",
			data: struct {
				A *struct {
					A coverPtrMarshalJSON `json:"a,omitempty"`
				}
			}{A: &(struct {
				A coverPtrMarshalJSON `json:"a,omitempty"`
			}{A: coverPtrMarshalJSON{}})},
		},
		{
			name: "PtrHeadPtrMarshalJSONNotRootString",
			data: struct {
				A *struct {
					A coverPtrMarshalJSON `json:"a,string"`
				}
			}{A: &(struct {
				A coverPtrMarshalJSON `json:"a,string"`
			}{A: coverPtrMarshalJSON{}})},
		},

		// PtrHeadMarshalJSONPtrNotRoot
		{
			name: "PtrHeadMarshalJSONPtrNotRoot",
			data: struct {
				A *struct {
					A *coverMarshalJSON `json:"a"`
				}
			}{A: &(struct {
				A *coverMarshalJSON `json:"a"`
			}{A: &coverMarshalJSON{}})},
		},
		{
			name: "PtrHeadMarshalJSONPtrNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *coverMarshalJSON `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *coverMarshalJSON `json:"a,omitempty"`
			}{A: &coverMarshalJSON{}})},
		},
		{
			name: "PtrHeadMarshalJSONPtrNotRootString",
			data: struct {
				A *struct {
					A *coverMarshalJSON `json:"a,string"`
				}
			}{A: &(struct {
				A *coverMarshalJSON `json:"a,string"`
			}{A: &coverMarshalJSON{}})},
		},
		{
			name: "PtrHeadPtrMarshalJSONPtrNotRoot",
			data: struct {
				A *struct {
					A *coverPtrMarshalJSON `json:"a"`
				}
			}{A: &(struct {
				A *coverPtrMarshalJSON `json:"a"`
			}{A: &coverPtrMarshalJSON{}})},
		},
		{
			name: "PtrHeadPtrMarshalJSONPtrNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *coverPtrMarshalJSON `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *coverPtrMarshalJSON `json:"a,omitempty"`
			}{A: &coverPtrMarshalJSON{}})},
		},
		{
			name: "PtrHeadPtrMarshalJSONPtrNotRootString",
			data: struct {
				A *struct {
					A *coverPtrMarshalJSON `json:"a,string"`
				}
			}{A: &(struct {
				A *coverPtrMarshalJSON `json:"a,string"`
			}{A: &coverPtrMarshalJSON{}})},
		},

		// PtrHeadMarshalJSONPtrNilNotRoot
		{
			name: "PtrHeadMarshalJSONPtrNilNotRoot",
			data: struct {
				A *struct {
					A *coverMarshalJSON `json:"a"`
				}
			}{A: &(struct {
				A *coverMarshalJSON `json:"a"`
			}{A: nil})},
		},
		{
			name: "PtrHeadMarshalJSONPtrNilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *coverMarshalJSON `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *coverMarshalJSON `json:"a,omitempty"`
			}{A: nil})},
		},
		{
			name: "PtrHeadMarshalJSONPtrNilNotRootString",
			data: struct {
				A *struct {
					A *coverMarshalJSON `json:"a,string"`
				}
			}{A: &(struct {
				A *coverMarshalJSON `json:"a,string"`
			}{A: nil})},
		},
		{
			name: "PtrHeadPtrMarshalJSONPtrNilNotRoot",
			data: struct {
				A *struct {
					A *coverPtrMarshalJSON `json:"a"`
				}
			}{A: &(struct {
				A *coverPtrMarshalJSON `json:"a"`
			}{A: nil})},
		},
		{
			name: "PtrHeadPtrMarshalJSONPtrNilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *coverPtrMarshalJSON `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *coverPtrMarshalJSON `json:"a,omitempty"`
			}{A: nil})},
		},
		{
			name: "PtrHeadPtrMarshalJSONPtrNilNotRootString",
			data: struct {
				A *struct {
					A *coverPtrMarshalJSON `json:"a,string"`
				}
			}{A: &(struct {
				A *coverPtrMarshalJSON `json:"a,string"`
			}{A: nil})},
		},

		// PtrHeadMarshalJSONNilNotRoot
		{
			name: "PtrHeadMarshalJSONNilNotRoot",
			data: struct {
				A *struct {
					A *coverMarshalJSON `json:"a"`
				}
			}{A: nil},
		},
		{
			name: "PtrHeadMarshalJSONNilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *coverMarshalJSON `json:"a,omitempty"`
				} `json:",omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadMarshalJSONNilNotRootString",
			data: struct {
				A *struct {
					A *coverMarshalJSON `json:"a,string"`
				} `json:",string"`
			}{A: nil},
		},
		{
			name: "PtrHeadPtrMarshalJSONNilNotRoot",
			data: struct {
				A *struct {
					A *coverPtrMarshalJSON `json:"a"`
				}
			}{A: nil},
		},
		{
			name: "PtrHeadPtrMarshalJSONNilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *coverPtrMarshalJSON `json:"a,omitempty"`
				} `json:",omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadPtrMarshalJSONNilNotRootString",
			data: struct {
				A *struct {
					A *coverPtrMarshalJSON `json:"a,string"`
				} `json:",string"`
			}{A: nil},
		},

		// HeadMarshalJSONZeroMultiFieldsNotRoot
		{
			name: "HeadMarshalJSONZeroMultiFieldsNotRoot",
			data: struct {
				A struct {
					A coverMarshalJSON `json:"a"`
				}
				B struct {
					B coverMarshalJSON `json:"b"`
				}
			}{},
		},
		{
			name: "HeadMarshalJSONZeroMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A coverMarshalJSON `json:"a,omitempty"`
				}
				B struct {
					B coverMarshalJSON `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "HeadMarshalJSONZeroMultiFieldsNotRootString",
			data: struct {
				A struct {
					A coverMarshalJSON `json:"a,string"`
				}
				B struct {
					B coverMarshalJSON `json:"b,string"`
				}
			}{},
		},
		{
			name: "HeadPtrMarshalJSONZeroMultiFieldsNotRoot",
			data: struct {
				A struct {
					A coverPtrMarshalJSON `json:"a"`
				}
				B struct {
					B coverPtrMarshalJSON `json:"b"`
				}
			}{},
		},
		{
			name: "HeadPtrMarshalJSONZeroMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A coverPtrMarshalJSON `json:"a,omitempty"`
				}
				B struct {
					B coverPtrMarshalJSON `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "HeadPtrMarshalJSONZeroMultiFieldsNotRootString",
			data: struct {
				A struct {
					A coverPtrMarshalJSON `json:"a,string"`
				}
				B struct {
					B coverPtrMarshalJSON `json:"b,string"`
				}
			}{},
		},

		// HeadMarshalJSONMultiFieldsNotRoot
		{
			name: "HeadMarshalJSONMultiFieldsNotRoot",
			data: struct {
				A struct {
					A coverMarshalJSON `json:"a"`
				}
				B struct {
					B coverMarshalJSON `json:"b"`
				}
			}{A: struct {
				A coverMarshalJSON `json:"a"`
			}{A: coverMarshalJSON{}}, B: struct {
				B coverMarshalJSON `json:"b"`
			}{B: coverMarshalJSON{}}},
		},
		{
			name: "HeadMarshalJSONMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A coverMarshalJSON `json:"a,omitempty"`
				}
				B struct {
					B coverMarshalJSON `json:"b,omitempty"`
				}
			}{A: struct {
				A coverMarshalJSON `json:"a,omitempty"`
			}{A: coverMarshalJSON{}}, B: struct {
				B coverMarshalJSON `json:"b,omitempty"`
			}{B: coverMarshalJSON{}}},
		},
		{
			name: "HeadMarshalJSONMultiFieldsNotRootString",
			data: struct {
				A struct {
					A coverMarshalJSON `json:"a,string"`
				}
				B struct {
					B coverMarshalJSON `json:"b,string"`
				}
			}{A: struct {
				A coverMarshalJSON `json:"a,string"`
			}{A: coverMarshalJSON{}}, B: struct {
				B coverMarshalJSON `json:"b,string"`
			}{B: coverMarshalJSON{}}},
		},
		{
			name: "HeadPtrMarshalJSONMultiFieldsNotRoot",
			data: struct {
				A struct {
					A coverPtrMarshalJSON `json:"a"`
				}
				B struct {
					B coverPtrMarshalJSON `json:"b"`
				}
			}{A: struct {
				A coverPtrMarshalJSON `json:"a"`
			}{A: coverPtrMarshalJSON{}}, B: struct {
				B coverPtrMarshalJSON `json:"b"`
			}{B: coverPtrMarshalJSON{}}},
		},
		{
			name: "HeadPtrMarshalJSONMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A coverPtrMarshalJSON `json:"a,omitempty"`
				}
				B struct {
					B coverPtrMarshalJSON `json:"b,omitempty"`
				}
			}{A: struct {
				A coverPtrMarshalJSON `json:"a,omitempty"`
			}{A: coverPtrMarshalJSON{}}, B: struct {
				B coverPtrMarshalJSON `json:"b,omitempty"`
			}{B: coverPtrMarshalJSON{}}},
		},
		{
			name: "HeadPtrMarshalJSONMultiFieldsNotRootString",
			data: struct {
				A struct {
					A coverPtrMarshalJSON `json:"a,string"`
				}
				B struct {
					B coverPtrMarshalJSON `json:"b,string"`
				}
			}{A: struct {
				A coverPtrMarshalJSON `json:"a,string"`
			}{A: coverPtrMarshalJSON{}}, B: struct {
				B coverPtrMarshalJSON `json:"b,string"`
			}{B: coverPtrMarshalJSON{}}},
		},

		// HeadMarshalJSONPtrMultiFieldsNotRoot
		{
			name: "HeadMarshalJSONPtrMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *coverMarshalJSON `json:"a"`
				}
				B struct {
					B *coverMarshalJSON `json:"b"`
				}
			}{A: struct {
				A *coverMarshalJSON `json:"a"`
			}{A: &coverMarshalJSON{}}, B: struct {
				B *coverMarshalJSON `json:"b"`
			}{B: &coverMarshalJSON{}}},
		},
		{
			name: "HeadMarshalJSONPtrMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *coverMarshalJSON `json:"a,omitempty"`
				}
				B struct {
					B *coverMarshalJSON `json:"b,omitempty"`
				}
			}{A: struct {
				A *coverMarshalJSON `json:"a,omitempty"`
			}{A: &coverMarshalJSON{}}, B: struct {
				B *coverMarshalJSON `json:"b,omitempty"`
			}{B: &coverMarshalJSON{}}},
		},
		{
			name: "HeadMarshalJSONPtrMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *coverMarshalJSON `json:"a,string"`
				}
				B struct {
					B *coverMarshalJSON `json:"b,string"`
				}
			}{A: struct {
				A *coverMarshalJSON `json:"a,string"`
			}{A: &coverMarshalJSON{}}, B: struct {
				B *coverMarshalJSON `json:"b,string"`
			}{B: &coverMarshalJSON{}}},
		},
		{
			name: "HeadPtrMarshalJSONPtrMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *coverPtrMarshalJSON `json:"a"`
				}
				B struct {
					B *coverPtrMarshalJSON `json:"b"`
				}
			}{A: struct {
				A *coverPtrMarshalJSON `json:"a"`
			}{A: &coverPtrMarshalJSON{}}, B: struct {
				B *coverPtrMarshalJSON `json:"b"`
			}{B: &coverPtrMarshalJSON{}}},
		},
		{
			name: "HeadPtrMarshalJSONPtrMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *coverPtrMarshalJSON `json:"a,omitempty"`
				}
				B struct {
					B *coverPtrMarshalJSON `json:"b,omitempty"`
				}
			}{A: struct {
				A *coverPtrMarshalJSON `json:"a,omitempty"`
			}{A: &coverPtrMarshalJSON{}}, B: struct {
				B *coverPtrMarshalJSON `json:"b,omitempty"`
			}{B: &coverPtrMarshalJSON{}}},
		},
		{
			name: "HeadPtrMarshalJSONPtrMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *coverPtrMarshalJSON `json:"a,string"`
				}
				B struct {
					B *coverPtrMarshalJSON `json:"b,string"`
				}
			}{A: struct {
				A *coverPtrMarshalJSON `json:"a,string"`
			}{A: &coverPtrMarshalJSON{}}, B: struct {
				B *coverPtrMarshalJSON `json:"b,string"`
			}{B: &coverPtrMarshalJSON{}}},
		},

		// HeadMarshalJSONPtrNilMultiFieldsNotRoot
		{
			name: "HeadMarshalJSONPtrNilMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *coverMarshalJSON `json:"a"`
				}
				B struct {
					B *coverMarshalJSON `json:"b"`
				}
			}{A: struct {
				A *coverMarshalJSON `json:"a"`
			}{A: nil}, B: struct {
				B *coverMarshalJSON `json:"b"`
			}{B: nil}},
		},
		{
			name: "HeadMarshalJSONPtrNilMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *coverMarshalJSON `json:"a,omitempty"`
				}
				B struct {
					B *coverMarshalJSON `json:"b,omitempty"`
				}
			}{A: struct {
				A *coverMarshalJSON `json:"a,omitempty"`
			}{A: nil}, B: struct {
				B *coverMarshalJSON `json:"b,omitempty"`
			}{B: nil}},
		},
		{
			name: "HeadMarshalJSONPtrNilMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *coverMarshalJSON `json:"a,string"`
				}
				B struct {
					B *coverMarshalJSON `json:"b,string"`
				}
			}{A: struct {
				A *coverMarshalJSON `json:"a,string"`
			}{A: nil}, B: struct {
				B *coverMarshalJSON `json:"b,string"`
			}{B: nil}},
		},
		{
			name: "HeadPtrMarshalJSONPtrNilMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *coverPtrMarshalJSON `json:"a"`
				}
				B struct {
					B *coverPtrMarshalJSON `json:"b"`
				}
			}{A: struct {
				A *coverPtrMarshalJSON `json:"a"`
			}{A: nil}, B: struct {
				B *coverPtrMarshalJSON `json:"b"`
			}{B: nil}},
		},
		{
			name: "HeadPtrMarshalJSONPtrNilMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *coverPtrMarshalJSON `json:"a,omitempty"`
				}
				B struct {
					B *coverPtrMarshalJSON `json:"b,omitempty"`
				}
			}{A: struct {
				A *coverPtrMarshalJSON `json:"a,omitempty"`
			}{A: nil}, B: struct {
				B *coverPtrMarshalJSON `json:"b,omitempty"`
			}{B: nil}},
		},
		{
			name: "HeadPtrMarshalJSONPtrNilMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *coverPtrMarshalJSON `json:"a,string"`
				}
				B struct {
					B *coverPtrMarshalJSON `json:"b,string"`
				}
			}{A: struct {
				A *coverPtrMarshalJSON `json:"a,string"`
			}{A: nil}, B: struct {
				B *coverPtrMarshalJSON `json:"b,string"`
			}{B: nil}},
		},

		// PtrHeadMarshalJSONZeroMultiFieldsNotRoot
		{
			name: "PtrHeadMarshalJSONZeroMultiFieldsNotRoot",
			data: &struct {
				A struct {
					A coverMarshalJSON `json:"a"`
				}
				B struct {
					B coverMarshalJSON `json:"b"`
				}
			}{},
		},
		{
			name: "PtrHeadMarshalJSONZeroMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A coverMarshalJSON `json:"a,omitempty"`
				}
				B struct {
					B coverMarshalJSON `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "PtrHeadMarshalJSONZeroMultiFieldsNotRootString",
			data: &struct {
				A struct {
					A coverMarshalJSON `json:"a,string"`
				}
				B struct {
					B coverMarshalJSON `json:"b,string"`
				}
			}{},
		},
		{
			name: "PtrHeadPtrMarshalJSONZeroMultiFieldsNotRoot",
			data: &struct {
				A struct {
					A coverPtrMarshalJSON `json:"a"`
				}
				B struct {
					B coverPtrMarshalJSON `json:"b"`
				}
			}{},
		},
		{
			name: "PtrHeadPtrMarshalJSONZeroMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A coverPtrMarshalJSON `json:"a,omitempty"`
				}
				B struct {
					B coverPtrMarshalJSON `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "PtrHeadPtrMarshalJSONZeroMultiFieldsNotRootString",
			data: &struct {
				A struct {
					A coverPtrMarshalJSON `json:"a,string"`
				}
				B struct {
					B coverPtrMarshalJSON `json:"b,string"`
				}
			}{},
		},

		// PtrHeadMarshalJSONMultiFieldsNotRoot
		{
			name: "PtrHeadMarshalJSONMultiFieldsNotRoot",
			data: &struct {
				A struct {
					A coverMarshalJSON `json:"a"`
				}
				B struct {
					B coverMarshalJSON `json:"b"`
				}
			}{A: struct {
				A coverMarshalJSON `json:"a"`
			}{A: coverMarshalJSON{}}, B: struct {
				B coverMarshalJSON `json:"b"`
			}{B: coverMarshalJSON{}}},
		},
		{
			name: "PtrHeadMarshalJSONMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A coverMarshalJSON `json:"a,omitempty"`
				}
				B struct {
					B coverMarshalJSON `json:"b,omitempty"`
				}
			}{A: struct {
				A coverMarshalJSON `json:"a,omitempty"`
			}{A: coverMarshalJSON{}}, B: struct {
				B coverMarshalJSON `json:"b,omitempty"`
			}{B: coverMarshalJSON{}}},
		},
		{
			name: "PtrHeadMarshalJSONMultiFieldsNotRootString",
			data: &struct {
				A struct {
					A coverMarshalJSON `json:"a,string"`
				}
				B struct {
					B coverMarshalJSON `json:"b,string"`
				}
			}{A: struct {
				A coverMarshalJSON `json:"a,string"`
			}{A: coverMarshalJSON{}}, B: struct {
				B coverMarshalJSON `json:"b,string"`
			}{B: coverMarshalJSON{}}},
		},
		{
			name: "PtrHeadPtrMarshalJSONMultiFieldsNotRoot",
			data: &struct {
				A struct {
					A coverPtrMarshalJSON `json:"a"`
				}
				B struct {
					B coverPtrMarshalJSON `json:"b"`
				}
			}{A: struct {
				A coverPtrMarshalJSON `json:"a"`
			}{A: coverPtrMarshalJSON{}}, B: struct {
				B coverPtrMarshalJSON `json:"b"`
			}{B: coverPtrMarshalJSON{}}},
		},
		{
			name: "PtrHeadPtrMarshalJSONMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A coverPtrMarshalJSON `json:"a,omitempty"`
				}
				B struct {
					B coverPtrMarshalJSON `json:"b,omitempty"`
				}
			}{A: struct {
				A coverPtrMarshalJSON `json:"a,omitempty"`
			}{A: coverPtrMarshalJSON{}}, B: struct {
				B coverPtrMarshalJSON `json:"b,omitempty"`
			}{B: coverPtrMarshalJSON{}}},
		},
		{
			name: "PtrHeadPtrMarshalJSONMultiFieldsNotRootString",
			data: &struct {
				A struct {
					A coverPtrMarshalJSON `json:"a,string"`
				}
				B struct {
					B coverPtrMarshalJSON `json:"b,string"`
				}
			}{A: struct {
				A coverPtrMarshalJSON `json:"a,string"`
			}{A: coverPtrMarshalJSON{}}, B: struct {
				B coverPtrMarshalJSON `json:"b,string"`
			}{B: coverPtrMarshalJSON{}}},
		},

		// PtrHeadMarshalJSONPtrMultiFieldsNotRoot
		{
			name: "PtrHeadMarshalJSONPtrMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *coverMarshalJSON `json:"a"`
				}
				B *struct {
					B *coverMarshalJSON `json:"b"`
				}
			}{A: &(struct {
				A *coverMarshalJSON `json:"a"`
			}{A: &coverMarshalJSON{}}), B: &(struct {
				B *coverMarshalJSON `json:"b"`
			}{B: &coverMarshalJSON{}})},
		},
		{
			name: "PtrHeadMarshalJSONPtrMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *coverMarshalJSON `json:"a,omitempty"`
				}
				B *struct {
					B *coverMarshalJSON `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *coverMarshalJSON `json:"a,omitempty"`
			}{A: &coverMarshalJSON{}}), B: &(struct {
				B *coverMarshalJSON `json:"b,omitempty"`
			}{B: &coverMarshalJSON{}})},
		},
		{
			name: "PtrHeadMarshalJSONPtrMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *coverMarshalJSON `json:"a,string"`
				}
				B *struct {
					B *coverMarshalJSON `json:"b,string"`
				}
			}{A: &(struct {
				A *coverMarshalJSON `json:"a,string"`
			}{A: &coverMarshalJSON{}}), B: &(struct {
				B *coverMarshalJSON `json:"b,string"`
			}{B: &coverMarshalJSON{}})},
		},
		{
			name: "PtrHeadPtrMarshalJSONPtrMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *coverPtrMarshalJSON `json:"a"`
				}
				B *struct {
					B *coverPtrMarshalJSON `json:"b"`
				}
			}{A: &(struct {
				A *coverPtrMarshalJSON `json:"a"`
			}{A: &coverPtrMarshalJSON{}}), B: &(struct {
				B *coverPtrMarshalJSON `json:"b"`
			}{B: &coverPtrMarshalJSON{}})},
		},
		{
			name: "PtrHeadPtrMarshalJSONPtrMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *coverPtrMarshalJSON `json:"a,omitempty"`
				}
				B *struct {
					B *coverPtrMarshalJSON `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *coverPtrMarshalJSON `json:"a,omitempty"`
			}{A: &coverPtrMarshalJSON{}}), B: &(struct {
				B *coverPtrMarshalJSON `json:"b,omitempty"`
			}{B: &coverPtrMarshalJSON{}})},
		},
		{
			name: "PtrHeadPtrMarshalJSONPtrMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *coverPtrMarshalJSON `json:"a,string"`
				}
				B *struct {
					B *coverPtrMarshalJSON `json:"b,string"`
				}
			}{A: &(struct {
				A *coverPtrMarshalJSON `json:"a,string"`
			}{A: &coverPtrMarshalJSON{}}), B: &(struct {
				B *coverPtrMarshalJSON `json:"b,string"`
			}{B: &coverPtrMarshalJSON{}})},
		},

		// PtrHeadMarshalJSONPtrNilMultiFieldsNotRoot
		{
			name: "PtrHeadMarshalJSONPtrNilMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *coverMarshalJSON `json:"a"`
				}
				B *struct {
					B *coverMarshalJSON `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadMarshalJSONPtrNilMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *coverMarshalJSON `json:"a,omitempty"`
				} `json:",omitempty"`
				B *struct {
					B *coverMarshalJSON `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadMarshalJSONPtrNilMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *coverMarshalJSON `json:"a,string"`
				} `json:",string"`
				B *struct {
					B *coverMarshalJSON `json:"b,string"`
				} `json:",string"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadPtrMarshalJSONPtrNilMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *coverPtrMarshalJSON `json:"a"`
				}
				B *struct {
					B *coverPtrMarshalJSON `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadPtrMarshalJSONPtrNilMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *coverPtrMarshalJSON `json:"a,omitempty"`
				} `json:",omitempty"`
				B *struct {
					B *coverPtrMarshalJSON `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadPtrMarshalJSONPtrNilMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *coverPtrMarshalJSON `json:"a,string"`
				} `json:",string"`
				B *struct {
					B *coverPtrMarshalJSON `json:"b,string"`
				} `json:",string"`
			}{A: nil, B: nil},
		},

		// PtrHeadMarshalJSONNilMultiFieldsNotRoot
		{
			name: "PtrHeadMarshalJSONNilMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *coverMarshalJSON `json:"a"`
				}
				B *struct {
					B *coverMarshalJSON `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadMarshalJSONNilMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *coverMarshalJSON `json:"a,omitempty"`
				}
				B *struct {
					B *coverMarshalJSON `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadMarshalJSONNilMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *coverMarshalJSON `json:"a,string"`
				}
				B *struct {
					B *coverMarshalJSON `json:"b,string"`
				}
			})(nil),
		},
		{
			name: "PtrHeadPtrMarshalJSONNilMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *coverPtrMarshalJSON `json:"a"`
				}
				B *struct {
					B *coverPtrMarshalJSON `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadPtrMarshalJSONNilMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *coverPtrMarshalJSON `json:"a,omitempty"`
				}
				B *struct {
					B *coverPtrMarshalJSON `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadPtrMarshalJSONNilMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *coverPtrMarshalJSON `json:"a,string"`
				}
				B *struct {
					B *coverPtrMarshalJSON `json:"b,string"`
				}
			})(nil),
		},

		// PtrHeadMarshalJSONDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadMarshalJSONDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A coverMarshalJSON `json:"a"`
					B coverMarshalJSON `json:"b"`
				}
				B *struct {
					A coverMarshalJSON `json:"a"`
					B coverMarshalJSON `json:"b"`
				}
			}{A: &(struct {
				A coverMarshalJSON `json:"a"`
				B coverMarshalJSON `json:"b"`
			}{A: coverMarshalJSON{}, B: coverMarshalJSON{}}), B: &(struct {
				A coverMarshalJSON `json:"a"`
				B coverMarshalJSON `json:"b"`
			}{A: coverMarshalJSON{}, B: coverMarshalJSON{}})},
		},
		{
			name: "PtrHeadMarshalJSONDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A coverMarshalJSON `json:"a,omitempty"`
					B coverMarshalJSON `json:"b,omitempty"`
				}
				B *struct {
					A coverMarshalJSON `json:"a,omitempty"`
					B coverMarshalJSON `json:"b,omitempty"`
				}
			}{A: &(struct {
				A coverMarshalJSON `json:"a,omitempty"`
				B coverMarshalJSON `json:"b,omitempty"`
			}{A: coverMarshalJSON{}, B: coverMarshalJSON{}}), B: &(struct {
				A coverMarshalJSON `json:"a,omitempty"`
				B coverMarshalJSON `json:"b,omitempty"`
			}{A: coverMarshalJSON{}, B: coverMarshalJSON{}})},
		},
		{
			name: "PtrHeadMarshalJSONDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A coverMarshalJSON `json:"a,string"`
					B coverMarshalJSON `json:"b,string"`
				}
				B *struct {
					A coverMarshalJSON `json:"a,string"`
					B coverMarshalJSON `json:"b,string"`
				}
			}{A: &(struct {
				A coverMarshalJSON `json:"a,string"`
				B coverMarshalJSON `json:"b,string"`
			}{A: coverMarshalJSON{}, B: coverMarshalJSON{}}), B: &(struct {
				A coverMarshalJSON `json:"a,string"`
				B coverMarshalJSON `json:"b,string"`
			}{A: coverMarshalJSON{}, B: coverMarshalJSON{}})},
		},
		{
			name: "PtrHeadPtrMarshalJSONDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A coverPtrMarshalJSON `json:"a"`
					B coverPtrMarshalJSON `json:"b"`
				}
				B *struct {
					A coverPtrMarshalJSON `json:"a"`
					B coverPtrMarshalJSON `json:"b"`
				}
			}{A: &(struct {
				A coverPtrMarshalJSON `json:"a"`
				B coverPtrMarshalJSON `json:"b"`
			}{A: coverPtrMarshalJSON{}, B: coverPtrMarshalJSON{}}), B: &(struct {
				A coverPtrMarshalJSON `json:"a"`
				B coverPtrMarshalJSON `json:"b"`
			}{A: coverPtrMarshalJSON{}, B: coverPtrMarshalJSON{}})},
		},
		{
			name: "PtrHeadPtrMarshalJSONDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A coverPtrMarshalJSON `json:"a,omitempty"`
					B coverPtrMarshalJSON `json:"b,omitempty"`
				}
				B *struct {
					A coverPtrMarshalJSON `json:"a,omitempty"`
					B coverPtrMarshalJSON `json:"b,omitempty"`
				}
			}{A: &(struct {
				A coverPtrMarshalJSON `json:"a,omitempty"`
				B coverPtrMarshalJSON `json:"b,omitempty"`
			}{A: coverPtrMarshalJSON{}, B: coverPtrMarshalJSON{}}), B: &(struct {
				A coverPtrMarshalJSON `json:"a,omitempty"`
				B coverPtrMarshalJSON `json:"b,omitempty"`
			}{A: coverPtrMarshalJSON{}, B: coverPtrMarshalJSON{}})},
		},
		{
			name: "PtrHeadPtrMarshalJSONDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A coverPtrMarshalJSON `json:"a,string"`
					B coverPtrMarshalJSON `json:"b,string"`
				}
				B *struct {
					A coverPtrMarshalJSON `json:"a,string"`
					B coverPtrMarshalJSON `json:"b,string"`
				}
			}{A: &(struct {
				A coverPtrMarshalJSON `json:"a,string"`
				B coverPtrMarshalJSON `json:"b,string"`
			}{A: coverPtrMarshalJSON{}, B: coverPtrMarshalJSON{}}), B: &(struct {
				A coverPtrMarshalJSON `json:"a,string"`
				B coverPtrMarshalJSON `json:"b,string"`
			}{A: coverPtrMarshalJSON{}, B: coverPtrMarshalJSON{}})},
		},

		// PtrHeadMarshalJSONNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadMarshalJSONNilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A coverMarshalJSON `json:"a"`
					B coverMarshalJSON `json:"b"`
				}
				B *struct {
					A coverMarshalJSON `json:"a"`
					B coverMarshalJSON `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadMarshalJSONNilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A coverMarshalJSON `json:"a,omitempty"`
					B coverMarshalJSON `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A coverMarshalJSON `json:"a,omitempty"`
					B coverMarshalJSON `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadMarshalJSONNilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A coverMarshalJSON `json:"a,string"`
					B coverMarshalJSON `json:"b,string"`
				}
				B *struct {
					A coverMarshalJSON `json:"a,string"`
					B coverMarshalJSON `json:"b,string"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadPtrMarshalJSONNilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A coverPtrMarshalJSON `json:"a"`
					B coverPtrMarshalJSON `json:"b"`
				}
				B *struct {
					A coverPtrMarshalJSON `json:"a"`
					B coverPtrMarshalJSON `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadPtrMarshalJSONNilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A coverPtrMarshalJSON `json:"a,omitempty"`
					B coverPtrMarshalJSON `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A coverPtrMarshalJSON `json:"a,omitempty"`
					B coverPtrMarshalJSON `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadPtrMarshalJSONNilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A coverPtrMarshalJSON `json:"a,string"`
					B coverPtrMarshalJSON `json:"b,string"`
				}
				B *struct {
					A coverPtrMarshalJSON `json:"a,string"`
					B coverPtrMarshalJSON `json:"b,string"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadMarshalJSONNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadMarshalJSONNilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A coverMarshalJSON `json:"a"`
					B coverMarshalJSON `json:"b"`
				}
				B *struct {
					A coverMarshalJSON `json:"a"`
					B coverMarshalJSON `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadMarshalJSONNilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A coverMarshalJSON `json:"a,omitempty"`
					B coverMarshalJSON `json:"b,omitempty"`
				}
				B *struct {
					A coverMarshalJSON `json:"a,omitempty"`
					B coverMarshalJSON `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadMarshalJSONNilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A coverMarshalJSON `json:"a,string"`
					B coverMarshalJSON `json:"b,string"`
				}
				B *struct {
					A coverMarshalJSON `json:"a,string"`
					B coverMarshalJSON `json:"b,string"`
				}
			})(nil),
		},
		{
			name: "PtrHeadPtrMarshalJSONNilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A coverPtrMarshalJSON `json:"a"`
					B coverPtrMarshalJSON `json:"b"`
				}
				B *struct {
					A coverPtrMarshalJSON `json:"a"`
					B coverPtrMarshalJSON `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadPtrMarshalJSONNilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A coverPtrMarshalJSON `json:"a,omitempty"`
					B coverPtrMarshalJSON `json:"b,omitempty"`
				}
				B *struct {
					A coverPtrMarshalJSON `json:"a,omitempty"`
					B coverPtrMarshalJSON `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadPtrMarshalJSONNilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A coverPtrMarshalJSON `json:"a,string"`
					B coverPtrMarshalJSON `json:"b,string"`
				}
				B *struct {
					A coverPtrMarshalJSON `json:"a,string"`
					B coverPtrMarshalJSON `json:"b,string"`
				}
			})(nil),
		},

		// PtrHeadMarshalJSONPtrDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadMarshalJSONPtrDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *coverMarshalJSON `json:"a"`
					B *coverMarshalJSON `json:"b"`
				}
				B *struct {
					A *coverMarshalJSON `json:"a"`
					B *coverMarshalJSON `json:"b"`
				}
			}{A: &(struct {
				A *coverMarshalJSON `json:"a"`
				B *coverMarshalJSON `json:"b"`
			}{A: &coverMarshalJSON{}, B: &coverMarshalJSON{}}), B: &(struct {
				A *coverMarshalJSON `json:"a"`
				B *coverMarshalJSON `json:"b"`
			}{A: nil, B: nil})},
		},
		{
			name: "PtrHeadMarshalJSONPtrDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *coverMarshalJSON `json:"a,omitempty"`
					B *coverMarshalJSON `json:"b,omitempty"`
				}
				B *struct {
					A *coverMarshalJSON `json:"a,omitempty"`
					B *coverMarshalJSON `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *coverMarshalJSON `json:"a,omitempty"`
				B *coverMarshalJSON `json:"b,omitempty"`
			}{A: &coverMarshalJSON{}, B: &coverMarshalJSON{}}), B: &(struct {
				A *coverMarshalJSON `json:"a,omitempty"`
				B *coverMarshalJSON `json:"b,omitempty"`
			}{A: nil, B: nil})},
		},
		{
			name: "PtrHeadMarshalJSONPtrDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *coverMarshalJSON `json:"a,string"`
					B *coverMarshalJSON `json:"b,string"`
				}
				B *struct {
					A *coverMarshalJSON `json:"a,string"`
					B *coverMarshalJSON `json:"b,string"`
				}
			}{A: &(struct {
				A *coverMarshalJSON `json:"a,string"`
				B *coverMarshalJSON `json:"b,string"`
			}{A: &coverMarshalJSON{}, B: &coverMarshalJSON{}}), B: &(struct {
				A *coverMarshalJSON `json:"a,string"`
				B *coverMarshalJSON `json:"b,string"`
			}{A: nil, B: nil})},
		},
		{
			name: "PtrHeadPtrMarshalJSONPtrDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *coverPtrMarshalJSON `json:"a"`
					B *coverPtrMarshalJSON `json:"b"`
				}
				B *struct {
					A *coverPtrMarshalJSON `json:"a"`
					B *coverPtrMarshalJSON `json:"b"`
				}
			}{A: &(struct {
				A *coverPtrMarshalJSON `json:"a"`
				B *coverPtrMarshalJSON `json:"b"`
			}{A: &coverPtrMarshalJSON{}, B: &coverPtrMarshalJSON{}}), B: &(struct {
				A *coverPtrMarshalJSON `json:"a"`
				B *coverPtrMarshalJSON `json:"b"`
			}{A: nil, B: nil})},
		},
		{
			name: "PtrHeadPtrMarshalJSONPtrDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *coverPtrMarshalJSON `json:"a,omitempty"`
					B *coverPtrMarshalJSON `json:"b,omitempty"`
				}
				B *struct {
					A *coverPtrMarshalJSON `json:"a,omitempty"`
					B *coverPtrMarshalJSON `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *coverPtrMarshalJSON `json:"a,omitempty"`
				B *coverPtrMarshalJSON `json:"b,omitempty"`
			}{A: &coverPtrMarshalJSON{}, B: &coverPtrMarshalJSON{}}), B: &(struct {
				A *coverPtrMarshalJSON `json:"a,omitempty"`
				B *coverPtrMarshalJSON `json:"b,omitempty"`
			}{A: nil, B: nil})},
		},
		{
			name: "PtrHeadPtrMarshalJSONPtrDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *coverPtrMarshalJSON `json:"a,string"`
					B *coverPtrMarshalJSON `json:"b,string"`
				}
				B *struct {
					A *coverPtrMarshalJSON `json:"a,string"`
					B *coverPtrMarshalJSON `json:"b,string"`
				}
			}{A: &(struct {
				A *coverPtrMarshalJSON `json:"a,string"`
				B *coverPtrMarshalJSON `json:"b,string"`
			}{A: &coverPtrMarshalJSON{}, B: &coverPtrMarshalJSON{}}), B: &(struct {
				A *coverPtrMarshalJSON `json:"a,string"`
				B *coverPtrMarshalJSON `json:"b,string"`
			}{A: nil, B: nil})},
		},

		// PtrHeadMarshalJSONPtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadMarshalJSONPtrNilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *coverMarshalJSON `json:"a"`
					B *coverMarshalJSON `json:"b"`
				}
				B *struct {
					A *coverMarshalJSON `json:"a"`
					B *coverMarshalJSON `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadMarshalJSONPtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *coverMarshalJSON `json:"a,omitempty"`
					B *coverMarshalJSON `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A *coverMarshalJSON `json:"a,omitempty"`
					B *coverMarshalJSON `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadMarshalJSONPtrNilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *coverMarshalJSON `json:"a,string"`
					B *coverMarshalJSON `json:"b,string"`
				}
				B *struct {
					A *coverMarshalJSON `json:"a,string"`
					B *coverMarshalJSON `json:"b,string"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadPtrMarshalJSONPtrNilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *coverPtrMarshalJSON `json:"a"`
					B *coverPtrMarshalJSON `json:"b"`
				}
				B *struct {
					A *coverPtrMarshalJSON `json:"a"`
					B *coverPtrMarshalJSON `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadPtrMarshalJSONPtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *coverPtrMarshalJSON `json:"a,omitempty"`
					B *coverPtrMarshalJSON `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A *coverPtrMarshalJSON `json:"a,omitempty"`
					B *coverPtrMarshalJSON `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadPtrMarshalJSONPtrNilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *coverPtrMarshalJSON `json:"a,string"`
					B *coverPtrMarshalJSON `json:"b,string"`
				}
				B *struct {
					A *coverPtrMarshalJSON `json:"a,string"`
					B *coverPtrMarshalJSON `json:"b,string"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadMarshalJSONPtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadMarshalJSONPtrNilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *coverMarshalJSON `json:"a"`
					B *coverMarshalJSON `json:"b"`
				}
				B *struct {
					A *coverMarshalJSON `json:"a"`
					B *coverMarshalJSON `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadMarshalJSONPtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *coverMarshalJSON `json:"a,omitempty"`
					B *coverMarshalJSON `json:"b,omitempty"`
				}
				B *struct {
					A *coverMarshalJSON `json:"a,omitempty"`
					B *coverMarshalJSON `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadMarshalJSONPtrNilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *coverMarshalJSON `json:"a,string"`
					B *coverMarshalJSON `json:"b,string"`
				}
				B *struct {
					A *coverMarshalJSON `json:"a,string"`
					B *coverMarshalJSON `json:"b,string"`
				}
			})(nil),
		},
		{
			name: "PtrHeadPtrMarshalJSONPtrNilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *coverPtrMarshalJSON `json:"a"`
					B *coverPtrMarshalJSON `json:"b"`
				}
				B *struct {
					A *coverPtrMarshalJSON `json:"a"`
					B *coverPtrMarshalJSON `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadPtrMarshalJSONPtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *coverPtrMarshalJSON `json:"a,omitempty"`
					B *coverPtrMarshalJSON `json:"b,omitempty"`
				}
				B *struct {
					A *coverPtrMarshalJSON `json:"a,omitempty"`
					B *coverPtrMarshalJSON `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadPtrMarshalJSONPtrNilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *coverPtrMarshalJSON `json:"a,string"`
					B *coverPtrMarshalJSON `json:"b,string"`
				}
				B *struct {
					A *coverPtrMarshalJSON `json:"a,string"`
					B *coverPtrMarshalJSON `json:"b,string"`
				}
			})(nil),
		},

		// AnonymousHeadMarshalJSON
		{
			name: "AnonymousHeadMarshalJSON",
			data: struct {
				structMarshalJSON
				B coverMarshalJSON `json:"b"`
			}{
				structMarshalJSON: structMarshalJSON{A: coverMarshalJSON{}},
				B:                 coverMarshalJSON{},
			},
		},
		{
			name: "AnonymousHeadMarshalJSONOmitEmpty",
			data: struct {
				structMarshalJSONOmitEmpty
				B coverMarshalJSON `json:"b,omitempty"`
			}{
				structMarshalJSONOmitEmpty: structMarshalJSONOmitEmpty{A: coverMarshalJSON{}},
				B:                          coverMarshalJSON{},
			},
		},
		{
			name: "AnonymousHeadMarshalJSONString",
			data: struct {
				structMarshalJSONString
				B coverMarshalJSON `json:"b,string"`
			}{
				structMarshalJSONString: structMarshalJSONString{A: coverMarshalJSON{}},
				B:                       coverMarshalJSON{},
			},
		},
		{
			name: "AnonymousHeadPtrMarshalJSON",
			data: struct {
				structPtrMarshalJSON
				B coverPtrMarshalJSON `json:"b"`
			}{
				structPtrMarshalJSON: structPtrMarshalJSON{A: coverPtrMarshalJSON{}},
				B:                    coverPtrMarshalJSON{},
			},
		},
		{
			name: "AnonymousHeadPtrMarshalJSONOmitEmpty",
			data: struct {
				structPtrMarshalJSONOmitEmpty
				B coverPtrMarshalJSON `json:"b,omitempty"`
			}{
				structPtrMarshalJSONOmitEmpty: structPtrMarshalJSONOmitEmpty{A: coverPtrMarshalJSON{}},
				B:                             coverPtrMarshalJSON{},
			},
		},
		{
			name: "AnonymousHeadPtrMarshalJSONString",
			data: struct {
				structPtrMarshalJSONString
				B coverPtrMarshalJSON `json:"b,string"`
			}{
				structPtrMarshalJSONString: structPtrMarshalJSONString{A: coverPtrMarshalJSON{}},
				B:                          coverPtrMarshalJSON{},
			},
		},

		// PtrAnonymousHeadMarshalJSON
		{
			name: "PtrAnonymousHeadMarshalJSON",
			data: struct {
				*structMarshalJSON
				B coverMarshalJSON `json:"b"`
			}{
				structMarshalJSON: &structMarshalJSON{A: coverMarshalJSON{}},
				B:                 coverMarshalJSON{},
			},
		},
		{
			name: "PtrAnonymousHeadMarshalJSONOmitEmpty",
			data: struct {
				*structMarshalJSONOmitEmpty
				B coverMarshalJSON `json:"b,omitempty"`
			}{
				structMarshalJSONOmitEmpty: &structMarshalJSONOmitEmpty{A: coverMarshalJSON{}},
				B:                          coverMarshalJSON{},
			},
		},
		{
			name: "PtrAnonymousHeadMarshalJSONString",
			data: struct {
				*structMarshalJSONString
				B coverMarshalJSON `json:"b,string"`
			}{
				structMarshalJSONString: &structMarshalJSONString{A: coverMarshalJSON{}},
				B:                       coverMarshalJSON{},
			},
		},
		{
			name: "PtrAnonymousHeadPtrMarshalJSON",
			data: struct {
				*structPtrMarshalJSON
				B coverPtrMarshalJSON `json:"b"`
			}{
				structPtrMarshalJSON: &structPtrMarshalJSON{A: coverPtrMarshalJSON{}},
				B:                    coverPtrMarshalJSON{},
			},
		},
		{
			name: "PtrAnonymousHeadPtrMarshalJSONOmitEmpty",
			data: struct {
				*structPtrMarshalJSONOmitEmpty
				B coverPtrMarshalJSON `json:"b,omitempty"`
			}{
				structPtrMarshalJSONOmitEmpty: &structPtrMarshalJSONOmitEmpty{A: coverPtrMarshalJSON{}},
				B:                             coverPtrMarshalJSON{},
			},
		},
		{
			name: "PtrAnonymousHeadPtrMarshalJSONString",
			data: struct {
				*structPtrMarshalJSONString
				B coverPtrMarshalJSON `json:"b,string"`
			}{
				structPtrMarshalJSONString: &structPtrMarshalJSONString{A: coverPtrMarshalJSON{}},
				B:                          coverPtrMarshalJSON{},
			},
		},

		// PtrAnonymousHeadMarshalJSONNil
		{
			name: "PtrAnonymousHeadMarshalJSONNil",
			data: struct {
				*structMarshalJSON
				B coverMarshalJSON `json:"b"`
			}{
				structMarshalJSON: &structMarshalJSON{A: coverMarshalJSON{}},
				B:                 coverMarshalJSON{},
			},
		},
		{
			name: "PtrAnonymousHeadMarshalJSONNilOmitEmpty",
			data: struct {
				*structMarshalJSONOmitEmpty
				B coverMarshalJSON `json:"b,omitempty"`
			}{
				structMarshalJSONOmitEmpty: &structMarshalJSONOmitEmpty{A: coverMarshalJSON{}},
				B:                          coverMarshalJSON{},
			},
		},
		{
			name: "PtrAnonymousHeadMarshalJSONNilString",
			data: struct {
				*structMarshalJSONString
				B coverMarshalJSON `json:"b,string"`
			}{
				structMarshalJSONString: &structMarshalJSONString{A: coverMarshalJSON{}},
				B:                       coverMarshalJSON{},
			},
		},
		{
			name: "PtrAnonymousHeadPtrMarshalJSONNil",
			data: struct {
				*structPtrMarshalJSON
				B coverPtrMarshalJSON `json:"b"`
			}{
				structPtrMarshalJSON: &structPtrMarshalJSON{A: coverPtrMarshalJSON{}},
				B:                    coverPtrMarshalJSON{},
			},
		},
		{
			name: "PtrAnonymousHeadPtrMarshalJSONNilOmitEmpty",
			data: struct {
				*structPtrMarshalJSONOmitEmpty
				B coverPtrMarshalJSON `json:"b,omitempty"`
			}{
				structPtrMarshalJSONOmitEmpty: &structPtrMarshalJSONOmitEmpty{A: coverPtrMarshalJSON{}},
				B:                             coverPtrMarshalJSON{},
			},
		},
		{
			name: "PtrAnonymousHeadPtrMarshalJSONNilString",
			data: struct {
				*structPtrMarshalJSONString
				B coverPtrMarshalJSON `json:"b,string"`
			}{
				structPtrMarshalJSONString: &structPtrMarshalJSONString{A: coverPtrMarshalJSON{}},
				B:                          coverPtrMarshalJSON{},
			},
		},

		// NilPtrAnonymousHeadMarshalJSON
		{
			name: "NilPtrAnonymousHeadMarshalJSON",
			data: struct {
				*structMarshalJSON
				B coverMarshalJSON `json:"b"`
			}{
				structMarshalJSON: nil,
				B:                 coverMarshalJSON{},
			},
		},
		{
			name: "NilPtrAnonymousHeadMarshalJSONOmitEmpty",
			data: struct {
				*structMarshalJSONOmitEmpty
				B coverMarshalJSON `json:"b,omitempty"`
			}{
				structMarshalJSONOmitEmpty: nil,
				B:                          coverMarshalJSON{},
			},
		},
		{
			name: "NilPtrAnonymousHeadMarshalJSONString",
			data: struct {
				*structMarshalJSONString
				B coverMarshalJSON `json:"b,string"`
			}{
				structMarshalJSONString: nil,
				B:                       coverMarshalJSON{},
			},
		},
		{
			name: "NilPtrAnonymousHeadPtrMarshalJSON",
			data: struct {
				*structPtrMarshalJSON
				B coverPtrMarshalJSON `json:"b"`
			}{
				structPtrMarshalJSON: nil,
				B:                    coverPtrMarshalJSON{},
			},
		},
		{
			name: "NilPtrAnonymousHeadPtrMarshalJSONOmitEmpty",
			data: struct {
				*structPtrMarshalJSONOmitEmpty
				B coverPtrMarshalJSON `json:"b,omitempty"`
			}{
				structPtrMarshalJSONOmitEmpty: nil,
				B:                             coverPtrMarshalJSON{},
			},
		},
		{
			name: "NilPtrAnonymousHeadPtrMarshalJSONString",
			data: struct {
				*structPtrMarshalJSONString
				B coverPtrMarshalJSON `json:"b,string"`
			}{
				structPtrMarshalJSONString: nil,
				B:                          coverPtrMarshalJSON{},
			},
		},

		// AnonymousHeadMarshalJSONPtr
		{
			name: "AnonymousHeadMarshalJSONPtr",
			data: struct {
				structMarshalJSONPtr
				B *coverMarshalJSON `json:"b"`
			}{
				structMarshalJSONPtr: structMarshalJSONPtr{A: &coverMarshalJSON{}},
				B:                    nil,
			},
		},
		{
			name: "AnonymousHeadMarshalJSONPtrOmitEmpty",
			data: struct {
				structMarshalJSONPtrOmitEmpty
				B *coverMarshalJSON `json:"b,omitempty"`
			}{
				structMarshalJSONPtrOmitEmpty: structMarshalJSONPtrOmitEmpty{A: &coverMarshalJSON{}},
				B:                             nil,
			},
		},
		{
			name: "AnonymousHeadMarshalJSONPtrString",
			data: struct {
				structMarshalJSONPtrString
				B *coverMarshalJSON `json:"b,string"`
			}{
				structMarshalJSONPtrString: structMarshalJSONPtrString{A: &coverMarshalJSON{}},
				B:                          nil,
			},
		},
		{
			name: "AnonymousHeadPtrMarshalJSONPtr",
			data: struct {
				structPtrMarshalJSONPtr
				B *coverPtrMarshalJSON `json:"b"`
			}{
				structPtrMarshalJSONPtr: structPtrMarshalJSONPtr{A: &coverPtrMarshalJSON{}},
				B:                       nil,
			},
		},
		{
			name: "AnonymousHeadPtrMarshalJSONPtrOmitEmpty",
			data: struct {
				structPtrMarshalJSONPtrOmitEmpty
				B *coverPtrMarshalJSON `json:"b,omitempty"`
			}{
				structPtrMarshalJSONPtrOmitEmpty: structPtrMarshalJSONPtrOmitEmpty{A: &coverPtrMarshalJSON{}},
				B:                                nil,
			},
		},
		{
			name: "AnonymousHeadPtrMarshalJSONPtrString",
			data: struct {
				structPtrMarshalJSONPtrString
				B *coverPtrMarshalJSON `json:"b,string"`
			}{
				structPtrMarshalJSONPtrString: structPtrMarshalJSONPtrString{A: &coverPtrMarshalJSON{}},
				B:                             nil,
			},
		},

		// AnonymousHeadMarshalJSONPtrNil
		{
			name: "AnonymousHeadMarshalJSONPtrNil",
			data: struct {
				structMarshalJSONPtr
				B *coverMarshalJSON `json:"b"`
			}{
				structMarshalJSONPtr: structMarshalJSONPtr{A: nil},
				B:                    &coverMarshalJSON{},
			},
		},
		{
			name: "AnonymousHeadMarshalJSONPtrNilOmitEmpty",
			data: struct {
				structMarshalJSONPtrOmitEmpty
				B *coverMarshalJSON `json:"b,omitempty"`
			}{
				structMarshalJSONPtrOmitEmpty: structMarshalJSONPtrOmitEmpty{A: nil},
				B:                             &coverMarshalJSON{},
			},
		},
		{
			name: "AnonymousHeadMarshalJSONPtrNilString",
			data: struct {
				structMarshalJSONPtrString
				B *coverMarshalJSON `json:"b,string"`
			}{
				structMarshalJSONPtrString: structMarshalJSONPtrString{A: nil},
				B:                          &coverMarshalJSON{},
			},
		},
		{
			name: "AnonymousHeadPtrMarshalJSONPtrNil",
			data: struct {
				structPtrMarshalJSONPtr
				B *coverPtrMarshalJSON `json:"b"`
			}{
				structPtrMarshalJSONPtr: structPtrMarshalJSONPtr{A: nil},
				B:                       &coverPtrMarshalJSON{},
			},
		},
		{
			name: "AnonymousHeadPtrMarshalJSONPtrNilOmitEmpty",
			data: struct {
				structPtrMarshalJSONPtrOmitEmpty
				B *coverPtrMarshalJSON `json:"b,omitempty"`
			}{
				structPtrMarshalJSONPtrOmitEmpty: structPtrMarshalJSONPtrOmitEmpty{A: nil},
				B:                                &coverPtrMarshalJSON{},
			},
		},
		{
			name: "AnonymousHeadPtrMarshalJSONPtrNilString",
			data: struct {
				structPtrMarshalJSONPtrString
				B *coverPtrMarshalJSON `json:"b,string"`
			}{
				structPtrMarshalJSONPtrString: structPtrMarshalJSONPtrString{A: nil},
				B:                             &coverPtrMarshalJSON{},
			},
		},

		// PtrAnonymousHeadMarshalJSONPtr
		{
			name: "PtrAnonymousHeadMarshalJSONPtr",
			data: struct {
				*structMarshalJSONPtr
				B *coverMarshalJSON `json:"b"`
			}{
				structMarshalJSONPtr: &structMarshalJSONPtr{A: &coverMarshalJSON{}},
				B:                    nil,
			},
		},
		{
			name: "PtrAnonymousHeadMarshalJSONPtrOmitEmpty",
			data: struct {
				*structMarshalJSONPtrOmitEmpty
				B *coverMarshalJSON `json:"b,omitempty"`
			}{
				structMarshalJSONPtrOmitEmpty: &structMarshalJSONPtrOmitEmpty{A: &coverMarshalJSON{}},
				B:                             nil,
			},
		},
		{
			name: "PtrAnonymousHeadMarshalJSONPtrString",
			data: struct {
				*structMarshalJSONPtrString
				B *coverMarshalJSON `json:"b,string"`
			}{
				structMarshalJSONPtrString: &structMarshalJSONPtrString{A: &coverMarshalJSON{}},
				B:                          nil,
			},
		},
		{
			name: "PtrAnonymousHeadPtrMarshalJSONPtr",
			data: struct {
				*structPtrMarshalJSONPtr
				B *coverPtrMarshalJSON `json:"b"`
			}{
				structPtrMarshalJSONPtr: &structPtrMarshalJSONPtr{A: &coverPtrMarshalJSON{}},
				B:                       nil,
			},
		},
		{
			name: "PtrAnonymousHeadPtrMarshalJSONPtrOmitEmpty",
			data: struct {
				*structPtrMarshalJSONPtrOmitEmpty
				B *coverPtrMarshalJSON `json:"b,omitempty"`
			}{
				structPtrMarshalJSONPtrOmitEmpty: &structPtrMarshalJSONPtrOmitEmpty{A: &coverPtrMarshalJSON{}},
				B:                                nil,
			},
		},
		{
			name: "PtrAnonymousHeadPtrMarshalJSONPtrString",
			data: struct {
				*structPtrMarshalJSONPtrString
				B *coverPtrMarshalJSON `json:"b,string"`
			}{
				structPtrMarshalJSONPtrString: &structPtrMarshalJSONPtrString{A: &coverPtrMarshalJSON{}},
				B:                             nil,
			},
		},

		// NilPtrAnonymousHeadMarshalJSONPtr
		{
			name: "NilPtrAnonymousHeadMarshalJSONPtr",
			data: struct {
				*structMarshalJSONPtr
				B *coverMarshalJSON `json:"b"`
			}{
				structMarshalJSONPtr: nil,
				B:                    &coverMarshalJSON{},
			},
		},
		{
			name: "NilPtrAnonymousHeadMarshalJSONPtrOmitEmpty",
			data: struct {
				*structMarshalJSONPtrOmitEmpty
				B *coverMarshalJSON `json:"b,omitempty"`
			}{
				structMarshalJSONPtrOmitEmpty: nil,
				B:                             &coverMarshalJSON{},
			},
		},
		{
			name: "NilPtrAnonymousHeadMarshalJSONPtrString",
			data: struct {
				*structMarshalJSONPtrString
				B *coverMarshalJSON `json:"b,string"`
			}{
				structMarshalJSONPtrString: nil,
				B:                          &coverMarshalJSON{},
			},
		},
		{
			name: "NilPtrAnonymousHeadPtrMarshalJSONPtr",
			data: struct {
				*structPtrMarshalJSONPtr
				B *coverPtrMarshalJSON `json:"b"`
			}{
				structPtrMarshalJSONPtr: nil,
				B:                       &coverPtrMarshalJSON{},
			},
		},
		{
			name: "NilPtrAnonymousHeadPtrMarshalJSONPtrOmitEmpty",
			data: struct {
				*structPtrMarshalJSONPtrOmitEmpty
				B *coverPtrMarshalJSON `json:"b,omitempty"`
			}{
				structPtrMarshalJSONPtrOmitEmpty: nil,
				B:                                &coverPtrMarshalJSON{},
			},
		},
		{
			name: "NilPtrAnonymousHeadPtrMarshalJSONPtrString",
			data: struct {
				*structPtrMarshalJSONPtrString
				B *coverPtrMarshalJSON `json:"b,string"`
			}{
				structPtrMarshalJSONPtrString: nil,
				B:                             &coverPtrMarshalJSON{},
			},
		},

		// AnonymousHeadMarshalJSONOnly
		{
			name: "AnonymousHeadMarshalJSONOnly",
			data: struct {
				structMarshalJSON
			}{
				structMarshalJSON: structMarshalJSON{A: coverMarshalJSON{}},
			},
		},
		{
			name: "AnonymousHeadMarshalJSONOnlyOmitEmpty",
			data: struct {
				structMarshalJSONOmitEmpty
			}{
				structMarshalJSONOmitEmpty: structMarshalJSONOmitEmpty{A: coverMarshalJSON{}},
			},
		},
		{
			name: "AnonymousHeadMarshalJSONOnlyString",
			data: struct {
				structMarshalJSONString
			}{
				structMarshalJSONString: structMarshalJSONString{A: coverMarshalJSON{}},
			},
		},
		{
			name: "AnonymousHeadPtrMarshalJSONOnly",
			data: struct {
				structPtrMarshalJSON
			}{
				structPtrMarshalJSON: structPtrMarshalJSON{A: coverPtrMarshalJSON{}},
			},
		},
		{
			name: "AnonymousHeadPtrMarshalJSONOnlyOmitEmpty",
			data: struct {
				structPtrMarshalJSONOmitEmpty
			}{
				structPtrMarshalJSONOmitEmpty: structPtrMarshalJSONOmitEmpty{A: coverPtrMarshalJSON{}},
			},
		},
		{
			name: "AnonymousHeadPtrMarshalJSONOnlyString",
			data: struct {
				structPtrMarshalJSONString
			}{
				structPtrMarshalJSONString: structPtrMarshalJSONString{A: coverPtrMarshalJSON{}},
			},
		},

		// PtrAnonymousHeadMarshalJSONOnly
		{
			name: "PtrAnonymousHeadMarshalJSONOnly",
			data: struct {
				*structMarshalJSON
			}{
				structMarshalJSON: &structMarshalJSON{A: coverMarshalJSON{}},
			},
		},
		{
			name: "PtrAnonymousHeadMarshalJSONOnlyOmitEmpty",
			data: struct {
				*structMarshalJSONOmitEmpty
			}{
				structMarshalJSONOmitEmpty: &structMarshalJSONOmitEmpty{A: coverMarshalJSON{}},
			},
		},
		{
			name: "PtrAnonymousHeadMarshalJSONOnlyString",
			data: struct {
				*structMarshalJSONString
			}{
				structMarshalJSONString: &structMarshalJSONString{A: coverMarshalJSON{}},
			},
		},
		{
			name: "PtrAnonymousHeadPtrMarshalJSONOnly",
			data: struct {
				*structPtrMarshalJSON
			}{
				structPtrMarshalJSON: &structPtrMarshalJSON{A: coverPtrMarshalJSON{}},
			},
		},
		{
			name: "PtrAnonymousHeadPtrMarshalJSONOnlyOmitEmpty",
			data: struct {
				*structPtrMarshalJSONOmitEmpty
			}{
				structPtrMarshalJSONOmitEmpty: &structPtrMarshalJSONOmitEmpty{A: coverPtrMarshalJSON{}},
			},
		},
		{
			name: "PtrAnonymousHeadPtrMarshalJSONOnlyString",
			data: struct {
				*structPtrMarshalJSONString
			}{
				structPtrMarshalJSONString: &structPtrMarshalJSONString{A: coverPtrMarshalJSON{}},
			},
		},

		// NilPtrAnonymousHeadMarshalJSONOnly
		{
			name: "NilPtrAnonymousHeadMarshalJSONOnly",
			data: struct {
				*structMarshalJSON
			}{
				structMarshalJSON: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadMarshalJSONOnlyOmitEmpty",
			data: struct {
				*structMarshalJSONOmitEmpty
			}{
				structMarshalJSONOmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadMarshalJSONOnlyString",
			data: struct {
				*structMarshalJSONString
			}{
				structMarshalJSONString: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadPtrMarshalJSONOnly",
			data: struct {
				*structPtrMarshalJSON
			}{
				structPtrMarshalJSON: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadPtrMarshalJSONOnlyOmitEmpty",
			data: struct {
				*structPtrMarshalJSONOmitEmpty
			}{
				structPtrMarshalJSONOmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadPtrMarshalJSONOnlyString",
			data: struct {
				*structPtrMarshalJSONString
			}{
				structPtrMarshalJSONString: nil,
			},
		},

		// AnonymousHeadMarshalJSONPtrOnly
		{
			name: "AnonymousHeadMarshalJSONPtrOnly",
			data: struct {
				structMarshalJSONPtr
			}{
				structMarshalJSONPtr: structMarshalJSONPtr{A: &coverMarshalJSON{}},
			},
		},
		{
			name: "AnonymousHeadMarshalJSONPtrOnlyOmitEmpty",
			data: struct {
				structMarshalJSONPtrOmitEmpty
			}{
				structMarshalJSONPtrOmitEmpty: structMarshalJSONPtrOmitEmpty{A: &coverMarshalJSON{}},
			},
		},
		{
			name: "AnonymousHeadMarshalJSONPtrOnlyString",
			data: struct {
				structMarshalJSONPtrString
			}{
				structMarshalJSONPtrString: structMarshalJSONPtrString{A: &coverMarshalJSON{}},
			},
		},
		{
			name: "AnonymousHeadPtrMarshalJSONPtrOnly",
			data: struct {
				structPtrMarshalJSONPtr
			}{
				structPtrMarshalJSONPtr: structPtrMarshalJSONPtr{A: &coverPtrMarshalJSON{}},
			},
		},
		{
			name: "AnonymousHeadPtrMarshalJSONPtrOnlyOmitEmpty",
			data: struct {
				structPtrMarshalJSONPtrOmitEmpty
			}{
				structPtrMarshalJSONPtrOmitEmpty: structPtrMarshalJSONPtrOmitEmpty{A: &coverPtrMarshalJSON{}},
			},
		},
		{
			name: "AnonymousHeadPtrMarshalJSONPtrOnlyString",
			data: struct {
				structPtrMarshalJSONPtrString
			}{
				structPtrMarshalJSONPtrString: structPtrMarshalJSONPtrString{A: &coverPtrMarshalJSON{}},
			},
		},

		// AnonymousHeadMarshalJSONPtrNilOnly
		{
			name: "AnonymousHeadMarshalJSONPtrNilOnly",
			data: struct {
				structMarshalJSONPtr
			}{
				structMarshalJSONPtr: structMarshalJSONPtr{A: nil},
			},
		},
		{
			name: "AnonymousHeadMarshalJSONPtrNilOnlyOmitEmpty",
			data: struct {
				structMarshalJSONPtrOmitEmpty
			}{
				structMarshalJSONPtrOmitEmpty: structMarshalJSONPtrOmitEmpty{A: nil},
			},
		},
		{
			name: "AnonymousHeadMarshalJSONPtrNilOnlyString",
			data: struct {
				structMarshalJSONPtrString
			}{
				structMarshalJSONPtrString: structMarshalJSONPtrString{A: nil},
			},
		},
		{
			name: "AnonymousHeadPtrMarshalJSONPtrNilOnly",
			data: struct {
				structPtrMarshalJSONPtr
			}{
				structPtrMarshalJSONPtr: structPtrMarshalJSONPtr{A: nil},
			},
		},
		{
			name: "AnonymousHeadPtrMarshalJSONPtrNilOnlyOmitEmpty",
			data: struct {
				structPtrMarshalJSONPtrOmitEmpty
			}{
				structPtrMarshalJSONPtrOmitEmpty: structPtrMarshalJSONPtrOmitEmpty{A: nil},
			},
		},
		{
			name: "AnonymousHeadPtrMarshalJSONPtrNilOnlyString",
			data: struct {
				structPtrMarshalJSONPtrString
			}{
				structPtrMarshalJSONPtrString: structPtrMarshalJSONPtrString{A: nil},
			},
		},

		// PtrAnonymousHeadMarshalJSONPtrOnly
		{
			name: "PtrAnonymousHeadMarshalJSONPtrOnly",
			data: struct {
				*structMarshalJSONPtr
			}{
				structMarshalJSONPtr: &structMarshalJSONPtr{A: &coverMarshalJSON{}},
			},
		},
		{
			name: "PtrAnonymousHeadMarshalJSONPtrOnlyOmitEmpty",
			data: struct {
				*structMarshalJSONPtrOmitEmpty
			}{
				structMarshalJSONPtrOmitEmpty: &structMarshalJSONPtrOmitEmpty{A: &coverMarshalJSON{}},
			},
		},
		{
			name: "PtrAnonymousHeadMarshalJSONPtrOnlyString",
			data: struct {
				*structMarshalJSONPtrString
			}{
				structMarshalJSONPtrString: &structMarshalJSONPtrString{A: &coverMarshalJSON{}},
			},
		},
		{
			name: "PtrAnonymousHeadPtrMarshalJSONPtrOnly",
			data: struct {
				*structPtrMarshalJSONPtr
			}{
				structPtrMarshalJSONPtr: &structPtrMarshalJSONPtr{A: &coverPtrMarshalJSON{}},
			},
		},
		{
			name: "PtrAnonymousHeadPtrMarshalJSONPtrOnlyOmitEmpty",
			data: struct {
				*structPtrMarshalJSONPtrOmitEmpty
			}{
				structPtrMarshalJSONPtrOmitEmpty: &structPtrMarshalJSONPtrOmitEmpty{A: &coverPtrMarshalJSON{}},
			},
		},
		{
			name: "PtrAnonymousHeadPtrMarshalJSONPtrOnlyString",
			data: struct {
				*structPtrMarshalJSONPtrString
			}{
				structPtrMarshalJSONPtrString: &structPtrMarshalJSONPtrString{A: &coverPtrMarshalJSON{}},
			},
		},

		// NilPtrAnonymousHeadMarshalJSONPtrOnly
		{
			name: "NilPtrAnonymousHeadMarshalJSONPtrOnly",
			data: struct {
				*structMarshalJSONPtr
			}{
				structMarshalJSONPtr: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadMarshalJSONPtrOnlyOmitEmpty",
			data: struct {
				*structMarshalJSONPtrOmitEmpty
			}{
				structMarshalJSONPtrOmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadMarshalJSONPtrOnlyString",
			data: struct {
				*structMarshalJSONPtrString
			}{
				structMarshalJSONPtrString: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadPtrMarshalJSONPtrOnly",
			data: struct {
				*structPtrMarshalJSONPtr
			}{
				structPtrMarshalJSONPtr: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadPtrMarshalJSONPtrOnlyOmitEmpty",
			data: struct {
				*structPtrMarshalJSONPtrOmitEmpty
			}{
				structPtrMarshalJSONPtrOmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadPtrMarshalJSONPtrOnlyString",
			data: struct {
				*structPtrMarshalJSONPtrString
			}{
				structPtrMarshalJSONPtrString: nil,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
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
							t.Fatalf("%s(htmlEscape:%v,indent:%v): %+v: %s", test.name, htmlEscape, indent, test.data, err)
						}
						stdresult := encodeByEncodingJSON(test.data, indent, htmlEscape)
						if buf.String() != stdresult {
							t.Errorf("%s(htmlEscape:%v,indent:%v): doesn't compatible with encoding/json. expected %q but got %q", test.name, htmlEscape, indent, stdresult, buf.String())
						}
					})
				}
			}
		})
	}
}
