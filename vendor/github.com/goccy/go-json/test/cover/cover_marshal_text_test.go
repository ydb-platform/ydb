package json_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/goccy/go-json"
)

type coverMarshalText struct {
	A int
}

func (c coverMarshalText) MarshalText() ([]byte, error) {
	return []byte(`"hello"`), nil
}

type coverPtrMarshalText struct {
	B int
}

func (c *coverPtrMarshalText) MarshalText() ([]byte, error) {
	return []byte(`"hello"`), nil
}

func TestCoverMarshalText(t *testing.T) {
	type structMarshalText struct {
		A coverMarshalText `json:"a"`
	}
	type structMarshalTextOmitEmpty struct {
		A coverMarshalText `json:"a,omitempty"`
	}
	type structMarshalTextString struct {
		A coverMarshalText `json:"a,string"`
	}
	type structPtrMarshalText struct {
		A coverPtrMarshalText `json:"a"`
	}
	type structPtrMarshalTextOmitEmpty struct {
		A coverPtrMarshalText `json:"a,omitempty"`
	}
	type structPtrMarshalTextString struct {
		A coverPtrMarshalText `json:"a,string"`
	}

	type structMarshalTextPtr struct {
		A *coverMarshalText `json:"a"`
	}
	type structMarshalTextPtrOmitEmpty struct {
		A *coverMarshalText `json:"a,omitempty"`
	}
	type structMarshalTextPtrString struct {
		A *coverMarshalText `json:"a,string"`
	}
	type structPtrMarshalTextPtr struct {
		A *coverPtrMarshalText `json:"a"`
	}
	type structPtrMarshalTextPtrOmitEmpty struct {
		A *coverPtrMarshalText `json:"a,omitempty"`
	}
	type structPtrMarshalTextPtrString struct {
		A *coverPtrMarshalText `json:"a,string"`
	}

	tests := []struct {
		name string
		data interface{}
	}{
		// HeadMarshalTextZero
		{
			name: "HeadMarshalTextZero",
			data: struct {
				A coverMarshalText `json:"a"`
			}{},
		},
		{
			name: "HeadMarshalTextZeroOmitEmpty",
			data: struct {
				A coverMarshalText `json:"a,omitempty"`
			}{},
		},
		{
			name: "HeadMarshalTextZeroString",
			data: struct {
				A coverMarshalText `json:"a,string"`
			}{},
		},
		{
			name: "HeadPtrMarshalTextZero",
			data: struct {
				A coverPtrMarshalText `json:"a"`
			}{},
		},
		{
			name: "HeadPtrMarshalTextZeroOmitEmpty",
			data: struct {
				A coverPtrMarshalText `json:"a,omitempty"`
			}{},
		},
		{
			name: "HeadPtrMarshalTextZeroString",
			data: struct {
				A coverPtrMarshalText `json:"a,string"`
			}{},
		},

		// HeadMarshalText
		{
			name: "HeadMarshalText",
			data: struct {
				A coverMarshalText `json:"a"`
			}{A: coverMarshalText{}},
		},
		{
			name: "HeadMarshalTextOmitEmpty",
			data: struct {
				A coverMarshalText `json:"a,omitempty"`
			}{A: coverMarshalText{}},
		},
		{
			name: "HeadMarshalTextString",
			data: struct {
				A coverMarshalText `json:"a,string"`
			}{A: coverMarshalText{}},
		},
		{
			name: "HeadPtrMarshalText",
			data: struct {
				A coverPtrMarshalText `json:"a"`
			}{A: coverPtrMarshalText{}},
		},
		{
			name: "HeadPtrMarshalTextOmitEmpty",
			data: struct {
				A coverPtrMarshalText `json:"a,omitempty"`
			}{A: coverPtrMarshalText{}},
		},
		{
			name: "HeadPtrMarshalTextString",
			data: struct {
				A coverPtrMarshalText `json:"a,string"`
			}{A: coverPtrMarshalText{}},
		},

		// HeadMarshalTextPtr
		{
			name: "HeadMarshalTextPtr",
			data: struct {
				A *coverMarshalText `json:"a"`
			}{A: &coverMarshalText{}},
		},
		{
			name: "HeadMarshalTextPtrOmitEmpty",
			data: struct {
				A *coverMarshalText `json:"a,omitempty"`
			}{A: &coverMarshalText{}},
		},
		{
			name: "HeadMarshalTextPtrString",
			data: struct {
				A *coverMarshalText `json:"a,string"`
			}{A: &coverMarshalText{}},
		},
		{
			name: "HeadPtrMarshalTextPtr",
			data: struct {
				A *coverPtrMarshalText `json:"a"`
			}{A: &coverPtrMarshalText{}},
		},
		{
			name: "HeadPtrMarshalTextPtrOmitEmpty",
			data: struct {
				A *coverPtrMarshalText `json:"a,omitempty"`
			}{A: &coverPtrMarshalText{}},
		},
		{
			name: "HeadPtrMarshalTextPtrString",
			data: struct {
				A *coverPtrMarshalText `json:"a,string"`
			}{A: &coverPtrMarshalText{}},
		},

		// HeadMarshalTextPtrNil
		{
			name: "HeadMarshalTextPtrNil",
			data: struct {
				A *coverMarshalText `json:"a"`
			}{A: nil},
		},
		{
			name: "HeadMarshalTextPtrNilOmitEmpty",
			data: struct {
				A *coverMarshalText `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "HeadMarshalTextPtrNilString",
			data: struct {
				A *coverMarshalText `json:"a,string"`
			}{A: nil},
		},
		{
			name: "HeadPtrMarshalTextPtrNil",
			data: struct {
				A *coverPtrMarshalText `json:"a"`
			}{A: nil},
		},
		{
			name: "HeadPtrMarshalTextPtrNilOmitEmpty",
			data: struct {
				A *coverPtrMarshalText `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "HeadPtrMarshalTextPtrNilString",
			data: struct {
				A *coverPtrMarshalText `json:"a,string"`
			}{A: nil},
		},

		// PtrHeadMarshalTextZero
		{
			name: "PtrHeadMarshalTextZero",
			data: &struct {
				A coverMarshalText `json:"a"`
			}{},
		},
		{
			name: "PtrHeadMarshalTextZeroOmitEmpty",
			data: &struct {
				A coverMarshalText `json:"a,omitempty"`
			}{},
		},
		{
			name: "PtrHeadMarshalTextZeroString",
			data: &struct {
				A coverMarshalText `json:"a,string"`
			}{},
		},
		{
			name: "PtrHeadPtrMarshalTextZero",
			data: &struct {
				A coverPtrMarshalText `json:"a"`
			}{},
		},
		{
			name: "PtrHeadPtrMarshalTextZeroOmitEmpty",
			data: &struct {
				A coverPtrMarshalText `json:"a,omitempty"`
			}{},
		},
		{
			name: "PtrHeadPtrMarshalTextZeroString",
			data: &struct {
				A coverPtrMarshalText `json:"a,string"`
			}{},
		},

		// PtrHeadMarshalText
		{
			name: "PtrHeadMarshalText",
			data: &struct {
				A coverMarshalText `json:"a"`
			}{A: coverMarshalText{}},
		},
		{
			name: "PtrHeadMarshalTextOmitEmpty",
			data: &struct {
				A coverMarshalText `json:"a,omitempty"`
			}{A: coverMarshalText{}},
		},
		{
			name: "PtrHeadMarshalTextString",
			data: &struct {
				A coverMarshalText `json:"a,string"`
			}{A: coverMarshalText{}},
		},
		{
			name: "PtrHeadPtrMarshalText",
			data: &struct {
				A coverPtrMarshalText `json:"a"`
			}{A: coverPtrMarshalText{}},
		},
		{
			name: "PtrHeadPtrMarshalTextOmitEmpty",
			data: &struct {
				A coverPtrMarshalText `json:"a,omitempty"`
			}{A: coverPtrMarshalText{}},
		},
		{
			name: "PtrHeadPtrMarshalTextString",
			data: &struct {
				A coverPtrMarshalText `json:"a,string"`
			}{A: coverPtrMarshalText{}},
		},

		// PtrHeadMarshalTextPtr
		{
			name: "PtrHeadMarshalTextPtr",
			data: &struct {
				A *coverMarshalText `json:"a"`
			}{A: &coverMarshalText{}},
		},
		{
			name: "PtrHeadMarshalTextPtrOmitEmpty",
			data: &struct {
				A *coverMarshalText `json:"a,omitempty"`
			}{A: &coverMarshalText{}},
		},
		{
			name: "PtrHeadMarshalTextPtrString",
			data: &struct {
				A *coverMarshalText `json:"a,string"`
			}{A: &coverMarshalText{}},
		},
		{
			name: "PtrHeadPtrMarshalTextPtr",
			data: &struct {
				A *coverPtrMarshalText `json:"a"`
			}{A: &coverPtrMarshalText{}},
		},
		{
			name: "PtrHeadPtrMarshalTextPtrOmitEmpty",
			data: &struct {
				A *coverPtrMarshalText `json:"a,omitempty"`
			}{A: &coverPtrMarshalText{}},
		},
		{
			name: "PtrHeadPtrMarshalTextPtrString",
			data: &struct {
				A *coverPtrMarshalText `json:"a,string"`
			}{A: &coverPtrMarshalText{}},
		},

		// PtrHeadMarshalTextPtrNil
		{
			name: "PtrHeadMarshalTextPtrNil",
			data: &struct {
				A *coverMarshalText `json:"a"`
			}{A: nil},
		},
		{
			name: "PtrHeadMarshalTextPtrNilOmitEmpty",
			data: &struct {
				A *coverMarshalText `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadMarshalTextPtrNilString",
			data: &struct {
				A *coverMarshalText `json:"a,string"`
			}{A: nil},
		},
		{
			name: "PtrHeadPtrMarshalTextPtrNil",
			data: &struct {
				A *coverPtrMarshalText `json:"a"`
			}{A: nil},
		},
		{
			name: "PtrHeadPtrMarshalTextPtrNilOmitEmpty",
			data: &struct {
				A *coverPtrMarshalText `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadPtrMarshalTextPtrNilString",
			data: &struct {
				A *coverPtrMarshalText `json:"a,string"`
			}{A: nil},
		},

		// PtrHeadMarshalTextNil
		{
			name: "PtrHeadMarshalTextNil",
			data: (*struct {
				A *coverMarshalText `json:"a"`
			})(nil),
		},
		{
			name: "PtrHeadMarshalTextNilOmitEmpty",
			data: (*struct {
				A *coverMarshalText `json:"a,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadMarshalTextNilString",
			data: (*struct {
				A *coverMarshalText `json:"a,string"`
			})(nil),
		},
		{
			name: "PtrHeadPtrMarshalTextNil",
			data: (*struct {
				A *coverPtrMarshalText `json:"a"`
			})(nil),
		},
		{
			name: "PtrHeadPtrMarshalTextNilOmitEmpty",
			data: (*struct {
				A *coverPtrMarshalText `json:"a,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadPtrMarshalTextNilString",
			data: (*struct {
				A *coverPtrMarshalText `json:"a,string"`
			})(nil),
		},

		// HeadMarshalTextZeroMultiFields
		{
			name: "HeadMarshalTextZeroMultiFields",
			data: struct {
				A coverMarshalText `json:"a"`
				B coverMarshalText `json:"b"`
				C coverMarshalText `json:"c"`
			}{},
		},
		{
			name: "HeadMarshalTextZeroMultiFieldsOmitEmpty",
			data: struct {
				A coverMarshalText `json:"a,omitempty"`
				B coverMarshalText `json:"b,omitempty"`
				C coverMarshalText `json:"c,omitempty"`
			}{},
		},
		{
			name: "HeadMarshalTextZeroMultiFields",
			data: struct {
				A coverMarshalText `json:"a,string"`
				B coverMarshalText `json:"b,string"`
				C coverMarshalText `json:"c,string"`
			}{},
		},
		{
			name: "HeadPtrMarshalTextZeroMultiFields",
			data: struct {
				A coverPtrMarshalText `json:"a"`
				B coverPtrMarshalText `json:"b"`
				C coverPtrMarshalText `json:"c"`
			}{},
		},
		{
			name: "HeadPtrMarshalTextZeroMultiFieldsOmitEmpty",
			data: struct {
				A coverPtrMarshalText `json:"a,omitempty"`
				B coverPtrMarshalText `json:"b,omitempty"`
				C coverPtrMarshalText `json:"c,omitempty"`
			}{},
		},
		{
			name: "HeadPtrMarshalTextZeroMultiFields",
			data: struct {
				A coverPtrMarshalText `json:"a,string"`
				B coverPtrMarshalText `json:"b,string"`
				C coverPtrMarshalText `json:"c,string"`
			}{},
		},

		// HeadMarshalTextMultiFields
		{
			name: "HeadMarshalTextMultiFields",
			data: struct {
				A coverMarshalText `json:"a"`
				B coverMarshalText `json:"b"`
				C coverMarshalText `json:"c"`
			}{A: coverMarshalText{}, B: coverMarshalText{}, C: coverMarshalText{}},
		},
		{
			name: "HeadMarshalTextMultiFieldsOmitEmpty",
			data: struct {
				A coverMarshalText `json:"a,omitempty"`
				B coverMarshalText `json:"b,omitempty"`
				C coverMarshalText `json:"c,omitempty"`
			}{A: coverMarshalText{}, B: coverMarshalText{}, C: coverMarshalText{}},
		},
		{
			name: "HeadMarshalTextMultiFieldsString",
			data: struct {
				A coverMarshalText `json:"a,string"`
				B coverMarshalText `json:"b,string"`
				C coverMarshalText `json:"c,string"`
			}{A: coverMarshalText{}, B: coverMarshalText{}, C: coverMarshalText{}},
		},
		{
			name: "HeadPtrMarshalTextMultiFields",
			data: struct {
				A coverPtrMarshalText `json:"a"`
				B coverPtrMarshalText `json:"b"`
				C coverPtrMarshalText `json:"c"`
			}{A: coverPtrMarshalText{}, B: coverPtrMarshalText{}, C: coverPtrMarshalText{}},
		},
		{
			name: "HeadPtrMarshalTextMultiFieldsOmitEmpty",
			data: struct {
				A coverPtrMarshalText `json:"a,omitempty"`
				B coverPtrMarshalText `json:"b,omitempty"`
				C coverPtrMarshalText `json:"c,omitempty"`
			}{A: coverPtrMarshalText{}, B: coverPtrMarshalText{}, C: coverPtrMarshalText{}},
		},
		{
			name: "HeadPtrMarshalTextMultiFieldsString",
			data: struct {
				A coverPtrMarshalText `json:"a,string"`
				B coverPtrMarshalText `json:"b,string"`
				C coverPtrMarshalText `json:"c,string"`
			}{A: coverPtrMarshalText{}, B: coverPtrMarshalText{}, C: coverPtrMarshalText{}},
		},

		// HeadMarshalTextPtrMultiFields
		{
			name: "HeadMarshalTextPtrMultiFields",
			data: struct {
				A *coverMarshalText `json:"a"`
				B *coverMarshalText `json:"b"`
				C *coverMarshalText `json:"c"`
			}{A: &coverMarshalText{}, B: &coverMarshalText{}, C: &coverMarshalText{}},
		},
		{
			name: "HeadMarshalTextPtrMultiFieldsOmitEmpty",
			data: struct {
				A *coverMarshalText `json:"a,omitempty"`
				B *coverMarshalText `json:"b,omitempty"`
				C *coverMarshalText `json:"c,omitempty"`
			}{A: &coverMarshalText{}, B: &coverMarshalText{}, C: &coverMarshalText{}},
		},
		{
			name: "HeadMarshalTextPtrMultiFieldsString",
			data: struct {
				A *coverMarshalText `json:"a,string"`
				B *coverMarshalText `json:"b,string"`
				C *coverMarshalText `json:"c,string"`
			}{A: &coverMarshalText{}, B: &coverMarshalText{}, C: &coverMarshalText{}},
		},
		{
			name: "HeadPtrMarshalTextPtrMultiFields",
			data: struct {
				A *coverPtrMarshalText `json:"a"`
				B *coverPtrMarshalText `json:"b"`
				C *coverPtrMarshalText `json:"c"`
			}{A: &coverPtrMarshalText{}, B: &coverPtrMarshalText{}, C: &coverPtrMarshalText{}},
		},
		{
			name: "HeadPtrMarshalTextPtrMultiFieldsOmitEmpty",
			data: struct {
				A *coverPtrMarshalText `json:"a,omitempty"`
				B *coverPtrMarshalText `json:"b,omitempty"`
				C *coverPtrMarshalText `json:"c,omitempty"`
			}{A: &coverPtrMarshalText{}, B: &coverPtrMarshalText{}, C: &coverPtrMarshalText{}},
		},
		{
			name: "HeadPtrMarshalTextPtrMultiFieldsString",
			data: struct {
				A *coverPtrMarshalText `json:"a,string"`
				B *coverPtrMarshalText `json:"b,string"`
				C *coverPtrMarshalText `json:"c,string"`
			}{A: &coverPtrMarshalText{}, B: &coverPtrMarshalText{}, C: &coverPtrMarshalText{}},
		},

		// HeadMarshalTextPtrNilMultiFields
		{
			name: "HeadMarshalTextPtrNilMultiFields",
			data: struct {
				A *coverMarshalText `json:"a"`
				B *coverMarshalText `json:"b"`
				C *coverMarshalText `json:"c"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadMarshalTextPtrNilMultiFieldsOmitEmpty",
			data: struct {
				A *coverMarshalText `json:"a,omitempty"`
				B *coverMarshalText `json:"b,omitempty"`
				C *coverMarshalText `json:"c,omitempty"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadMarshalTextPtrNilMultiFieldsString",
			data: struct {
				A *coverMarshalText `json:"a,string"`
				B *coverMarshalText `json:"b,string"`
				C *coverMarshalText `json:"c,string"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadPtrMarshalTextPtrNilMultiFields",
			data: struct {
				A *coverPtrMarshalText `json:"a"`
				B *coverPtrMarshalText `json:"b"`
				C *coverPtrMarshalText `json:"c"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadPtrMarshalTextPtrNilMultiFieldsOmitEmpty",
			data: struct {
				A *coverPtrMarshalText `json:"a,omitempty"`
				B *coverPtrMarshalText `json:"b,omitempty"`
				C *coverPtrMarshalText `json:"c,omitempty"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadPtrMarshalTextPtrNilMultiFieldsString",
			data: struct {
				A *coverPtrMarshalText `json:"a,string"`
				B *coverPtrMarshalText `json:"b,string"`
				C *coverPtrMarshalText `json:"c,string"`
			}{A: nil, B: nil, C: nil},
		},

		// PtrHeadMarshalTextZeroMultiFields
		{
			name: "PtrHeadMarshalTextZeroMultiFields",
			data: &struct {
				A coverMarshalText `json:"a"`
				B coverMarshalText `json:"b"`
			}{},
		},
		{
			name: "PtrHeadMarshalTextZeroMultiFieldsOmitEmpty",
			data: &struct {
				A coverMarshalText `json:"a,omitempty"`
				B coverMarshalText `json:"b,omitempty"`
			}{},
		},
		{
			name: "PtrHeadMarshalTextZeroMultiFieldsString",
			data: &struct {
				A coverMarshalText `json:"a,string"`
				B coverMarshalText `json:"b,string"`
			}{},
		},
		{
			name: "PtrHeadPtrMarshalTextZeroMultiFields",
			data: &struct {
				A coverPtrMarshalText `json:"a"`
				B coverPtrMarshalText `json:"b"`
			}{},
		},
		{
			name: "PtrHeadPtrMarshalTextZeroMultiFieldsOmitEmpty",
			data: &struct {
				A coverPtrMarshalText `json:"a,omitempty"`
				B coverPtrMarshalText `json:"b,omitempty"`
			}{},
		},
		{
			name: "PtrHeadPtrMarshalTextZeroMultiFieldsString",
			data: &struct {
				A coverPtrMarshalText `json:"a,string"`
				B coverPtrMarshalText `json:"b,string"`
			}{},
		},

		// PtrHeadMarshalTextMultiFields
		{
			name: "PtrHeadMarshalTextMultiFields",
			data: &struct {
				A coverMarshalText `json:"a"`
				B coverMarshalText `json:"b"`
			}{A: coverMarshalText{}, B: coverMarshalText{}},
		},
		{
			name: "PtrHeadMarshalTextMultiFieldsOmitEmpty",
			data: &struct {
				A coverMarshalText `json:"a,omitempty"`
				B coverMarshalText `json:"b,omitempty"`
			}{A: coverMarshalText{}, B: coverMarshalText{}},
		},
		{
			name: "PtrHeadMarshalTextMultiFieldsString",
			data: &struct {
				A coverMarshalText `json:"a,string"`
				B coverMarshalText `json:"b,string"`
			}{A: coverMarshalText{}, B: coverMarshalText{}},
		},
		{
			name: "PtrHeadPtrMarshalTextMultiFields",
			data: &struct {
				A coverPtrMarshalText `json:"a"`
				B coverPtrMarshalText `json:"b"`
			}{A: coverPtrMarshalText{}, B: coverPtrMarshalText{}},
		},
		{
			name: "PtrHeadPtrMarshalTextMultiFieldsOmitEmpty",
			data: &struct {
				A coverPtrMarshalText `json:"a,omitempty"`
				B coverPtrMarshalText `json:"b,omitempty"`
			}{A: coverPtrMarshalText{}, B: coverPtrMarshalText{}},
		},
		{
			name: "PtrHeadPtrMarshalTextMultiFieldsString",
			data: &struct {
				A coverPtrMarshalText `json:"a,string"`
				B coverPtrMarshalText `json:"b,string"`
			}{A: coverPtrMarshalText{}, B: coverPtrMarshalText{}},
		},

		// PtrHeadMarshalTextPtrMultiFields
		{
			name: "PtrHeadMarshalTextPtrMultiFields",
			data: &struct {
				A *coverMarshalText `json:"a"`
				B *coverMarshalText `json:"b"`
			}{A: &coverMarshalText{}, B: &coverMarshalText{}},
		},
		{
			name: "PtrHeadMarshalTextPtrMultiFieldsOmitEmpty",
			data: &struct {
				A *coverMarshalText `json:"a,omitempty"`
				B *coverMarshalText `json:"b,omitempty"`
			}{A: &coverMarshalText{}, B: &coverMarshalText{}},
		},
		{
			name: "PtrHeadMarshalTextPtrMultiFieldsString",
			data: &struct {
				A *coverMarshalText `json:"a,string"`
				B *coverMarshalText `json:"b,string"`
			}{A: &coverMarshalText{}, B: &coverMarshalText{}},
		},
		{
			name: "PtrHeadPtrMarshalTextPtrMultiFields",
			data: &struct {
				A *coverPtrMarshalText `json:"a"`
				B *coverPtrMarshalText `json:"b"`
			}{A: &coverPtrMarshalText{}, B: &coverPtrMarshalText{}},
		},
		{
			name: "PtrHeadPtrMarshalTextPtrMultiFieldsOmitEmpty",
			data: &struct {
				A *coverPtrMarshalText `json:"a,omitempty"`
				B *coverPtrMarshalText `json:"b,omitempty"`
			}{A: &coverPtrMarshalText{}, B: &coverPtrMarshalText{}},
		},
		{
			name: "PtrHeadPtrMarshalTextPtrMultiFieldsString",
			data: &struct {
				A *coverPtrMarshalText `json:"a,string"`
				B *coverPtrMarshalText `json:"b,string"`
			}{A: &coverPtrMarshalText{}, B: &coverPtrMarshalText{}},
		},

		// PtrHeadMarshalTextPtrNilMultiFields
		{
			name: "PtrHeadMarshalTextPtrNilMultiFields",
			data: &struct {
				A *coverMarshalText `json:"a"`
				B *coverMarshalText `json:"b"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadMarshalTextPtrNilMultiFieldsOmitEmpty",
			data: &struct {
				A *coverMarshalText `json:"a,omitempty"`
				B *coverMarshalText `json:"b,omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadMarshalTextPtrNilMultiFieldsString",
			data: &struct {
				A *coverMarshalText `json:"a,string"`
				B *coverMarshalText `json:"b,string"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadPtrMarshalTextPtrNilMultiFields",
			data: &struct {
				A *coverPtrMarshalText `json:"a"`
				B *coverPtrMarshalText `json:"b"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadPtrMarshalTextPtrNilMultiFieldsOmitEmpty",
			data: &struct {
				A *coverPtrMarshalText `json:"a,omitempty"`
				B *coverPtrMarshalText `json:"b,omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadPtrMarshalTextPtrNilMultiFieldsString",
			data: &struct {
				A *coverPtrMarshalText `json:"a,string"`
				B *coverPtrMarshalText `json:"b,string"`
			}{A: nil, B: nil},
		},

		// PtrHeadMarshalTextNilMultiFields
		{
			name: "PtrHeadMarshalTextNilMultiFields",
			data: (*struct {
				A coverMarshalText `json:"a"`
				B coverMarshalText `json:"b"`
			})(nil),
		},
		{
			name: "PtrHeadMarshalTextNilMultiFieldsOmitEmpty",
			data: (*struct {
				A coverMarshalText `json:"a,omitempty"`
				B coverMarshalText `json:"b,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadMarshalTextNilMultiFieldsString",
			data: (*struct {
				A coverMarshalText `json:"a,string"`
				B coverMarshalText `json:"b,string"`
			})(nil),
		},
		{
			name: "PtrHeadPtrMarshalTextNilMultiFields",
			data: (*struct {
				A coverPtrMarshalText `json:"a"`
				B coverPtrMarshalText `json:"b"`
			})(nil),
		},
		{
			name: "PtrHeadPtrMarshalTextNilMultiFieldsOmitEmpty",
			data: (*struct {
				A coverPtrMarshalText `json:"a,omitempty"`
				B coverPtrMarshalText `json:"b,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadPtrMarshalTextNilMultiFieldsString",
			data: (*struct {
				A coverPtrMarshalText `json:"a,string"`
				B coverPtrMarshalText `json:"b,string"`
			})(nil),
		},

		// PtrHeadMarshalTextNilMultiFields
		{
			name: "PtrHeadMarshalTextNilMultiFields",
			data: (*struct {
				A *coverMarshalText `json:"a"`
				B *coverMarshalText `json:"b"`
			})(nil),
		},
		{
			name: "PtrHeadMarshalTextNilMultiFieldsOmitEmpty",
			data: (*struct {
				A *coverMarshalText `json:"a,omitempty"`
				B *coverMarshalText `json:"b,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadMarshalTextNilMultiFieldsString",
			data: (*struct {
				A *coverMarshalText `json:"a,string"`
				B *coverMarshalText `json:"b,string"`
			})(nil),
		},
		{
			name: "PtrHeadPtrMarshalTextNilMultiFields",
			data: (*struct {
				A *coverPtrMarshalText `json:"a"`
				B *coverPtrMarshalText `json:"b"`
			})(nil),
		},
		{
			name: "PtrHeadPtrMarshalTextNilMultiFieldsOmitEmpty",
			data: (*struct {
				A *coverPtrMarshalText `json:"a,omitempty"`
				B *coverPtrMarshalText `json:"b,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadPtrMarshalTextNilMultiFieldsString",
			data: (*struct {
				A *coverPtrMarshalText `json:"a,string"`
				B *coverPtrMarshalText `json:"b,string"`
			})(nil),
		},

		// HeadMarshalTextZeroNotRoot
		{
			name: "HeadMarshalTextZeroNotRoot",
			data: struct {
				A struct {
					A coverMarshalText `json:"a"`
				}
			}{},
		},
		{
			name: "HeadMarshalTextZeroNotRootOmitEmpty",
			data: struct {
				A struct {
					A coverMarshalText `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadMarshalTextZeroNotRootString",
			data: struct {
				A struct {
					A coverMarshalText `json:"a,string"`
				}
			}{},
		},
		{
			name: "HeadPtrMarshalTextZeroNotRoot",
			data: struct {
				A struct {
					A coverPtrMarshalText `json:"a"`
				}
			}{},
		},
		{
			name: "HeadPtrMarshalTextZeroNotRootOmitEmpty",
			data: struct {
				A struct {
					A coverPtrMarshalText `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadPtrMarshalTextZeroNotRootString",
			data: struct {
				A struct {
					A coverPtrMarshalText `json:"a,string"`
				}
			}{},
		},

		// HeadMarshalTextNotRoot
		{
			name: "HeadMarshalTextNotRoot",
			data: struct {
				A struct {
					A coverMarshalText `json:"a"`
				}
			}{A: struct {
				A coverMarshalText `json:"a"`
			}{A: coverMarshalText{}}},
		},
		{
			name: "HeadMarshalTextNotRootOmitEmpty",
			data: struct {
				A struct {
					A coverMarshalText `json:"a,omitempty"`
				}
			}{A: struct {
				A coverMarshalText `json:"a,omitempty"`
			}{A: coverMarshalText{}}},
		},
		{
			name: "HeadMarshalTextNotRootString",
			data: struct {
				A struct {
					A coverMarshalText `json:"a,string"`
				}
			}{A: struct {
				A coverMarshalText `json:"a,string"`
			}{A: coverMarshalText{}}},
		},
		{
			name: "HeadMarshalTextNotRoot",
			data: struct {
				A struct {
					A coverPtrMarshalText `json:"a"`
				}
			}{A: struct {
				A coverPtrMarshalText `json:"a"`
			}{A: coverPtrMarshalText{}}},
		},
		{
			name: "HeadMarshalTextNotRootOmitEmpty",
			data: struct {
				A struct {
					A coverPtrMarshalText `json:"a,omitempty"`
				}
			}{A: struct {
				A coverPtrMarshalText `json:"a,omitempty"`
			}{A: coverPtrMarshalText{}}},
		},
		{
			name: "HeadMarshalTextNotRootString",
			data: struct {
				A struct {
					A coverPtrMarshalText `json:"a,string"`
				}
			}{A: struct {
				A coverPtrMarshalText `json:"a,string"`
			}{A: coverPtrMarshalText{}}},
		},

		// HeadMarshalTextPtrNotRoot
		{
			name: "HeadMarshalTextPtrNotRoot",
			data: struct {
				A struct {
					A *coverMarshalText `json:"a"`
				}
			}{A: struct {
				A *coverMarshalText `json:"a"`
			}{&coverMarshalText{}}},
		},
		{
			name: "HeadMarshalTextPtrNotRootOmitEmpty",
			data: struct {
				A struct {
					A *coverMarshalText `json:"a,omitempty"`
				}
			}{A: struct {
				A *coverMarshalText `json:"a,omitempty"`
			}{&coverMarshalText{}}},
		},
		{
			name: "HeadMarshalTextPtrNotRootString",
			data: struct {
				A struct {
					A *coverMarshalText `json:"a,string"`
				}
			}{A: struct {
				A *coverMarshalText `json:"a,string"`
			}{&coverMarshalText{}}},
		},
		{
			name: "HeadPtrMarshalTextPtrNotRoot",
			data: struct {
				A struct {
					A *coverPtrMarshalText `json:"a"`
				}
			}{A: struct {
				A *coverPtrMarshalText `json:"a"`
			}{&coverPtrMarshalText{}}},
		},
		{
			name: "HeadPtrMarshalTextPtrNotRootOmitEmpty",
			data: struct {
				A struct {
					A *coverPtrMarshalText `json:"a,omitempty"`
				}
			}{A: struct {
				A *coverPtrMarshalText `json:"a,omitempty"`
			}{&coverPtrMarshalText{}}},
		},
		{
			name: "HeadPtrMarshalTextPtrNotRootString",
			data: struct {
				A struct {
					A *coverPtrMarshalText `json:"a,string"`
				}
			}{A: struct {
				A *coverPtrMarshalText `json:"a,string"`
			}{&coverPtrMarshalText{}}},
		},

		// HeadMarshalTextPtrNilNotRoot
		{
			name: "HeadMarshalTextPtrNilNotRoot",
			data: struct {
				A struct {
					A *coverMarshalText `json:"a"`
				}
			}{},
		},
		{
			name: "HeadMarshalTextPtrNilNotRootOmitEmpty",
			data: struct {
				A struct {
					A *coverMarshalText `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadMarshalTextPtrNilNotRootString",
			data: struct {
				A struct {
					A *coverMarshalText `json:"a,string"`
				}
			}{},
		},
		{
			name: "HeadPtrMarshalTextPtrNilNotRoot",
			data: struct {
				A struct {
					A *coverPtrMarshalText `json:"a"`
				}
			}{},
		},
		{
			name: "HeadPtrMarshalTextPtrNilNotRootOmitEmpty",
			data: struct {
				A struct {
					A *coverPtrMarshalText `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadPtrMarshalTextPtrNilNotRootString",
			data: struct {
				A struct {
					A *coverPtrMarshalText `json:"a,string"`
				}
			}{},
		},

		// PtrHeadMarshalTextZeroNotRoot
		{
			name: "PtrHeadMarshalTextZeroNotRoot",
			data: struct {
				A *struct {
					A coverMarshalText `json:"a"`
				}
			}{A: new(struct {
				A coverMarshalText `json:"a"`
			})},
		},
		{
			name: "PtrHeadMarshalTextZeroNotRootOmitEmpty",
			data: struct {
				A *struct {
					A coverMarshalText `json:"a,omitempty"`
				}
			}{A: new(struct {
				A coverMarshalText `json:"a,omitempty"`
			})},
		},
		{
			name: "PtrHeadMarshalTextZeroNotRootString",
			data: struct {
				A *struct {
					A coverMarshalText `json:"a,string"`
				}
			}{A: new(struct {
				A coverMarshalText `json:"a,string"`
			})},
		},
		{
			name: "PtrHeadPtrMarshalTextZeroNotRoot",
			data: struct {
				A *struct {
					A coverPtrMarshalText `json:"a"`
				}
			}{A: new(struct {
				A coverPtrMarshalText `json:"a"`
			})},
		},
		{
			name: "PtrHeadPtrMarshalTextZeroNotRootOmitEmpty",
			data: struct {
				A *struct {
					A coverPtrMarshalText `json:"a,omitempty"`
				}
			}{A: new(struct {
				A coverPtrMarshalText `json:"a,omitempty"`
			})},
		},
		{
			name: "PtrHeadPtrMarshalTextZeroNotRootString",
			data: struct {
				A *struct {
					A coverPtrMarshalText `json:"a,string"`
				}
			}{A: new(struct {
				A coverPtrMarshalText `json:"a,string"`
			})},
		},

		// PtrHeadMarshalTextNotRoot
		{
			name: "PtrHeadMarshalTextNotRoot",
			data: struct {
				A *struct {
					A coverMarshalText `json:"a"`
				}
			}{A: &(struct {
				A coverMarshalText `json:"a"`
			}{A: coverMarshalText{}})},
		},
		{
			name: "PtrHeadMarshalTextNotRootOmitEmpty",
			data: struct {
				A *struct {
					A coverMarshalText `json:"a,omitempty"`
				}
			}{A: &(struct {
				A coverMarshalText `json:"a,omitempty"`
			}{A: coverMarshalText{}})},
		},
		{
			name: "PtrHeadMarshalTextNotRootString",
			data: struct {
				A *struct {
					A coverMarshalText `json:"a,string"`
				}
			}{A: &(struct {
				A coverMarshalText `json:"a,string"`
			}{A: coverMarshalText{}})},
		},
		{
			name: "PtrHeadPtrMarshalTextNotRoot",
			data: struct {
				A *struct {
					A coverPtrMarshalText `json:"a"`
				}
			}{A: &(struct {
				A coverPtrMarshalText `json:"a"`
			}{A: coverPtrMarshalText{}})},
		},
		{
			name: "PtrHeadPtrMarshalTextNotRootOmitEmpty",
			data: struct {
				A *struct {
					A coverPtrMarshalText `json:"a,omitempty"`
				}
			}{A: &(struct {
				A coverPtrMarshalText `json:"a,omitempty"`
			}{A: coverPtrMarshalText{}})},
		},
		{
			name: "PtrHeadPtrMarshalTextNotRootString",
			data: struct {
				A *struct {
					A coverPtrMarshalText `json:"a,string"`
				}
			}{A: &(struct {
				A coverPtrMarshalText `json:"a,string"`
			}{A: coverPtrMarshalText{}})},
		},

		// PtrHeadMarshalTextPtrNotRoot
		{
			name: "PtrHeadMarshalTextPtrNotRoot",
			data: struct {
				A *struct {
					A *coverMarshalText `json:"a"`
				}
			}{A: &(struct {
				A *coverMarshalText `json:"a"`
			}{A: &coverMarshalText{}})},
		},
		{
			name: "PtrHeadMarshalTextPtrNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *coverMarshalText `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *coverMarshalText `json:"a,omitempty"`
			}{A: &coverMarshalText{}})},
		},
		{
			name: "PtrHeadMarshalTextPtrNotRootString",
			data: struct {
				A *struct {
					A *coverMarshalText `json:"a,string"`
				}
			}{A: &(struct {
				A *coverMarshalText `json:"a,string"`
			}{A: &coverMarshalText{}})},
		},
		{
			name: "PtrHeadPtrMarshalTextPtrNotRoot",
			data: struct {
				A *struct {
					A *coverPtrMarshalText `json:"a"`
				}
			}{A: &(struct {
				A *coverPtrMarshalText `json:"a"`
			}{A: &coverPtrMarshalText{}})},
		},
		{
			name: "PtrHeadPtrMarshalTextPtrNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *coverPtrMarshalText `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *coverPtrMarshalText `json:"a,omitempty"`
			}{A: &coverPtrMarshalText{}})},
		},
		{
			name: "PtrHeadPtrMarshalTextPtrNotRootString",
			data: struct {
				A *struct {
					A *coverPtrMarshalText `json:"a,string"`
				}
			}{A: &(struct {
				A *coverPtrMarshalText `json:"a,string"`
			}{A: &coverPtrMarshalText{}})},
		},

		// PtrHeadMarshalTextPtrNilNotRoot
		{
			name: "PtrHeadMarshalTextPtrNilNotRoot",
			data: struct {
				A *struct {
					A *coverMarshalText `json:"a"`
				}
			}{A: &(struct {
				A *coverMarshalText `json:"a"`
			}{A: nil})},
		},
		{
			name: "PtrHeadMarshalTextPtrNilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *coverMarshalText `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *coverMarshalText `json:"a,omitempty"`
			}{A: nil})},
		},
		{
			name: "PtrHeadMarshalTextPtrNilNotRootString",
			data: struct {
				A *struct {
					A *coverMarshalText `json:"a,string"`
				}
			}{A: &(struct {
				A *coverMarshalText `json:"a,string"`
			}{A: nil})},
		},
		{
			name: "PtrHeadPtrMarshalTextPtrNilNotRoot",
			data: struct {
				A *struct {
					A *coverPtrMarshalText `json:"a"`
				}
			}{A: &(struct {
				A *coverPtrMarshalText `json:"a"`
			}{A: nil})},
		},
		{
			name: "PtrHeadPtrMarshalTextPtrNilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *coverPtrMarshalText `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *coverPtrMarshalText `json:"a,omitempty"`
			}{A: nil})},
		},
		{
			name: "PtrHeadPtrMarshalTextPtrNilNotRootString",
			data: struct {
				A *struct {
					A *coverPtrMarshalText `json:"a,string"`
				}
			}{A: &(struct {
				A *coverPtrMarshalText `json:"a,string"`
			}{A: nil})},
		},

		// PtrHeadMarshalTextNilNotRoot
		{
			name: "PtrHeadMarshalTextNilNotRoot",
			data: struct {
				A *struct {
					A *coverMarshalText `json:"a"`
				}
			}{A: nil},
		},
		{
			name: "PtrHeadMarshalTextNilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *coverMarshalText `json:"a,omitempty"`
				} `json:",omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadMarshalTextNilNotRootString",
			data: struct {
				A *struct {
					A *coverMarshalText `json:"a,string"`
				} `json:",string"`
			}{A: nil},
		},
		{
			name: "PtrHeadPtrMarshalTextNilNotRoot",
			data: struct {
				A *struct {
					A *coverPtrMarshalText `json:"a"`
				}
			}{A: nil},
		},
		{
			name: "PtrHeadPtrMarshalTextNilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *coverPtrMarshalText `json:"a,omitempty"`
				} `json:",omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadPtrMarshalTextNilNotRootString",
			data: struct {
				A *struct {
					A *coverPtrMarshalText `json:"a,string"`
				} `json:",string"`
			}{A: nil},
		},

		// HeadMarshalTextZeroMultiFieldsNotRoot
		{
			name: "HeadMarshalTextZeroMultiFieldsNotRoot",
			data: struct {
				A struct {
					A coverMarshalText `json:"a"`
				}
				B struct {
					B coverMarshalText `json:"b"`
				}
			}{},
		},
		{
			name: "HeadMarshalTextZeroMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A coverMarshalText `json:"a,omitempty"`
				}
				B struct {
					B coverMarshalText `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "HeadMarshalTextZeroMultiFieldsNotRootString",
			data: struct {
				A struct {
					A coverMarshalText `json:"a,string"`
				}
				B struct {
					B coverMarshalText `json:"b,string"`
				}
			}{},
		},
		{
			name: "HeadPtrMarshalTextZeroMultiFieldsNotRoot",
			data: struct {
				A struct {
					A coverPtrMarshalText `json:"a"`
				}
				B struct {
					B coverPtrMarshalText `json:"b"`
				}
			}{},
		},
		{
			name: "HeadPtrMarshalTextZeroMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A coverPtrMarshalText `json:"a,omitempty"`
				}
				B struct {
					B coverPtrMarshalText `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "HeadPtrMarshalTextZeroMultiFieldsNotRootString",
			data: struct {
				A struct {
					A coverPtrMarshalText `json:"a,string"`
				}
				B struct {
					B coverPtrMarshalText `json:"b,string"`
				}
			}{},
		},

		// HeadMarshalTextMultiFieldsNotRoot
		{
			name: "HeadMarshalTextMultiFieldsNotRoot",
			data: struct {
				A struct {
					A coverMarshalText `json:"a"`
				}
				B struct {
					B coverMarshalText `json:"b"`
				}
			}{A: struct {
				A coverMarshalText `json:"a"`
			}{A: coverMarshalText{}}, B: struct {
				B coverMarshalText `json:"b"`
			}{B: coverMarshalText{}}},
		},
		{
			name: "HeadMarshalTextMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A coverMarshalText `json:"a,omitempty"`
				}
				B struct {
					B coverMarshalText `json:"b,omitempty"`
				}
			}{A: struct {
				A coverMarshalText `json:"a,omitempty"`
			}{A: coverMarshalText{}}, B: struct {
				B coverMarshalText `json:"b,omitempty"`
			}{B: coverMarshalText{}}},
		},
		{
			name: "HeadMarshalTextMultiFieldsNotRootString",
			data: struct {
				A struct {
					A coverMarshalText `json:"a,string"`
				}
				B struct {
					B coverMarshalText `json:"b,string"`
				}
			}{A: struct {
				A coverMarshalText `json:"a,string"`
			}{A: coverMarshalText{}}, B: struct {
				B coverMarshalText `json:"b,string"`
			}{B: coverMarshalText{}}},
		},
		{
			name: "HeadPtrMarshalTextMultiFieldsNotRoot",
			data: struct {
				A struct {
					A coverPtrMarshalText `json:"a"`
				}
				B struct {
					B coverPtrMarshalText `json:"b"`
				}
			}{A: struct {
				A coverPtrMarshalText `json:"a"`
			}{A: coverPtrMarshalText{}}, B: struct {
				B coverPtrMarshalText `json:"b"`
			}{B: coverPtrMarshalText{}}},
		},
		{
			name: "HeadPtrMarshalTextMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A coverPtrMarshalText `json:"a,omitempty"`
				}
				B struct {
					B coverPtrMarshalText `json:"b,omitempty"`
				}
			}{A: struct {
				A coverPtrMarshalText `json:"a,omitempty"`
			}{A: coverPtrMarshalText{}}, B: struct {
				B coverPtrMarshalText `json:"b,omitempty"`
			}{B: coverPtrMarshalText{}}},
		},
		{
			name: "HeadPtrMarshalTextMultiFieldsNotRootString",
			data: struct {
				A struct {
					A coverPtrMarshalText `json:"a,string"`
				}
				B struct {
					B coverPtrMarshalText `json:"b,string"`
				}
			}{A: struct {
				A coverPtrMarshalText `json:"a,string"`
			}{A: coverPtrMarshalText{}}, B: struct {
				B coverPtrMarshalText `json:"b,string"`
			}{B: coverPtrMarshalText{}}},
		},

		// HeadMarshalTextPtrMultiFieldsNotRoot
		{
			name: "HeadMarshalTextPtrMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *coverMarshalText `json:"a"`
				}
				B struct {
					B *coverMarshalText `json:"b"`
				}
			}{A: struct {
				A *coverMarshalText `json:"a"`
			}{A: &coverMarshalText{}}, B: struct {
				B *coverMarshalText `json:"b"`
			}{B: &coverMarshalText{}}},
		},
		{
			name: "HeadMarshalTextPtrMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *coverMarshalText `json:"a,omitempty"`
				}
				B struct {
					B *coverMarshalText `json:"b,omitempty"`
				}
			}{A: struct {
				A *coverMarshalText `json:"a,omitempty"`
			}{A: &coverMarshalText{}}, B: struct {
				B *coverMarshalText `json:"b,omitempty"`
			}{B: &coverMarshalText{}}},
		},
		{
			name: "HeadMarshalTextPtrMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *coverMarshalText `json:"a,string"`
				}
				B struct {
					B *coverMarshalText `json:"b,string"`
				}
			}{A: struct {
				A *coverMarshalText `json:"a,string"`
			}{A: &coverMarshalText{}}, B: struct {
				B *coverMarshalText `json:"b,string"`
			}{B: &coverMarshalText{}}},
		},
		{
			name: "HeadPtrMarshalTextPtrMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *coverPtrMarshalText `json:"a"`
				}
				B struct {
					B *coverPtrMarshalText `json:"b"`
				}
			}{A: struct {
				A *coverPtrMarshalText `json:"a"`
			}{A: &coverPtrMarshalText{}}, B: struct {
				B *coverPtrMarshalText `json:"b"`
			}{B: &coverPtrMarshalText{}}},
		},
		{
			name: "HeadPtrMarshalTextPtrMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *coverPtrMarshalText `json:"a,omitempty"`
				}
				B struct {
					B *coverPtrMarshalText `json:"b,omitempty"`
				}
			}{A: struct {
				A *coverPtrMarshalText `json:"a,omitempty"`
			}{A: &coverPtrMarshalText{}}, B: struct {
				B *coverPtrMarshalText `json:"b,omitempty"`
			}{B: &coverPtrMarshalText{}}},
		},
		{
			name: "HeadPtrMarshalTextPtrMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *coverPtrMarshalText `json:"a,string"`
				}
				B struct {
					B *coverPtrMarshalText `json:"b,string"`
				}
			}{A: struct {
				A *coverPtrMarshalText `json:"a,string"`
			}{A: &coverPtrMarshalText{}}, B: struct {
				B *coverPtrMarshalText `json:"b,string"`
			}{B: &coverPtrMarshalText{}}},
		},

		// HeadMarshalTextPtrNilMultiFieldsNotRoot
		{
			name: "HeadMarshalTextPtrNilMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *coverMarshalText `json:"a"`
				}
				B struct {
					B *coverMarshalText `json:"b"`
				}
			}{A: struct {
				A *coverMarshalText `json:"a"`
			}{A: nil}, B: struct {
				B *coverMarshalText `json:"b"`
			}{B: nil}},
		},
		{
			name: "HeadMarshalTextPtrNilMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *coverMarshalText `json:"a,omitempty"`
				}
				B struct {
					B *coverMarshalText `json:"b,omitempty"`
				}
			}{A: struct {
				A *coverMarshalText `json:"a,omitempty"`
			}{A: nil}, B: struct {
				B *coverMarshalText `json:"b,omitempty"`
			}{B: nil}},
		},
		{
			name: "HeadMarshalTextPtrNilMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *coverMarshalText `json:"a,string"`
				}
				B struct {
					B *coverMarshalText `json:"b,string"`
				}
			}{A: struct {
				A *coverMarshalText `json:"a,string"`
			}{A: nil}, B: struct {
				B *coverMarshalText `json:"b,string"`
			}{B: nil}},
		},
		{
			name: "HeadPtrMarshalTextPtrNilMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *coverPtrMarshalText `json:"a"`
				}
				B struct {
					B *coverPtrMarshalText `json:"b"`
				}
			}{A: struct {
				A *coverPtrMarshalText `json:"a"`
			}{A: nil}, B: struct {
				B *coverPtrMarshalText `json:"b"`
			}{B: nil}},
		},
		{
			name: "HeadPtrMarshalTextPtrNilMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *coverPtrMarshalText `json:"a,omitempty"`
				}
				B struct {
					B *coverPtrMarshalText `json:"b,omitempty"`
				}
			}{A: struct {
				A *coverPtrMarshalText `json:"a,omitempty"`
			}{A: nil}, B: struct {
				B *coverPtrMarshalText `json:"b,omitempty"`
			}{B: nil}},
		},
		{
			name: "HeadPtrMarshalTextPtrNilMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *coverPtrMarshalText `json:"a,string"`
				}
				B struct {
					B *coverPtrMarshalText `json:"b,string"`
				}
			}{A: struct {
				A *coverPtrMarshalText `json:"a,string"`
			}{A: nil}, B: struct {
				B *coverPtrMarshalText `json:"b,string"`
			}{B: nil}},
		},

		// PtrHeadMarshalTextZeroMultiFieldsNotRoot
		{
			name: "PtrHeadMarshalTextZeroMultiFieldsNotRoot",
			data: &struct {
				A struct {
					A coverMarshalText `json:"a"`
				}
				B struct {
					B coverMarshalText `json:"b"`
				}
			}{},
		},
		{
			name: "PtrHeadMarshalTextZeroMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A coverMarshalText `json:"a,omitempty"`
				}
				B struct {
					B coverMarshalText `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "PtrHeadMarshalTextZeroMultiFieldsNotRootString",
			data: &struct {
				A struct {
					A coverMarshalText `json:"a,string"`
				}
				B struct {
					B coverMarshalText `json:"b,string"`
				}
			}{},
		},
		{
			name: "PtrHeadPtrMarshalTextZeroMultiFieldsNotRoot",
			data: &struct {
				A struct {
					A coverPtrMarshalText `json:"a"`
				}
				B struct {
					B coverPtrMarshalText `json:"b"`
				}
			}{},
		},
		{
			name: "PtrHeadPtrMarshalTextZeroMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A coverPtrMarshalText `json:"a,omitempty"`
				}
				B struct {
					B coverPtrMarshalText `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "PtrHeadPtrMarshalTextZeroMultiFieldsNotRootString",
			data: &struct {
				A struct {
					A coverPtrMarshalText `json:"a,string"`
				}
				B struct {
					B coverPtrMarshalText `json:"b,string"`
				}
			}{},
		},

		// PtrHeadMarshalTextMultiFieldsNotRoot
		{
			name: "PtrHeadMarshalTextMultiFieldsNotRoot",
			data: &struct {
				A struct {
					A coverMarshalText `json:"a"`
				}
				B struct {
					B coverMarshalText `json:"b"`
				}
			}{A: struct {
				A coverMarshalText `json:"a"`
			}{A: coverMarshalText{}}, B: struct {
				B coverMarshalText `json:"b"`
			}{B: coverMarshalText{}}},
		},
		{
			name: "PtrHeadMarshalTextMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A coverMarshalText `json:"a,omitempty"`
				}
				B struct {
					B coverMarshalText `json:"b,omitempty"`
				}
			}{A: struct {
				A coverMarshalText `json:"a,omitempty"`
			}{A: coverMarshalText{}}, B: struct {
				B coverMarshalText `json:"b,omitempty"`
			}{B: coverMarshalText{}}},
		},
		{
			name: "PtrHeadMarshalTextMultiFieldsNotRootString",
			data: &struct {
				A struct {
					A coverMarshalText `json:"a,string"`
				}
				B struct {
					B coverMarshalText `json:"b,string"`
				}
			}{A: struct {
				A coverMarshalText `json:"a,string"`
			}{A: coverMarshalText{}}, B: struct {
				B coverMarshalText `json:"b,string"`
			}{B: coverMarshalText{}}},
		},
		{
			name: "PtrHeadPtrMarshalTextMultiFieldsNotRoot",
			data: &struct {
				A struct {
					A coverPtrMarshalText `json:"a"`
				}
				B struct {
					B coverPtrMarshalText `json:"b"`
				}
			}{A: struct {
				A coverPtrMarshalText `json:"a"`
			}{A: coverPtrMarshalText{}}, B: struct {
				B coverPtrMarshalText `json:"b"`
			}{B: coverPtrMarshalText{}}},
		},
		{
			name: "PtrHeadPtrMarshalTextMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A coverPtrMarshalText `json:"a,omitempty"`
				}
				B struct {
					B coverPtrMarshalText `json:"b,omitempty"`
				}
			}{A: struct {
				A coverPtrMarshalText `json:"a,omitempty"`
			}{A: coverPtrMarshalText{}}, B: struct {
				B coverPtrMarshalText `json:"b,omitempty"`
			}{B: coverPtrMarshalText{}}},
		},
		{
			name: "PtrHeadPtrMarshalTextMultiFieldsNotRootString",
			data: &struct {
				A struct {
					A coverPtrMarshalText `json:"a,string"`
				}
				B struct {
					B coverPtrMarshalText `json:"b,string"`
				}
			}{A: struct {
				A coverPtrMarshalText `json:"a,string"`
			}{A: coverPtrMarshalText{}}, B: struct {
				B coverPtrMarshalText `json:"b,string"`
			}{B: coverPtrMarshalText{}}},
		},

		// PtrHeadMarshalTextPtrMultiFieldsNotRoot
		{
			name: "PtrHeadMarshalTextPtrMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *coverMarshalText `json:"a"`
				}
				B *struct {
					B *coverMarshalText `json:"b"`
				}
			}{A: &(struct {
				A *coverMarshalText `json:"a"`
			}{A: &coverMarshalText{}}), B: &(struct {
				B *coverMarshalText `json:"b"`
			}{B: &coverMarshalText{}})},
		},
		{
			name: "PtrHeadMarshalTextPtrMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *coverMarshalText `json:"a,omitempty"`
				}
				B *struct {
					B *coverMarshalText `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *coverMarshalText `json:"a,omitempty"`
			}{A: &coverMarshalText{}}), B: &(struct {
				B *coverMarshalText `json:"b,omitempty"`
			}{B: &coverMarshalText{}})},
		},
		{
			name: "PtrHeadMarshalTextPtrMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *coverMarshalText `json:"a,string"`
				}
				B *struct {
					B *coverMarshalText `json:"b,string"`
				}
			}{A: &(struct {
				A *coverMarshalText `json:"a,string"`
			}{A: &coverMarshalText{}}), B: &(struct {
				B *coverMarshalText `json:"b,string"`
			}{B: &coverMarshalText{}})},
		},
		{
			name: "PtrHeadPtrMarshalTextPtrMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *coverPtrMarshalText `json:"a"`
				}
				B *struct {
					B *coverPtrMarshalText `json:"b"`
				}
			}{A: &(struct {
				A *coverPtrMarshalText `json:"a"`
			}{A: &coverPtrMarshalText{}}), B: &(struct {
				B *coverPtrMarshalText `json:"b"`
			}{B: &coverPtrMarshalText{}})},
		},
		{
			name: "PtrHeadPtrMarshalTextPtrMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *coverPtrMarshalText `json:"a,omitempty"`
				}
				B *struct {
					B *coverPtrMarshalText `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *coverPtrMarshalText `json:"a,omitempty"`
			}{A: &coverPtrMarshalText{}}), B: &(struct {
				B *coverPtrMarshalText `json:"b,omitempty"`
			}{B: &coverPtrMarshalText{}})},
		},
		{
			name: "PtrHeadPtrMarshalTextPtrMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *coverPtrMarshalText `json:"a,string"`
				}
				B *struct {
					B *coverPtrMarshalText `json:"b,string"`
				}
			}{A: &(struct {
				A *coverPtrMarshalText `json:"a,string"`
			}{A: &coverPtrMarshalText{}}), B: &(struct {
				B *coverPtrMarshalText `json:"b,string"`
			}{B: &coverPtrMarshalText{}})},
		},

		// PtrHeadMarshalTextPtrNilMultiFieldsNotRoot
		{
			name: "PtrHeadMarshalTextPtrNilMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *coverMarshalText `json:"a"`
				}
				B *struct {
					B *coverMarshalText `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadMarshalTextPtrNilMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *coverMarshalText `json:"a,omitempty"`
				} `json:",omitempty"`
				B *struct {
					B *coverMarshalText `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadMarshalTextPtrNilMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *coverMarshalText `json:"a,string"`
				} `json:",string"`
				B *struct {
					B *coverMarshalText `json:"b,string"`
				} `json:",string"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadPtrMarshalTextPtrNilMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *coverPtrMarshalText `json:"a"`
				}
				B *struct {
					B *coverPtrMarshalText `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadPtrMarshalTextPtrNilMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *coverPtrMarshalText `json:"a,omitempty"`
				} `json:",omitempty"`
				B *struct {
					B *coverPtrMarshalText `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadPtrMarshalTextPtrNilMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *coverPtrMarshalText `json:"a,string"`
				} `json:",string"`
				B *struct {
					B *coverPtrMarshalText `json:"b,string"`
				} `json:",string"`
			}{A: nil, B: nil},
		},

		// PtrHeadMarshalTextNilMultiFieldsNotRoot
		{
			name: "PtrHeadMarshalTextNilMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *coverMarshalText `json:"a"`
				}
				B *struct {
					B *coverMarshalText `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadMarshalTextNilMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *coverMarshalText `json:"a,omitempty"`
				}
				B *struct {
					B *coverMarshalText `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadMarshalTextNilMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *coverMarshalText `json:"a,string"`
				}
				B *struct {
					B *coverMarshalText `json:"b,string"`
				}
			})(nil),
		},
		{
			name: "PtrHeadPtrMarshalTextNilMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *coverPtrMarshalText `json:"a"`
				}
				B *struct {
					B *coverPtrMarshalText `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadPtrMarshalTextNilMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *coverPtrMarshalText `json:"a,omitempty"`
				}
				B *struct {
					B *coverPtrMarshalText `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadPtrMarshalTextNilMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *coverPtrMarshalText `json:"a,string"`
				}
				B *struct {
					B *coverPtrMarshalText `json:"b,string"`
				}
			})(nil),
		},

		// PtrHeadMarshalTextDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadMarshalTextDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A coverMarshalText `json:"a"`
					B coverMarshalText `json:"b"`
				}
				B *struct {
					A coverMarshalText `json:"a"`
					B coverMarshalText `json:"b"`
				}
			}{A: &(struct {
				A coverMarshalText `json:"a"`
				B coverMarshalText `json:"b"`
			}{A: coverMarshalText{}, B: coverMarshalText{}}), B: &(struct {
				A coverMarshalText `json:"a"`
				B coverMarshalText `json:"b"`
			}{A: coverMarshalText{}, B: coverMarshalText{}})},
		},
		{
			name: "PtrHeadMarshalTextDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A coverMarshalText `json:"a,omitempty"`
					B coverMarshalText `json:"b,omitempty"`
				}
				B *struct {
					A coverMarshalText `json:"a,omitempty"`
					B coverMarshalText `json:"b,omitempty"`
				}
			}{A: &(struct {
				A coverMarshalText `json:"a,omitempty"`
				B coverMarshalText `json:"b,omitempty"`
			}{A: coverMarshalText{}, B: coverMarshalText{}}), B: &(struct {
				A coverMarshalText `json:"a,omitempty"`
				B coverMarshalText `json:"b,omitempty"`
			}{A: coverMarshalText{}, B: coverMarshalText{}})},
		},
		{
			name: "PtrHeadMarshalTextDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A coverMarshalText `json:"a,string"`
					B coverMarshalText `json:"b,string"`
				}
				B *struct {
					A coverMarshalText `json:"a,string"`
					B coverMarshalText `json:"b,string"`
				}
			}{A: &(struct {
				A coverMarshalText `json:"a,string"`
				B coverMarshalText `json:"b,string"`
			}{A: coverMarshalText{}, B: coverMarshalText{}}), B: &(struct {
				A coverMarshalText `json:"a,string"`
				B coverMarshalText `json:"b,string"`
			}{A: coverMarshalText{}, B: coverMarshalText{}})},
		},
		{
			name: "PtrHeadPtrMarshalTextDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A coverPtrMarshalText `json:"a"`
					B coverPtrMarshalText `json:"b"`
				}
				B *struct {
					A coverPtrMarshalText `json:"a"`
					B coverPtrMarshalText `json:"b"`
				}
			}{A: &(struct {
				A coverPtrMarshalText `json:"a"`
				B coverPtrMarshalText `json:"b"`
			}{A: coverPtrMarshalText{}, B: coverPtrMarshalText{}}), B: &(struct {
				A coverPtrMarshalText `json:"a"`
				B coverPtrMarshalText `json:"b"`
			}{A: coverPtrMarshalText{}, B: coverPtrMarshalText{}})},
		},
		{
			name: "PtrHeadPtrMarshalTextDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A coverPtrMarshalText `json:"a,omitempty"`
					B coverPtrMarshalText `json:"b,omitempty"`
				}
				B *struct {
					A coverPtrMarshalText `json:"a,omitempty"`
					B coverPtrMarshalText `json:"b,omitempty"`
				}
			}{A: &(struct {
				A coverPtrMarshalText `json:"a,omitempty"`
				B coverPtrMarshalText `json:"b,omitempty"`
			}{A: coverPtrMarshalText{}, B: coverPtrMarshalText{}}), B: &(struct {
				A coverPtrMarshalText `json:"a,omitempty"`
				B coverPtrMarshalText `json:"b,omitempty"`
			}{A: coverPtrMarshalText{}, B: coverPtrMarshalText{}})},
		},
		{
			name: "PtrHeadPtrMarshalTextDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A coverPtrMarshalText `json:"a,string"`
					B coverPtrMarshalText `json:"b,string"`
				}
				B *struct {
					A coverPtrMarshalText `json:"a,string"`
					B coverPtrMarshalText `json:"b,string"`
				}
			}{A: &(struct {
				A coverPtrMarshalText `json:"a,string"`
				B coverPtrMarshalText `json:"b,string"`
			}{A: coverPtrMarshalText{}, B: coverPtrMarshalText{}}), B: &(struct {
				A coverPtrMarshalText `json:"a,string"`
				B coverPtrMarshalText `json:"b,string"`
			}{A: coverPtrMarshalText{}, B: coverPtrMarshalText{}})},
		},

		// PtrHeadMarshalTextNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadMarshalTextNilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A coverMarshalText `json:"a"`
					B coverMarshalText `json:"b"`
				}
				B *struct {
					A coverMarshalText `json:"a"`
					B coverMarshalText `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadMarshalTextNilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A coverMarshalText `json:"a,omitempty"`
					B coverMarshalText `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A coverMarshalText `json:"a,omitempty"`
					B coverMarshalText `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadMarshalTextNilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A coverMarshalText `json:"a,string"`
					B coverMarshalText `json:"b,string"`
				}
				B *struct {
					A coverMarshalText `json:"a,string"`
					B coverMarshalText `json:"b,string"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadPtrMarshalTextNilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A coverPtrMarshalText `json:"a"`
					B coverPtrMarshalText `json:"b"`
				}
				B *struct {
					A coverPtrMarshalText `json:"a"`
					B coverPtrMarshalText `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadPtrMarshalTextNilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A coverPtrMarshalText `json:"a,omitempty"`
					B coverPtrMarshalText `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A coverPtrMarshalText `json:"a,omitempty"`
					B coverPtrMarshalText `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadPtrMarshalTextNilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A coverPtrMarshalText `json:"a,string"`
					B coverPtrMarshalText `json:"b,string"`
				}
				B *struct {
					A coverPtrMarshalText `json:"a,string"`
					B coverPtrMarshalText `json:"b,string"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadMarshalTextNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadMarshalTextNilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A coverMarshalText `json:"a"`
					B coverMarshalText `json:"b"`
				}
				B *struct {
					A coverMarshalText `json:"a"`
					B coverMarshalText `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadMarshalTextNilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A coverMarshalText `json:"a,omitempty"`
					B coverMarshalText `json:"b,omitempty"`
				}
				B *struct {
					A coverMarshalText `json:"a,omitempty"`
					B coverMarshalText `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadMarshalTextNilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A coverMarshalText `json:"a,string"`
					B coverMarshalText `json:"b,string"`
				}
				B *struct {
					A coverMarshalText `json:"a,string"`
					B coverMarshalText `json:"b,string"`
				}
			})(nil),
		},
		{
			name: "PtrHeadPtrMarshalTextNilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A coverPtrMarshalText `json:"a"`
					B coverPtrMarshalText `json:"b"`
				}
				B *struct {
					A coverPtrMarshalText `json:"a"`
					B coverPtrMarshalText `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadPtrMarshalTextNilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A coverPtrMarshalText `json:"a,omitempty"`
					B coverPtrMarshalText `json:"b,omitempty"`
				}
				B *struct {
					A coverPtrMarshalText `json:"a,omitempty"`
					B coverPtrMarshalText `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadPtrMarshalTextNilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A coverPtrMarshalText `json:"a,string"`
					B coverPtrMarshalText `json:"b,string"`
				}
				B *struct {
					A coverPtrMarshalText `json:"a,string"`
					B coverPtrMarshalText `json:"b,string"`
				}
			})(nil),
		},

		// PtrHeadMarshalTextPtrDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadMarshalTextPtrDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *coverMarshalText `json:"a"`
					B *coverMarshalText `json:"b"`
				}
				B *struct {
					A *coverMarshalText `json:"a"`
					B *coverMarshalText `json:"b"`
				}
			}{A: &(struct {
				A *coverMarshalText `json:"a"`
				B *coverMarshalText `json:"b"`
			}{A: &coverMarshalText{}, B: &coverMarshalText{}}), B: &(struct {
				A *coverMarshalText `json:"a"`
				B *coverMarshalText `json:"b"`
			}{A: nil, B: nil})},
		},
		{
			name: "PtrHeadMarshalTextPtrDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *coverMarshalText `json:"a,omitempty"`
					B *coverMarshalText `json:"b,omitempty"`
				}
				B *struct {
					A *coverMarshalText `json:"a,omitempty"`
					B *coverMarshalText `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *coverMarshalText `json:"a,omitempty"`
				B *coverMarshalText `json:"b,omitempty"`
			}{A: &coverMarshalText{}, B: &coverMarshalText{}}), B: &(struct {
				A *coverMarshalText `json:"a,omitempty"`
				B *coverMarshalText `json:"b,omitempty"`
			}{A: nil, B: nil})},
		},
		{
			name: "PtrHeadMarshalTextPtrDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *coverMarshalText `json:"a,string"`
					B *coverMarshalText `json:"b,string"`
				}
				B *struct {
					A *coverMarshalText `json:"a,string"`
					B *coverMarshalText `json:"b,string"`
				}
			}{A: &(struct {
				A *coverMarshalText `json:"a,string"`
				B *coverMarshalText `json:"b,string"`
			}{A: &coverMarshalText{}, B: &coverMarshalText{}}), B: &(struct {
				A *coverMarshalText `json:"a,string"`
				B *coverMarshalText `json:"b,string"`
			}{A: nil, B: nil})},
		},
		{
			name: "PtrHeadPtrMarshalTextPtrDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *coverPtrMarshalText `json:"a"`
					B *coverPtrMarshalText `json:"b"`
				}
				B *struct {
					A *coverPtrMarshalText `json:"a"`
					B *coverPtrMarshalText `json:"b"`
				}
			}{A: &(struct {
				A *coverPtrMarshalText `json:"a"`
				B *coverPtrMarshalText `json:"b"`
			}{A: &coverPtrMarshalText{}, B: &coverPtrMarshalText{}}), B: &(struct {
				A *coverPtrMarshalText `json:"a"`
				B *coverPtrMarshalText `json:"b"`
			}{A: nil, B: nil})},
		},
		{
			name: "PtrHeadPtrMarshalTextPtrDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *coverPtrMarshalText `json:"a,omitempty"`
					B *coverPtrMarshalText `json:"b,omitempty"`
				}
				B *struct {
					A *coverPtrMarshalText `json:"a,omitempty"`
					B *coverPtrMarshalText `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *coverPtrMarshalText `json:"a,omitempty"`
				B *coverPtrMarshalText `json:"b,omitempty"`
			}{A: &coverPtrMarshalText{}, B: &coverPtrMarshalText{}}), B: &(struct {
				A *coverPtrMarshalText `json:"a,omitempty"`
				B *coverPtrMarshalText `json:"b,omitempty"`
			}{A: nil, B: nil})},
		},
		{
			name: "PtrHeadPtrMarshalTextPtrDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *coverPtrMarshalText `json:"a,string"`
					B *coverPtrMarshalText `json:"b,string"`
				}
				B *struct {
					A *coverPtrMarshalText `json:"a,string"`
					B *coverPtrMarshalText `json:"b,string"`
				}
			}{A: &(struct {
				A *coverPtrMarshalText `json:"a,string"`
				B *coverPtrMarshalText `json:"b,string"`
			}{A: &coverPtrMarshalText{}, B: &coverPtrMarshalText{}}), B: &(struct {
				A *coverPtrMarshalText `json:"a,string"`
				B *coverPtrMarshalText `json:"b,string"`
			}{A: nil, B: nil})},
		},

		// PtrHeadMarshalTextPtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadMarshalTextPtrNilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *coverMarshalText `json:"a"`
					B *coverMarshalText `json:"b"`
				}
				B *struct {
					A *coverMarshalText `json:"a"`
					B *coverMarshalText `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadMarshalTextPtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *coverMarshalText `json:"a,omitempty"`
					B *coverMarshalText `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A *coverMarshalText `json:"a,omitempty"`
					B *coverMarshalText `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadMarshalTextPtrNilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *coverMarshalText `json:"a,string"`
					B *coverMarshalText `json:"b,string"`
				}
				B *struct {
					A *coverMarshalText `json:"a,string"`
					B *coverMarshalText `json:"b,string"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadPtrMarshalTextPtrNilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *coverPtrMarshalText `json:"a"`
					B *coverPtrMarshalText `json:"b"`
				}
				B *struct {
					A *coverPtrMarshalText `json:"a"`
					B *coverPtrMarshalText `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadPtrMarshalTextPtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *coverPtrMarshalText `json:"a,omitempty"`
					B *coverPtrMarshalText `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A *coverPtrMarshalText `json:"a,omitempty"`
					B *coverPtrMarshalText `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadPtrMarshalTextPtrNilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *coverPtrMarshalText `json:"a,string"`
					B *coverPtrMarshalText `json:"b,string"`
				}
				B *struct {
					A *coverPtrMarshalText `json:"a,string"`
					B *coverPtrMarshalText `json:"b,string"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadMarshalTextPtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadMarshalTextPtrNilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *coverMarshalText `json:"a"`
					B *coverMarshalText `json:"b"`
				}
				B *struct {
					A *coverMarshalText `json:"a"`
					B *coverMarshalText `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadMarshalTextPtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *coverMarshalText `json:"a,omitempty"`
					B *coverMarshalText `json:"b,omitempty"`
				}
				B *struct {
					A *coverMarshalText `json:"a,omitempty"`
					B *coverMarshalText `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadMarshalTextPtrNilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *coverMarshalText `json:"a,string"`
					B *coverMarshalText `json:"b,string"`
				}
				B *struct {
					A *coverMarshalText `json:"a,string"`
					B *coverMarshalText `json:"b,string"`
				}
			})(nil),
		},
		{
			name: "PtrHeadPtrMarshalTextPtrNilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *coverPtrMarshalText `json:"a"`
					B *coverPtrMarshalText `json:"b"`
				}
				B *struct {
					A *coverPtrMarshalText `json:"a"`
					B *coverPtrMarshalText `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadPtrMarshalTextPtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *coverPtrMarshalText `json:"a,omitempty"`
					B *coverPtrMarshalText `json:"b,omitempty"`
				}
				B *struct {
					A *coverPtrMarshalText `json:"a,omitempty"`
					B *coverPtrMarshalText `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadPtrMarshalTextPtrNilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *coverPtrMarshalText `json:"a,string"`
					B *coverPtrMarshalText `json:"b,string"`
				}
				B *struct {
					A *coverPtrMarshalText `json:"a,string"`
					B *coverPtrMarshalText `json:"b,string"`
				}
			})(nil),
		},

		// AnonymousHeadMarshalText
		{
			name: "AnonymousHeadMarshalText",
			data: struct {
				structMarshalText
				B coverMarshalText `json:"b"`
			}{
				structMarshalText: structMarshalText{A: coverMarshalText{}},
				B:                 coverMarshalText{},
			},
		},
		{
			name: "AnonymousHeadMarshalTextOmitEmpty",
			data: struct {
				structMarshalTextOmitEmpty
				B coverMarshalText `json:"b,omitempty"`
			}{
				structMarshalTextOmitEmpty: structMarshalTextOmitEmpty{A: coverMarshalText{}},
				B:                          coverMarshalText{},
			},
		},
		{
			name: "AnonymousHeadMarshalTextString",
			data: struct {
				structMarshalTextString
				B coverMarshalText `json:"b,string"`
			}{
				structMarshalTextString: structMarshalTextString{A: coverMarshalText{}},
				B:                       coverMarshalText{},
			},
		},
		{
			name: "AnonymousHeadPtrMarshalText",
			data: struct {
				structPtrMarshalText
				B coverPtrMarshalText `json:"b"`
			}{
				structPtrMarshalText: structPtrMarshalText{A: coverPtrMarshalText{}},
				B:                    coverPtrMarshalText{},
			},
		},
		{
			name: "AnonymousHeadPtrMarshalTextOmitEmpty",
			data: struct {
				structPtrMarshalTextOmitEmpty
				B coverPtrMarshalText `json:"b,omitempty"`
			}{
				structPtrMarshalTextOmitEmpty: structPtrMarshalTextOmitEmpty{A: coverPtrMarshalText{}},
				B:                             coverPtrMarshalText{},
			},
		},
		{
			name: "AnonymousHeadPtrMarshalTextString",
			data: struct {
				structPtrMarshalTextString
				B coverPtrMarshalText `json:"b,string"`
			}{
				structPtrMarshalTextString: structPtrMarshalTextString{A: coverPtrMarshalText{}},
				B:                          coverPtrMarshalText{},
			},
		},

		// PtrAnonymousHeadMarshalText
		{
			name: "PtrAnonymousHeadMarshalText",
			data: struct {
				*structMarshalText
				B coverMarshalText `json:"b"`
			}{
				structMarshalText: &structMarshalText{A: coverMarshalText{}},
				B:                 coverMarshalText{},
			},
		},
		{
			name: "PtrAnonymousHeadMarshalTextOmitEmpty",
			data: struct {
				*structMarshalTextOmitEmpty
				B coverMarshalText `json:"b,omitempty"`
			}{
				structMarshalTextOmitEmpty: &structMarshalTextOmitEmpty{A: coverMarshalText{}},
				B:                          coverMarshalText{},
			},
		},
		{
			name: "PtrAnonymousHeadMarshalTextString",
			data: struct {
				*structMarshalTextString
				B coverMarshalText `json:"b,string"`
			}{
				structMarshalTextString: &structMarshalTextString{A: coverMarshalText{}},
				B:                       coverMarshalText{},
			},
		},
		{
			name: "PtrAnonymousHeadPtrMarshalText",
			data: struct {
				*structPtrMarshalText
				B coverPtrMarshalText `json:"b"`
			}{
				structPtrMarshalText: &structPtrMarshalText{A: coverPtrMarshalText{}},
				B:                    coverPtrMarshalText{},
			},
		},
		{
			name: "PtrAnonymousHeadPtrMarshalTextOmitEmpty",
			data: struct {
				*structPtrMarshalTextOmitEmpty
				B coverPtrMarshalText `json:"b,omitempty"`
			}{
				structPtrMarshalTextOmitEmpty: &structPtrMarshalTextOmitEmpty{A: coverPtrMarshalText{}},
				B:                             coverPtrMarshalText{},
			},
		},
		{
			name: "PtrAnonymousHeadPtrMarshalTextString",
			data: struct {
				*structPtrMarshalTextString
				B coverPtrMarshalText `json:"b,string"`
			}{
				structPtrMarshalTextString: &structPtrMarshalTextString{A: coverPtrMarshalText{}},
				B:                          coverPtrMarshalText{},
			},
		},

		// PtrAnonymousHeadMarshalTextNil
		{
			name: "PtrAnonymousHeadMarshalTextNil",
			data: struct {
				*structMarshalText
				B coverMarshalText `json:"b"`
			}{
				structMarshalText: &structMarshalText{A: coverMarshalText{}},
				B:                 coverMarshalText{},
			},
		},
		{
			name: "PtrAnonymousHeadMarshalTextNilOmitEmpty",
			data: struct {
				*structMarshalTextOmitEmpty
				B coverMarshalText `json:"b,omitempty"`
			}{
				structMarshalTextOmitEmpty: &structMarshalTextOmitEmpty{A: coverMarshalText{}},
				B:                          coverMarshalText{},
			},
		},
		{
			name: "PtrAnonymousHeadMarshalTextNilString",
			data: struct {
				*structMarshalTextString
				B coverMarshalText `json:"b,string"`
			}{
				structMarshalTextString: &structMarshalTextString{A: coverMarshalText{}},
				B:                       coverMarshalText{},
			},
		},
		{
			name: "PtrAnonymousHeadPtrMarshalTextNil",
			data: struct {
				*structPtrMarshalText
				B coverPtrMarshalText `json:"b"`
			}{
				structPtrMarshalText: &structPtrMarshalText{A: coverPtrMarshalText{}},
				B:                    coverPtrMarshalText{},
			},
		},
		{
			name: "PtrAnonymousHeadPtrMarshalTextNilOmitEmpty",
			data: struct {
				*structPtrMarshalTextOmitEmpty
				B coverPtrMarshalText `json:"b,omitempty"`
			}{
				structPtrMarshalTextOmitEmpty: &structPtrMarshalTextOmitEmpty{A: coverPtrMarshalText{}},
				B:                             coverPtrMarshalText{},
			},
		},
		{
			name: "PtrAnonymousHeadPtrMarshalTextNilString",
			data: struct {
				*structPtrMarshalTextString
				B coverPtrMarshalText `json:"b,string"`
			}{
				structPtrMarshalTextString: &structPtrMarshalTextString{A: coverPtrMarshalText{}},
				B:                          coverPtrMarshalText{},
			},
		},

		// NilPtrAnonymousHeadMarshalText
		{
			name: "NilPtrAnonymousHeadMarshalText",
			data: struct {
				*structMarshalText
				B coverMarshalText `json:"b"`
			}{
				structMarshalText: nil,
				B:                 coverMarshalText{},
			},
		},
		{
			name: "NilPtrAnonymousHeadMarshalTextOmitEmpty",
			data: struct {
				*structMarshalTextOmitEmpty
				B coverMarshalText `json:"b,omitempty"`
			}{
				structMarshalTextOmitEmpty: nil,
				B:                          coverMarshalText{},
			},
		},
		{
			name: "NilPtrAnonymousHeadMarshalTextString",
			data: struct {
				*structMarshalTextString
				B coverMarshalText `json:"b,string"`
			}{
				structMarshalTextString: nil,
				B:                       coverMarshalText{},
			},
		},
		{
			name: "NilPtrAnonymousHeadPtrMarshalText",
			data: struct {
				*structPtrMarshalText
				B coverPtrMarshalText `json:"b"`
			}{
				structPtrMarshalText: nil,
				B:                    coverPtrMarshalText{},
			},
		},
		{
			name: "NilPtrAnonymousHeadPtrMarshalTextOmitEmpty",
			data: struct {
				*structPtrMarshalTextOmitEmpty
				B coverPtrMarshalText `json:"b,omitempty"`
			}{
				structPtrMarshalTextOmitEmpty: nil,
				B:                             coverPtrMarshalText{},
			},
		},
		{
			name: "NilPtrAnonymousHeadPtrMarshalTextString",
			data: struct {
				*structPtrMarshalTextString
				B coverPtrMarshalText `json:"b,string"`
			}{
				structPtrMarshalTextString: nil,
				B:                          coverPtrMarshalText{},
			},
		},

		// AnonymousHeadMarshalTextPtr
		{
			name: "AnonymousHeadMarshalTextPtr",
			data: struct {
				structMarshalTextPtr
				B *coverMarshalText `json:"b"`
			}{
				structMarshalTextPtr: structMarshalTextPtr{A: &coverMarshalText{}},
				B:                    nil,
			},
		},
		{
			name: "AnonymousHeadMarshalTextPtrOmitEmpty",
			data: struct {
				structMarshalTextPtrOmitEmpty
				B *coverMarshalText `json:"b,omitempty"`
			}{
				structMarshalTextPtrOmitEmpty: structMarshalTextPtrOmitEmpty{A: &coverMarshalText{}},
				B:                             nil,
			},
		},
		{
			name: "AnonymousHeadMarshalTextPtrString",
			data: struct {
				structMarshalTextPtrString
				B *coverMarshalText `json:"b,string"`
			}{
				structMarshalTextPtrString: structMarshalTextPtrString{A: &coverMarshalText{}},
				B:                          nil,
			},
		},
		{
			name: "AnonymousHeadPtrMarshalTextPtr",
			data: struct {
				structPtrMarshalTextPtr
				B *coverPtrMarshalText `json:"b"`
			}{
				structPtrMarshalTextPtr: structPtrMarshalTextPtr{A: &coverPtrMarshalText{}},
				B:                       nil,
			},
		},
		{
			name: "AnonymousHeadPtrMarshalTextPtrOmitEmpty",
			data: struct {
				structPtrMarshalTextPtrOmitEmpty
				B *coverPtrMarshalText `json:"b,omitempty"`
			}{
				structPtrMarshalTextPtrOmitEmpty: structPtrMarshalTextPtrOmitEmpty{A: &coverPtrMarshalText{}},
				B:                                nil,
			},
		},
		{
			name: "AnonymousHeadPtrMarshalTextPtrString",
			data: struct {
				structPtrMarshalTextPtrString
				B *coverPtrMarshalText `json:"b,string"`
			}{
				structPtrMarshalTextPtrString: structPtrMarshalTextPtrString{A: &coverPtrMarshalText{}},
				B:                             nil,
			},
		},

		// AnonymousHeadMarshalTextPtrNil
		{
			name: "AnonymousHeadMarshalTextPtrNil",
			data: struct {
				structMarshalTextPtr
				B *coverMarshalText `json:"b"`
			}{
				structMarshalTextPtr: structMarshalTextPtr{A: nil},
				B:                    &coverMarshalText{},
			},
		},
		{
			name: "AnonymousHeadMarshalTextPtrNilOmitEmpty",
			data: struct {
				structMarshalTextPtrOmitEmpty
				B *coverMarshalText `json:"b,omitempty"`
			}{
				structMarshalTextPtrOmitEmpty: structMarshalTextPtrOmitEmpty{A: nil},
				B:                             &coverMarshalText{},
			},
		},
		{
			name: "AnonymousHeadMarshalTextPtrNilString",
			data: struct {
				structMarshalTextPtrString
				B *coverMarshalText `json:"b,string"`
			}{
				structMarshalTextPtrString: structMarshalTextPtrString{A: nil},
				B:                          &coverMarshalText{},
			},
		},
		{
			name: "AnonymousHeadPtrMarshalTextPtrNil",
			data: struct {
				structPtrMarshalTextPtr
				B *coverPtrMarshalText `json:"b"`
			}{
				structPtrMarshalTextPtr: structPtrMarshalTextPtr{A: nil},
				B:                       &coverPtrMarshalText{},
			},
		},
		{
			name: "AnonymousHeadPtrMarshalTextPtrNilOmitEmpty",
			data: struct {
				structPtrMarshalTextPtrOmitEmpty
				B *coverPtrMarshalText `json:"b,omitempty"`
			}{
				structPtrMarshalTextPtrOmitEmpty: structPtrMarshalTextPtrOmitEmpty{A: nil},
				B:                                &coverPtrMarshalText{},
			},
		},
		{
			name: "AnonymousHeadPtrMarshalTextPtrNilString",
			data: struct {
				structPtrMarshalTextPtrString
				B *coverPtrMarshalText `json:"b,string"`
			}{
				structPtrMarshalTextPtrString: structPtrMarshalTextPtrString{A: nil},
				B:                             &coverPtrMarshalText{},
			},
		},

		// PtrAnonymousHeadMarshalTextPtr
		{
			name: "PtrAnonymousHeadMarshalTextPtr",
			data: struct {
				*structMarshalTextPtr
				B *coverMarshalText `json:"b"`
			}{
				structMarshalTextPtr: &structMarshalTextPtr{A: &coverMarshalText{}},
				B:                    nil,
			},
		},
		{
			name: "PtrAnonymousHeadMarshalTextPtrOmitEmpty",
			data: struct {
				*structMarshalTextPtrOmitEmpty
				B *coverMarshalText `json:"b,omitempty"`
			}{
				structMarshalTextPtrOmitEmpty: &structMarshalTextPtrOmitEmpty{A: &coverMarshalText{}},
				B:                             nil,
			},
		},
		{
			name: "PtrAnonymousHeadMarshalTextPtrString",
			data: struct {
				*structMarshalTextPtrString
				B *coverMarshalText `json:"b,string"`
			}{
				structMarshalTextPtrString: &structMarshalTextPtrString{A: &coverMarshalText{}},
				B:                          nil,
			},
		},
		{
			name: "PtrAnonymousHeadPtrMarshalTextPtr",
			data: struct {
				*structPtrMarshalTextPtr
				B *coverPtrMarshalText `json:"b"`
			}{
				structPtrMarshalTextPtr: &structPtrMarshalTextPtr{A: &coverPtrMarshalText{}},
				B:                       nil,
			},
		},
		{
			name: "PtrAnonymousHeadPtrMarshalTextPtrOmitEmpty",
			data: struct {
				*structPtrMarshalTextPtrOmitEmpty
				B *coverPtrMarshalText `json:"b,omitempty"`
			}{
				structPtrMarshalTextPtrOmitEmpty: &structPtrMarshalTextPtrOmitEmpty{A: &coverPtrMarshalText{}},
				B:                                nil,
			},
		},
		{
			name: "PtrAnonymousHeadPtrMarshalTextPtrString",
			data: struct {
				*structPtrMarshalTextPtrString
				B *coverPtrMarshalText `json:"b,string"`
			}{
				structPtrMarshalTextPtrString: &structPtrMarshalTextPtrString{A: &coverPtrMarshalText{}},
				B:                             nil,
			},
		},

		// NilPtrAnonymousHeadMarshalTextPtr
		{
			name: "NilPtrAnonymousHeadMarshalTextPtr",
			data: struct {
				*structMarshalTextPtr
				B *coverMarshalText `json:"b"`
			}{
				structMarshalTextPtr: nil,
				B:                    &coverMarshalText{},
			},
		},
		{
			name: "NilPtrAnonymousHeadMarshalTextPtrOmitEmpty",
			data: struct {
				*structMarshalTextPtrOmitEmpty
				B *coverMarshalText `json:"b,omitempty"`
			}{
				structMarshalTextPtrOmitEmpty: nil,
				B:                             &coverMarshalText{},
			},
		},
		{
			name: "NilPtrAnonymousHeadMarshalTextPtrString",
			data: struct {
				*structMarshalTextPtrString
				B *coverMarshalText `json:"b,string"`
			}{
				structMarshalTextPtrString: nil,
				B:                          &coverMarshalText{},
			},
		},
		{
			name: "NilPtrAnonymousHeadPtrMarshalTextPtr",
			data: struct {
				*structPtrMarshalTextPtr
				B *coverPtrMarshalText `json:"b"`
			}{
				structPtrMarshalTextPtr: nil,
				B:                       &coverPtrMarshalText{},
			},
		},
		{
			name: "NilPtrAnonymousHeadPtrMarshalTextPtrOmitEmpty",
			data: struct {
				*structPtrMarshalTextPtrOmitEmpty
				B *coverPtrMarshalText `json:"b,omitempty"`
			}{
				structPtrMarshalTextPtrOmitEmpty: nil,
				B:                                &coverPtrMarshalText{},
			},
		},
		{
			name: "NilPtrAnonymousHeadPtrMarshalTextPtrString",
			data: struct {
				*structPtrMarshalTextPtrString
				B *coverPtrMarshalText `json:"b,string"`
			}{
				structPtrMarshalTextPtrString: nil,
				B:                             &coverPtrMarshalText{},
			},
		},

		// AnonymousHeadMarshalTextOnly
		{
			name: "AnonymousHeadMarshalTextOnly",
			data: struct {
				structMarshalText
			}{
				structMarshalText: structMarshalText{A: coverMarshalText{}},
			},
		},
		{
			name: "AnonymousHeadMarshalTextOnlyOmitEmpty",
			data: struct {
				structMarshalTextOmitEmpty
			}{
				structMarshalTextOmitEmpty: structMarshalTextOmitEmpty{A: coverMarshalText{}},
			},
		},
		{
			name: "AnonymousHeadMarshalTextOnlyString",
			data: struct {
				structMarshalTextString
			}{
				structMarshalTextString: structMarshalTextString{A: coverMarshalText{}},
			},
		},
		{
			name: "AnonymousHeadPtrMarshalTextOnly",
			data: struct {
				structPtrMarshalText
			}{
				structPtrMarshalText: structPtrMarshalText{A: coverPtrMarshalText{}},
			},
		},
		{
			name: "AnonymousHeadPtrMarshalTextOnlyOmitEmpty",
			data: struct {
				structPtrMarshalTextOmitEmpty
			}{
				structPtrMarshalTextOmitEmpty: structPtrMarshalTextOmitEmpty{A: coverPtrMarshalText{}},
			},
		},
		{
			name: "AnonymousHeadPtrMarshalTextOnlyString",
			data: struct {
				structPtrMarshalTextString
			}{
				structPtrMarshalTextString: structPtrMarshalTextString{A: coverPtrMarshalText{}},
			},
		},

		// PtrAnonymousHeadMarshalTextOnly
		{
			name: "PtrAnonymousHeadMarshalTextOnly",
			data: struct {
				*structMarshalText
			}{
				structMarshalText: &structMarshalText{A: coverMarshalText{}},
			},
		},
		{
			name: "PtrAnonymousHeadMarshalTextOnlyOmitEmpty",
			data: struct {
				*structMarshalTextOmitEmpty
			}{
				structMarshalTextOmitEmpty: &structMarshalTextOmitEmpty{A: coverMarshalText{}},
			},
		},
		{
			name: "PtrAnonymousHeadMarshalTextOnlyString",
			data: struct {
				*structMarshalTextString
			}{
				structMarshalTextString: &structMarshalTextString{A: coverMarshalText{}},
			},
		},
		{
			name: "PtrAnonymousHeadPtrMarshalTextOnly",
			data: struct {
				*structPtrMarshalText
			}{
				structPtrMarshalText: &structPtrMarshalText{A: coverPtrMarshalText{}},
			},
		},
		{
			name: "PtrAnonymousHeadPtrMarshalTextOnlyOmitEmpty",
			data: struct {
				*structPtrMarshalTextOmitEmpty
			}{
				structPtrMarshalTextOmitEmpty: &structPtrMarshalTextOmitEmpty{A: coverPtrMarshalText{}},
			},
		},
		{
			name: "PtrAnonymousHeadPtrMarshalTextOnlyString",
			data: struct {
				*structPtrMarshalTextString
			}{
				structPtrMarshalTextString: &structPtrMarshalTextString{A: coverPtrMarshalText{}},
			},
		},

		// NilPtrAnonymousHeadMarshalTextOnly
		{
			name: "NilPtrAnonymousHeadMarshalTextOnly",
			data: struct {
				*structMarshalText
			}{
				structMarshalText: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadMarshalTextOnlyOmitEmpty",
			data: struct {
				*structMarshalTextOmitEmpty
			}{
				structMarshalTextOmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadMarshalTextOnlyString",
			data: struct {
				*structMarshalTextString
			}{
				structMarshalTextString: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadPtrMarshalTextOnly",
			data: struct {
				*structPtrMarshalText
			}{
				structPtrMarshalText: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadPtrMarshalTextOnlyOmitEmpty",
			data: struct {
				*structPtrMarshalTextOmitEmpty
			}{
				structPtrMarshalTextOmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadPtrMarshalTextOnlyString",
			data: struct {
				*structPtrMarshalTextString
			}{
				structPtrMarshalTextString: nil,
			},
		},

		// AnonymousHeadMarshalTextPtrOnly
		{
			name: "AnonymousHeadMarshalTextPtrOnly",
			data: struct {
				structMarshalTextPtr
			}{
				structMarshalTextPtr: structMarshalTextPtr{A: &coverMarshalText{}},
			},
		},
		{
			name: "AnonymousHeadMarshalTextPtrOnlyOmitEmpty",
			data: struct {
				structMarshalTextPtrOmitEmpty
			}{
				structMarshalTextPtrOmitEmpty: structMarshalTextPtrOmitEmpty{A: &coverMarshalText{}},
			},
		},
		{
			name: "AnonymousHeadMarshalTextPtrOnlyString",
			data: struct {
				structMarshalTextPtrString
			}{
				structMarshalTextPtrString: structMarshalTextPtrString{A: &coverMarshalText{}},
			},
		},
		{
			name: "AnonymousHeadPtrMarshalTextPtrOnly",
			data: struct {
				structPtrMarshalTextPtr
			}{
				structPtrMarshalTextPtr: structPtrMarshalTextPtr{A: &coverPtrMarshalText{}},
			},
		},
		{
			name: "AnonymousHeadPtrMarshalTextPtrOnlyOmitEmpty",
			data: struct {
				structPtrMarshalTextPtrOmitEmpty
			}{
				structPtrMarshalTextPtrOmitEmpty: structPtrMarshalTextPtrOmitEmpty{A: &coverPtrMarshalText{}},
			},
		},
		{
			name: "AnonymousHeadPtrMarshalTextPtrOnlyString",
			data: struct {
				structPtrMarshalTextPtrString
			}{
				structPtrMarshalTextPtrString: structPtrMarshalTextPtrString{A: &coverPtrMarshalText{}},
			},
		},

		// AnonymousHeadMarshalTextPtrNilOnly
		{
			name: "AnonymousHeadMarshalTextPtrNilOnly",
			data: struct {
				structMarshalTextPtr
			}{
				structMarshalTextPtr: structMarshalTextPtr{A: nil},
			},
		},
		{
			name: "AnonymousHeadMarshalTextPtrNilOnlyOmitEmpty",
			data: struct {
				structMarshalTextPtrOmitEmpty
			}{
				structMarshalTextPtrOmitEmpty: structMarshalTextPtrOmitEmpty{A: nil},
			},
		},
		{
			name: "AnonymousHeadMarshalTextPtrNilOnlyString",
			data: struct {
				structMarshalTextPtrString
			}{
				structMarshalTextPtrString: structMarshalTextPtrString{A: nil},
			},
		},
		{
			name: "AnonymousHeadPtrMarshalTextPtrNilOnly",
			data: struct {
				structPtrMarshalTextPtr
			}{
				structPtrMarshalTextPtr: structPtrMarshalTextPtr{A: nil},
			},
		},
		{
			name: "AnonymousHeadPtrMarshalTextPtrNilOnlyOmitEmpty",
			data: struct {
				structPtrMarshalTextPtrOmitEmpty
			}{
				structPtrMarshalTextPtrOmitEmpty: structPtrMarshalTextPtrOmitEmpty{A: nil},
			},
		},
		{
			name: "AnonymousHeadPtrMarshalTextPtrNilOnlyString",
			data: struct {
				structPtrMarshalTextPtrString
			}{
				structPtrMarshalTextPtrString: structPtrMarshalTextPtrString{A: nil},
			},
		},

		// PtrAnonymousHeadMarshalTextPtrOnly
		{
			name: "PtrAnonymousHeadMarshalTextPtrOnly",
			data: struct {
				*structMarshalTextPtr
			}{
				structMarshalTextPtr: &structMarshalTextPtr{A: &coverMarshalText{}},
			},
		},
		{
			name: "PtrAnonymousHeadMarshalTextPtrOnlyOmitEmpty",
			data: struct {
				*structMarshalTextPtrOmitEmpty
			}{
				structMarshalTextPtrOmitEmpty: &structMarshalTextPtrOmitEmpty{A: &coverMarshalText{}},
			},
		},
		{
			name: "PtrAnonymousHeadMarshalTextPtrOnlyString",
			data: struct {
				*structMarshalTextPtrString
			}{
				structMarshalTextPtrString: &structMarshalTextPtrString{A: &coverMarshalText{}},
			},
		},
		{
			name: "PtrAnonymousHeadPtrMarshalTextPtrOnly",
			data: struct {
				*structPtrMarshalTextPtr
			}{
				structPtrMarshalTextPtr: &structPtrMarshalTextPtr{A: &coverPtrMarshalText{}},
			},
		},
		{
			name: "PtrAnonymousHeadPtrMarshalTextPtrOnlyOmitEmpty",
			data: struct {
				*structPtrMarshalTextPtrOmitEmpty
			}{
				structPtrMarshalTextPtrOmitEmpty: &structPtrMarshalTextPtrOmitEmpty{A: &coverPtrMarshalText{}},
			},
		},
		{
			name: "PtrAnonymousHeadPtrMarshalTextPtrOnlyString",
			data: struct {
				*structPtrMarshalTextPtrString
			}{
				structPtrMarshalTextPtrString: &structPtrMarshalTextPtrString{A: &coverPtrMarshalText{}},
			},
		},

		// NilPtrAnonymousHeadMarshalTextPtrOnly
		{
			name: "NilPtrAnonymousHeadMarshalTextPtrOnly",
			data: struct {
				*structMarshalTextPtr
			}{
				structMarshalTextPtr: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadMarshalTextPtrOnlyOmitEmpty",
			data: struct {
				*structMarshalTextPtrOmitEmpty
			}{
				structMarshalTextPtrOmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadMarshalTextPtrOnlyString",
			data: struct {
				*structMarshalTextPtrString
			}{
				structMarshalTextPtrString: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadPtrMarshalTextPtrOnly",
			data: struct {
				*structPtrMarshalTextPtr
			}{
				structPtrMarshalTextPtr: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadPtrMarshalTextPtrOnlyOmitEmpty",
			data: struct {
				*structPtrMarshalTextPtrOmitEmpty
			}{
				structPtrMarshalTextPtrOmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadPtrMarshalTextPtrOnlyString",
			data: struct {
				*structPtrMarshalTextPtrString
			}{
				structPtrMarshalTextPtrString: nil,
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
						t.Fatalf("%s(htmlEscape:%v,indent:%v): %+v: %s", test.name, htmlEscape, indent, test.data, err)
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
