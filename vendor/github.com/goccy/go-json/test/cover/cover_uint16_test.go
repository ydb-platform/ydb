package json_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/goccy/go-json"
)

func TestCoverUint16(t *testing.T) {
	type structUint16 struct {
		A uint16 `json:"a"`
	}
	type structUint16OmitEmpty struct {
		A uint16 `json:"a,omitempty"`
	}
	type structUint16String struct {
		A uint16 `json:"a,string"`
	}

	type structUint16Ptr struct {
		A *uint16 `json:"a"`
	}
	type structUint16PtrOmitEmpty struct {
		A *uint16 `json:"a,omitempty"`
	}
	type structUint16PtrString struct {
		A *uint16 `json:"a,string"`
	}

	tests := []struct {
		name string
		data interface{}
	}{
		{
			name: "Uint16",
			data: uint16(10),
		},
		{
			name: "Uint16Ptr",
			data: uint16ptr(10),
		},
		{
			name: "Uint16Ptr3",
			data: uint16ptr3(10),
		},
		{
			name: "Uint16PtrNil",
			data: (*uint16)(nil),
		},
		{
			name: "Uint16Ptr3Nil",
			data: (***uint16)(nil),
		},

		// HeadUint16Zero
		{
			name: "HeadUint16Zero",
			data: struct {
				A uint16 `json:"a"`
			}{},
		},
		{
			name: "HeadUint16ZeroOmitEmpty",
			data: struct {
				A uint16 `json:"a,omitempty"`
			}{},
		},
		{
			name: "HeadUint16ZeroString",
			data: struct {
				A uint16 `json:"a,string"`
			}{},
		},

		// HeadUint16
		{
			name: "HeadUint16",
			data: struct {
				A uint16 `json:"a"`
			}{A: 1},
		},
		{
			name: "HeadUint16OmitEmpty",
			data: struct {
				A uint16 `json:"a,omitempty"`
			}{A: 1},
		},
		{
			name: "HeadUint16String",
			data: struct {
				A uint16 `json:"a,string"`
			}{A: 1},
		},

		// HeadUint16Ptr
		{
			name: "HeadUint16Ptr",
			data: struct {
				A *uint16 `json:"a"`
			}{A: uint16ptr(1)},
		},
		{
			name: "HeadUint16PtrOmitEmpty",
			data: struct {
				A *uint16 `json:"a,omitempty"`
			}{A: uint16ptr(1)},
		},
		{
			name: "HeadUint16PtrString",
			data: struct {
				A *uint16 `json:"a,string"`
			}{A: uint16ptr(1)},
		},

		// HeadUint16PtrNil
		{
			name: "HeadUint16PtrNil",
			data: struct {
				A *uint16 `json:"a"`
			}{A: nil},
		},
		{
			name: "HeadUint16PtrNilOmitEmpty",
			data: struct {
				A *uint16 `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "HeadUint16PtrNilString",
			data: struct {
				A *uint16 `json:"a,string"`
			}{A: nil},
		},

		// PtrHeadUint16Zero
		{
			name: "PtrHeadUint16Zero",
			data: &struct {
				A uint16 `json:"a"`
			}{},
		},
		{
			name: "PtrHeadUint16ZeroOmitEmpty",
			data: &struct {
				A uint16 `json:"a,omitempty"`
			}{},
		},
		{
			name: "PtrHeadUint16ZeroString",
			data: &struct {
				A uint16 `json:"a,string"`
			}{},
		},

		// PtrHeadUint16
		{
			name: "PtrHeadUint16",
			data: &struct {
				A uint16 `json:"a"`
			}{A: 1},
		},
		{
			name: "PtrHeadUint16OmitEmpty",
			data: &struct {
				A uint16 `json:"a,omitempty"`
			}{A: 1},
		},
		{
			name: "PtrHeadUint16String",
			data: &struct {
				A uint16 `json:"a,string"`
			}{A: 1},
		},

		// PtrHeadUint16Ptr
		{
			name: "PtrHeadUint16Ptr",
			data: &struct {
				A *uint16 `json:"a"`
			}{A: uint16ptr(1)},
		},
		{
			name: "PtrHeadUint16PtrOmitEmpty",
			data: &struct {
				A *uint16 `json:"a,omitempty"`
			}{A: uint16ptr(1)},
		},
		{
			name: "PtrHeadUint16PtrString",
			data: &struct {
				A *uint16 `json:"a,string"`
			}{A: uint16ptr(1)},
		},

		// PtrHeadUint16PtrNil
		{
			name: "PtrHeadUint16PtrNil",
			data: &struct {
				A *uint16 `json:"a"`
			}{A: nil},
		},
		{
			name: "PtrHeadUint16PtrNilOmitEmpty",
			data: &struct {
				A *uint16 `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadUint16PtrNilString",
			data: &struct {
				A *uint16 `json:"a,string"`
			}{A: nil},
		},

		// PtrHeadUint16Nil
		{
			name: "PtrHeadUint16Nil",
			data: (*struct {
				A *uint16 `json:"a"`
			})(nil),
		},
		{
			name: "PtrHeadUint16NilOmitEmpty",
			data: (*struct {
				A *uint16 `json:"a,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadUint16NilString",
			data: (*struct {
				A *uint16 `json:"a,string"`
			})(nil),
		},

		// HeadUint16ZeroMultiFields
		{
			name: "HeadUint16ZeroMultiFields",
			data: struct {
				A uint16 `json:"a"`
				B uint16 `json:"b"`
				C uint16 `json:"c"`
			}{},
		},
		{
			name: "HeadUint16ZeroMultiFieldsOmitEmpty",
			data: struct {
				A uint16 `json:"a,omitempty"`
				B uint16 `json:"b,omitempty"`
				C uint16 `json:"c,omitempty"`
			}{},
		},
		{
			name: "HeadUint16ZeroMultiFields",
			data: struct {
				A uint16 `json:"a,string"`
				B uint16 `json:"b,string"`
				C uint16 `json:"c,string"`
			}{},
		},

		// HeadUint16MultiFields
		{
			name: "HeadUint16MultiFields",
			data: struct {
				A uint16 `json:"a"`
				B uint16 `json:"b"`
				C uint16 `json:"c"`
			}{A: 1, B: 2, C: 3},
		},
		{
			name: "HeadUint16MultiFieldsOmitEmpty",
			data: struct {
				A uint16 `json:"a,omitempty"`
				B uint16 `json:"b,omitempty"`
				C uint16 `json:"c,omitempty"`
			}{A: 1, B: 2, C: 3},
		},
		{
			name: "HeadUint16MultiFieldsString",
			data: struct {
				A uint16 `json:"a,string"`
				B uint16 `json:"b,string"`
				C uint16 `json:"c,string"`
			}{A: 1, B: 2, C: 3},
		},

		// HeadUint16PtrMultiFields
		{
			name: "HeadUint16PtrMultiFields",
			data: struct {
				A *uint16 `json:"a"`
				B *uint16 `json:"b"`
				C *uint16 `json:"c"`
			}{A: uint16ptr(1), B: uint16ptr(2), C: uint16ptr(3)},
		},
		{
			name: "HeadUint16PtrMultiFieldsOmitEmpty",
			data: struct {
				A *uint16 `json:"a,omitempty"`
				B *uint16 `json:"b,omitempty"`
				C *uint16 `json:"c,omitempty"`
			}{A: uint16ptr(1), B: uint16ptr(2), C: uint16ptr(3)},
		},
		{
			name: "HeadUint16PtrMultiFieldsString",
			data: struct {
				A *uint16 `json:"a,string"`
				B *uint16 `json:"b,string"`
				C *uint16 `json:"c,string"`
			}{A: uint16ptr(1), B: uint16ptr(2), C: uint16ptr(3)},
		},

		// HeadUint16PtrNilMultiFields
		{
			name: "HeadUint16PtrNilMultiFields",
			data: struct {
				A *uint16 `json:"a"`
				B *uint16 `json:"b"`
				C *uint16 `json:"c"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadUint16PtrNilMultiFieldsOmitEmpty",
			data: struct {
				A *uint16 `json:"a,omitempty"`
				B *uint16 `json:"b,omitempty"`
				C *uint16 `json:"c,omitempty"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadUint16PtrNilMultiFieldsString",
			data: struct {
				A *uint16 `json:"a,string"`
				B *uint16 `json:"b,string"`
				C *uint16 `json:"c,string"`
			}{A: nil, B: nil, C: nil},
		},

		// PtrHeadUint16ZeroMultiFields
		{
			name: "PtrHeadUint16ZeroMultiFields",
			data: &struct {
				A uint16 `json:"a"`
				B uint16 `json:"b"`
			}{},
		},
		{
			name: "PtrHeadUint16ZeroMultiFieldsOmitEmpty",
			data: &struct {
				A uint16 `json:"a,omitempty"`
				B uint16 `json:"b,omitempty"`
			}{},
		},
		{
			name: "PtrHeadUint16ZeroMultiFieldsString",
			data: &struct {
				A uint16 `json:"a,string"`
				B uint16 `json:"b,string"`
			}{},
		},

		// PtrHeadUint16MultiFields
		{
			name: "PtrHeadUint16MultiFields",
			data: &struct {
				A uint16 `json:"a"`
				B uint16 `json:"b"`
			}{A: 1, B: 2},
		},
		{
			name: "PtrHeadUint16MultiFieldsOmitEmpty",
			data: &struct {
				A uint16 `json:"a,omitempty"`
				B uint16 `json:"b,omitempty"`
			}{A: 1, B: 2},
		},
		{
			name: "PtrHeadUint16MultiFieldsString",
			data: &struct {
				A uint16 `json:"a,string"`
				B uint16 `json:"b,string"`
			}{A: 1, B: 2},
		},

		// PtrHeadUint16PtrMultiFields
		{
			name: "PtrHeadUint16PtrMultiFields",
			data: &struct {
				A *uint16 `json:"a"`
				B *uint16 `json:"b"`
			}{A: uint16ptr(1), B: uint16ptr(2)},
		},
		{
			name: "PtrHeadUint16PtrMultiFieldsOmitEmpty",
			data: &struct {
				A *uint16 `json:"a,omitempty"`
				B *uint16 `json:"b,omitempty"`
			}{A: uint16ptr(1), B: uint16ptr(2)},
		},
		{
			name: "PtrHeadUint16PtrMultiFieldsString",
			data: &struct {
				A *uint16 `json:"a,string"`
				B *uint16 `json:"b,string"`
			}{A: uint16ptr(1), B: uint16ptr(2)},
		},

		// PtrHeadUint16PtrNilMultiFields
		{
			name: "PtrHeadUint16PtrNilMultiFields",
			data: &struct {
				A *uint16 `json:"a"`
				B *uint16 `json:"b"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUint16PtrNilMultiFieldsOmitEmpty",
			data: &struct {
				A *uint16 `json:"a,omitempty"`
				B *uint16 `json:"b,omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUint16PtrNilMultiFieldsString",
			data: &struct {
				A *uint16 `json:"a,string"`
				B *uint16 `json:"b,string"`
			}{A: nil, B: nil},
		},

		// PtrHeadUint16NilMultiFields
		{
			name: "PtrHeadUint16NilMultiFields",
			data: (*struct {
				A *uint16 `json:"a"`
				B *uint16 `json:"b"`
			})(nil),
		},
		{
			name: "PtrHeadUint16NilMultiFieldsOmitEmpty",
			data: (*struct {
				A *uint16 `json:"a,omitempty"`
				B *uint16 `json:"b,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadUint16NilMultiFieldsString",
			data: (*struct {
				A *uint16 `json:"a,string"`
				B *uint16 `json:"b,string"`
			})(nil),
		},

		// HeadUint16ZeroNotRoot
		{
			name: "HeadUint16ZeroNotRoot",
			data: struct {
				A struct {
					A uint16 `json:"a"`
				}
			}{},
		},
		{
			name: "HeadUint16ZeroNotRootOmitEmpty",
			data: struct {
				A struct {
					A uint16 `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadUint16ZeroNotRootString",
			data: struct {
				A struct {
					A uint16 `json:"a,string"`
				}
			}{},
		},

		// HeadUint16NotRoot
		{
			name: "HeadUint16NotRoot",
			data: struct {
				A struct {
					A uint16 `json:"a"`
				}
			}{A: struct {
				A uint16 `json:"a"`
			}{A: 1}},
		},
		{
			name: "HeadUint16NotRootOmitEmpty",
			data: struct {
				A struct {
					A uint16 `json:"a,omitempty"`
				}
			}{A: struct {
				A uint16 `json:"a,omitempty"`
			}{A: 1}},
		},
		{
			name: "HeadUint16NotRootString",
			data: struct {
				A struct {
					A uint16 `json:"a,string"`
				}
			}{A: struct {
				A uint16 `json:"a,string"`
			}{A: 1}},
		},

		// HeadUint16PtrNotRoot
		{
			name: "HeadUint16PtrNotRoot",
			data: struct {
				A struct {
					A *uint16 `json:"a"`
				}
			}{A: struct {
				A *uint16 `json:"a"`
			}{uint16ptr(1)}},
		},
		{
			name: "HeadUint16PtrNotRootOmitEmpty",
			data: struct {
				A struct {
					A *uint16 `json:"a,omitempty"`
				}
			}{A: struct {
				A *uint16 `json:"a,omitempty"`
			}{uint16ptr(1)}},
		},
		{
			name: "HeadUint16PtrNotRootString",
			data: struct {
				A struct {
					A *uint16 `json:"a,string"`
				}
			}{A: struct {
				A *uint16 `json:"a,string"`
			}{uint16ptr(1)}},
		},

		// HeadUint16PtrNilNotRoot
		{
			name: "HeadUint16PtrNilNotRoot",
			data: struct {
				A struct {
					A *uint16 `json:"a"`
				}
			}{},
		},
		{
			name: "HeadUint16PtrNilNotRootOmitEmpty",
			data: struct {
				A struct {
					A *uint16 `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadUint16PtrNilNotRootString",
			data: struct {
				A struct {
					A *uint16 `json:"a,string"`
				}
			}{},
		},

		// PtrHeadUint16ZeroNotRoot
		{
			name: "PtrHeadUint16ZeroNotRoot",
			data: struct {
				A *struct {
					A uint16 `json:"a"`
				}
			}{A: new(struct {
				A uint16 `json:"a"`
			})},
		},
		{
			name: "PtrHeadUint16ZeroNotRootOmitEmpty",
			data: struct {
				A *struct {
					A uint16 `json:"a,omitempty"`
				}
			}{A: new(struct {
				A uint16 `json:"a,omitempty"`
			})},
		},
		{
			name: "PtrHeadUint16ZeroNotRootString",
			data: struct {
				A *struct {
					A uint16 `json:"a,string"`
				}
			}{A: new(struct {
				A uint16 `json:"a,string"`
			})},
		},

		// PtrHeadUint16NotRoot
		{
			name: "PtrHeadUint16NotRoot",
			data: struct {
				A *struct {
					A uint16 `json:"a"`
				}
			}{A: &(struct {
				A uint16 `json:"a"`
			}{A: 1})},
		},
		{
			name: "PtrHeadUint16NotRootOmitEmpty",
			data: struct {
				A *struct {
					A uint16 `json:"a,omitempty"`
				}
			}{A: &(struct {
				A uint16 `json:"a,omitempty"`
			}{A: 1})},
		},
		{
			name: "PtrHeadUint16NotRootString",
			data: struct {
				A *struct {
					A uint16 `json:"a,string"`
				}
			}{A: &(struct {
				A uint16 `json:"a,string"`
			}{A: 1})},
		},

		// PtrHeadUint16PtrNotRoot
		{
			name: "PtrHeadUint16PtrNotRoot",
			data: struct {
				A *struct {
					A *uint16 `json:"a"`
				}
			}{A: &(struct {
				A *uint16 `json:"a"`
			}{A: uint16ptr(1)})},
		},
		{
			name: "PtrHeadUint16PtrNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *uint16 `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *uint16 `json:"a,omitempty"`
			}{A: uint16ptr(1)})},
		},
		{
			name: "PtrHeadUint16PtrNotRootString",
			data: struct {
				A *struct {
					A *uint16 `json:"a,string"`
				}
			}{A: &(struct {
				A *uint16 `json:"a,string"`
			}{A: uint16ptr(1)})},
		},

		// PtrHeadUint16PtrNilNotRoot
		{
			name: "PtrHeadUint16PtrNilNotRoot",
			data: struct {
				A *struct {
					A *uint16 `json:"a"`
				}
			}{A: &(struct {
				A *uint16 `json:"a"`
			}{A: nil})},
		},
		{
			name: "PtrHeadUint16PtrNilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *uint16 `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *uint16 `json:"a,omitempty"`
			}{A: nil})},
		},
		{
			name: "PtrHeadUint16PtrNilNotRootString",
			data: struct {
				A *struct {
					A *uint16 `json:"a,string"`
				}
			}{A: &(struct {
				A *uint16 `json:"a,string"`
			}{A: nil})},
		},

		// PtrHeadUint16NilNotRoot
		{
			name: "PtrHeadUint16NilNotRoot",
			data: struct {
				A *struct {
					A *uint16 `json:"a"`
				}
			}{A: nil},
		},
		{
			name: "PtrHeadUint16NilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *uint16 `json:"a,omitempty"`
				} `json:",omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadUint16NilNotRootString",
			data: struct {
				A *struct {
					A *uint16 `json:"a,string"`
				} `json:",string"`
			}{A: nil},
		},

		// HeadUint16ZeroMultiFieldsNotRoot
		{
			name: "HeadUint16ZeroMultiFieldsNotRoot",
			data: struct {
				A struct {
					A uint16 `json:"a"`
				}
				B struct {
					B uint16 `json:"b"`
				}
			}{},
		},
		{
			name: "HeadUint16ZeroMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A uint16 `json:"a,omitempty"`
				}
				B struct {
					B uint16 `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "HeadUint16ZeroMultiFieldsNotRootString",
			data: struct {
				A struct {
					A uint16 `json:"a,string"`
				}
				B struct {
					B uint16 `json:"b,string"`
				}
			}{},
		},

		// HeadUint16MultiFieldsNotRoot
		{
			name: "HeadUint16MultiFieldsNotRoot",
			data: struct {
				A struct {
					A uint16 `json:"a"`
				}
				B struct {
					B uint16 `json:"b"`
				}
			}{A: struct {
				A uint16 `json:"a"`
			}{A: 1}, B: struct {
				B uint16 `json:"b"`
			}{B: 2}},
		},
		{
			name: "HeadUint16MultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A uint16 `json:"a,omitempty"`
				}
				B struct {
					B uint16 `json:"b,omitempty"`
				}
			}{A: struct {
				A uint16 `json:"a,omitempty"`
			}{A: 1}, B: struct {
				B uint16 `json:"b,omitempty"`
			}{B: 2}},
		},
		{
			name: "HeadUint16MultiFieldsNotRootString",
			data: struct {
				A struct {
					A uint16 `json:"a,string"`
				}
				B struct {
					B uint16 `json:"b,string"`
				}
			}{A: struct {
				A uint16 `json:"a,string"`
			}{A: 1}, B: struct {
				B uint16 `json:"b,string"`
			}{B: 2}},
		},

		// HeadUint16PtrMultiFieldsNotRoot
		{
			name: "HeadUint16PtrMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *uint16 `json:"a"`
				}
				B struct {
					B *uint16 `json:"b"`
				}
			}{A: struct {
				A *uint16 `json:"a"`
			}{A: uint16ptr(1)}, B: struct {
				B *uint16 `json:"b"`
			}{B: uint16ptr(2)}},
		},
		{
			name: "HeadUint16PtrMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *uint16 `json:"a,omitempty"`
				}
				B struct {
					B *uint16 `json:"b,omitempty"`
				}
			}{A: struct {
				A *uint16 `json:"a,omitempty"`
			}{A: uint16ptr(1)}, B: struct {
				B *uint16 `json:"b,omitempty"`
			}{B: uint16ptr(2)}},
		},
		{
			name: "HeadUint16PtrMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *uint16 `json:"a,string"`
				}
				B struct {
					B *uint16 `json:"b,string"`
				}
			}{A: struct {
				A *uint16 `json:"a,string"`
			}{A: uint16ptr(1)}, B: struct {
				B *uint16 `json:"b,string"`
			}{B: uint16ptr(2)}},
		},

		// HeadUint16PtrNilMultiFieldsNotRoot
		{
			name: "HeadUint16PtrNilMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *uint16 `json:"a"`
				}
				B struct {
					B *uint16 `json:"b"`
				}
			}{A: struct {
				A *uint16 `json:"a"`
			}{A: nil}, B: struct {
				B *uint16 `json:"b"`
			}{B: nil}},
		},
		{
			name: "HeadUint16PtrNilMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *uint16 `json:"a,omitempty"`
				}
				B struct {
					B *uint16 `json:"b,omitempty"`
				}
			}{A: struct {
				A *uint16 `json:"a,omitempty"`
			}{A: nil}, B: struct {
				B *uint16 `json:"b,omitempty"`
			}{B: nil}},
		},
		{
			name: "HeadUint16PtrNilMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *uint16 `json:"a,string"`
				}
				B struct {
					B *uint16 `json:"b,string"`
				}
			}{A: struct {
				A *uint16 `json:"a,string"`
			}{A: nil}, B: struct {
				B *uint16 `json:"b,string"`
			}{B: nil}},
		},

		// PtrHeadUint16ZeroMultiFieldsNotRoot
		{
			name: "PtrHeadUint16ZeroMultiFieldsNotRoot",
			data: &struct {
				A struct {
					A uint16 `json:"a"`
				}
				B struct {
					B uint16 `json:"b"`
				}
			}{},
		},
		{
			name: "PtrHeadUint16ZeroMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A uint16 `json:"a,omitempty"`
				}
				B struct {
					B uint16 `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "PtrHeadUint16ZeroMultiFieldsNotRootString",
			data: &struct {
				A struct {
					A uint16 `json:"a,string"`
				}
				B struct {
					B uint16 `json:"b,string"`
				}
			}{},
		},

		// PtrHeadUint16MultiFieldsNotRoot
		{
			name: "PtrHeadUint16MultiFieldsNotRoot",
			data: &struct {
				A struct {
					A uint16 `json:"a"`
				}
				B struct {
					B uint16 `json:"b"`
				}
			}{A: struct {
				A uint16 `json:"a"`
			}{A: 1}, B: struct {
				B uint16 `json:"b"`
			}{B: 2}},
		},
		{
			name: "PtrHeadUint16MultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A uint16 `json:"a,omitempty"`
				}
				B struct {
					B uint16 `json:"b,omitempty"`
				}
			}{A: struct {
				A uint16 `json:"a,omitempty"`
			}{A: 1}, B: struct {
				B uint16 `json:"b,omitempty"`
			}{B: 2}},
		},
		{
			name: "PtrHeadUint16MultiFieldsNotRootString",
			data: &struct {
				A struct {
					A uint16 `json:"a,string"`
				}
				B struct {
					B uint16 `json:"b,string"`
				}
			}{A: struct {
				A uint16 `json:"a,string"`
			}{A: 1}, B: struct {
				B uint16 `json:"b,string"`
			}{B: 2}},
		},

		// PtrHeadUint16PtrMultiFieldsNotRoot
		{
			name: "PtrHeadUint16PtrMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *uint16 `json:"a"`
				}
				B *struct {
					B *uint16 `json:"b"`
				}
			}{A: &(struct {
				A *uint16 `json:"a"`
			}{A: uint16ptr(1)}), B: &(struct {
				B *uint16 `json:"b"`
			}{B: uint16ptr(2)})},
		},
		{
			name: "PtrHeadUint16PtrMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *uint16 `json:"a,omitempty"`
				}
				B *struct {
					B *uint16 `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *uint16 `json:"a,omitempty"`
			}{A: uint16ptr(1)}), B: &(struct {
				B *uint16 `json:"b,omitempty"`
			}{B: uint16ptr(2)})},
		},
		{
			name: "PtrHeadUint16PtrMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *uint16 `json:"a,string"`
				}
				B *struct {
					B *uint16 `json:"b,string"`
				}
			}{A: &(struct {
				A *uint16 `json:"a,string"`
			}{A: uint16ptr(1)}), B: &(struct {
				B *uint16 `json:"b,string"`
			}{B: uint16ptr(2)})},
		},

		// PtrHeadUint16PtrNilMultiFieldsNotRoot
		{
			name: "PtrHeadUint16PtrNilMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *uint16 `json:"a"`
				}
				B *struct {
					B *uint16 `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUint16PtrNilMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *uint16 `json:"a,omitempty"`
				} `json:",omitempty"`
				B *struct {
					B *uint16 `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUint16PtrNilMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *uint16 `json:"a,string"`
				} `json:",string"`
				B *struct {
					B *uint16 `json:"b,string"`
				} `json:",string"`
			}{A: nil, B: nil},
		},

		// PtrHeadUint16NilMultiFieldsNotRoot
		{
			name: "PtrHeadUint16NilMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *uint16 `json:"a"`
				}
				B *struct {
					B *uint16 `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadUint16NilMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *uint16 `json:"a,omitempty"`
				}
				B *struct {
					B *uint16 `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadUint16NilMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *uint16 `json:"a,string"`
				}
				B *struct {
					B *uint16 `json:"b,string"`
				}
			})(nil),
		},

		// PtrHeadUint16DoubleMultiFieldsNotRoot
		{
			name: "PtrHeadUint16DoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A uint16 `json:"a"`
					B uint16 `json:"b"`
				}
				B *struct {
					A uint16 `json:"a"`
					B uint16 `json:"b"`
				}
			}{A: &(struct {
				A uint16 `json:"a"`
				B uint16 `json:"b"`
			}{A: 1, B: 2}), B: &(struct {
				A uint16 `json:"a"`
				B uint16 `json:"b"`
			}{A: 3, B: 4})},
		},
		{
			name: "PtrHeadUint16DoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A uint16 `json:"a,omitempty"`
					B uint16 `json:"b,omitempty"`
				}
				B *struct {
					A uint16 `json:"a,omitempty"`
					B uint16 `json:"b,omitempty"`
				}
			}{A: &(struct {
				A uint16 `json:"a,omitempty"`
				B uint16 `json:"b,omitempty"`
			}{A: 1, B: 2}), B: &(struct {
				A uint16 `json:"a,omitempty"`
				B uint16 `json:"b,omitempty"`
			}{A: 3, B: 4})},
		},
		{
			name: "PtrHeadUint16DoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A uint16 `json:"a,string"`
					B uint16 `json:"b,string"`
				}
				B *struct {
					A uint16 `json:"a,string"`
					B uint16 `json:"b,string"`
				}
			}{A: &(struct {
				A uint16 `json:"a,string"`
				B uint16 `json:"b,string"`
			}{A: 1, B: 2}), B: &(struct {
				A uint16 `json:"a,string"`
				B uint16 `json:"b,string"`
			}{A: 3, B: 4})},
		},

		// PtrHeadUint16NilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadUint16NilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A uint16 `json:"a"`
					B uint16 `json:"b"`
				}
				B *struct {
					A uint16 `json:"a"`
					B uint16 `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUint16NilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A uint16 `json:"a,omitempty"`
					B uint16 `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A uint16 `json:"a,omitempty"`
					B uint16 `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUint16NilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A uint16 `json:"a,string"`
					B uint16 `json:"b,string"`
				}
				B *struct {
					A uint16 `json:"a,string"`
					B uint16 `json:"b,string"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadUint16NilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadUint16NilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A uint16 `json:"a"`
					B uint16 `json:"b"`
				}
				B *struct {
					A uint16 `json:"a"`
					B uint16 `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadUint16NilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A uint16 `json:"a,omitempty"`
					B uint16 `json:"b,omitempty"`
				}
				B *struct {
					A uint16 `json:"a,omitempty"`
					B uint16 `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadUint16NilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A uint16 `json:"a,string"`
					B uint16 `json:"b,string"`
				}
				B *struct {
					A uint16 `json:"a,string"`
					B uint16 `json:"b,string"`
				}
			})(nil),
		},

		// PtrHeadUint16PtrDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadUint16PtrDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *uint16 `json:"a"`
					B *uint16 `json:"b"`
				}
				B *struct {
					A *uint16 `json:"a"`
					B *uint16 `json:"b"`
				}
			}{A: &(struct {
				A *uint16 `json:"a"`
				B *uint16 `json:"b"`
			}{A: uint16ptr(1), B: uint16ptr(2)}), B: &(struct {
				A *uint16 `json:"a"`
				B *uint16 `json:"b"`
			}{A: uint16ptr(3), B: uint16ptr(4)})},
		},
		{
			name: "PtrHeadUint16PtrDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *uint16 `json:"a,omitempty"`
					B *uint16 `json:"b,omitempty"`
				}
				B *struct {
					A *uint16 `json:"a,omitempty"`
					B *uint16 `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *uint16 `json:"a,omitempty"`
				B *uint16 `json:"b,omitempty"`
			}{A: uint16ptr(1), B: uint16ptr(2)}), B: &(struct {
				A *uint16 `json:"a,omitempty"`
				B *uint16 `json:"b,omitempty"`
			}{A: uint16ptr(3), B: uint16ptr(4)})},
		},
		{
			name: "PtrHeadUint16PtrDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *uint16 `json:"a,string"`
					B *uint16 `json:"b,string"`
				}
				B *struct {
					A *uint16 `json:"a,string"`
					B *uint16 `json:"b,string"`
				}
			}{A: &(struct {
				A *uint16 `json:"a,string"`
				B *uint16 `json:"b,string"`
			}{A: uint16ptr(1), B: uint16ptr(2)}), B: &(struct {
				A *uint16 `json:"a,string"`
				B *uint16 `json:"b,string"`
			}{A: uint16ptr(3), B: uint16ptr(4)})},
		},

		// PtrHeadUint16PtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadUint16PtrNilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *uint16 `json:"a"`
					B *uint16 `json:"b"`
				}
				B *struct {
					A *uint16 `json:"a"`
					B *uint16 `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUint16PtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *uint16 `json:"a,omitempty"`
					B *uint16 `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A *uint16 `json:"a,omitempty"`
					B *uint16 `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUint16PtrNilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *uint16 `json:"a,string"`
					B *uint16 `json:"b,string"`
				}
				B *struct {
					A *uint16 `json:"a,string"`
					B *uint16 `json:"b,string"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadUint16PtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadUint16PtrNilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *uint16 `json:"a"`
					B *uint16 `json:"b"`
				}
				B *struct {
					A *uint16 `json:"a"`
					B *uint16 `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadUint16PtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *uint16 `json:"a,omitempty"`
					B *uint16 `json:"b,omitempty"`
				}
				B *struct {
					A *uint16 `json:"a,omitempty"`
					B *uint16 `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadUint16PtrNilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *uint16 `json:"a,string"`
					B *uint16 `json:"b,string"`
				}
				B *struct {
					A *uint16 `json:"a,string"`
					B *uint16 `json:"b,string"`
				}
			})(nil),
		},

		// AnonymousHeadUint16
		{
			name: "AnonymousHeadUint16",
			data: struct {
				structUint16
				B uint16 `json:"b"`
			}{
				structUint16: structUint16{A: 1},
				B:            2,
			},
		},
		{
			name: "AnonymousHeadUint16OmitEmpty",
			data: struct {
				structUint16OmitEmpty
				B uint16 `json:"b,omitempty"`
			}{
				structUint16OmitEmpty: structUint16OmitEmpty{A: 1},
				B:                     2,
			},
		},
		{
			name: "AnonymousHeadUint16String",
			data: struct {
				structUint16String
				B uint16 `json:"b,string"`
			}{
				structUint16String: structUint16String{A: 1},
				B:                  2,
			},
		},

		// PtrAnonymousHeadUint16
		{
			name: "PtrAnonymousHeadUint16",
			data: struct {
				*structUint16
				B uint16 `json:"b"`
			}{
				structUint16: &structUint16{A: 1},
				B:            2,
			},
		},
		{
			name: "PtrAnonymousHeadUint16OmitEmpty",
			data: struct {
				*structUint16OmitEmpty
				B uint16 `json:"b,omitempty"`
			}{
				structUint16OmitEmpty: &structUint16OmitEmpty{A: 1},
				B:                     2,
			},
		},
		{
			name: "PtrAnonymousHeadUint16String",
			data: struct {
				*structUint16String
				B uint16 `json:"b,string"`
			}{
				structUint16String: &structUint16String{A: 1},
				B:                  2,
			},
		},

		// NilPtrAnonymousHeadUint16
		{
			name: "NilPtrAnonymousHeadUint16",
			data: struct {
				*structUint16
				B uint16 `json:"b"`
			}{
				structUint16: nil,
				B:            2,
			},
		},
		{
			name: "NilPtrAnonymousHeadUint16OmitEmpty",
			data: struct {
				*structUint16OmitEmpty
				B uint16 `json:"b,omitempty"`
			}{
				structUint16OmitEmpty: nil,
				B:                     2,
			},
		},
		{
			name: "NilPtrAnonymousHeadUint16String",
			data: struct {
				*structUint16String
				B uint16 `json:"b,string"`
			}{
				structUint16String: nil,
				B:                  2,
			},
		},

		// AnonymousHeadUint16Ptr
		{
			name: "AnonymousHeadUint16Ptr",
			data: struct {
				structUint16Ptr
				B *uint16 `json:"b"`
			}{
				structUint16Ptr: structUint16Ptr{A: uint16ptr(1)},
				B:               uint16ptr(2),
			},
		},
		{
			name: "AnonymousHeadUint16PtrOmitEmpty",
			data: struct {
				structUint16PtrOmitEmpty
				B *uint16 `json:"b,omitempty"`
			}{
				structUint16PtrOmitEmpty: structUint16PtrOmitEmpty{A: uint16ptr(1)},
				B:                        uint16ptr(2),
			},
		},
		{
			name: "AnonymousHeadUint16PtrString",
			data: struct {
				structUint16PtrString
				B *uint16 `json:"b,string"`
			}{
				structUint16PtrString: structUint16PtrString{A: uint16ptr(1)},
				B:                     uint16ptr(2),
			},
		},

		// AnonymousHeadUint16PtrNil
		{
			name: "AnonymousHeadUint16PtrNil",
			data: struct {
				structUint16Ptr
				B *uint16 `json:"b"`
			}{
				structUint16Ptr: structUint16Ptr{A: nil},
				B:               uint16ptr(2),
			},
		},
		{
			name: "AnonymousHeadUint16PtrNilOmitEmpty",
			data: struct {
				structUint16PtrOmitEmpty
				B *uint16 `json:"b,omitempty"`
			}{
				structUint16PtrOmitEmpty: structUint16PtrOmitEmpty{A: nil},
				B:                        uint16ptr(2),
			},
		},
		{
			name: "AnonymousHeadUint16PtrNilString",
			data: struct {
				structUint16PtrString
				B *uint16 `json:"b,string"`
			}{
				structUint16PtrString: structUint16PtrString{A: nil},
				B:                     uint16ptr(2),
			},
		},

		// PtrAnonymousHeadUint16Ptr
		{
			name: "PtrAnonymousHeadUint16Ptr",
			data: struct {
				*structUint16Ptr
				B *uint16 `json:"b"`
			}{
				structUint16Ptr: &structUint16Ptr{A: uint16ptr(1)},
				B:               uint16ptr(2),
			},
		},
		{
			name: "PtrAnonymousHeadUint16PtrOmitEmpty",
			data: struct {
				*structUint16PtrOmitEmpty
				B *uint16 `json:"b,omitempty"`
			}{
				structUint16PtrOmitEmpty: &structUint16PtrOmitEmpty{A: uint16ptr(1)},
				B:                        uint16ptr(2),
			},
		},
		{
			name: "PtrAnonymousHeadUint16PtrString",
			data: struct {
				*structUint16PtrString
				B *uint16 `json:"b,string"`
			}{
				structUint16PtrString: &structUint16PtrString{A: uint16ptr(1)},
				B:                     uint16ptr(2),
			},
		},

		// NilPtrAnonymousHeadUint16Ptr
		{
			name: "NilPtrAnonymousHeadUint16Ptr",
			data: struct {
				*structUint16Ptr
				B *uint16 `json:"b"`
			}{
				structUint16Ptr: nil,
				B:               uint16ptr(2),
			},
		},
		{
			name: "NilPtrAnonymousHeadUint16PtrOmitEmpty",
			data: struct {
				*structUint16PtrOmitEmpty
				B *uint16 `json:"b,omitempty"`
			}{
				structUint16PtrOmitEmpty: nil,
				B:                        uint16ptr(2),
			},
		},
		{
			name: "NilPtrAnonymousHeadUint16PtrString",
			data: struct {
				*structUint16PtrString
				B *uint16 `json:"b,string"`
			}{
				structUint16PtrString: nil,
				B:                     uint16ptr(2),
			},
		},

		// AnonymousHeadUint16Only
		{
			name: "AnonymousHeadUint16Only",
			data: struct {
				structUint16
			}{
				structUint16: structUint16{A: 1},
			},
		},
		{
			name: "AnonymousHeadUint16OnlyOmitEmpty",
			data: struct {
				structUint16OmitEmpty
			}{
				structUint16OmitEmpty: structUint16OmitEmpty{A: 1},
			},
		},
		{
			name: "AnonymousHeadUint16OnlyString",
			data: struct {
				structUint16String
			}{
				structUint16String: structUint16String{A: 1},
			},
		},

		// PtrAnonymousHeadUint16Only
		{
			name: "PtrAnonymousHeadUint16Only",
			data: struct {
				*structUint16
			}{
				structUint16: &structUint16{A: 1},
			},
		},
		{
			name: "PtrAnonymousHeadUint16OnlyOmitEmpty",
			data: struct {
				*structUint16OmitEmpty
			}{
				structUint16OmitEmpty: &structUint16OmitEmpty{A: 1},
			},
		},
		{
			name: "PtrAnonymousHeadUint16OnlyString",
			data: struct {
				*structUint16String
			}{
				structUint16String: &structUint16String{A: 1},
			},
		},

		// NilPtrAnonymousHeadUint16Only
		{
			name: "NilPtrAnonymousHeadUint16Only",
			data: struct {
				*structUint16
			}{
				structUint16: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadUint16OnlyOmitEmpty",
			data: struct {
				*structUint16OmitEmpty
			}{
				structUint16OmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadUint16OnlyString",
			data: struct {
				*structUint16String
			}{
				structUint16String: nil,
			},
		},

		// AnonymousHeadUint16PtrOnly
		{
			name: "AnonymousHeadUint16PtrOnly",
			data: struct {
				structUint16Ptr
			}{
				structUint16Ptr: structUint16Ptr{A: uint16ptr(1)},
			},
		},
		{
			name: "AnonymousHeadUint16PtrOnlyOmitEmpty",
			data: struct {
				structUint16PtrOmitEmpty
			}{
				structUint16PtrOmitEmpty: structUint16PtrOmitEmpty{A: uint16ptr(1)},
			},
		},
		{
			name: "AnonymousHeadUint16PtrOnlyString",
			data: struct {
				structUint16PtrString
			}{
				structUint16PtrString: structUint16PtrString{A: uint16ptr(1)},
			},
		},

		// AnonymousHeadUint16PtrNilOnly
		{
			name: "AnonymousHeadUint16PtrNilOnly",
			data: struct {
				structUint16Ptr
			}{
				structUint16Ptr: structUint16Ptr{A: nil},
			},
		},
		{
			name: "AnonymousHeadUint16PtrNilOnlyOmitEmpty",
			data: struct {
				structUint16PtrOmitEmpty
			}{
				structUint16PtrOmitEmpty: structUint16PtrOmitEmpty{A: nil},
			},
		},
		{
			name: "AnonymousHeadUint16PtrNilOnlyString",
			data: struct {
				structUint16PtrString
			}{
				structUint16PtrString: structUint16PtrString{A: nil},
			},
		},

		// PtrAnonymousHeadUint16PtrOnly
		{
			name: "PtrAnonymousHeadUint16PtrOnly",
			data: struct {
				*structUint16Ptr
			}{
				structUint16Ptr: &structUint16Ptr{A: uint16ptr(1)},
			},
		},
		{
			name: "PtrAnonymousHeadUint16PtrOnlyOmitEmpty",
			data: struct {
				*structUint16PtrOmitEmpty
			}{
				structUint16PtrOmitEmpty: &structUint16PtrOmitEmpty{A: uint16ptr(1)},
			},
		},
		{
			name: "PtrAnonymousHeadUint16PtrOnlyString",
			data: struct {
				*structUint16PtrString
			}{
				structUint16PtrString: &structUint16PtrString{A: uint16ptr(1)},
			},
		},

		// NilPtrAnonymousHeadUint16PtrOnly
		{
			name: "NilPtrAnonymousHeadUint16PtrOnly",
			data: struct {
				*structUint16Ptr
			}{
				structUint16Ptr: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadUint16PtrOnlyOmitEmpty",
			data: struct {
				*structUint16PtrOmitEmpty
			}{
				structUint16PtrOmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadUint16PtrOnlyString",
			data: struct {
				*structUint16PtrString
			}{
				structUint16PtrString: nil,
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
