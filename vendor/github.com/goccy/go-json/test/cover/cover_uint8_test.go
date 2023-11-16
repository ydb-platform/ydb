package json_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/goccy/go-json"
)

func TestCoverUint8(t *testing.T) {
	type structUint8 struct {
		A uint8 `json:"a"`
	}
	type structUint8OmitEmpty struct {
		A uint8 `json:"a,omitempty"`
	}
	type structUint8String struct {
		A uint8 `json:"a,string"`
	}

	type structUint8Ptr struct {
		A *uint8 `json:"a"`
	}
	type structUint8PtrOmitEmpty struct {
		A *uint8 `json:"a,omitempty"`
	}
	type structUint8PtrString struct {
		A *uint8 `json:"a,string"`
	}

	tests := []struct {
		name string
		data interface{}
	}{
		{
			name: "Uint8",
			data: uint8(10),
		},
		{
			name: "Uint8Ptr",
			data: uint8ptr(10),
		},
		{
			name: "Uint8Ptr3",
			data: uint8ptr3(10),
		},
		{
			name: "Uint8PtrNil",
			data: (*uint8)(nil),
		},
		{
			name: "Uint8Ptr3Nil",
			data: (***uint8)(nil),
		},

		// HeadUint8Zero
		{
			name: "HeadUint8Zero",
			data: struct {
				A uint8 `json:"a"`
			}{},
		},
		{
			name: "HeadUint8ZeroOmitEmpty",
			data: struct {
				A uint8 `json:"a,omitempty"`
			}{},
		},
		{
			name: "HeadUint8ZeroString",
			data: struct {
				A uint8 `json:"a,string"`
			}{},
		},

		// HeadUint8
		{
			name: "HeadUint8",
			data: struct {
				A uint8 `json:"a"`
			}{A: 1},
		},
		{
			name: "HeadUint8OmitEmpty",
			data: struct {
				A uint8 `json:"a,omitempty"`
			}{A: 1},
		},
		{
			name: "HeadUint8String",
			data: struct {
				A uint8 `json:"a,string"`
			}{A: 1},
		},

		// HeadUint8Ptr
		{
			name: "HeadUint8Ptr",
			data: struct {
				A *uint8 `json:"a"`
			}{A: uint8ptr(1)},
		},
		{
			name: "HeadUint8PtrOmitEmpty",
			data: struct {
				A *uint8 `json:"a,omitempty"`
			}{A: uint8ptr(1)},
		},
		{
			name: "HeadUint8PtrString",
			data: struct {
				A *uint8 `json:"a,string"`
			}{A: uint8ptr(1)},
		},

		// HeadUint8PtrNil
		{
			name: "HeadUint8PtrNil",
			data: struct {
				A *uint8 `json:"a"`
			}{A: nil},
		},
		{
			name: "HeadUint8PtrNilOmitEmpty",
			data: struct {
				A *uint8 `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "HeadUint8PtrNilString",
			data: struct {
				A *uint8 `json:"a,string"`
			}{A: nil},
		},

		// PtrHeadUint8Zero
		{
			name: "PtrHeadUint8Zero",
			data: &struct {
				A uint8 `json:"a"`
			}{},
		},
		{
			name: "PtrHeadUint8ZeroOmitEmpty",
			data: &struct {
				A uint8 `json:"a,omitempty"`
			}{},
		},
		{
			name: "PtrHeadUint8ZeroString",
			data: &struct {
				A uint8 `json:"a,string"`
			}{},
		},

		// PtrHeadUint8
		{
			name: "PtrHeadUint8",
			data: &struct {
				A uint8 `json:"a"`
			}{A: 1},
		},
		{
			name: "PtrHeadUint8OmitEmpty",
			data: &struct {
				A uint8 `json:"a,omitempty"`
			}{A: 1},
		},
		{
			name: "PtrHeadUint8String",
			data: &struct {
				A uint8 `json:"a,string"`
			}{A: 1},
		},

		// PtrHeadUint8Ptr
		{
			name: "PtrHeadUint8Ptr",
			data: &struct {
				A *uint8 `json:"a"`
			}{A: uint8ptr(1)},
		},
		{
			name: "PtrHeadUint8PtrOmitEmpty",
			data: &struct {
				A *uint8 `json:"a,omitempty"`
			}{A: uint8ptr(1)},
		},
		{
			name: "PtrHeadUint8PtrString",
			data: &struct {
				A *uint8 `json:"a,string"`
			}{A: uint8ptr(1)},
		},

		// PtrHeadUint8PtrNil
		{
			name: "PtrHeadUint8PtrNil",
			data: &struct {
				A *uint8 `json:"a"`
			}{A: nil},
		},
		{
			name: "PtrHeadUint8PtrNilOmitEmpty",
			data: &struct {
				A *uint8 `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadUint8PtrNilString",
			data: &struct {
				A *uint8 `json:"a,string"`
			}{A: nil},
		},

		// PtrHeadUint8Nil
		{
			name: "PtrHeadUint8Nil",
			data: (*struct {
				A *uint8 `json:"a"`
			})(nil),
		},
		{
			name: "PtrHeadUint8NilOmitEmpty",
			data: (*struct {
				A *uint8 `json:"a,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadUint8NilString",
			data: (*struct {
				A *uint8 `json:"a,string"`
			})(nil),
		},

		// HeadUint8ZeroMultiFields
		{
			name: "HeadUint8ZeroMultiFields",
			data: struct {
				A uint8 `json:"a"`
				B uint8 `json:"b"`
				C uint8 `json:"c"`
			}{},
		},
		{
			name: "HeadUint8ZeroMultiFieldsOmitEmpty",
			data: struct {
				A uint8 `json:"a,omitempty"`
				B uint8 `json:"b,omitempty"`
				C uint8 `json:"c,omitempty"`
			}{},
		},
		{
			name: "HeadUint8ZeroMultiFields",
			data: struct {
				A uint8 `json:"a,string"`
				B uint8 `json:"b,string"`
				C uint8 `json:"c,string"`
			}{},
		},

		// HeadUint8MultiFields
		{
			name: "HeadUint8MultiFields",
			data: struct {
				A uint8 `json:"a"`
				B uint8 `json:"b"`
				C uint8 `json:"c"`
			}{A: 1, B: 2, C: 3},
		},
		{
			name: "HeadUint8MultiFieldsOmitEmpty",
			data: struct {
				A uint8 `json:"a,omitempty"`
				B uint8 `json:"b,omitempty"`
				C uint8 `json:"c,omitempty"`
			}{A: 1, B: 2, C: 3},
		},
		{
			name: "HeadUint8MultiFieldsString",
			data: struct {
				A uint8 `json:"a,string"`
				B uint8 `json:"b,string"`
				C uint8 `json:"c,string"`
			}{A: 1, B: 2, C: 3},
		},

		// HeadUint8PtrMultiFields
		{
			name: "HeadUint8PtrMultiFields",
			data: struct {
				A *uint8 `json:"a"`
				B *uint8 `json:"b"`
				C *uint8 `json:"c"`
			}{A: uint8ptr(1), B: uint8ptr(2), C: uint8ptr(3)},
		},
		{
			name: "HeadUint8PtrMultiFieldsOmitEmpty",
			data: struct {
				A *uint8 `json:"a,omitempty"`
				B *uint8 `json:"b,omitempty"`
				C *uint8 `json:"c,omitempty"`
			}{A: uint8ptr(1), B: uint8ptr(2), C: uint8ptr(3)},
		},
		{
			name: "HeadUint8PtrMultiFieldsString",
			data: struct {
				A *uint8 `json:"a,string"`
				B *uint8 `json:"b,string"`
				C *uint8 `json:"c,string"`
			}{A: uint8ptr(1), B: uint8ptr(2), C: uint8ptr(3)},
		},

		// HeadUint8PtrNilMultiFields
		{
			name: "HeadUint8PtrNilMultiFields",
			data: struct {
				A *uint8 `json:"a"`
				B *uint8 `json:"b"`
				C *uint8 `json:"c"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadUint8PtrNilMultiFieldsOmitEmpty",
			data: struct {
				A *uint8 `json:"a,omitempty"`
				B *uint8 `json:"b,omitempty"`
				C *uint8 `json:"c,omitempty"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadUint8PtrNilMultiFieldsString",
			data: struct {
				A *uint8 `json:"a,string"`
				B *uint8 `json:"b,string"`
				C *uint8 `json:"c,string"`
			}{A: nil, B: nil, C: nil},
		},

		// PtrHeadUint8ZeroMultiFields
		{
			name: "PtrHeadUint8ZeroMultiFields",
			data: &struct {
				A uint8 `json:"a"`
				B uint8 `json:"b"`
			}{},
		},
		{
			name: "PtrHeadUint8ZeroMultiFieldsOmitEmpty",
			data: &struct {
				A uint8 `json:"a,omitempty"`
				B uint8 `json:"b,omitempty"`
			}{},
		},
		{
			name: "PtrHeadUint8ZeroMultiFieldsString",
			data: &struct {
				A uint8 `json:"a,string"`
				B uint8 `json:"b,string"`
			}{},
		},

		// PtrHeadUint8MultiFields
		{
			name: "PtrHeadUint8MultiFields",
			data: &struct {
				A uint8 `json:"a"`
				B uint8 `json:"b"`
			}{A: 1, B: 2},
		},
		{
			name: "PtrHeadUint8MultiFieldsOmitEmpty",
			data: &struct {
				A uint8 `json:"a,omitempty"`
				B uint8 `json:"b,omitempty"`
			}{A: 1, B: 2},
		},
		{
			name: "PtrHeadUint8MultiFieldsString",
			data: &struct {
				A uint8 `json:"a,string"`
				B uint8 `json:"b,string"`
			}{A: 1, B: 2},
		},

		// PtrHeadUint8PtrMultiFields
		{
			name: "PtrHeadUint8PtrMultiFields",
			data: &struct {
				A *uint8 `json:"a"`
				B *uint8 `json:"b"`
			}{A: uint8ptr(1), B: uint8ptr(2)},
		},
		{
			name: "PtrHeadUint8PtrMultiFieldsOmitEmpty",
			data: &struct {
				A *uint8 `json:"a,omitempty"`
				B *uint8 `json:"b,omitempty"`
			}{A: uint8ptr(1), B: uint8ptr(2)},
		},
		{
			name: "PtrHeadUint8PtrMultiFieldsString",
			data: &struct {
				A *uint8 `json:"a,string"`
				B *uint8 `json:"b,string"`
			}{A: uint8ptr(1), B: uint8ptr(2)},
		},

		// PtrHeadUint8PtrNilMultiFields
		{
			name: "PtrHeadUint8PtrNilMultiFields",
			data: &struct {
				A *uint8 `json:"a"`
				B *uint8 `json:"b"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUint8PtrNilMultiFieldsOmitEmpty",
			data: &struct {
				A *uint8 `json:"a,omitempty"`
				B *uint8 `json:"b,omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUint8PtrNilMultiFieldsString",
			data: &struct {
				A *uint8 `json:"a,string"`
				B *uint8 `json:"b,string"`
			}{A: nil, B: nil},
		},

		// PtrHeadUint8NilMultiFields
		{
			name: "PtrHeadUint8NilMultiFields",
			data: (*struct {
				A *uint8 `json:"a"`
				B *uint8 `json:"b"`
			})(nil),
		},
		{
			name: "PtrHeadUint8NilMultiFieldsOmitEmpty",
			data: (*struct {
				A *uint8 `json:"a,omitempty"`
				B *uint8 `json:"b,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadUint8NilMultiFieldsString",
			data: (*struct {
				A *uint8 `json:"a,string"`
				B *uint8 `json:"b,string"`
			})(nil),
		},

		// HeadUint8ZeroNotRoot
		{
			name: "HeadUint8ZeroNotRoot",
			data: struct {
				A struct {
					A uint8 `json:"a"`
				}
			}{},
		},
		{
			name: "HeadUint8ZeroNotRootOmitEmpty",
			data: struct {
				A struct {
					A uint8 `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadUint8ZeroNotRootString",
			data: struct {
				A struct {
					A uint8 `json:"a,string"`
				}
			}{},
		},

		// HeadUint8NotRoot
		{
			name: "HeadUint8NotRoot",
			data: struct {
				A struct {
					A uint8 `json:"a"`
				}
			}{A: struct {
				A uint8 `json:"a"`
			}{A: 1}},
		},
		{
			name: "HeadUint8NotRootOmitEmpty",
			data: struct {
				A struct {
					A uint8 `json:"a,omitempty"`
				}
			}{A: struct {
				A uint8 `json:"a,omitempty"`
			}{A: 1}},
		},
		{
			name: "HeadUint8NotRootString",
			data: struct {
				A struct {
					A uint8 `json:"a,string"`
				}
			}{A: struct {
				A uint8 `json:"a,string"`
			}{A: 1}},
		},

		// HeadUint8PtrNotRoot
		{
			name: "HeadUint8PtrNotRoot",
			data: struct {
				A struct {
					A *uint8 `json:"a"`
				}
			}{A: struct {
				A *uint8 `json:"a"`
			}{uint8ptr(1)}},
		},
		{
			name: "HeadUint8PtrNotRootOmitEmpty",
			data: struct {
				A struct {
					A *uint8 `json:"a,omitempty"`
				}
			}{A: struct {
				A *uint8 `json:"a,omitempty"`
			}{uint8ptr(1)}},
		},
		{
			name: "HeadUint8PtrNotRootString",
			data: struct {
				A struct {
					A *uint8 `json:"a,string"`
				}
			}{A: struct {
				A *uint8 `json:"a,string"`
			}{uint8ptr(1)}},
		},

		// HeadUint8PtrNilNotRoot
		{
			name: "HeadUint8PtrNilNotRoot",
			data: struct {
				A struct {
					A *uint8 `json:"a"`
				}
			}{},
		},
		{
			name: "HeadUint8PtrNilNotRootOmitEmpty",
			data: struct {
				A struct {
					A *uint8 `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadUint8PtrNilNotRootString",
			data: struct {
				A struct {
					A *uint8 `json:"a,string"`
				}
			}{},
		},

		// PtrHeadUint8ZeroNotRoot
		{
			name: "PtrHeadUint8ZeroNotRoot",
			data: struct {
				A *struct {
					A uint8 `json:"a"`
				}
			}{A: new(struct {
				A uint8 `json:"a"`
			})},
		},
		{
			name: "PtrHeadUint8ZeroNotRootOmitEmpty",
			data: struct {
				A *struct {
					A uint8 `json:"a,omitempty"`
				}
			}{A: new(struct {
				A uint8 `json:"a,omitempty"`
			})},
		},
		{
			name: "PtrHeadUint8ZeroNotRootString",
			data: struct {
				A *struct {
					A uint8 `json:"a,string"`
				}
			}{A: new(struct {
				A uint8 `json:"a,string"`
			})},
		},

		// PtrHeadUint8NotRoot
		{
			name: "PtrHeadUint8NotRoot",
			data: struct {
				A *struct {
					A uint8 `json:"a"`
				}
			}{A: &(struct {
				A uint8 `json:"a"`
			}{A: 1})},
		},
		{
			name: "PtrHeadUint8NotRootOmitEmpty",
			data: struct {
				A *struct {
					A uint8 `json:"a,omitempty"`
				}
			}{A: &(struct {
				A uint8 `json:"a,omitempty"`
			}{A: 1})},
		},
		{
			name: "PtrHeadUint8NotRootString",
			data: struct {
				A *struct {
					A uint8 `json:"a,string"`
				}
			}{A: &(struct {
				A uint8 `json:"a,string"`
			}{A: 1})},
		},

		// PtrHeadUint8PtrNotRoot
		{
			name: "PtrHeadUint8PtrNotRoot",
			data: struct {
				A *struct {
					A *uint8 `json:"a"`
				}
			}{A: &(struct {
				A *uint8 `json:"a"`
			}{A: uint8ptr(1)})},
		},
		{
			name: "PtrHeadUint8PtrNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *uint8 `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *uint8 `json:"a,omitempty"`
			}{A: uint8ptr(1)})},
		},
		{
			name: "PtrHeadUint8PtrNotRootString",
			data: struct {
				A *struct {
					A *uint8 `json:"a,string"`
				}
			}{A: &(struct {
				A *uint8 `json:"a,string"`
			}{A: uint8ptr(1)})},
		},

		// PtrHeadUint8PtrNilNotRoot
		{
			name: "PtrHeadUint8PtrNilNotRoot",
			data: struct {
				A *struct {
					A *uint8 `json:"a"`
				}
			}{A: &(struct {
				A *uint8 `json:"a"`
			}{A: nil})},
		},
		{
			name: "PtrHeadUint8PtrNilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *uint8 `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *uint8 `json:"a,omitempty"`
			}{A: nil})},
		},
		{
			name: "PtrHeadUint8PtrNilNotRootString",
			data: struct {
				A *struct {
					A *uint8 `json:"a,string"`
				}
			}{A: &(struct {
				A *uint8 `json:"a,string"`
			}{A: nil})},
		},

		// PtrHeadUint8NilNotRoot
		{
			name: "PtrHeadUint8NilNotRoot",
			data: struct {
				A *struct {
					A *uint8 `json:"a"`
				}
			}{A: nil},
		},
		{
			name: "PtrHeadUint8NilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *uint8 `json:"a,omitempty"`
				} `json:",omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadUint8NilNotRootString",
			data: struct {
				A *struct {
					A *uint8 `json:"a,string"`
				} `json:",string"`
			}{A: nil},
		},

		// HeadUint8ZeroMultiFieldsNotRoot
		{
			name: "HeadUint8ZeroMultiFieldsNotRoot",
			data: struct {
				A struct {
					A uint8 `json:"a"`
				}
				B struct {
					B uint8 `json:"b"`
				}
			}{},
		},
		{
			name: "HeadUint8ZeroMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A uint8 `json:"a,omitempty"`
				}
				B struct {
					B uint8 `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "HeadUint8ZeroMultiFieldsNotRootString",
			data: struct {
				A struct {
					A uint8 `json:"a,string"`
				}
				B struct {
					B uint8 `json:"b,string"`
				}
			}{},
		},

		// HeadUint8MultiFieldsNotRoot
		{
			name: "HeadUint8MultiFieldsNotRoot",
			data: struct {
				A struct {
					A uint8 `json:"a"`
				}
				B struct {
					B uint8 `json:"b"`
				}
			}{A: struct {
				A uint8 `json:"a"`
			}{A: 1}, B: struct {
				B uint8 `json:"b"`
			}{B: 2}},
		},
		{
			name: "HeadUint8MultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A uint8 `json:"a,omitempty"`
				}
				B struct {
					B uint8 `json:"b,omitempty"`
				}
			}{A: struct {
				A uint8 `json:"a,omitempty"`
			}{A: 1}, B: struct {
				B uint8 `json:"b,omitempty"`
			}{B: 2}},
		},
		{
			name: "HeadUint8MultiFieldsNotRootString",
			data: struct {
				A struct {
					A uint8 `json:"a,string"`
				}
				B struct {
					B uint8 `json:"b,string"`
				}
			}{A: struct {
				A uint8 `json:"a,string"`
			}{A: 1}, B: struct {
				B uint8 `json:"b,string"`
			}{B: 2}},
		},

		// HeadUint8PtrMultiFieldsNotRoot
		{
			name: "HeadUint8PtrMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *uint8 `json:"a"`
				}
				B struct {
					B *uint8 `json:"b"`
				}
			}{A: struct {
				A *uint8 `json:"a"`
			}{A: uint8ptr(1)}, B: struct {
				B *uint8 `json:"b"`
			}{B: uint8ptr(2)}},
		},
		{
			name: "HeadUint8PtrMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *uint8 `json:"a,omitempty"`
				}
				B struct {
					B *uint8 `json:"b,omitempty"`
				}
			}{A: struct {
				A *uint8 `json:"a,omitempty"`
			}{A: uint8ptr(1)}, B: struct {
				B *uint8 `json:"b,omitempty"`
			}{B: uint8ptr(2)}},
		},
		{
			name: "HeadUint8PtrMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *uint8 `json:"a,string"`
				}
				B struct {
					B *uint8 `json:"b,string"`
				}
			}{A: struct {
				A *uint8 `json:"a,string"`
			}{A: uint8ptr(1)}, B: struct {
				B *uint8 `json:"b,string"`
			}{B: uint8ptr(2)}},
		},

		// HeadUint8PtrNilMultiFieldsNotRoot
		{
			name: "HeadUint8PtrNilMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *uint8 `json:"a"`
				}
				B struct {
					B *uint8 `json:"b"`
				}
			}{A: struct {
				A *uint8 `json:"a"`
			}{A: nil}, B: struct {
				B *uint8 `json:"b"`
			}{B: nil}},
		},
		{
			name: "HeadUint8PtrNilMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *uint8 `json:"a,omitempty"`
				}
				B struct {
					B *uint8 `json:"b,omitempty"`
				}
			}{A: struct {
				A *uint8 `json:"a,omitempty"`
			}{A: nil}, B: struct {
				B *uint8 `json:"b,omitempty"`
			}{B: nil}},
		},
		{
			name: "HeadUint8PtrNilMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *uint8 `json:"a,string"`
				}
				B struct {
					B *uint8 `json:"b,string"`
				}
			}{A: struct {
				A *uint8 `json:"a,string"`
			}{A: nil}, B: struct {
				B *uint8 `json:"b,string"`
			}{B: nil}},
		},

		// PtrHeadUint8ZeroMultiFieldsNotRoot
		{
			name: "PtrHeadUint8ZeroMultiFieldsNotRoot",
			data: &struct {
				A struct {
					A uint8 `json:"a"`
				}
				B struct {
					B uint8 `json:"b"`
				}
			}{},
		},
		{
			name: "PtrHeadUint8ZeroMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A uint8 `json:"a,omitempty"`
				}
				B struct {
					B uint8 `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "PtrHeadUint8ZeroMultiFieldsNotRootString",
			data: &struct {
				A struct {
					A uint8 `json:"a,string"`
				}
				B struct {
					B uint8 `json:"b,string"`
				}
			}{},
		},

		// PtrHeadUint8MultiFieldsNotRoot
		{
			name: "PtrHeadUint8MultiFieldsNotRoot",
			data: &struct {
				A struct {
					A uint8 `json:"a"`
				}
				B struct {
					B uint8 `json:"b"`
				}
			}{A: struct {
				A uint8 `json:"a"`
			}{A: 1}, B: struct {
				B uint8 `json:"b"`
			}{B: 2}},
		},
		{
			name: "PtrHeadUint8MultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A uint8 `json:"a,omitempty"`
				}
				B struct {
					B uint8 `json:"b,omitempty"`
				}
			}{A: struct {
				A uint8 `json:"a,omitempty"`
			}{A: 1}, B: struct {
				B uint8 `json:"b,omitempty"`
			}{B: 2}},
		},
		{
			name: "PtrHeadUint8MultiFieldsNotRootString",
			data: &struct {
				A struct {
					A uint8 `json:"a,string"`
				}
				B struct {
					B uint8 `json:"b,string"`
				}
			}{A: struct {
				A uint8 `json:"a,string"`
			}{A: 1}, B: struct {
				B uint8 `json:"b,string"`
			}{B: 2}},
		},

		// PtrHeadUint8PtrMultiFieldsNotRoot
		{
			name: "PtrHeadUint8PtrMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *uint8 `json:"a"`
				}
				B *struct {
					B *uint8 `json:"b"`
				}
			}{A: &(struct {
				A *uint8 `json:"a"`
			}{A: uint8ptr(1)}), B: &(struct {
				B *uint8 `json:"b"`
			}{B: uint8ptr(2)})},
		},
		{
			name: "PtrHeadUint8PtrMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *uint8 `json:"a,omitempty"`
				}
				B *struct {
					B *uint8 `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *uint8 `json:"a,omitempty"`
			}{A: uint8ptr(1)}), B: &(struct {
				B *uint8 `json:"b,omitempty"`
			}{B: uint8ptr(2)})},
		},
		{
			name: "PtrHeadUint8PtrMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *uint8 `json:"a,string"`
				}
				B *struct {
					B *uint8 `json:"b,string"`
				}
			}{A: &(struct {
				A *uint8 `json:"a,string"`
			}{A: uint8ptr(1)}), B: &(struct {
				B *uint8 `json:"b,string"`
			}{B: uint8ptr(2)})},
		},

		// PtrHeadUint8PtrNilMultiFieldsNotRoot
		{
			name: "PtrHeadUint8PtrNilMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *uint8 `json:"a"`
				}
				B *struct {
					B *uint8 `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUint8PtrNilMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *uint8 `json:"a,omitempty"`
				} `json:",omitempty"`
				B *struct {
					B *uint8 `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUint8PtrNilMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *uint8 `json:"a,string"`
				} `json:",string"`
				B *struct {
					B *uint8 `json:"b,string"`
				} `json:",string"`
			}{A: nil, B: nil},
		},

		// PtrHeadUint8NilMultiFieldsNotRoot
		{
			name: "PtrHeadUint8NilMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *uint8 `json:"a"`
				}
				B *struct {
					B *uint8 `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadUint8NilMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *uint8 `json:"a,omitempty"`
				}
				B *struct {
					B *uint8 `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadUint8NilMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *uint8 `json:"a,string"`
				}
				B *struct {
					B *uint8 `json:"b,string"`
				}
			})(nil),
		},

		// PtrHeadUint8DoubleMultiFieldsNotRoot
		{
			name: "PtrHeadUint8DoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A uint8 `json:"a"`
					B uint8 `json:"b"`
				}
				B *struct {
					A uint8 `json:"a"`
					B uint8 `json:"b"`
				}
			}{A: &(struct {
				A uint8 `json:"a"`
				B uint8 `json:"b"`
			}{A: 1, B: 2}), B: &(struct {
				A uint8 `json:"a"`
				B uint8 `json:"b"`
			}{A: 3, B: 4})},
		},
		{
			name: "PtrHeadUint8DoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A uint8 `json:"a,omitempty"`
					B uint8 `json:"b,omitempty"`
				}
				B *struct {
					A uint8 `json:"a,omitempty"`
					B uint8 `json:"b,omitempty"`
				}
			}{A: &(struct {
				A uint8 `json:"a,omitempty"`
				B uint8 `json:"b,omitempty"`
			}{A: 1, B: 2}), B: &(struct {
				A uint8 `json:"a,omitempty"`
				B uint8 `json:"b,omitempty"`
			}{A: 3, B: 4})},
		},
		{
			name: "PtrHeadUint8DoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A uint8 `json:"a,string"`
					B uint8 `json:"b,string"`
				}
				B *struct {
					A uint8 `json:"a,string"`
					B uint8 `json:"b,string"`
				}
			}{A: &(struct {
				A uint8 `json:"a,string"`
				B uint8 `json:"b,string"`
			}{A: 1, B: 2}), B: &(struct {
				A uint8 `json:"a,string"`
				B uint8 `json:"b,string"`
			}{A: 3, B: 4})},
		},

		// PtrHeadUint8NilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadUint8NilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A uint8 `json:"a"`
					B uint8 `json:"b"`
				}
				B *struct {
					A uint8 `json:"a"`
					B uint8 `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUint8NilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A uint8 `json:"a,omitempty"`
					B uint8 `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A uint8 `json:"a,omitempty"`
					B uint8 `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUint8NilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A uint8 `json:"a,string"`
					B uint8 `json:"b,string"`
				}
				B *struct {
					A uint8 `json:"a,string"`
					B uint8 `json:"b,string"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadUint8NilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadUint8NilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A uint8 `json:"a"`
					B uint8 `json:"b"`
				}
				B *struct {
					A uint8 `json:"a"`
					B uint8 `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadUint8NilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A uint8 `json:"a,omitempty"`
					B uint8 `json:"b,omitempty"`
				}
				B *struct {
					A uint8 `json:"a,omitempty"`
					B uint8 `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadUint8NilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A uint8 `json:"a,string"`
					B uint8 `json:"b,string"`
				}
				B *struct {
					A uint8 `json:"a,string"`
					B uint8 `json:"b,string"`
				}
			})(nil),
		},

		// PtrHeadUint8PtrDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadUint8PtrDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *uint8 `json:"a"`
					B *uint8 `json:"b"`
				}
				B *struct {
					A *uint8 `json:"a"`
					B *uint8 `json:"b"`
				}
			}{A: &(struct {
				A *uint8 `json:"a"`
				B *uint8 `json:"b"`
			}{A: uint8ptr(1), B: uint8ptr(2)}), B: &(struct {
				A *uint8 `json:"a"`
				B *uint8 `json:"b"`
			}{A: uint8ptr(3), B: uint8ptr(4)})},
		},
		{
			name: "PtrHeadUint8PtrDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *uint8 `json:"a,omitempty"`
					B *uint8 `json:"b,omitempty"`
				}
				B *struct {
					A *uint8 `json:"a,omitempty"`
					B *uint8 `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *uint8 `json:"a,omitempty"`
				B *uint8 `json:"b,omitempty"`
			}{A: uint8ptr(1), B: uint8ptr(2)}), B: &(struct {
				A *uint8 `json:"a,omitempty"`
				B *uint8 `json:"b,omitempty"`
			}{A: uint8ptr(3), B: uint8ptr(4)})},
		},
		{
			name: "PtrHeadUint8PtrDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *uint8 `json:"a,string"`
					B *uint8 `json:"b,string"`
				}
				B *struct {
					A *uint8 `json:"a,string"`
					B *uint8 `json:"b,string"`
				}
			}{A: &(struct {
				A *uint8 `json:"a,string"`
				B *uint8 `json:"b,string"`
			}{A: uint8ptr(1), B: uint8ptr(2)}), B: &(struct {
				A *uint8 `json:"a,string"`
				B *uint8 `json:"b,string"`
			}{A: uint8ptr(3), B: uint8ptr(4)})},
		},

		// PtrHeadUint8PtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadUint8PtrNilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *uint8 `json:"a"`
					B *uint8 `json:"b"`
				}
				B *struct {
					A *uint8 `json:"a"`
					B *uint8 `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUint8PtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *uint8 `json:"a,omitempty"`
					B *uint8 `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A *uint8 `json:"a,omitempty"`
					B *uint8 `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUint8PtrNilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *uint8 `json:"a,string"`
					B *uint8 `json:"b,string"`
				}
				B *struct {
					A *uint8 `json:"a,string"`
					B *uint8 `json:"b,string"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadUint8PtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadUint8PtrNilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *uint8 `json:"a"`
					B *uint8 `json:"b"`
				}
				B *struct {
					A *uint8 `json:"a"`
					B *uint8 `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadUint8PtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *uint8 `json:"a,omitempty"`
					B *uint8 `json:"b,omitempty"`
				}
				B *struct {
					A *uint8 `json:"a,omitempty"`
					B *uint8 `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadUint8PtrNilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *uint8 `json:"a,string"`
					B *uint8 `json:"b,string"`
				}
				B *struct {
					A *uint8 `json:"a,string"`
					B *uint8 `json:"b,string"`
				}
			})(nil),
		},

		// AnonymousHeadUint8
		{
			name: "AnonymousHeadUint8",
			data: struct {
				structUint8
				B uint8 `json:"b"`
			}{
				structUint8: structUint8{A: 1},
				B:           2,
			},
		},
		{
			name: "AnonymousHeadUint8OmitEmpty",
			data: struct {
				structUint8OmitEmpty
				B uint8 `json:"b,omitempty"`
			}{
				structUint8OmitEmpty: structUint8OmitEmpty{A: 1},
				B:                    2,
			},
		},
		{
			name: "AnonymousHeadUint8String",
			data: struct {
				structUint8String
				B uint8 `json:"b,string"`
			}{
				structUint8String: structUint8String{A: 1},
				B:                 2,
			},
		},

		// PtrAnonymousHeadUint8
		{
			name: "PtrAnonymousHeadUint8",
			data: struct {
				*structUint8
				B uint8 `json:"b"`
			}{
				structUint8: &structUint8{A: 1},
				B:           2,
			},
		},
		{
			name: "PtrAnonymousHeadUint8OmitEmpty",
			data: struct {
				*structUint8OmitEmpty
				B uint8 `json:"b,omitempty"`
			}{
				structUint8OmitEmpty: &structUint8OmitEmpty{A: 1},
				B:                    2,
			},
		},
		{
			name: "PtrAnonymousHeadUint8String",
			data: struct {
				*structUint8String
				B uint8 `json:"b,string"`
			}{
				structUint8String: &structUint8String{A: 1},
				B:                 2,
			},
		},

		// NilPtrAnonymousHeadUint8
		{
			name: "NilPtrAnonymousHeadUint8",
			data: struct {
				*structUint8
				B uint8 `json:"b"`
			}{
				structUint8: nil,
				B:           2,
			},
		},
		{
			name: "NilPtrAnonymousHeadUint8OmitEmpty",
			data: struct {
				*structUint8OmitEmpty
				B uint8 `json:"b,omitempty"`
			}{
				structUint8OmitEmpty: nil,
				B:                    2,
			},
		},
		{
			name: "NilPtrAnonymousHeadUint8String",
			data: struct {
				*structUint8String
				B uint8 `json:"b,string"`
			}{
				structUint8String: nil,
				B:                 2,
			},
		},

		// AnonymousHeadUint8Ptr
		{
			name: "AnonymousHeadUint8Ptr",
			data: struct {
				structUint8Ptr
				B *uint8 `json:"b"`
			}{
				structUint8Ptr: structUint8Ptr{A: uint8ptr(1)},
				B:              uint8ptr(2),
			},
		},
		{
			name: "AnonymousHeadUint8PtrOmitEmpty",
			data: struct {
				structUint8PtrOmitEmpty
				B *uint8 `json:"b,omitempty"`
			}{
				structUint8PtrOmitEmpty: structUint8PtrOmitEmpty{A: uint8ptr(1)},
				B:                       uint8ptr(2),
			},
		},
		{
			name: "AnonymousHeadUint8PtrString",
			data: struct {
				structUint8PtrString
				B *uint8 `json:"b,string"`
			}{
				structUint8PtrString: structUint8PtrString{A: uint8ptr(1)},
				B:                    uint8ptr(2),
			},
		},

		// AnonymousHeadUint8PtrNil
		{
			name: "AnonymousHeadUint8PtrNil",
			data: struct {
				structUint8Ptr
				B *uint8 `json:"b"`
			}{
				structUint8Ptr: structUint8Ptr{A: nil},
				B:              uint8ptr(2),
			},
		},
		{
			name: "AnonymousHeadUint8PtrNilOmitEmpty",
			data: struct {
				structUint8PtrOmitEmpty
				B *uint8 `json:"b,omitempty"`
			}{
				structUint8PtrOmitEmpty: structUint8PtrOmitEmpty{A: nil},
				B:                       uint8ptr(2),
			},
		},
		{
			name: "AnonymousHeadUint8PtrNilString",
			data: struct {
				structUint8PtrString
				B *uint8 `json:"b,string"`
			}{
				structUint8PtrString: structUint8PtrString{A: nil},
				B:                    uint8ptr(2),
			},
		},

		// PtrAnonymousHeadUint8Ptr
		{
			name: "PtrAnonymousHeadUint8Ptr",
			data: struct {
				*structUint8Ptr
				B *uint8 `json:"b"`
			}{
				structUint8Ptr: &structUint8Ptr{A: uint8ptr(1)},
				B:              uint8ptr(2),
			},
		},
		{
			name: "PtrAnonymousHeadUint8PtrOmitEmpty",
			data: struct {
				*structUint8PtrOmitEmpty
				B *uint8 `json:"b,omitempty"`
			}{
				structUint8PtrOmitEmpty: &structUint8PtrOmitEmpty{A: uint8ptr(1)},
				B:                       uint8ptr(2),
			},
		},
		{
			name: "PtrAnonymousHeadUint8PtrString",
			data: struct {
				*structUint8PtrString
				B *uint8 `json:"b,string"`
			}{
				structUint8PtrString: &structUint8PtrString{A: uint8ptr(1)},
				B:                    uint8ptr(2),
			},
		},

		// NilPtrAnonymousHeadUint8Ptr
		{
			name: "NilPtrAnonymousHeadUint8Ptr",
			data: struct {
				*structUint8Ptr
				B *uint8 `json:"b"`
			}{
				structUint8Ptr: nil,
				B:              uint8ptr(2),
			},
		},
		{
			name: "NilPtrAnonymousHeadUint8PtrOmitEmpty",
			data: struct {
				*structUint8PtrOmitEmpty
				B *uint8 `json:"b,omitempty"`
			}{
				structUint8PtrOmitEmpty: nil,
				B:                       uint8ptr(2),
			},
		},
		{
			name: "NilPtrAnonymousHeadUint8PtrString",
			data: struct {
				*structUint8PtrString
				B *uint8 `json:"b,string"`
			}{
				structUint8PtrString: nil,
				B:                    uint8ptr(2),
			},
		},

		// AnonymousHeadUint8Only
		{
			name: "AnonymousHeadUint8Only",
			data: struct {
				structUint8
			}{
				structUint8: structUint8{A: 1},
			},
		},
		{
			name: "AnonymousHeadUint8OnlyOmitEmpty",
			data: struct {
				structUint8OmitEmpty
			}{
				structUint8OmitEmpty: structUint8OmitEmpty{A: 1},
			},
		},
		{
			name: "AnonymousHeadUint8OnlyString",
			data: struct {
				structUint8String
			}{
				structUint8String: structUint8String{A: 1},
			},
		},

		// PtrAnonymousHeadUint8Only
		{
			name: "PtrAnonymousHeadUint8Only",
			data: struct {
				*structUint8
			}{
				structUint8: &structUint8{A: 1},
			},
		},
		{
			name: "PtrAnonymousHeadUint8OnlyOmitEmpty",
			data: struct {
				*structUint8OmitEmpty
			}{
				structUint8OmitEmpty: &structUint8OmitEmpty{A: 1},
			},
		},
		{
			name: "PtrAnonymousHeadUint8OnlyString",
			data: struct {
				*structUint8String
			}{
				structUint8String: &structUint8String{A: 1},
			},
		},

		// NilPtrAnonymousHeadUint8Only
		{
			name: "NilPtrAnonymousHeadUint8Only",
			data: struct {
				*structUint8
			}{
				structUint8: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadUint8OnlyOmitEmpty",
			data: struct {
				*structUint8OmitEmpty
			}{
				structUint8OmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadUint8OnlyString",
			data: struct {
				*structUint8String
			}{
				structUint8String: nil,
			},
		},

		// AnonymousHeadUint8PtrOnly
		{
			name: "AnonymousHeadUint8PtrOnly",
			data: struct {
				structUint8Ptr
			}{
				structUint8Ptr: structUint8Ptr{A: uint8ptr(1)},
			},
		},
		{
			name: "AnonymousHeadUint8PtrOnlyOmitEmpty",
			data: struct {
				structUint8PtrOmitEmpty
			}{
				structUint8PtrOmitEmpty: structUint8PtrOmitEmpty{A: uint8ptr(1)},
			},
		},
		{
			name: "AnonymousHeadUint8PtrOnlyString",
			data: struct {
				structUint8PtrString
			}{
				structUint8PtrString: structUint8PtrString{A: uint8ptr(1)},
			},
		},

		// AnonymousHeadUint8PtrNilOnly
		{
			name: "AnonymousHeadUint8PtrNilOnly",
			data: struct {
				structUint8Ptr
			}{
				structUint8Ptr: structUint8Ptr{A: nil},
			},
		},
		{
			name: "AnonymousHeadUint8PtrNilOnlyOmitEmpty",
			data: struct {
				structUint8PtrOmitEmpty
			}{
				structUint8PtrOmitEmpty: structUint8PtrOmitEmpty{A: nil},
			},
		},
		{
			name: "AnonymousHeadUint8PtrNilOnlyString",
			data: struct {
				structUint8PtrString
			}{
				structUint8PtrString: structUint8PtrString{A: nil},
			},
		},

		// PtrAnonymousHeadUint8PtrOnly
		{
			name: "PtrAnonymousHeadUint8PtrOnly",
			data: struct {
				*structUint8Ptr
			}{
				structUint8Ptr: &structUint8Ptr{A: uint8ptr(1)},
			},
		},
		{
			name: "PtrAnonymousHeadUint8PtrOnlyOmitEmpty",
			data: struct {
				*structUint8PtrOmitEmpty
			}{
				structUint8PtrOmitEmpty: &structUint8PtrOmitEmpty{A: uint8ptr(1)},
			},
		},
		{
			name: "PtrAnonymousHeadUint8PtrOnlyString",
			data: struct {
				*structUint8PtrString
			}{
				structUint8PtrString: &structUint8PtrString{A: uint8ptr(1)},
			},
		},

		// NilPtrAnonymousHeadUint8PtrOnly
		{
			name: "NilPtrAnonymousHeadUint8PtrOnly",
			data: struct {
				*structUint8Ptr
			}{
				structUint8Ptr: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadUint8PtrOnlyOmitEmpty",
			data: struct {
				*structUint8PtrOmitEmpty
			}{
				structUint8PtrOmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadUint8PtrOnlyString",
			data: struct {
				*structUint8PtrString
			}{
				structUint8PtrString: nil,
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
						t.Fatalf("%s(htmlEscape:%T): %+v: %s", test.name, htmlEscape, test.data, err)
					}
					stdresult := encodeByEncodingJSON(test.data, indent, htmlEscape)
					if buf.String() != stdresult {
						t.Errorf("%s(htmlEscape:%T): doesn't compatible with encoding/json. expected %q but got %q", test.name, htmlEscape, stdresult, buf.String())
					}
				})
			}
		}
	}
}
