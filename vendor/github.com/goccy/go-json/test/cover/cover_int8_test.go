package json_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/goccy/go-json"
)

func TestCoverInt8(t *testing.T) {
	type structInt8 struct {
		A int8 `json:"a"`
	}
	type structInt8OmitEmpty struct {
		A int8 `json:"a,omitempty"`
	}
	type structInt8String struct {
		A int8 `json:"a,string"`
	}

	type structInt8Ptr struct {
		A *int8 `json:"a"`
	}
	type structInt8PtrOmitEmpty struct {
		A *int8 `json:"a,omitempty"`
	}
	type structInt8PtrString struct {
		A *int8 `json:"a,string"`
	}

	tests := []struct {
		name string
		data interface{}
	}{
		{
			name: "Int8",
			data: int8(10),
		},
		{
			name: "Int8Ptr",
			data: int8ptr(10),
		},
		{
			name: "Int8Ptr3",
			data: int8ptr3(10),
		},
		{
			name: "Int8PtrNil",
			data: (*int8)(nil),
		},
		{
			name: "Int8Ptr3Nil",
			data: (***int8)(nil),
		},

		// HeadInt8Zero
		{
			name: "HeadInt8Zero",
			data: struct {
				A int8 `json:"a"`
			}{},
		},
		{
			name: "HeadInt8ZeroOmitEmpty",
			data: struct {
				A int8 `json:"a,omitempty"`
			}{},
		},
		{
			name: "HeadInt8ZeroString",
			data: struct {
				A int8 `json:"a,string"`
			}{},
		},

		// HeadInt8
		{
			name: "HeadInt8",
			data: struct {
				A int8 `json:"a"`
			}{A: -1},
		},
		{
			name: "HeadInt8OmitEmpty",
			data: struct {
				A int8 `json:"a,omitempty"`
			}{A: -1},
		},
		{
			name: "HeadInt8String",
			data: struct {
				A int8 `json:"a,string"`
			}{A: -1},
		},

		// HeadInt8Ptr
		{
			name: "HeadInt8Ptr",
			data: struct {
				A *int8 `json:"a"`
			}{A: int8ptr(-1)},
		},
		{
			name: "HeadInt8PtrOmitEmpty",
			data: struct {
				A *int8 `json:"a,omitempty"`
			}{A: int8ptr(-1)},
		},
		{
			name: "HeadInt8PtrString",
			data: struct {
				A *int8 `json:"a,string"`
			}{A: int8ptr(-1)},
		},

		// HeadInt8PtrNil
		{
			name: "HeadInt8PtrNil",
			data: struct {
				A *int8 `json:"a"`
			}{A: nil},
		},
		{
			name: "HeadInt8PtrNilOmitEmpty",
			data: struct {
				A *int8 `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "HeadInt8PtrNilString",
			data: struct {
				A *int8 `json:"a,string"`
			}{A: nil},
		},

		// PtrHeadInt8Zero
		{
			name: "PtrHeadInt8Zero",
			data: &struct {
				A int8 `json:"a"`
			}{},
		},
		{
			name: "PtrHeadInt8ZeroOmitEmpty",
			data: &struct {
				A int8 `json:"a,omitempty"`
			}{},
		},
		{
			name: "PtrHeadInt8ZeroString",
			data: &struct {
				A int8 `json:"a,string"`
			}{},
		},

		// PtrHeadInt8
		{
			name: "PtrHeadInt8",
			data: &struct {
				A int8 `json:"a"`
			}{A: -1},
		},
		{
			name: "PtrHeadInt8OmitEmpty",
			data: &struct {
				A int8 `json:"a,omitempty"`
			}{A: -1},
		},
		{
			name: "PtrHeadInt8String",
			data: &struct {
				A int8 `json:"a,string"`
			}{A: -1},
		},

		// PtrHeadInt8Ptr
		{
			name: "PtrHeadInt8Ptr",
			data: &struct {
				A *int8 `json:"a"`
			}{A: int8ptr(-1)},
		},
		{
			name: "PtrHeadInt8PtrOmitEmpty",
			data: &struct {
				A *int8 `json:"a,omitempty"`
			}{A: int8ptr(-1)},
		},
		{
			name: "PtrHeadInt8PtrString",
			data: &struct {
				A *int8 `json:"a,string"`
			}{A: int8ptr(-1)},
		},

		// PtrHeadInt8PtrNil
		{
			name: "PtrHeadInt8PtrNil",
			data: &struct {
				A *int8 `json:"a"`
			}{A: nil},
		},
		{
			name: "PtrHeadInt8PtrNilOmitEmpty",
			data: &struct {
				A *int8 `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadInt8PtrNilString",
			data: &struct {
				A *int8 `json:"a,string"`
			}{A: nil},
		},

		// PtrHeadInt8Nil
		{
			name: "PtrHeadInt8Nil",
			data: (*struct {
				A *int8 `json:"a"`
			})(nil),
		},
		{
			name: "PtrHeadInt8NilOmitEmpty",
			data: (*struct {
				A *int8 `json:"a,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadInt8NilString",
			data: (*struct {
				A *int8 `json:"a,string"`
			})(nil),
		},

		// HeadInt8ZeroMultiFields
		{
			name: "HeadInt8ZeroMultiFields",
			data: struct {
				A int8 `json:"a"`
				B int8 `json:"b"`
				C int8 `json:"c"`
			}{},
		},
		{
			name: "HeadInt8ZeroMultiFieldsOmitEmpty",
			data: struct {
				A int8 `json:"a,omitempty"`
				B int8 `json:"b,omitempty"`
				C int8 `json:"c,omitempty"`
			}{},
		},
		{
			name: "HeadInt8ZeroMultiFields",
			data: struct {
				A int8 `json:"a,string"`
				B int8 `json:"b,string"`
				C int8 `json:"c,string"`
			}{},
		},

		// HeadInt8MultiFields
		{
			name: "HeadInt8MultiFields",
			data: struct {
				A int8 `json:"a"`
				B int8 `json:"b"`
				C int8 `json:"c"`
			}{A: -1, B: 2, C: 3},
		},
		{
			name: "HeadInt8MultiFieldsOmitEmpty",
			data: struct {
				A int8 `json:"a,omitempty"`
				B int8 `json:"b,omitempty"`
				C int8 `json:"c,omitempty"`
			}{A: -1, B: 2, C: 3},
		},
		{
			name: "HeadInt8MultiFieldsString",
			data: struct {
				A int8 `json:"a,string"`
				B int8 `json:"b,string"`
				C int8 `json:"c,string"`
			}{A: -1, B: 2, C: 3},
		},

		// HeadInt8PtrMultiFields
		{
			name: "HeadInt8PtrMultiFields",
			data: struct {
				A *int8 `json:"a"`
				B *int8 `json:"b"`
				C *int8 `json:"c"`
			}{A: int8ptr(-1), B: int8ptr(2), C: int8ptr(3)},
		},
		{
			name: "HeadInt8PtrMultiFieldsOmitEmpty",
			data: struct {
				A *int8 `json:"a,omitempty"`
				B *int8 `json:"b,omitempty"`
				C *int8 `json:"c,omitempty"`
			}{A: int8ptr(-1), B: int8ptr(2), C: int8ptr(3)},
		},
		{
			name: "HeadInt8PtrMultiFieldsString",
			data: struct {
				A *int8 `json:"a,string"`
				B *int8 `json:"b,string"`
				C *int8 `json:"c,string"`
			}{A: int8ptr(-1), B: int8ptr(2), C: int8ptr(3)},
		},

		// HeadInt8PtrNilMultiFields
		{
			name: "HeadInt8PtrNilMultiFields",
			data: struct {
				A *int8 `json:"a"`
				B *int8 `json:"b"`
				C *int8 `json:"c"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadInt8PtrNilMultiFieldsOmitEmpty",
			data: struct {
				A *int8 `json:"a,omitempty"`
				B *int8 `json:"b,omitempty"`
				C *int8 `json:"c,omitempty"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadInt8PtrNilMultiFieldsString",
			data: struct {
				A *int8 `json:"a,string"`
				B *int8 `json:"b,string"`
				C *int8 `json:"c,string"`
			}{A: nil, B: nil, C: nil},
		},

		// PtrHeadInt8ZeroMultiFields
		{
			name: "PtrHeadInt8ZeroMultiFields",
			data: &struct {
				A int8 `json:"a"`
				B int8 `json:"b"`
			}{},
		},
		{
			name: "PtrHeadInt8ZeroMultiFieldsOmitEmpty",
			data: &struct {
				A int8 `json:"a,omitempty"`
				B int8 `json:"b,omitempty"`
			}{},
		},
		{
			name: "PtrHeadInt8ZeroMultiFieldsString",
			data: &struct {
				A int8 `json:"a,string"`
				B int8 `json:"b,string"`
			}{},
		},

		// PtrHeadInt8MultiFields
		{
			name: "PtrHeadInt8MultiFields",
			data: &struct {
				A int8 `json:"a"`
				B int8 `json:"b"`
			}{A: -1, B: 2},
		},
		{
			name: "PtrHeadInt8MultiFieldsOmitEmpty",
			data: &struct {
				A int8 `json:"a,omitempty"`
				B int8 `json:"b,omitempty"`
			}{A: -1, B: 2},
		},
		{
			name: "PtrHeadInt8MultiFieldsString",
			data: &struct {
				A int8 `json:"a,string"`
				B int8 `json:"b,string"`
			}{A: -1, B: 2},
		},

		// PtrHeadInt8PtrMultiFields
		{
			name: "PtrHeadInt8PtrMultiFields",
			data: &struct {
				A *int8 `json:"a"`
				B *int8 `json:"b"`
			}{A: int8ptr(-1), B: int8ptr(2)},
		},
		{
			name: "PtrHeadInt8PtrMultiFieldsOmitEmpty",
			data: &struct {
				A *int8 `json:"a,omitempty"`
				B *int8 `json:"b,omitempty"`
			}{A: int8ptr(-1), B: int8ptr(2)},
		},
		{
			name: "PtrHeadInt8PtrMultiFieldsString",
			data: &struct {
				A *int8 `json:"a,string"`
				B *int8 `json:"b,string"`
			}{A: int8ptr(-1), B: int8ptr(2)},
		},

		// PtrHeadInt8PtrNilMultiFields
		{
			name: "PtrHeadInt8PtrNilMultiFields",
			data: &struct {
				A *int8 `json:"a"`
				B *int8 `json:"b"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadInt8PtrNilMultiFieldsOmitEmpty",
			data: &struct {
				A *int8 `json:"a,omitempty"`
				B *int8 `json:"b,omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadInt8PtrNilMultiFieldsString",
			data: &struct {
				A *int8 `json:"a,string"`
				B *int8 `json:"b,string"`
			}{A: nil, B: nil},
		},

		// PtrHeadInt8NilMultiFields
		{
			name: "PtrHeadInt8NilMultiFields",
			data: (*struct {
				A *int8 `json:"a"`
				B *int8 `json:"b"`
			})(nil),
		},
		{
			name: "PtrHeadInt8NilMultiFieldsOmitEmpty",
			data: (*struct {
				A *int8 `json:"a,omitempty"`
				B *int8 `json:"b,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadInt8NilMultiFieldsString",
			data: (*struct {
				A *int8 `json:"a,string"`
				B *int8 `json:"b,string"`
			})(nil),
		},

		// HeadInt8ZeroNotRoot
		{
			name: "HeadInt8ZeroNotRoot",
			data: struct {
				A struct {
					A int8 `json:"a"`
				}
			}{},
		},
		{
			name: "HeadInt8ZeroNotRootOmitEmpty",
			data: struct {
				A struct {
					A int8 `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadInt8ZeroNotRootString",
			data: struct {
				A struct {
					A int8 `json:"a,string"`
				}
			}{},
		},

		// HeadInt8NotRoot
		{
			name: "HeadInt8NotRoot",
			data: struct {
				A struct {
					A int8 `json:"a"`
				}
			}{A: struct {
				A int8 `json:"a"`
			}{A: -1}},
		},
		{
			name: "HeadInt8NotRootOmitEmpty",
			data: struct {
				A struct {
					A int8 `json:"a,omitempty"`
				}
			}{A: struct {
				A int8 `json:"a,omitempty"`
			}{A: -1}},
		},
		{
			name: "HeadInt8NotRootString",
			data: struct {
				A struct {
					A int8 `json:"a,string"`
				}
			}{A: struct {
				A int8 `json:"a,string"`
			}{A: -1}},
		},

		// HeadInt8PtrNotRoot
		{
			name: "HeadInt8PtrNotRoot",
			data: struct {
				A struct {
					A *int8 `json:"a"`
				}
			}{A: struct {
				A *int8 `json:"a"`
			}{int8ptr(-1)}},
		},
		{
			name: "HeadInt8PtrNotRootOmitEmpty",
			data: struct {
				A struct {
					A *int8 `json:"a,omitempty"`
				}
			}{A: struct {
				A *int8 `json:"a,omitempty"`
			}{int8ptr(-1)}},
		},
		{
			name: "HeadInt8PtrNotRootString",
			data: struct {
				A struct {
					A *int8 `json:"a,string"`
				}
			}{A: struct {
				A *int8 `json:"a,string"`
			}{int8ptr(-1)}},
		},

		// HeadInt8PtrNilNotRoot
		{
			name: "HeadInt8PtrNilNotRoot",
			data: struct {
				A struct {
					A *int8 `json:"a"`
				}
			}{},
		},
		{
			name: "HeadInt8PtrNilNotRootOmitEmpty",
			data: struct {
				A struct {
					A *int8 `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadInt8PtrNilNotRootString",
			data: struct {
				A struct {
					A *int8 `json:"a,string"`
				}
			}{},
		},

		// PtrHeadInt8ZeroNotRoot
		{
			name: "PtrHeadInt8ZeroNotRoot",
			data: struct {
				A *struct {
					A int8 `json:"a"`
				}
			}{A: new(struct {
				A int8 `json:"a"`
			})},
		},
		{
			name: "PtrHeadInt8ZeroNotRootOmitEmpty",
			data: struct {
				A *struct {
					A int8 `json:"a,omitempty"`
				}
			}{A: new(struct {
				A int8 `json:"a,omitempty"`
			})},
		},
		{
			name: "PtrHeadInt8ZeroNotRootString",
			data: struct {
				A *struct {
					A int8 `json:"a,string"`
				}
			}{A: new(struct {
				A int8 `json:"a,string"`
			})},
		},

		// PtrHeadInt8NotRoot
		{
			name: "PtrHeadInt8NotRoot",
			data: struct {
				A *struct {
					A int8 `json:"a"`
				}
			}{A: &(struct {
				A int8 `json:"a"`
			}{A: -1})},
		},
		{
			name: "PtrHeadInt8NotRootOmitEmpty",
			data: struct {
				A *struct {
					A int8 `json:"a,omitempty"`
				}
			}{A: &(struct {
				A int8 `json:"a,omitempty"`
			}{A: -1})},
		},
		{
			name: "PtrHeadInt8NotRootString",
			data: struct {
				A *struct {
					A int8 `json:"a,string"`
				}
			}{A: &(struct {
				A int8 `json:"a,string"`
			}{A: -1})},
		},

		// PtrHeadInt8PtrNotRoot
		{
			name: "PtrHeadInt8PtrNotRoot",
			data: struct {
				A *struct {
					A *int8 `json:"a"`
				}
			}{A: &(struct {
				A *int8 `json:"a"`
			}{A: int8ptr(-1)})},
		},
		{
			name: "PtrHeadInt8PtrNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *int8 `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *int8 `json:"a,omitempty"`
			}{A: int8ptr(-1)})},
		},
		{
			name: "PtrHeadInt8PtrNotRootString",
			data: struct {
				A *struct {
					A *int8 `json:"a,string"`
				}
			}{A: &(struct {
				A *int8 `json:"a,string"`
			}{A: int8ptr(-1)})},
		},

		// PtrHeadInt8PtrNilNotRoot
		{
			name: "PtrHeadInt8PtrNilNotRoot",
			data: struct {
				A *struct {
					A *int8 `json:"a"`
				}
			}{A: &(struct {
				A *int8 `json:"a"`
			}{A: nil})},
		},
		{
			name: "PtrHeadInt8PtrNilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *int8 `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *int8 `json:"a,omitempty"`
			}{A: nil})},
		},
		{
			name: "PtrHeadInt8PtrNilNotRootString",
			data: struct {
				A *struct {
					A *int8 `json:"a,string"`
				}
			}{A: &(struct {
				A *int8 `json:"a,string"`
			}{A: nil})},
		},

		// PtrHeadInt8NilNotRoot
		{
			name: "PtrHeadInt8NilNotRoot",
			data: struct {
				A *struct {
					A *int8 `json:"a"`
				}
			}{A: nil},
		},
		{
			name: "PtrHeadInt8NilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *int8 `json:"a,omitempty"`
				} `json:",omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadInt8NilNotRootString",
			data: struct {
				A *struct {
					A *int8 `json:"a,string"`
				} `json:",string"`
			}{A: nil},
		},

		// HeadInt8ZeroMultiFieldsNotRoot
		{
			name: "HeadInt8ZeroMultiFieldsNotRoot",
			data: struct {
				A struct {
					A int8 `json:"a"`
				}
				B struct {
					B int8 `json:"b"`
				}
			}{},
		},
		{
			name: "HeadInt8ZeroMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A int8 `json:"a,omitempty"`
				}
				B struct {
					B int8 `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "HeadInt8ZeroMultiFieldsNotRootString",
			data: struct {
				A struct {
					A int8 `json:"a,string"`
				}
				B struct {
					B int8 `json:"b,string"`
				}
			}{},
		},

		// HeadInt8MultiFieldsNotRoot
		{
			name: "HeadInt8MultiFieldsNotRoot",
			data: struct {
				A struct {
					A int8 `json:"a"`
				}
				B struct {
					B int8 `json:"b"`
				}
			}{A: struct {
				A int8 `json:"a"`
			}{A: -1}, B: struct {
				B int8 `json:"b"`
			}{B: 2}},
		},
		{
			name: "HeadInt8MultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A int8 `json:"a,omitempty"`
				}
				B struct {
					B int8 `json:"b,omitempty"`
				}
			}{A: struct {
				A int8 `json:"a,omitempty"`
			}{A: -1}, B: struct {
				B int8 `json:"b,omitempty"`
			}{B: 2}},
		},
		{
			name: "HeadInt8MultiFieldsNotRootString",
			data: struct {
				A struct {
					A int8 `json:"a,string"`
				}
				B struct {
					B int8 `json:"b,string"`
				}
			}{A: struct {
				A int8 `json:"a,string"`
			}{A: -1}, B: struct {
				B int8 `json:"b,string"`
			}{B: 2}},
		},

		// HeadInt8PtrMultiFieldsNotRoot
		{
			name: "HeadInt8PtrMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *int8 `json:"a"`
				}
				B struct {
					B *int8 `json:"b"`
				}
			}{A: struct {
				A *int8 `json:"a"`
			}{A: int8ptr(-1)}, B: struct {
				B *int8 `json:"b"`
			}{B: int8ptr(2)}},
		},
		{
			name: "HeadInt8PtrMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *int8 `json:"a,omitempty"`
				}
				B struct {
					B *int8 `json:"b,omitempty"`
				}
			}{A: struct {
				A *int8 `json:"a,omitempty"`
			}{A: int8ptr(-1)}, B: struct {
				B *int8 `json:"b,omitempty"`
			}{B: int8ptr(2)}},
		},
		{
			name: "HeadInt8PtrMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *int8 `json:"a,string"`
				}
				B struct {
					B *int8 `json:"b,string"`
				}
			}{A: struct {
				A *int8 `json:"a,string"`
			}{A: int8ptr(-1)}, B: struct {
				B *int8 `json:"b,string"`
			}{B: int8ptr(2)}},
		},

		// HeadInt8PtrNilMultiFieldsNotRoot
		{
			name: "HeadInt8PtrNilMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *int8 `json:"a"`
				}
				B struct {
					B *int8 `json:"b"`
				}
			}{A: struct {
				A *int8 `json:"a"`
			}{A: nil}, B: struct {
				B *int8 `json:"b"`
			}{B: nil}},
		},
		{
			name: "HeadInt8PtrNilMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *int8 `json:"a,omitempty"`
				}
				B struct {
					B *int8 `json:"b,omitempty"`
				}
			}{A: struct {
				A *int8 `json:"a,omitempty"`
			}{A: nil}, B: struct {
				B *int8 `json:"b,omitempty"`
			}{B: nil}},
		},
		{
			name: "HeadInt8PtrNilMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *int8 `json:"a,string"`
				}
				B struct {
					B *int8 `json:"b,string"`
				}
			}{A: struct {
				A *int8 `json:"a,string"`
			}{A: nil}, B: struct {
				B *int8 `json:"b,string"`
			}{B: nil}},
		},

		// PtrHeadInt8ZeroMultiFieldsNotRoot
		{
			name: "PtrHeadInt8ZeroMultiFieldsNotRoot",
			data: &struct {
				A struct {
					A int8 `json:"a"`
				}
				B struct {
					B int8 `json:"b"`
				}
			}{},
		},
		{
			name: "PtrHeadInt8ZeroMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A int8 `json:"a,omitempty"`
				}
				B struct {
					B int8 `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "PtrHeadInt8ZeroMultiFieldsNotRootString",
			data: &struct {
				A struct {
					A int8 `json:"a,string"`
				}
				B struct {
					B int8 `json:"b,string"`
				}
			}{},
		},

		// PtrHeadInt8MultiFieldsNotRoot
		{
			name: "PtrHeadInt8MultiFieldsNotRoot",
			data: &struct {
				A struct {
					A int8 `json:"a"`
				}
				B struct {
					B int8 `json:"b"`
				}
			}{A: struct {
				A int8 `json:"a"`
			}{A: -1}, B: struct {
				B int8 `json:"b"`
			}{B: 2}},
		},
		{
			name: "PtrHeadInt8MultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A int8 `json:"a,omitempty"`
				}
				B struct {
					B int8 `json:"b,omitempty"`
				}
			}{A: struct {
				A int8 `json:"a,omitempty"`
			}{A: -1}, B: struct {
				B int8 `json:"b,omitempty"`
			}{B: 2}},
		},
		{
			name: "PtrHeadInt8MultiFieldsNotRootString",
			data: &struct {
				A struct {
					A int8 `json:"a,string"`
				}
				B struct {
					B int8 `json:"b,string"`
				}
			}{A: struct {
				A int8 `json:"a,string"`
			}{A: -1}, B: struct {
				B int8 `json:"b,string"`
			}{B: 2}},
		},

		// PtrHeadInt8PtrMultiFieldsNotRoot
		{
			name: "PtrHeadInt8PtrMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *int8 `json:"a"`
				}
				B *struct {
					B *int8 `json:"b"`
				}
			}{A: &(struct {
				A *int8 `json:"a"`
			}{A: int8ptr(-1)}), B: &(struct {
				B *int8 `json:"b"`
			}{B: int8ptr(2)})},
		},
		{
			name: "PtrHeadInt8PtrMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *int8 `json:"a,omitempty"`
				}
				B *struct {
					B *int8 `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *int8 `json:"a,omitempty"`
			}{A: int8ptr(-1)}), B: &(struct {
				B *int8 `json:"b,omitempty"`
			}{B: int8ptr(2)})},
		},
		{
			name: "PtrHeadInt8PtrMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *int8 `json:"a,string"`
				}
				B *struct {
					B *int8 `json:"b,string"`
				}
			}{A: &(struct {
				A *int8 `json:"a,string"`
			}{A: int8ptr(-1)}), B: &(struct {
				B *int8 `json:"b,string"`
			}{B: int8ptr(2)})},
		},

		// PtrHeadInt8PtrNilMultiFieldsNotRoot
		{
			name: "PtrHeadInt8PtrNilMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *int8 `json:"a"`
				}
				B *struct {
					B *int8 `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadInt8PtrNilMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *int8 `json:"a,omitempty"`
				} `json:",omitempty"`
				B *struct {
					B *int8 `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadInt8PtrNilMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *int8 `json:"a,string"`
				} `json:",string"`
				B *struct {
					B *int8 `json:"b,string"`
				} `json:",string"`
			}{A: nil, B: nil},
		},

		// PtrHeadInt8NilMultiFieldsNotRoot
		{
			name: "PtrHeadInt8NilMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *int8 `json:"a"`
				}
				B *struct {
					B *int8 `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadInt8NilMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *int8 `json:"a,omitempty"`
				}
				B *struct {
					B *int8 `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadInt8NilMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *int8 `json:"a,string"`
				}
				B *struct {
					B *int8 `json:"b,string"`
				}
			})(nil),
		},

		// PtrHeadInt8DoubleMultiFieldsNotRoot
		{
			name: "PtrHeadInt8DoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A int8 `json:"a"`
					B int8 `json:"b"`
				}
				B *struct {
					A int8 `json:"a"`
					B int8 `json:"b"`
				}
			}{A: &(struct {
				A int8 `json:"a"`
				B int8 `json:"b"`
			}{A: -1, B: 2}), B: &(struct {
				A int8 `json:"a"`
				B int8 `json:"b"`
			}{A: 3, B: 4})},
		},
		{
			name: "PtrHeadInt8DoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A int8 `json:"a,omitempty"`
					B int8 `json:"b,omitempty"`
				}
				B *struct {
					A int8 `json:"a,omitempty"`
					B int8 `json:"b,omitempty"`
				}
			}{A: &(struct {
				A int8 `json:"a,omitempty"`
				B int8 `json:"b,omitempty"`
			}{A: -1, B: 2}), B: &(struct {
				A int8 `json:"a,omitempty"`
				B int8 `json:"b,omitempty"`
			}{A: 3, B: 4})},
		},
		{
			name: "PtrHeadInt8DoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A int8 `json:"a,string"`
					B int8 `json:"b,string"`
				}
				B *struct {
					A int8 `json:"a,string"`
					B int8 `json:"b,string"`
				}
			}{A: &(struct {
				A int8 `json:"a,string"`
				B int8 `json:"b,string"`
			}{A: -1, B: 2}), B: &(struct {
				A int8 `json:"a,string"`
				B int8 `json:"b,string"`
			}{A: 3, B: 4})},
		},

		// PtrHeadInt8NilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadInt8NilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A int8 `json:"a"`
					B int8 `json:"b"`
				}
				B *struct {
					A int8 `json:"a"`
					B int8 `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadInt8NilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A int8 `json:"a,omitempty"`
					B int8 `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A int8 `json:"a,omitempty"`
					B int8 `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadInt8NilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A int8 `json:"a,string"`
					B int8 `json:"b,string"`
				}
				B *struct {
					A int8 `json:"a,string"`
					B int8 `json:"b,string"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadInt8NilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadInt8NilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A int8 `json:"a"`
					B int8 `json:"b"`
				}
				B *struct {
					A int8 `json:"a"`
					B int8 `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadInt8NilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A int8 `json:"a,omitempty"`
					B int8 `json:"b,omitempty"`
				}
				B *struct {
					A int8 `json:"a,omitempty"`
					B int8 `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadInt8NilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A int8 `json:"a,string"`
					B int8 `json:"b,string"`
				}
				B *struct {
					A int8 `json:"a,string"`
					B int8 `json:"b,string"`
				}
			})(nil),
		},

		// PtrHeadInt8PtrDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadInt8PtrDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *int8 `json:"a"`
					B *int8 `json:"b"`
				}
				B *struct {
					A *int8 `json:"a"`
					B *int8 `json:"b"`
				}
			}{A: &(struct {
				A *int8 `json:"a"`
				B *int8 `json:"b"`
			}{A: int8ptr(-1), B: int8ptr(2)}), B: &(struct {
				A *int8 `json:"a"`
				B *int8 `json:"b"`
			}{A: int8ptr(3), B: int8ptr(4)})},
		},
		{
			name: "PtrHeadInt8PtrDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *int8 `json:"a,omitempty"`
					B *int8 `json:"b,omitempty"`
				}
				B *struct {
					A *int8 `json:"a,omitempty"`
					B *int8 `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *int8 `json:"a,omitempty"`
				B *int8 `json:"b,omitempty"`
			}{A: int8ptr(-1), B: int8ptr(2)}), B: &(struct {
				A *int8 `json:"a,omitempty"`
				B *int8 `json:"b,omitempty"`
			}{A: int8ptr(3), B: int8ptr(4)})},
		},
		{
			name: "PtrHeadInt8PtrDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *int8 `json:"a,string"`
					B *int8 `json:"b,string"`
				}
				B *struct {
					A *int8 `json:"a,string"`
					B *int8 `json:"b,string"`
				}
			}{A: &(struct {
				A *int8 `json:"a,string"`
				B *int8 `json:"b,string"`
			}{A: int8ptr(-1), B: int8ptr(2)}), B: &(struct {
				A *int8 `json:"a,string"`
				B *int8 `json:"b,string"`
			}{A: int8ptr(3), B: int8ptr(4)})},
		},

		// PtrHeadInt8PtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadInt8PtrNilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *int8 `json:"a"`
					B *int8 `json:"b"`
				}
				B *struct {
					A *int8 `json:"a"`
					B *int8 `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadInt8PtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *int8 `json:"a,omitempty"`
					B *int8 `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A *int8 `json:"a,omitempty"`
					B *int8 `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadInt8PtrNilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *int8 `json:"a,string"`
					B *int8 `json:"b,string"`
				}
				B *struct {
					A *int8 `json:"a,string"`
					B *int8 `json:"b,string"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadInt8PtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadInt8PtrNilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *int8 `json:"a"`
					B *int8 `json:"b"`
				}
				B *struct {
					A *int8 `json:"a"`
					B *int8 `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadInt8PtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *int8 `json:"a,omitempty"`
					B *int8 `json:"b,omitempty"`
				}
				B *struct {
					A *int8 `json:"a,omitempty"`
					B *int8 `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadInt8PtrNilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *int8 `json:"a,string"`
					B *int8 `json:"b,string"`
				}
				B *struct {
					A *int8 `json:"a,string"`
					B *int8 `json:"b,string"`
				}
			})(nil),
		},

		// AnonymousHeadInt8
		{
			name: "AnonymousHeadInt8",
			data: struct {
				structInt8
				B int8 `json:"b"`
			}{
				structInt8: structInt8{A: -1},
				B:          2,
			},
		},
		{
			name: "AnonymousHeadInt8OmitEmpty",
			data: struct {
				structInt8OmitEmpty
				B int8 `json:"b,omitempty"`
			}{
				structInt8OmitEmpty: structInt8OmitEmpty{A: -1},
				B:                   2,
			},
		},
		{
			name: "AnonymousHeadInt8String",
			data: struct {
				structInt8String
				B int8 `json:"b,string"`
			}{
				structInt8String: structInt8String{A: -1},
				B:                2,
			},
		},

		// PtrAnonymousHeadInt8
		{
			name: "PtrAnonymousHeadInt8",
			data: struct {
				*structInt8
				B int8 `json:"b"`
			}{
				structInt8: &structInt8{A: -1},
				B:          2,
			},
		},
		{
			name: "PtrAnonymousHeadInt8OmitEmpty",
			data: struct {
				*structInt8OmitEmpty
				B int8 `json:"b,omitempty"`
			}{
				structInt8OmitEmpty: &structInt8OmitEmpty{A: -1},
				B:                   2,
			},
		},
		{
			name: "PtrAnonymousHeadInt8String",
			data: struct {
				*structInt8String
				B int8 `json:"b,string"`
			}{
				structInt8String: &structInt8String{A: -1},
				B:                2,
			},
		},

		// NilPtrAnonymousHeadInt8
		{
			name: "NilPtrAnonymousHeadInt8",
			data: struct {
				*structInt8
				B int8 `json:"b"`
			}{
				structInt8: nil,
				B:          2,
			},
		},
		{
			name: "NilPtrAnonymousHeadInt8OmitEmpty",
			data: struct {
				*structInt8OmitEmpty
				B int8 `json:"b,omitempty"`
			}{
				structInt8OmitEmpty: nil,
				B:                   2,
			},
		},
		{
			name: "NilPtrAnonymousHeadInt8String",
			data: struct {
				*structInt8String
				B int8 `json:"b,string"`
			}{
				structInt8String: nil,
				B:                2,
			},
		},

		// AnonymousHeadInt8Ptr
		{
			name: "AnonymousHeadInt8Ptr",
			data: struct {
				structInt8Ptr
				B *int8 `json:"b"`
			}{
				structInt8Ptr: structInt8Ptr{A: int8ptr(-1)},
				B:             int8ptr(2),
			},
		},
		{
			name: "AnonymousHeadInt8PtrOmitEmpty",
			data: struct {
				structInt8PtrOmitEmpty
				B *int8 `json:"b,omitempty"`
			}{
				structInt8PtrOmitEmpty: structInt8PtrOmitEmpty{A: int8ptr(-1)},
				B:                      int8ptr(2),
			},
		},
		{
			name: "AnonymousHeadInt8PtrString",
			data: struct {
				structInt8PtrString
				B *int8 `json:"b,string"`
			}{
				structInt8PtrString: structInt8PtrString{A: int8ptr(-1)},
				B:                   int8ptr(2),
			},
		},

		// AnonymousHeadInt8PtrNil
		{
			name: "AnonymousHeadInt8PtrNil",
			data: struct {
				structInt8Ptr
				B *int8 `json:"b"`
			}{
				structInt8Ptr: structInt8Ptr{A: nil},
				B:             int8ptr(2),
			},
		},
		{
			name: "AnonymousHeadInt8PtrNilOmitEmpty",
			data: struct {
				structInt8PtrOmitEmpty
				B *int8 `json:"b,omitempty"`
			}{
				structInt8PtrOmitEmpty: structInt8PtrOmitEmpty{A: nil},
				B:                      int8ptr(2),
			},
		},
		{
			name: "AnonymousHeadInt8PtrNilString",
			data: struct {
				structInt8PtrString
				B *int8 `json:"b,string"`
			}{
				structInt8PtrString: structInt8PtrString{A: nil},
				B:                   int8ptr(2),
			},
		},

		// PtrAnonymousHeadInt8Ptr
		{
			name: "PtrAnonymousHeadInt8Ptr",
			data: struct {
				*structInt8Ptr
				B *int8 `json:"b"`
			}{
				structInt8Ptr: &structInt8Ptr{A: int8ptr(-1)},
				B:             int8ptr(2),
			},
		},
		{
			name: "PtrAnonymousHeadInt8PtrOmitEmpty",
			data: struct {
				*structInt8PtrOmitEmpty
				B *int8 `json:"b,omitempty"`
			}{
				structInt8PtrOmitEmpty: &structInt8PtrOmitEmpty{A: int8ptr(-1)},
				B:                      int8ptr(2),
			},
		},
		{
			name: "PtrAnonymousHeadInt8PtrString",
			data: struct {
				*structInt8PtrString
				B *int8 `json:"b,string"`
			}{
				structInt8PtrString: &structInt8PtrString{A: int8ptr(-1)},
				B:                   int8ptr(2),
			},
		},

		// NilPtrAnonymousHeadInt8Ptr
		{
			name: "NilPtrAnonymousHeadInt8Ptr",
			data: struct {
				*structInt8Ptr
				B *int8 `json:"b"`
			}{
				structInt8Ptr: nil,
				B:             int8ptr(2),
			},
		},
		{
			name: "NilPtrAnonymousHeadInt8PtrOmitEmpty",
			data: struct {
				*structInt8PtrOmitEmpty
				B *int8 `json:"b,omitempty"`
			}{
				structInt8PtrOmitEmpty: nil,
				B:                      int8ptr(2),
			},
		},
		{
			name: "NilPtrAnonymousHeadInt8PtrString",
			data: struct {
				*structInt8PtrString
				B *int8 `json:"b,string"`
			}{
				structInt8PtrString: nil,
				B:                   int8ptr(2),
			},
		},

		// AnonymousHeadInt8Only
		{
			name: "AnonymousHeadInt8Only",
			data: struct {
				structInt8
			}{
				structInt8: structInt8{A: -1},
			},
		},
		{
			name: "AnonymousHeadInt8OnlyOmitEmpty",
			data: struct {
				structInt8OmitEmpty
			}{
				structInt8OmitEmpty: structInt8OmitEmpty{A: -1},
			},
		},
		{
			name: "AnonymousHeadInt8OnlyString",
			data: struct {
				structInt8String
			}{
				structInt8String: structInt8String{A: -1},
			},
		},

		// PtrAnonymousHeadInt8Only
		{
			name: "PtrAnonymousHeadInt8Only",
			data: struct {
				*structInt8
			}{
				structInt8: &structInt8{A: -1},
			},
		},
		{
			name: "PtrAnonymousHeadInt8OnlyOmitEmpty",
			data: struct {
				*structInt8OmitEmpty
			}{
				structInt8OmitEmpty: &structInt8OmitEmpty{A: -1},
			},
		},
		{
			name: "PtrAnonymousHeadInt8OnlyString",
			data: struct {
				*structInt8String
			}{
				structInt8String: &structInt8String{A: -1},
			},
		},

		// NilPtrAnonymousHeadInt8Only
		{
			name: "NilPtrAnonymousHeadInt8Only",
			data: struct {
				*structInt8
			}{
				structInt8: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadInt8OnlyOmitEmpty",
			data: struct {
				*structInt8OmitEmpty
			}{
				structInt8OmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadInt8OnlyString",
			data: struct {
				*structInt8String
			}{
				structInt8String: nil,
			},
		},

		// AnonymousHeadInt8PtrOnly
		{
			name: "AnonymousHeadInt8PtrOnly",
			data: struct {
				structInt8Ptr
			}{
				structInt8Ptr: structInt8Ptr{A: int8ptr(-1)},
			},
		},
		{
			name: "AnonymousHeadInt8PtrOnlyOmitEmpty",
			data: struct {
				structInt8PtrOmitEmpty
			}{
				structInt8PtrOmitEmpty: structInt8PtrOmitEmpty{A: int8ptr(-1)},
			},
		},
		{
			name: "AnonymousHeadInt8PtrOnlyString",
			data: struct {
				structInt8PtrString
			}{
				structInt8PtrString: structInt8PtrString{A: int8ptr(-1)},
			},
		},

		// AnonymousHeadInt8PtrNilOnly
		{
			name: "AnonymousHeadInt8PtrNilOnly",
			data: struct {
				structInt8Ptr
			}{
				structInt8Ptr: structInt8Ptr{A: nil},
			},
		},
		{
			name: "AnonymousHeadInt8PtrNilOnlyOmitEmpty",
			data: struct {
				structInt8PtrOmitEmpty
			}{
				structInt8PtrOmitEmpty: structInt8PtrOmitEmpty{A: nil},
			},
		},
		{
			name: "AnonymousHeadInt8PtrNilOnlyString",
			data: struct {
				structInt8PtrString
			}{
				structInt8PtrString: structInt8PtrString{A: nil},
			},
		},

		// PtrAnonymousHeadInt8PtrOnly
		{
			name: "PtrAnonymousHeadInt8PtrOnly",
			data: struct {
				*structInt8Ptr
			}{
				structInt8Ptr: &structInt8Ptr{A: int8ptr(-1)},
			},
		},
		{
			name: "PtrAnonymousHeadInt8PtrOnlyOmitEmpty",
			data: struct {
				*structInt8PtrOmitEmpty
			}{
				structInt8PtrOmitEmpty: &structInt8PtrOmitEmpty{A: int8ptr(-1)},
			},
		},
		{
			name: "PtrAnonymousHeadInt8PtrOnlyString",
			data: struct {
				*structInt8PtrString
			}{
				structInt8PtrString: &structInt8PtrString{A: int8ptr(-1)},
			},
		},

		// NilPtrAnonymousHeadInt8PtrOnly
		{
			name: "NilPtrAnonymousHeadInt8PtrOnly",
			data: struct {
				*structInt8Ptr
			}{
				structInt8Ptr: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadInt8PtrOnlyOmitEmpty",
			data: struct {
				*structInt8PtrOmitEmpty
			}{
				structInt8PtrOmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadInt8PtrOnlyString",
			data: struct {
				*structInt8PtrString
			}{
				structInt8PtrString: nil,
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
