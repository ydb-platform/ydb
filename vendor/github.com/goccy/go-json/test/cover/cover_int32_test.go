package json_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/goccy/go-json"
)

func TestCoverInt32(t *testing.T) {
	type structInt32 struct {
		A int32 `json:"a"`
	}
	type structInt32OmitEmpty struct {
		A int32 `json:"a,omitempty"`
	}
	type structInt32String struct {
		A int32 `json:"a,string"`
	}

	type structInt32Ptr struct {
		A *int32 `json:"a"`
	}
	type structInt32PtrOmitEmpty struct {
		A *int32 `json:"a,omitempty"`
	}
	type structInt32PtrString struct {
		A *int32 `json:"a,string"`
	}

	tests := []struct {
		name string
		data interface{}
	}{
		{
			name: "Int32",
			data: int32(10),
		},
		{
			name: "Int32Ptr",
			data: int32ptr(10),
		},
		{
			name: "Int32Ptr3",
			data: int32ptr3(10),
		},
		{
			name: "Int32PtrNil",
			data: (*int32)(nil),
		},
		{
			name: "Int32Ptr3Nil",
			data: (***int32)(nil),
		},

		// HeadInt32Zero
		{
			name: "HeadInt32Zero",
			data: struct {
				A int32 `json:"a"`
			}{},
		},
		{
			name: "HeadInt32ZeroOmitEmpty",
			data: struct {
				A int32 `json:"a,omitempty"`
			}{},
		},
		{
			name: "HeadInt32ZeroString",
			data: struct {
				A int32 `json:"a,string"`
			}{},
		},

		// HeadInt32
		{
			name: "HeadInt32",
			data: struct {
				A int32 `json:"a"`
			}{A: -1},
		},
		{
			name: "HeadInt32OmitEmpty",
			data: struct {
				A int32 `json:"a,omitempty"`
			}{A: -1},
		},
		{
			name: "HeadInt32String",
			data: struct {
				A int32 `json:"a,string"`
			}{A: -1},
		},

		// HeadInt32Ptr
		{
			name: "HeadInt32Ptr",
			data: struct {
				A *int32 `json:"a"`
			}{A: int32ptr(-1)},
		},
		{
			name: "HeadInt32PtrOmitEmpty",
			data: struct {
				A *int32 `json:"a,omitempty"`
			}{A: int32ptr(-1)},
		},
		{
			name: "HeadInt32PtrString",
			data: struct {
				A *int32 `json:"a,string"`
			}{A: int32ptr(-1)},
		},

		// HeadInt32PtrNil
		{
			name: "HeadInt32PtrNil",
			data: struct {
				A *int32 `json:"a"`
			}{A: nil},
		},
		{
			name: "HeadInt32PtrNilOmitEmpty",
			data: struct {
				A *int32 `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "HeadInt32PtrNilString",
			data: struct {
				A *int32 `json:"a,string"`
			}{A: nil},
		},

		// PtrHeadInt32Zero
		{
			name: "PtrHeadInt32Zero",
			data: &struct {
				A int32 `json:"a"`
			}{},
		},
		{
			name: "PtrHeadInt32ZeroOmitEmpty",
			data: &struct {
				A int32 `json:"a,omitempty"`
			}{},
		},
		{
			name: "PtrHeadInt32ZeroString",
			data: &struct {
				A int32 `json:"a,string"`
			}{},
		},

		// PtrHeadInt32
		{
			name: "PtrHeadInt32",
			data: &struct {
				A int32 `json:"a"`
			}{A: -1},
		},
		{
			name: "PtrHeadInt32OmitEmpty",
			data: &struct {
				A int32 `json:"a,omitempty"`
			}{A: -1},
		},
		{
			name: "PtrHeadInt32String",
			data: &struct {
				A int32 `json:"a,string"`
			}{A: -1},
		},

		// PtrHeadInt32Ptr
		{
			name: "PtrHeadInt32Ptr",
			data: &struct {
				A *int32 `json:"a"`
			}{A: int32ptr(-1)},
		},
		{
			name: "PtrHeadInt32PtrOmitEmpty",
			data: &struct {
				A *int32 `json:"a,omitempty"`
			}{A: int32ptr(-1)},
		},
		{
			name: "PtrHeadInt32PtrString",
			data: &struct {
				A *int32 `json:"a,string"`
			}{A: int32ptr(-1)},
		},

		// PtrHeadInt32PtrNil
		{
			name: "PtrHeadInt32PtrNil",
			data: &struct {
				A *int32 `json:"a"`
			}{A: nil},
		},
		{
			name: "PtrHeadInt32PtrNilOmitEmpty",
			data: &struct {
				A *int32 `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadInt32PtrNilString",
			data: &struct {
				A *int32 `json:"a,string"`
			}{A: nil},
		},

		// PtrHeadInt32Nil
		{
			name: "PtrHeadInt32Nil",
			data: (*struct {
				A *int32 `json:"a"`
			})(nil),
		},
		{
			name: "PtrHeadInt32NilOmitEmpty",
			data: (*struct {
				A *int32 `json:"a,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadInt32NilString",
			data: (*struct {
				A *int32 `json:"a,string"`
			})(nil),
		},

		// HeadInt32ZeroMultiFields
		{
			name: "HeadInt32ZeroMultiFields",
			data: struct {
				A int32 `json:"a"`
				B int32 `json:"b"`
				C int32 `json:"c"`
			}{},
		},
		{
			name: "HeadInt32ZeroMultiFieldsOmitEmpty",
			data: struct {
				A int32 `json:"a,omitempty"`
				B int32 `json:"b,omitempty"`
				C int32 `json:"c,omitempty"`
			}{},
		},
		{
			name: "HeadInt32ZeroMultiFields",
			data: struct {
				A int32 `json:"a,string"`
				B int32 `json:"b,string"`
				C int32 `json:"c,string"`
			}{},
		},

		// HeadInt32MultiFields
		{
			name: "HeadInt32MultiFields",
			data: struct {
				A int32 `json:"a"`
				B int32 `json:"b"`
				C int32 `json:"c"`
			}{A: -1, B: 2, C: 3},
		},
		{
			name: "HeadInt32MultiFieldsOmitEmpty",
			data: struct {
				A int32 `json:"a,omitempty"`
				B int32 `json:"b,omitempty"`
				C int32 `json:"c,omitempty"`
			}{A: -1, B: 2, C: 3},
		},
		{
			name: "HeadInt32MultiFieldsString",
			data: struct {
				A int32 `json:"a,string"`
				B int32 `json:"b,string"`
				C int32 `json:"c,string"`
			}{A: -1, B: 2, C: 3},
		},

		// HeadInt32PtrMultiFields
		{
			name: "HeadInt32PtrMultiFields",
			data: struct {
				A *int32 `json:"a"`
				B *int32 `json:"b"`
				C *int32 `json:"c"`
			}{A: int32ptr(-1), B: int32ptr(2), C: int32ptr(3)},
		},
		{
			name: "HeadInt32PtrMultiFieldsOmitEmpty",
			data: struct {
				A *int32 `json:"a,omitempty"`
				B *int32 `json:"b,omitempty"`
				C *int32 `json:"c,omitempty"`
			}{A: int32ptr(-1), B: int32ptr(2), C: int32ptr(3)},
		},
		{
			name: "HeadInt32PtrMultiFieldsString",
			data: struct {
				A *int32 `json:"a,string"`
				B *int32 `json:"b,string"`
				C *int32 `json:"c,string"`
			}{A: int32ptr(-1), B: int32ptr(2), C: int32ptr(3)},
		},

		// HeadInt32PtrNilMultiFields
		{
			name: "HeadInt32PtrNilMultiFields",
			data: struct {
				A *int32 `json:"a"`
				B *int32 `json:"b"`
				C *int32 `json:"c"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadInt32PtrNilMultiFieldsOmitEmpty",
			data: struct {
				A *int32 `json:"a,omitempty"`
				B *int32 `json:"b,omitempty"`
				C *int32 `json:"c,omitempty"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadInt32PtrNilMultiFieldsString",
			data: struct {
				A *int32 `json:"a,string"`
				B *int32 `json:"b,string"`
				C *int32 `json:"c,string"`
			}{A: nil, B: nil, C: nil},
		},

		// PtrHeadInt32ZeroMultiFields
		{
			name: "PtrHeadInt32ZeroMultiFields",
			data: &struct {
				A int32 `json:"a"`
				B int32 `json:"b"`
			}{},
		},
		{
			name: "PtrHeadInt32ZeroMultiFieldsOmitEmpty",
			data: &struct {
				A int32 `json:"a,omitempty"`
				B int32 `json:"b,omitempty"`
			}{},
		},
		{
			name: "PtrHeadInt32ZeroMultiFieldsString",
			data: &struct {
				A int32 `json:"a,string"`
				B int32 `json:"b,string"`
			}{},
		},

		// PtrHeadInt32MultiFields
		{
			name: "PtrHeadInt32MultiFields",
			data: &struct {
				A int32 `json:"a"`
				B int32 `json:"b"`
			}{A: -1, B: 2},
		},
		{
			name: "PtrHeadInt32MultiFieldsOmitEmpty",
			data: &struct {
				A int32 `json:"a,omitempty"`
				B int32 `json:"b,omitempty"`
			}{A: -1, B: 2},
		},
		{
			name: "PtrHeadInt32MultiFieldsString",
			data: &struct {
				A int32 `json:"a,string"`
				B int32 `json:"b,string"`
			}{A: -1, B: 2},
		},

		// PtrHeadInt32PtrMultiFields
		{
			name: "PtrHeadInt32PtrMultiFields",
			data: &struct {
				A *int32 `json:"a"`
				B *int32 `json:"b"`
			}{A: int32ptr(-1), B: int32ptr(2)},
		},
		{
			name: "PtrHeadInt32PtrMultiFieldsOmitEmpty",
			data: &struct {
				A *int32 `json:"a,omitempty"`
				B *int32 `json:"b,omitempty"`
			}{A: int32ptr(-1), B: int32ptr(2)},
		},
		{
			name: "PtrHeadInt32PtrMultiFieldsString",
			data: &struct {
				A *int32 `json:"a,string"`
				B *int32 `json:"b,string"`
			}{A: int32ptr(-1), B: int32ptr(2)},
		},

		// PtrHeadInt32PtrNilMultiFields
		{
			name: "PtrHeadInt32PtrNilMultiFields",
			data: &struct {
				A *int32 `json:"a"`
				B *int32 `json:"b"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadInt32PtrNilMultiFieldsOmitEmpty",
			data: &struct {
				A *int32 `json:"a,omitempty"`
				B *int32 `json:"b,omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadInt32PtrNilMultiFieldsString",
			data: &struct {
				A *int32 `json:"a,string"`
				B *int32 `json:"b,string"`
			}{A: nil, B: nil},
		},

		// PtrHeadInt32NilMultiFields
		{
			name: "PtrHeadInt32NilMultiFields",
			data: (*struct {
				A *int32 `json:"a"`
				B *int32 `json:"b"`
			})(nil),
		},
		{
			name: "PtrHeadInt32NilMultiFieldsOmitEmpty",
			data: (*struct {
				A *int32 `json:"a,omitempty"`
				B *int32 `json:"b,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadInt32NilMultiFieldsString",
			data: (*struct {
				A *int32 `json:"a,string"`
				B *int32 `json:"b,string"`
			})(nil),
		},

		// HeadInt32ZeroNotRoot
		{
			name: "HeadInt32ZeroNotRoot",
			data: struct {
				A struct {
					A int32 `json:"a"`
				}
			}{},
		},
		{
			name: "HeadInt32ZeroNotRootOmitEmpty",
			data: struct {
				A struct {
					A int32 `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadInt32ZeroNotRootString",
			data: struct {
				A struct {
					A int32 `json:"a,string"`
				}
			}{},
		},

		// HeadInt32NotRoot
		{
			name: "HeadInt32NotRoot",
			data: struct {
				A struct {
					A int32 `json:"a"`
				}
			}{A: struct {
				A int32 `json:"a"`
			}{A: -1}},
		},
		{
			name: "HeadInt32NotRootOmitEmpty",
			data: struct {
				A struct {
					A int32 `json:"a,omitempty"`
				}
			}{A: struct {
				A int32 `json:"a,omitempty"`
			}{A: -1}},
		},
		{
			name: "HeadInt32NotRootString",
			data: struct {
				A struct {
					A int32 `json:"a,string"`
				}
			}{A: struct {
				A int32 `json:"a,string"`
			}{A: -1}},
		},

		// HeadInt32PtrNotRoot
		{
			name: "HeadInt32PtrNotRoot",
			data: struct {
				A struct {
					A *int32 `json:"a"`
				}
			}{A: struct {
				A *int32 `json:"a"`
			}{int32ptr(-1)}},
		},
		{
			name: "HeadInt32PtrNotRootOmitEmpty",
			data: struct {
				A struct {
					A *int32 `json:"a,omitempty"`
				}
			}{A: struct {
				A *int32 `json:"a,omitempty"`
			}{int32ptr(-1)}},
		},
		{
			name: "HeadInt32PtrNotRootString",
			data: struct {
				A struct {
					A *int32 `json:"a,string"`
				}
			}{A: struct {
				A *int32 `json:"a,string"`
			}{int32ptr(-1)}},
		},

		// HeadInt32PtrNilNotRoot
		{
			name: "HeadInt32PtrNilNotRoot",
			data: struct {
				A struct {
					A *int32 `json:"a"`
				}
			}{},
		},
		{
			name: "HeadInt32PtrNilNotRootOmitEmpty",
			data: struct {
				A struct {
					A *int32 `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadInt32PtrNilNotRootString",
			data: struct {
				A struct {
					A *int32 `json:"a,string"`
				}
			}{},
		},

		// PtrHeadInt32ZeroNotRoot
		{
			name: "PtrHeadInt32ZeroNotRoot",
			data: struct {
				A *struct {
					A int32 `json:"a"`
				}
			}{A: new(struct {
				A int32 `json:"a"`
			})},
		},
		{
			name: "PtrHeadInt32ZeroNotRootOmitEmpty",
			data: struct {
				A *struct {
					A int32 `json:"a,omitempty"`
				}
			}{A: new(struct {
				A int32 `json:"a,omitempty"`
			})},
		},
		{
			name: "PtrHeadInt32ZeroNotRootString",
			data: struct {
				A *struct {
					A int32 `json:"a,string"`
				}
			}{A: new(struct {
				A int32 `json:"a,string"`
			})},
		},

		// PtrHeadInt32NotRoot
		{
			name: "PtrHeadInt32NotRoot",
			data: struct {
				A *struct {
					A int32 `json:"a"`
				}
			}{A: &(struct {
				A int32 `json:"a"`
			}{A: -1})},
		},
		{
			name: "PtrHeadInt32NotRootOmitEmpty",
			data: struct {
				A *struct {
					A int32 `json:"a,omitempty"`
				}
			}{A: &(struct {
				A int32 `json:"a,omitempty"`
			}{A: -1})},
		},
		{
			name: "PtrHeadInt32NotRootString",
			data: struct {
				A *struct {
					A int32 `json:"a,string"`
				}
			}{A: &(struct {
				A int32 `json:"a,string"`
			}{A: -1})},
		},

		// PtrHeadInt32PtrNotRoot
		{
			name: "PtrHeadInt32PtrNotRoot",
			data: struct {
				A *struct {
					A *int32 `json:"a"`
				}
			}{A: &(struct {
				A *int32 `json:"a"`
			}{A: int32ptr(-1)})},
		},
		{
			name: "PtrHeadInt32PtrNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *int32 `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *int32 `json:"a,omitempty"`
			}{A: int32ptr(-1)})},
		},
		{
			name: "PtrHeadInt32PtrNotRootString",
			data: struct {
				A *struct {
					A *int32 `json:"a,string"`
				}
			}{A: &(struct {
				A *int32 `json:"a,string"`
			}{A: int32ptr(-1)})},
		},

		// PtrHeadInt32PtrNilNotRoot
		{
			name: "PtrHeadInt32PtrNilNotRoot",
			data: struct {
				A *struct {
					A *int32 `json:"a"`
				}
			}{A: &(struct {
				A *int32 `json:"a"`
			}{A: nil})},
		},
		{
			name: "PtrHeadInt32PtrNilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *int32 `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *int32 `json:"a,omitempty"`
			}{A: nil})},
		},
		{
			name: "PtrHeadInt32PtrNilNotRootString",
			data: struct {
				A *struct {
					A *int32 `json:"a,string"`
				}
			}{A: &(struct {
				A *int32 `json:"a,string"`
			}{A: nil})},
		},

		// PtrHeadInt32NilNotRoot
		{
			name: "PtrHeadInt32NilNotRoot",
			data: struct {
				A *struct {
					A *int32 `json:"a"`
				}
			}{A: nil},
		},
		{
			name: "PtrHeadInt32NilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *int32 `json:"a,omitempty"`
				} `json:",omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadInt32NilNotRootString",
			data: struct {
				A *struct {
					A *int32 `json:"a,string"`
				} `json:",string"`
			}{A: nil},
		},

		// HeadInt32ZeroMultiFieldsNotRoot
		{
			name: "HeadInt32ZeroMultiFieldsNotRoot",
			data: struct {
				A struct {
					A int32 `json:"a"`
				}
				B struct {
					B int32 `json:"b"`
				}
			}{},
		},
		{
			name: "HeadInt32ZeroMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A int32 `json:"a,omitempty"`
				}
				B struct {
					B int32 `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "HeadInt32ZeroMultiFieldsNotRootString",
			data: struct {
				A struct {
					A int32 `json:"a,string"`
				}
				B struct {
					B int32 `json:"b,string"`
				}
			}{},
		},

		// HeadInt32MultiFieldsNotRoot
		{
			name: "HeadInt32MultiFieldsNotRoot",
			data: struct {
				A struct {
					A int32 `json:"a"`
				}
				B struct {
					B int32 `json:"b"`
				}
			}{A: struct {
				A int32 `json:"a"`
			}{A: -1}, B: struct {
				B int32 `json:"b"`
			}{B: 2}},
		},
		{
			name: "HeadInt32MultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A int32 `json:"a,omitempty"`
				}
				B struct {
					B int32 `json:"b,omitempty"`
				}
			}{A: struct {
				A int32 `json:"a,omitempty"`
			}{A: -1}, B: struct {
				B int32 `json:"b,omitempty"`
			}{B: 2}},
		},
		{
			name: "HeadInt32MultiFieldsNotRootString",
			data: struct {
				A struct {
					A int32 `json:"a,string"`
				}
				B struct {
					B int32 `json:"b,string"`
				}
			}{A: struct {
				A int32 `json:"a,string"`
			}{A: -1}, B: struct {
				B int32 `json:"b,string"`
			}{B: 2}},
		},

		// HeadInt32PtrMultiFieldsNotRoot
		{
			name: "HeadInt32PtrMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *int32 `json:"a"`
				}
				B struct {
					B *int32 `json:"b"`
				}
			}{A: struct {
				A *int32 `json:"a"`
			}{A: int32ptr(-1)}, B: struct {
				B *int32 `json:"b"`
			}{B: int32ptr(2)}},
		},
		{
			name: "HeadInt32PtrMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *int32 `json:"a,omitempty"`
				}
				B struct {
					B *int32 `json:"b,omitempty"`
				}
			}{A: struct {
				A *int32 `json:"a,omitempty"`
			}{A: int32ptr(-1)}, B: struct {
				B *int32 `json:"b,omitempty"`
			}{B: int32ptr(2)}},
		},
		{
			name: "HeadInt32PtrMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *int32 `json:"a,string"`
				}
				B struct {
					B *int32 `json:"b,string"`
				}
			}{A: struct {
				A *int32 `json:"a,string"`
			}{A: int32ptr(-1)}, B: struct {
				B *int32 `json:"b,string"`
			}{B: int32ptr(2)}},
		},

		// HeadInt32PtrNilMultiFieldsNotRoot
		{
			name: "HeadInt32PtrNilMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *int32 `json:"a"`
				}
				B struct {
					B *int32 `json:"b"`
				}
			}{A: struct {
				A *int32 `json:"a"`
			}{A: nil}, B: struct {
				B *int32 `json:"b"`
			}{B: nil}},
		},
		{
			name: "HeadInt32PtrNilMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *int32 `json:"a,omitempty"`
				}
				B struct {
					B *int32 `json:"b,omitempty"`
				}
			}{A: struct {
				A *int32 `json:"a,omitempty"`
			}{A: nil}, B: struct {
				B *int32 `json:"b,omitempty"`
			}{B: nil}},
		},
		{
			name: "HeadInt32PtrNilMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *int32 `json:"a,string"`
				}
				B struct {
					B *int32 `json:"b,string"`
				}
			}{A: struct {
				A *int32 `json:"a,string"`
			}{A: nil}, B: struct {
				B *int32 `json:"b,string"`
			}{B: nil}},
		},

		// PtrHeadInt32ZeroMultiFieldsNotRoot
		{
			name: "PtrHeadInt32ZeroMultiFieldsNotRoot",
			data: &struct {
				A struct {
					A int32 `json:"a"`
				}
				B struct {
					B int32 `json:"b"`
				}
			}{},
		},
		{
			name: "PtrHeadInt32ZeroMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A int32 `json:"a,omitempty"`
				}
				B struct {
					B int32 `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "PtrHeadInt32ZeroMultiFieldsNotRootString",
			data: &struct {
				A struct {
					A int32 `json:"a,string"`
				}
				B struct {
					B int32 `json:"b,string"`
				}
			}{},
		},

		// PtrHeadInt32MultiFieldsNotRoot
		{
			name: "PtrHeadInt32MultiFieldsNotRoot",
			data: &struct {
				A struct {
					A int32 `json:"a"`
				}
				B struct {
					B int32 `json:"b"`
				}
			}{A: struct {
				A int32 `json:"a"`
			}{A: -1}, B: struct {
				B int32 `json:"b"`
			}{B: 2}},
		},
		{
			name: "PtrHeadInt32MultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A int32 `json:"a,omitempty"`
				}
				B struct {
					B int32 `json:"b,omitempty"`
				}
			}{A: struct {
				A int32 `json:"a,omitempty"`
			}{A: -1}, B: struct {
				B int32 `json:"b,omitempty"`
			}{B: 2}},
		},
		{
			name: "PtrHeadInt32MultiFieldsNotRootString",
			data: &struct {
				A struct {
					A int32 `json:"a,string"`
				}
				B struct {
					B int32 `json:"b,string"`
				}
			}{A: struct {
				A int32 `json:"a,string"`
			}{A: -1}, B: struct {
				B int32 `json:"b,string"`
			}{B: 2}},
		},

		// PtrHeadInt32PtrMultiFieldsNotRoot
		{
			name: "PtrHeadInt32PtrMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *int32 `json:"a"`
				}
				B *struct {
					B *int32 `json:"b"`
				}
			}{A: &(struct {
				A *int32 `json:"a"`
			}{A: int32ptr(-1)}), B: &(struct {
				B *int32 `json:"b"`
			}{B: int32ptr(2)})},
		},
		{
			name: "PtrHeadInt32PtrMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *int32 `json:"a,omitempty"`
				}
				B *struct {
					B *int32 `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *int32 `json:"a,omitempty"`
			}{A: int32ptr(-1)}), B: &(struct {
				B *int32 `json:"b,omitempty"`
			}{B: int32ptr(2)})},
		},
		{
			name: "PtrHeadInt32PtrMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *int32 `json:"a,string"`
				}
				B *struct {
					B *int32 `json:"b,string"`
				}
			}{A: &(struct {
				A *int32 `json:"a,string"`
			}{A: int32ptr(-1)}), B: &(struct {
				B *int32 `json:"b,string"`
			}{B: int32ptr(2)})},
		},

		// PtrHeadInt32PtrNilMultiFieldsNotRoot
		{
			name: "PtrHeadInt32PtrNilMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *int32 `json:"a"`
				}
				B *struct {
					B *int32 `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadInt32PtrNilMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *int32 `json:"a,omitempty"`
				} `json:",omitempty"`
				B *struct {
					B *int32 `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadInt32PtrNilMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *int32 `json:"a,string"`
				} `json:",string"`
				B *struct {
					B *int32 `json:"b,string"`
				} `json:",string"`
			}{A: nil, B: nil},
		},

		// PtrHeadInt32NilMultiFieldsNotRoot
		{
			name: "PtrHeadInt32NilMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *int32 `json:"a"`
				}
				B *struct {
					B *int32 `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadInt32NilMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *int32 `json:"a,omitempty"`
				}
				B *struct {
					B *int32 `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadInt32NilMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *int32 `json:"a,string"`
				}
				B *struct {
					B *int32 `json:"b,string"`
				}
			})(nil),
		},

		// PtrHeadInt32DoubleMultiFieldsNotRoot
		{
			name: "PtrHeadInt32DoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A int32 `json:"a"`
					B int32 `json:"b"`
				}
				B *struct {
					A int32 `json:"a"`
					B int32 `json:"b"`
				}
			}{A: &(struct {
				A int32 `json:"a"`
				B int32 `json:"b"`
			}{A: -1, B: 2}), B: &(struct {
				A int32 `json:"a"`
				B int32 `json:"b"`
			}{A: 3, B: 4})},
		},
		{
			name: "PtrHeadInt32DoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A int32 `json:"a,omitempty"`
					B int32 `json:"b,omitempty"`
				}
				B *struct {
					A int32 `json:"a,omitempty"`
					B int32 `json:"b,omitempty"`
				}
			}{A: &(struct {
				A int32 `json:"a,omitempty"`
				B int32 `json:"b,omitempty"`
			}{A: -1, B: 2}), B: &(struct {
				A int32 `json:"a,omitempty"`
				B int32 `json:"b,omitempty"`
			}{A: 3, B: 4})},
		},
		{
			name: "PtrHeadInt32DoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A int32 `json:"a,string"`
					B int32 `json:"b,string"`
				}
				B *struct {
					A int32 `json:"a,string"`
					B int32 `json:"b,string"`
				}
			}{A: &(struct {
				A int32 `json:"a,string"`
				B int32 `json:"b,string"`
			}{A: -1, B: 2}), B: &(struct {
				A int32 `json:"a,string"`
				B int32 `json:"b,string"`
			}{A: 3, B: 4})},
		},

		// PtrHeadInt32NilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadInt32NilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A int32 `json:"a"`
					B int32 `json:"b"`
				}
				B *struct {
					A int32 `json:"a"`
					B int32 `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadInt32NilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A int32 `json:"a,omitempty"`
					B int32 `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A int32 `json:"a,omitempty"`
					B int32 `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadInt32NilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A int32 `json:"a,string"`
					B int32 `json:"b,string"`
				}
				B *struct {
					A int32 `json:"a,string"`
					B int32 `json:"b,string"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadInt32NilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadInt32NilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A int32 `json:"a"`
					B int32 `json:"b"`
				}
				B *struct {
					A int32 `json:"a"`
					B int32 `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadInt32NilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A int32 `json:"a,omitempty"`
					B int32 `json:"b,omitempty"`
				}
				B *struct {
					A int32 `json:"a,omitempty"`
					B int32 `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadInt32NilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A int32 `json:"a,string"`
					B int32 `json:"b,string"`
				}
				B *struct {
					A int32 `json:"a,string"`
					B int32 `json:"b,string"`
				}
			})(nil),
		},

		// PtrHeadInt32PtrDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadInt32PtrDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *int32 `json:"a"`
					B *int32 `json:"b"`
				}
				B *struct {
					A *int32 `json:"a"`
					B *int32 `json:"b"`
				}
			}{A: &(struct {
				A *int32 `json:"a"`
				B *int32 `json:"b"`
			}{A: int32ptr(-1), B: int32ptr(2)}), B: &(struct {
				A *int32 `json:"a"`
				B *int32 `json:"b"`
			}{A: int32ptr(3), B: int32ptr(4)})},
		},
		{
			name: "PtrHeadInt32PtrDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *int32 `json:"a,omitempty"`
					B *int32 `json:"b,omitempty"`
				}
				B *struct {
					A *int32 `json:"a,omitempty"`
					B *int32 `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *int32 `json:"a,omitempty"`
				B *int32 `json:"b,omitempty"`
			}{A: int32ptr(-1), B: int32ptr(2)}), B: &(struct {
				A *int32 `json:"a,omitempty"`
				B *int32 `json:"b,omitempty"`
			}{A: int32ptr(3), B: int32ptr(4)})},
		},
		{
			name: "PtrHeadInt32PtrDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *int32 `json:"a,string"`
					B *int32 `json:"b,string"`
				}
				B *struct {
					A *int32 `json:"a,string"`
					B *int32 `json:"b,string"`
				}
			}{A: &(struct {
				A *int32 `json:"a,string"`
				B *int32 `json:"b,string"`
			}{A: int32ptr(-1), B: int32ptr(2)}), B: &(struct {
				A *int32 `json:"a,string"`
				B *int32 `json:"b,string"`
			}{A: int32ptr(3), B: int32ptr(4)})},
		},

		// PtrHeadInt32PtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadInt32PtrNilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *int32 `json:"a"`
					B *int32 `json:"b"`
				}
				B *struct {
					A *int32 `json:"a"`
					B *int32 `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadInt32PtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *int32 `json:"a,omitempty"`
					B *int32 `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A *int32 `json:"a,omitempty"`
					B *int32 `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadInt32PtrNilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *int32 `json:"a,string"`
					B *int32 `json:"b,string"`
				}
				B *struct {
					A *int32 `json:"a,string"`
					B *int32 `json:"b,string"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadInt32PtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadInt32PtrNilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *int32 `json:"a"`
					B *int32 `json:"b"`
				}
				B *struct {
					A *int32 `json:"a"`
					B *int32 `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadInt32PtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *int32 `json:"a,omitempty"`
					B *int32 `json:"b,omitempty"`
				}
				B *struct {
					A *int32 `json:"a,omitempty"`
					B *int32 `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadInt32PtrNilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *int32 `json:"a,string"`
					B *int32 `json:"b,string"`
				}
				B *struct {
					A *int32 `json:"a,string"`
					B *int32 `json:"b,string"`
				}
			})(nil),
		},

		// AnonymousHeadInt32
		{
			name: "AnonymousHeadInt32",
			data: struct {
				structInt32
				B int32 `json:"b"`
			}{
				structInt32: structInt32{A: -1},
				B:           2,
			},
		},
		{
			name: "AnonymousHeadInt32OmitEmpty",
			data: struct {
				structInt32OmitEmpty
				B int32 `json:"b,omitempty"`
			}{
				structInt32OmitEmpty: structInt32OmitEmpty{A: -1},
				B:                    2,
			},
		},
		{
			name: "AnonymousHeadInt32String",
			data: struct {
				structInt32String
				B int32 `json:"b,string"`
			}{
				structInt32String: structInt32String{A: -1},
				B:                 2,
			},
		},

		// PtrAnonymousHeadInt32
		{
			name: "PtrAnonymousHeadInt32",
			data: struct {
				*structInt32
				B int32 `json:"b"`
			}{
				structInt32: &structInt32{A: -1},
				B:           2,
			},
		},
		{
			name: "PtrAnonymousHeadInt32OmitEmpty",
			data: struct {
				*structInt32OmitEmpty
				B int32 `json:"b,omitempty"`
			}{
				structInt32OmitEmpty: &structInt32OmitEmpty{A: -1},
				B:                    2,
			},
		},
		{
			name: "PtrAnonymousHeadInt32String",
			data: struct {
				*structInt32String
				B int32 `json:"b,string"`
			}{
				structInt32String: &structInt32String{A: -1},
				B:                 2,
			},
		},

		// NilPtrAnonymousHeadInt32
		{
			name: "NilPtrAnonymousHeadInt32",
			data: struct {
				*structInt32
				B int32 `json:"b"`
			}{
				structInt32: nil,
				B:           2,
			},
		},
		{
			name: "NilPtrAnonymousHeadInt32OmitEmpty",
			data: struct {
				*structInt32OmitEmpty
				B int32 `json:"b,omitempty"`
			}{
				structInt32OmitEmpty: nil,
				B:                    2,
			},
		},
		{
			name: "NilPtrAnonymousHeadInt32String",
			data: struct {
				*structInt32String
				B int32 `json:"b,string"`
			}{
				structInt32String: nil,
				B:                 2,
			},
		},

		// AnonymousHeadInt32Ptr
		{
			name: "AnonymousHeadInt32Ptr",
			data: struct {
				structInt32Ptr
				B *int32 `json:"b"`
			}{
				structInt32Ptr: structInt32Ptr{A: int32ptr(-1)},
				B:              int32ptr(2),
			},
		},
		{
			name: "AnonymousHeadInt32PtrOmitEmpty",
			data: struct {
				structInt32PtrOmitEmpty
				B *int32 `json:"b,omitempty"`
			}{
				structInt32PtrOmitEmpty: structInt32PtrOmitEmpty{A: int32ptr(-1)},
				B:                       int32ptr(2),
			},
		},
		{
			name: "AnonymousHeadInt32PtrString",
			data: struct {
				structInt32PtrString
				B *int32 `json:"b,string"`
			}{
				structInt32PtrString: structInt32PtrString{A: int32ptr(-1)},
				B:                    int32ptr(2),
			},
		},

		// AnonymousHeadInt32PtrNil
		{
			name: "AnonymousHeadInt32PtrNil",
			data: struct {
				structInt32Ptr
				B *int32 `json:"b"`
			}{
				structInt32Ptr: structInt32Ptr{A: nil},
				B:              int32ptr(2),
			},
		},
		{
			name: "AnonymousHeadInt32PtrNilOmitEmpty",
			data: struct {
				structInt32PtrOmitEmpty
				B *int32 `json:"b,omitempty"`
			}{
				structInt32PtrOmitEmpty: structInt32PtrOmitEmpty{A: nil},
				B:                       int32ptr(2),
			},
		},
		{
			name: "AnonymousHeadInt32PtrNilString",
			data: struct {
				structInt32PtrString
				B *int32 `json:"b,string"`
			}{
				structInt32PtrString: structInt32PtrString{A: nil},
				B:                    int32ptr(2),
			},
		},

		// PtrAnonymousHeadInt32Ptr
		{
			name: "PtrAnonymousHeadInt32Ptr",
			data: struct {
				*structInt32Ptr
				B *int32 `json:"b"`
			}{
				structInt32Ptr: &structInt32Ptr{A: int32ptr(-1)},
				B:              int32ptr(2),
			},
		},
		{
			name: "PtrAnonymousHeadInt32PtrOmitEmpty",
			data: struct {
				*structInt32PtrOmitEmpty
				B *int32 `json:"b,omitempty"`
			}{
				structInt32PtrOmitEmpty: &structInt32PtrOmitEmpty{A: int32ptr(-1)},
				B:                       int32ptr(2),
			},
		},
		{
			name: "PtrAnonymousHeadInt32PtrString",
			data: struct {
				*structInt32PtrString
				B *int32 `json:"b,string"`
			}{
				structInt32PtrString: &structInt32PtrString{A: int32ptr(-1)},
				B:                    int32ptr(2),
			},
		},

		// NilPtrAnonymousHeadInt32Ptr
		{
			name: "NilPtrAnonymousHeadInt32Ptr",
			data: struct {
				*structInt32Ptr
				B *int32 `json:"b"`
			}{
				structInt32Ptr: nil,
				B:              int32ptr(2),
			},
		},
		{
			name: "NilPtrAnonymousHeadInt32PtrOmitEmpty",
			data: struct {
				*structInt32PtrOmitEmpty
				B *int32 `json:"b,omitempty"`
			}{
				structInt32PtrOmitEmpty: nil,
				B:                       int32ptr(2),
			},
		},
		{
			name: "NilPtrAnonymousHeadInt32PtrString",
			data: struct {
				*structInt32PtrString
				B *int32 `json:"b,string"`
			}{
				structInt32PtrString: nil,
				B:                    int32ptr(2),
			},
		},

		// AnonymousHeadInt32Only
		{
			name: "AnonymousHeadInt32Only",
			data: struct {
				structInt32
			}{
				structInt32: structInt32{A: -1},
			},
		},
		{
			name: "AnonymousHeadInt32OnlyOmitEmpty",
			data: struct {
				structInt32OmitEmpty
			}{
				structInt32OmitEmpty: structInt32OmitEmpty{A: -1},
			},
		},
		{
			name: "AnonymousHeadInt32OnlyString",
			data: struct {
				structInt32String
			}{
				structInt32String: structInt32String{A: -1},
			},
		},

		// PtrAnonymousHeadInt32Only
		{
			name: "PtrAnonymousHeadInt32Only",
			data: struct {
				*structInt32
			}{
				structInt32: &structInt32{A: -1},
			},
		},
		{
			name: "PtrAnonymousHeadInt32OnlyOmitEmpty",
			data: struct {
				*structInt32OmitEmpty
			}{
				structInt32OmitEmpty: &structInt32OmitEmpty{A: -1},
			},
		},
		{
			name: "PtrAnonymousHeadInt32OnlyString",
			data: struct {
				*structInt32String
			}{
				structInt32String: &structInt32String{A: -1},
			},
		},

		// NilPtrAnonymousHeadInt32Only
		{
			name: "NilPtrAnonymousHeadInt32Only",
			data: struct {
				*structInt32
			}{
				structInt32: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadInt32OnlyOmitEmpty",
			data: struct {
				*structInt32OmitEmpty
			}{
				structInt32OmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadInt32OnlyString",
			data: struct {
				*structInt32String
			}{
				structInt32String: nil,
			},
		},

		// AnonymousHeadInt32PtrOnly
		{
			name: "AnonymousHeadInt32PtrOnly",
			data: struct {
				structInt32Ptr
			}{
				structInt32Ptr: structInt32Ptr{A: int32ptr(-1)},
			},
		},
		{
			name: "AnonymousHeadInt32PtrOnlyOmitEmpty",
			data: struct {
				structInt32PtrOmitEmpty
			}{
				structInt32PtrOmitEmpty: structInt32PtrOmitEmpty{A: int32ptr(-1)},
			},
		},
		{
			name: "AnonymousHeadInt32PtrOnlyString",
			data: struct {
				structInt32PtrString
			}{
				structInt32PtrString: structInt32PtrString{A: int32ptr(-1)},
			},
		},

		// AnonymousHeadInt32PtrNilOnly
		{
			name: "AnonymousHeadInt32PtrNilOnly",
			data: struct {
				structInt32Ptr
			}{
				structInt32Ptr: structInt32Ptr{A: nil},
			},
		},
		{
			name: "AnonymousHeadInt32PtrNilOnlyOmitEmpty",
			data: struct {
				structInt32PtrOmitEmpty
			}{
				structInt32PtrOmitEmpty: structInt32PtrOmitEmpty{A: nil},
			},
		},
		{
			name: "AnonymousHeadInt32PtrNilOnlyString",
			data: struct {
				structInt32PtrString
			}{
				structInt32PtrString: structInt32PtrString{A: nil},
			},
		},

		// PtrAnonymousHeadInt32PtrOnly
		{
			name: "PtrAnonymousHeadInt32PtrOnly",
			data: struct {
				*structInt32Ptr
			}{
				structInt32Ptr: &structInt32Ptr{A: int32ptr(-1)},
			},
		},
		{
			name: "PtrAnonymousHeadInt32PtrOnlyOmitEmpty",
			data: struct {
				*structInt32PtrOmitEmpty
			}{
				structInt32PtrOmitEmpty: &structInt32PtrOmitEmpty{A: int32ptr(-1)},
			},
		},
		{
			name: "PtrAnonymousHeadInt32PtrOnlyString",
			data: struct {
				*structInt32PtrString
			}{
				structInt32PtrString: &structInt32PtrString{A: int32ptr(-1)},
			},
		},

		// NilPtrAnonymousHeadInt32PtrOnly
		{
			name: "NilPtrAnonymousHeadInt32PtrOnly",
			data: struct {
				*structInt32Ptr
			}{
				structInt32Ptr: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadInt32PtrOnlyOmitEmpty",
			data: struct {
				*structInt32PtrOmitEmpty
			}{
				structInt32PtrOmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadInt32PtrOnlyString",
			data: struct {
				*structInt32PtrString
			}{
				structInt32PtrString: nil,
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
