package json_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/goccy/go-json"
)

func TestCoverUint32(t *testing.T) {
	type structUint32 struct {
		A uint32 `json:"a"`
	}
	type structUint32OmitEmpty struct {
		A uint32 `json:"a,omitempty"`
	}
	type structUint32String struct {
		A uint32 `json:"a,string"`
	}

	type structUint32Ptr struct {
		A *uint32 `json:"a"`
	}
	type structUint32PtrOmitEmpty struct {
		A *uint32 `json:"a,omitempty"`
	}
	type structUint32PtrString struct {
		A *uint32 `json:"a,string"`
	}

	tests := []struct {
		name string
		data interface{}
	}{
		{
			name: "Uint32",
			data: uint32(10),
		},
		{
			name: "Uint32Ptr",
			data: uint32ptr(10),
		},
		{
			name: "Uint32Ptr3",
			data: uint32ptr3(10),
		},
		{
			name: "Uint32PtrNil",
			data: (*uint32)(nil),
		},
		{
			name: "Uint32Ptr3Nil",
			data: (***uint32)(nil),
		},

		// HeadUint32Zero
		{
			name: "HeadUint32Zero",
			data: struct {
				A uint32 `json:"a"`
			}{},
		},
		{
			name: "HeadUint32ZeroOmitEmpty",
			data: struct {
				A uint32 `json:"a,omitempty"`
			}{},
		},
		{
			name: "HeadUint32ZeroString",
			data: struct {
				A uint32 `json:"a,string"`
			}{},
		},

		// HeadUint32
		{
			name: "HeadUint32",
			data: struct {
				A uint32 `json:"a"`
			}{A: 1},
		},
		{
			name: "HeadUint32OmitEmpty",
			data: struct {
				A uint32 `json:"a,omitempty"`
			}{A: 1},
		},
		{
			name: "HeadUint32String",
			data: struct {
				A uint32 `json:"a,string"`
			}{A: 1},
		},

		// HeadUint32Ptr
		{
			name: "HeadUint32Ptr",
			data: struct {
				A *uint32 `json:"a"`
			}{A: uint32ptr(1)},
		},
		{
			name: "HeadUint32PtrOmitEmpty",
			data: struct {
				A *uint32 `json:"a,omitempty"`
			}{A: uint32ptr(1)},
		},
		{
			name: "HeadUint32PtrString",
			data: struct {
				A *uint32 `json:"a,string"`
			}{A: uint32ptr(1)},
		},

		// HeadUint32PtrNil
		{
			name: "HeadUint32PtrNil",
			data: struct {
				A *uint32 `json:"a"`
			}{A: nil},
		},
		{
			name: "HeadUint32PtrNilOmitEmpty",
			data: struct {
				A *uint32 `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "HeadUint32PtrNilString",
			data: struct {
				A *uint32 `json:"a,string"`
			}{A: nil},
		},

		// PtrHeadUint32Zero
		{
			name: "PtrHeadUint32Zero",
			data: &struct {
				A uint32 `json:"a"`
			}{},
		},
		{
			name: "PtrHeadUint32ZeroOmitEmpty",
			data: &struct {
				A uint32 `json:"a,omitempty"`
			}{},
		},
		{
			name: "PtrHeadUint32ZeroString",
			data: &struct {
				A uint32 `json:"a,string"`
			}{},
		},

		// PtrHeadUint32
		{
			name: "PtrHeadUint32",
			data: &struct {
				A uint32 `json:"a"`
			}{A: 1},
		},
		{
			name: "PtrHeadUint32OmitEmpty",
			data: &struct {
				A uint32 `json:"a,omitempty"`
			}{A: 1},
		},
		{
			name: "PtrHeadUint32String",
			data: &struct {
				A uint32 `json:"a,string"`
			}{A: 1},
		},

		// PtrHeadUint32Ptr
		{
			name: "PtrHeadUint32Ptr",
			data: &struct {
				A *uint32 `json:"a"`
			}{A: uint32ptr(1)},
		},
		{
			name: "PtrHeadUint32PtrOmitEmpty",
			data: &struct {
				A *uint32 `json:"a,omitempty"`
			}{A: uint32ptr(1)},
		},
		{
			name: "PtrHeadUint32PtrString",
			data: &struct {
				A *uint32 `json:"a,string"`
			}{A: uint32ptr(1)},
		},

		// PtrHeadUint32PtrNil
		{
			name: "PtrHeadUint32PtrNil",
			data: &struct {
				A *uint32 `json:"a"`
			}{A: nil},
		},
		{
			name: "PtrHeadUint32PtrNilOmitEmpty",
			data: &struct {
				A *uint32 `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadUint32PtrNilString",
			data: &struct {
				A *uint32 `json:"a,string"`
			}{A: nil},
		},

		// PtrHeadUint32Nil
		{
			name: "PtrHeadUint32Nil",
			data: (*struct {
				A *uint32 `json:"a"`
			})(nil),
		},
		{
			name: "PtrHeadUint32NilOmitEmpty",
			data: (*struct {
				A *uint32 `json:"a,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadUint32NilString",
			data: (*struct {
				A *uint32 `json:"a,string"`
			})(nil),
		},

		// HeadUint32ZeroMultiFields
		{
			name: "HeadUint32ZeroMultiFields",
			data: struct {
				A uint32 `json:"a"`
				B uint32 `json:"b"`
				C uint32 `json:"c"`
			}{},
		},
		{
			name: "HeadUint32ZeroMultiFieldsOmitEmpty",
			data: struct {
				A uint32 `json:"a,omitempty"`
				B uint32 `json:"b,omitempty"`
				C uint32 `json:"c,omitempty"`
			}{},
		},
		{
			name: "HeadUint32ZeroMultiFields",
			data: struct {
				A uint32 `json:"a,string"`
				B uint32 `json:"b,string"`
				C uint32 `json:"c,string"`
			}{},
		},

		// HeadUint32MultiFields
		{
			name: "HeadUint32MultiFields",
			data: struct {
				A uint32 `json:"a"`
				B uint32 `json:"b"`
				C uint32 `json:"c"`
			}{A: 1, B: 2, C: 3},
		},
		{
			name: "HeadUint32MultiFieldsOmitEmpty",
			data: struct {
				A uint32 `json:"a,omitempty"`
				B uint32 `json:"b,omitempty"`
				C uint32 `json:"c,omitempty"`
			}{A: 1, B: 2, C: 3},
		},
		{
			name: "HeadUint32MultiFieldsString",
			data: struct {
				A uint32 `json:"a,string"`
				B uint32 `json:"b,string"`
				C uint32 `json:"c,string"`
			}{A: 1, B: 2, C: 3},
		},

		// HeadUint32PtrMultiFields
		{
			name: "HeadUint32PtrMultiFields",
			data: struct {
				A *uint32 `json:"a"`
				B *uint32 `json:"b"`
				C *uint32 `json:"c"`
			}{A: uint32ptr(1), B: uint32ptr(2), C: uint32ptr(3)},
		},
		{
			name: "HeadUint32PtrMultiFieldsOmitEmpty",
			data: struct {
				A *uint32 `json:"a,omitempty"`
				B *uint32 `json:"b,omitempty"`
				C *uint32 `json:"c,omitempty"`
			}{A: uint32ptr(1), B: uint32ptr(2), C: uint32ptr(3)},
		},
		{
			name: "HeadUint32PtrMultiFieldsString",
			data: struct {
				A *uint32 `json:"a,string"`
				B *uint32 `json:"b,string"`
				C *uint32 `json:"c,string"`
			}{A: uint32ptr(1), B: uint32ptr(2), C: uint32ptr(3)},
		},

		// HeadUint32PtrNilMultiFields
		{
			name: "HeadUint32PtrNilMultiFields",
			data: struct {
				A *uint32 `json:"a"`
				B *uint32 `json:"b"`
				C *uint32 `json:"c"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadUint32PtrNilMultiFieldsOmitEmpty",
			data: struct {
				A *uint32 `json:"a,omitempty"`
				B *uint32 `json:"b,omitempty"`
				C *uint32 `json:"c,omitempty"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadUint32PtrNilMultiFieldsString",
			data: struct {
				A *uint32 `json:"a,string"`
				B *uint32 `json:"b,string"`
				C *uint32 `json:"c,string"`
			}{A: nil, B: nil, C: nil},
		},

		// PtrHeadUint32ZeroMultiFields
		{
			name: "PtrHeadUint32ZeroMultiFields",
			data: &struct {
				A uint32 `json:"a"`
				B uint32 `json:"b"`
			}{},
		},
		{
			name: "PtrHeadUint32ZeroMultiFieldsOmitEmpty",
			data: &struct {
				A uint32 `json:"a,omitempty"`
				B uint32 `json:"b,omitempty"`
			}{},
		},
		{
			name: "PtrHeadUint32ZeroMultiFieldsString",
			data: &struct {
				A uint32 `json:"a,string"`
				B uint32 `json:"b,string"`
			}{},
		},

		// PtrHeadUint32MultiFields
		{
			name: "PtrHeadUint32MultiFields",
			data: &struct {
				A uint32 `json:"a"`
				B uint32 `json:"b"`
			}{A: 1, B: 2},
		},
		{
			name: "PtrHeadUint32MultiFieldsOmitEmpty",
			data: &struct {
				A uint32 `json:"a,omitempty"`
				B uint32 `json:"b,omitempty"`
			}{A: 1, B: 2},
		},
		{
			name: "PtrHeadUint32MultiFieldsString",
			data: &struct {
				A uint32 `json:"a,string"`
				B uint32 `json:"b,string"`
			}{A: 1, B: 2},
		},

		// PtrHeadUint32PtrMultiFields
		{
			name: "PtrHeadUint32PtrMultiFields",
			data: &struct {
				A *uint32 `json:"a"`
				B *uint32 `json:"b"`
			}{A: uint32ptr(1), B: uint32ptr(2)},
		},
		{
			name: "PtrHeadUint32PtrMultiFieldsOmitEmpty",
			data: &struct {
				A *uint32 `json:"a,omitempty"`
				B *uint32 `json:"b,omitempty"`
			}{A: uint32ptr(1), B: uint32ptr(2)},
		},
		{
			name: "PtrHeadUint32PtrMultiFieldsString",
			data: &struct {
				A *uint32 `json:"a,string"`
				B *uint32 `json:"b,string"`
			}{A: uint32ptr(1), B: uint32ptr(2)},
		},

		// PtrHeadUint32PtrNilMultiFields
		{
			name: "PtrHeadUint32PtrNilMultiFields",
			data: &struct {
				A *uint32 `json:"a"`
				B *uint32 `json:"b"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUint32PtrNilMultiFieldsOmitEmpty",
			data: &struct {
				A *uint32 `json:"a,omitempty"`
				B *uint32 `json:"b,omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUint32PtrNilMultiFieldsString",
			data: &struct {
				A *uint32 `json:"a,string"`
				B *uint32 `json:"b,string"`
			}{A: nil, B: nil},
		},

		// PtrHeadUint32NilMultiFields
		{
			name: "PtrHeadUint32NilMultiFields",
			data: (*struct {
				A *uint32 `json:"a"`
				B *uint32 `json:"b"`
			})(nil),
		},
		{
			name: "PtrHeadUint32NilMultiFieldsOmitEmpty",
			data: (*struct {
				A *uint32 `json:"a,omitempty"`
				B *uint32 `json:"b,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadUint32NilMultiFieldsString",
			data: (*struct {
				A *uint32 `json:"a,string"`
				B *uint32 `json:"b,string"`
			})(nil),
		},

		// HeadUint32ZeroNotRoot
		{
			name: "HeadUint32ZeroNotRoot",
			data: struct {
				A struct {
					A uint32 `json:"a"`
				}
			}{},
		},
		{
			name: "HeadUint32ZeroNotRootOmitEmpty",
			data: struct {
				A struct {
					A uint32 `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadUint32ZeroNotRootString",
			data: struct {
				A struct {
					A uint32 `json:"a,string"`
				}
			}{},
		},

		// HeadUint32NotRoot
		{
			name: "HeadUint32NotRoot",
			data: struct {
				A struct {
					A uint32 `json:"a"`
				}
			}{A: struct {
				A uint32 `json:"a"`
			}{A: 1}},
		},
		{
			name: "HeadUint32NotRootOmitEmpty",
			data: struct {
				A struct {
					A uint32 `json:"a,omitempty"`
				}
			}{A: struct {
				A uint32 `json:"a,omitempty"`
			}{A: 1}},
		},
		{
			name: "HeadUint32NotRootString",
			data: struct {
				A struct {
					A uint32 `json:"a,string"`
				}
			}{A: struct {
				A uint32 `json:"a,string"`
			}{A: 1}},
		},

		// HeadUint32PtrNotRoot
		{
			name: "HeadUint32PtrNotRoot",
			data: struct {
				A struct {
					A *uint32 `json:"a"`
				}
			}{A: struct {
				A *uint32 `json:"a"`
			}{uint32ptr(1)}},
		},
		{
			name: "HeadUint32PtrNotRootOmitEmpty",
			data: struct {
				A struct {
					A *uint32 `json:"a,omitempty"`
				}
			}{A: struct {
				A *uint32 `json:"a,omitempty"`
			}{uint32ptr(1)}},
		},
		{
			name: "HeadUint32PtrNotRootString",
			data: struct {
				A struct {
					A *uint32 `json:"a,string"`
				}
			}{A: struct {
				A *uint32 `json:"a,string"`
			}{uint32ptr(1)}},
		},

		// HeadUint32PtrNilNotRoot
		{
			name: "HeadUint32PtrNilNotRoot",
			data: struct {
				A struct {
					A *uint32 `json:"a"`
				}
			}{},
		},
		{
			name: "HeadUint32PtrNilNotRootOmitEmpty",
			data: struct {
				A struct {
					A *uint32 `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadUint32PtrNilNotRootString",
			data: struct {
				A struct {
					A *uint32 `json:"a,string"`
				}
			}{},
		},

		// PtrHeadUint32ZeroNotRoot
		{
			name: "PtrHeadUint32ZeroNotRoot",
			data: struct {
				A *struct {
					A uint32 `json:"a"`
				}
			}{A: new(struct {
				A uint32 `json:"a"`
			})},
		},
		{
			name: "PtrHeadUint32ZeroNotRootOmitEmpty",
			data: struct {
				A *struct {
					A uint32 `json:"a,omitempty"`
				}
			}{A: new(struct {
				A uint32 `json:"a,omitempty"`
			})},
		},
		{
			name: "PtrHeadUint32ZeroNotRootString",
			data: struct {
				A *struct {
					A uint32 `json:"a,string"`
				}
			}{A: new(struct {
				A uint32 `json:"a,string"`
			})},
		},

		// PtrHeadUint32NotRoot
		{
			name: "PtrHeadUint32NotRoot",
			data: struct {
				A *struct {
					A uint32 `json:"a"`
				}
			}{A: &(struct {
				A uint32 `json:"a"`
			}{A: 1})},
		},
		{
			name: "PtrHeadUint32NotRootOmitEmpty",
			data: struct {
				A *struct {
					A uint32 `json:"a,omitempty"`
				}
			}{A: &(struct {
				A uint32 `json:"a,omitempty"`
			}{A: 1})},
		},
		{
			name: "PtrHeadUint32NotRootString",
			data: struct {
				A *struct {
					A uint32 `json:"a,string"`
				}
			}{A: &(struct {
				A uint32 `json:"a,string"`
			}{A: 1})},
		},

		// PtrHeadUint32PtrNotRoot
		{
			name: "PtrHeadUint32PtrNotRoot",
			data: struct {
				A *struct {
					A *uint32 `json:"a"`
				}
			}{A: &(struct {
				A *uint32 `json:"a"`
			}{A: uint32ptr(1)})},
		},
		{
			name: "PtrHeadUint32PtrNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *uint32 `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *uint32 `json:"a,omitempty"`
			}{A: uint32ptr(1)})},
		},
		{
			name: "PtrHeadUint32PtrNotRootString",
			data: struct {
				A *struct {
					A *uint32 `json:"a,string"`
				}
			}{A: &(struct {
				A *uint32 `json:"a,string"`
			}{A: uint32ptr(1)})},
		},

		// PtrHeadUint32PtrNilNotRoot
		{
			name: "PtrHeadUint32PtrNilNotRoot",
			data: struct {
				A *struct {
					A *uint32 `json:"a"`
				}
			}{A: &(struct {
				A *uint32 `json:"a"`
			}{A: nil})},
		},
		{
			name: "PtrHeadUint32PtrNilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *uint32 `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *uint32 `json:"a,omitempty"`
			}{A: nil})},
		},
		{
			name: "PtrHeadUint32PtrNilNotRootString",
			data: struct {
				A *struct {
					A *uint32 `json:"a,string"`
				}
			}{A: &(struct {
				A *uint32 `json:"a,string"`
			}{A: nil})},
		},

		// PtrHeadUint32NilNotRoot
		{
			name: "PtrHeadUint32NilNotRoot",
			data: struct {
				A *struct {
					A *uint32 `json:"a"`
				}
			}{A: nil},
		},
		{
			name: "PtrHeadUint32NilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *uint32 `json:"a,omitempty"`
				} `json:",omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadUint32NilNotRootString",
			data: struct {
				A *struct {
					A *uint32 `json:"a,string"`
				} `json:",string"`
			}{A: nil},
		},

		// HeadUint32ZeroMultiFieldsNotRoot
		{
			name: "HeadUint32ZeroMultiFieldsNotRoot",
			data: struct {
				A struct {
					A uint32 `json:"a"`
				}
				B struct {
					B uint32 `json:"b"`
				}
			}{},
		},
		{
			name: "HeadUint32ZeroMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A uint32 `json:"a,omitempty"`
				}
				B struct {
					B uint32 `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "HeadUint32ZeroMultiFieldsNotRootString",
			data: struct {
				A struct {
					A uint32 `json:"a,string"`
				}
				B struct {
					B uint32 `json:"b,string"`
				}
			}{},
		},

		// HeadUint32MultiFieldsNotRoot
		{
			name: "HeadUint32MultiFieldsNotRoot",
			data: struct {
				A struct {
					A uint32 `json:"a"`
				}
				B struct {
					B uint32 `json:"b"`
				}
			}{A: struct {
				A uint32 `json:"a"`
			}{A: 1}, B: struct {
				B uint32 `json:"b"`
			}{B: 2}},
		},
		{
			name: "HeadUint32MultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A uint32 `json:"a,omitempty"`
				}
				B struct {
					B uint32 `json:"b,omitempty"`
				}
			}{A: struct {
				A uint32 `json:"a,omitempty"`
			}{A: 1}, B: struct {
				B uint32 `json:"b,omitempty"`
			}{B: 2}},
		},
		{
			name: "HeadUint32MultiFieldsNotRootString",
			data: struct {
				A struct {
					A uint32 `json:"a,string"`
				}
				B struct {
					B uint32 `json:"b,string"`
				}
			}{A: struct {
				A uint32 `json:"a,string"`
			}{A: 1}, B: struct {
				B uint32 `json:"b,string"`
			}{B: 2}},
		},

		// HeadUint32PtrMultiFieldsNotRoot
		{
			name: "HeadUint32PtrMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *uint32 `json:"a"`
				}
				B struct {
					B *uint32 `json:"b"`
				}
			}{A: struct {
				A *uint32 `json:"a"`
			}{A: uint32ptr(1)}, B: struct {
				B *uint32 `json:"b"`
			}{B: uint32ptr(2)}},
		},
		{
			name: "HeadUint32PtrMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *uint32 `json:"a,omitempty"`
				}
				B struct {
					B *uint32 `json:"b,omitempty"`
				}
			}{A: struct {
				A *uint32 `json:"a,omitempty"`
			}{A: uint32ptr(1)}, B: struct {
				B *uint32 `json:"b,omitempty"`
			}{B: uint32ptr(2)}},
		},
		{
			name: "HeadUint32PtrMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *uint32 `json:"a,string"`
				}
				B struct {
					B *uint32 `json:"b,string"`
				}
			}{A: struct {
				A *uint32 `json:"a,string"`
			}{A: uint32ptr(1)}, B: struct {
				B *uint32 `json:"b,string"`
			}{B: uint32ptr(2)}},
		},

		// HeadUint32PtrNilMultiFieldsNotRoot
		{
			name: "HeadUint32PtrNilMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *uint32 `json:"a"`
				}
				B struct {
					B *uint32 `json:"b"`
				}
			}{A: struct {
				A *uint32 `json:"a"`
			}{A: nil}, B: struct {
				B *uint32 `json:"b"`
			}{B: nil}},
		},
		{
			name: "HeadUint32PtrNilMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *uint32 `json:"a,omitempty"`
				}
				B struct {
					B *uint32 `json:"b,omitempty"`
				}
			}{A: struct {
				A *uint32 `json:"a,omitempty"`
			}{A: nil}, B: struct {
				B *uint32 `json:"b,omitempty"`
			}{B: nil}},
		},
		{
			name: "HeadUint32PtrNilMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *uint32 `json:"a,string"`
				}
				B struct {
					B *uint32 `json:"b,string"`
				}
			}{A: struct {
				A *uint32 `json:"a,string"`
			}{A: nil}, B: struct {
				B *uint32 `json:"b,string"`
			}{B: nil}},
		},

		// PtrHeadUint32ZeroMultiFieldsNotRoot
		{
			name: "PtrHeadUint32ZeroMultiFieldsNotRoot",
			data: &struct {
				A struct {
					A uint32 `json:"a"`
				}
				B struct {
					B uint32 `json:"b"`
				}
			}{},
		},
		{
			name: "PtrHeadUint32ZeroMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A uint32 `json:"a,omitempty"`
				}
				B struct {
					B uint32 `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "PtrHeadUint32ZeroMultiFieldsNotRootString",
			data: &struct {
				A struct {
					A uint32 `json:"a,string"`
				}
				B struct {
					B uint32 `json:"b,string"`
				}
			}{},
		},

		// PtrHeadUint32MultiFieldsNotRoot
		{
			name: "PtrHeadUint32MultiFieldsNotRoot",
			data: &struct {
				A struct {
					A uint32 `json:"a"`
				}
				B struct {
					B uint32 `json:"b"`
				}
			}{A: struct {
				A uint32 `json:"a"`
			}{A: 1}, B: struct {
				B uint32 `json:"b"`
			}{B: 2}},
		},
		{
			name: "PtrHeadUint32MultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A uint32 `json:"a,omitempty"`
				}
				B struct {
					B uint32 `json:"b,omitempty"`
				}
			}{A: struct {
				A uint32 `json:"a,omitempty"`
			}{A: 1}, B: struct {
				B uint32 `json:"b,omitempty"`
			}{B: 2}},
		},
		{
			name: "PtrHeadUint32MultiFieldsNotRootString",
			data: &struct {
				A struct {
					A uint32 `json:"a,string"`
				}
				B struct {
					B uint32 `json:"b,string"`
				}
			}{A: struct {
				A uint32 `json:"a,string"`
			}{A: 1}, B: struct {
				B uint32 `json:"b,string"`
			}{B: 2}},
		},

		// PtrHeadUint32PtrMultiFieldsNotRoot
		{
			name: "PtrHeadUint32PtrMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *uint32 `json:"a"`
				}
				B *struct {
					B *uint32 `json:"b"`
				}
			}{A: &(struct {
				A *uint32 `json:"a"`
			}{A: uint32ptr(1)}), B: &(struct {
				B *uint32 `json:"b"`
			}{B: uint32ptr(2)})},
		},
		{
			name: "PtrHeadUint32PtrMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *uint32 `json:"a,omitempty"`
				}
				B *struct {
					B *uint32 `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *uint32 `json:"a,omitempty"`
			}{A: uint32ptr(1)}), B: &(struct {
				B *uint32 `json:"b,omitempty"`
			}{B: uint32ptr(2)})},
		},
		{
			name: "PtrHeadUint32PtrMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *uint32 `json:"a,string"`
				}
				B *struct {
					B *uint32 `json:"b,string"`
				}
			}{A: &(struct {
				A *uint32 `json:"a,string"`
			}{A: uint32ptr(1)}), B: &(struct {
				B *uint32 `json:"b,string"`
			}{B: uint32ptr(2)})},
		},

		// PtrHeadUint32PtrNilMultiFieldsNotRoot
		{
			name: "PtrHeadUint32PtrNilMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *uint32 `json:"a"`
				}
				B *struct {
					B *uint32 `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUint32PtrNilMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *uint32 `json:"a,omitempty"`
				} `json:",omitempty"`
				B *struct {
					B *uint32 `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUint32PtrNilMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *uint32 `json:"a,string"`
				} `json:",string"`
				B *struct {
					B *uint32 `json:"b,string"`
				} `json:",string"`
			}{A: nil, B: nil},
		},

		// PtrHeadUint32NilMultiFieldsNotRoot
		{
			name: "PtrHeadUint32NilMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *uint32 `json:"a"`
				}
				B *struct {
					B *uint32 `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadUint32NilMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *uint32 `json:"a,omitempty"`
				}
				B *struct {
					B *uint32 `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadUint32NilMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *uint32 `json:"a,string"`
				}
				B *struct {
					B *uint32 `json:"b,string"`
				}
			})(nil),
		},

		// PtrHeadUint32DoubleMultiFieldsNotRoot
		{
			name: "PtrHeadUint32DoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A uint32 `json:"a"`
					B uint32 `json:"b"`
				}
				B *struct {
					A uint32 `json:"a"`
					B uint32 `json:"b"`
				}
			}{A: &(struct {
				A uint32 `json:"a"`
				B uint32 `json:"b"`
			}{A: 1, B: 2}), B: &(struct {
				A uint32 `json:"a"`
				B uint32 `json:"b"`
			}{A: 3, B: 4})},
		},
		{
			name: "PtrHeadUint32DoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A uint32 `json:"a,omitempty"`
					B uint32 `json:"b,omitempty"`
				}
				B *struct {
					A uint32 `json:"a,omitempty"`
					B uint32 `json:"b,omitempty"`
				}
			}{A: &(struct {
				A uint32 `json:"a,omitempty"`
				B uint32 `json:"b,omitempty"`
			}{A: 1, B: 2}), B: &(struct {
				A uint32 `json:"a,omitempty"`
				B uint32 `json:"b,omitempty"`
			}{A: 3, B: 4})},
		},
		{
			name: "PtrHeadUint32DoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A uint32 `json:"a,string"`
					B uint32 `json:"b,string"`
				}
				B *struct {
					A uint32 `json:"a,string"`
					B uint32 `json:"b,string"`
				}
			}{A: &(struct {
				A uint32 `json:"a,string"`
				B uint32 `json:"b,string"`
			}{A: 1, B: 2}), B: &(struct {
				A uint32 `json:"a,string"`
				B uint32 `json:"b,string"`
			}{A: 3, B: 4})},
		},

		// PtrHeadUint32NilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadUint32NilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A uint32 `json:"a"`
					B uint32 `json:"b"`
				}
				B *struct {
					A uint32 `json:"a"`
					B uint32 `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUint32NilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A uint32 `json:"a,omitempty"`
					B uint32 `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A uint32 `json:"a,omitempty"`
					B uint32 `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUint32NilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A uint32 `json:"a,string"`
					B uint32 `json:"b,string"`
				}
				B *struct {
					A uint32 `json:"a,string"`
					B uint32 `json:"b,string"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadUint32NilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadUint32NilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A uint32 `json:"a"`
					B uint32 `json:"b"`
				}
				B *struct {
					A uint32 `json:"a"`
					B uint32 `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadUint32NilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A uint32 `json:"a,omitempty"`
					B uint32 `json:"b,omitempty"`
				}
				B *struct {
					A uint32 `json:"a,omitempty"`
					B uint32 `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadUint32NilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A uint32 `json:"a,string"`
					B uint32 `json:"b,string"`
				}
				B *struct {
					A uint32 `json:"a,string"`
					B uint32 `json:"b,string"`
				}
			})(nil),
		},

		// PtrHeadUint32PtrDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadUint32PtrDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *uint32 `json:"a"`
					B *uint32 `json:"b"`
				}
				B *struct {
					A *uint32 `json:"a"`
					B *uint32 `json:"b"`
				}
			}{A: &(struct {
				A *uint32 `json:"a"`
				B *uint32 `json:"b"`
			}{A: uint32ptr(1), B: uint32ptr(2)}), B: &(struct {
				A *uint32 `json:"a"`
				B *uint32 `json:"b"`
			}{A: uint32ptr(3), B: uint32ptr(4)})},
		},
		{
			name: "PtrHeadUint32PtrDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *uint32 `json:"a,omitempty"`
					B *uint32 `json:"b,omitempty"`
				}
				B *struct {
					A *uint32 `json:"a,omitempty"`
					B *uint32 `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *uint32 `json:"a,omitempty"`
				B *uint32 `json:"b,omitempty"`
			}{A: uint32ptr(1), B: uint32ptr(2)}), B: &(struct {
				A *uint32 `json:"a,omitempty"`
				B *uint32 `json:"b,omitempty"`
			}{A: uint32ptr(3), B: uint32ptr(4)})},
		},
		{
			name: "PtrHeadUint32PtrDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *uint32 `json:"a,string"`
					B *uint32 `json:"b,string"`
				}
				B *struct {
					A *uint32 `json:"a,string"`
					B *uint32 `json:"b,string"`
				}
			}{A: &(struct {
				A *uint32 `json:"a,string"`
				B *uint32 `json:"b,string"`
			}{A: uint32ptr(1), B: uint32ptr(2)}), B: &(struct {
				A *uint32 `json:"a,string"`
				B *uint32 `json:"b,string"`
			}{A: uint32ptr(3), B: uint32ptr(4)})},
		},

		// PtrHeadUint32PtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadUint32PtrNilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *uint32 `json:"a"`
					B *uint32 `json:"b"`
				}
				B *struct {
					A *uint32 `json:"a"`
					B *uint32 `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUint32PtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *uint32 `json:"a,omitempty"`
					B *uint32 `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A *uint32 `json:"a,omitempty"`
					B *uint32 `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUint32PtrNilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *uint32 `json:"a,string"`
					B *uint32 `json:"b,string"`
				}
				B *struct {
					A *uint32 `json:"a,string"`
					B *uint32 `json:"b,string"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadUint32PtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadUint32PtrNilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *uint32 `json:"a"`
					B *uint32 `json:"b"`
				}
				B *struct {
					A *uint32 `json:"a"`
					B *uint32 `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadUint32PtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *uint32 `json:"a,omitempty"`
					B *uint32 `json:"b,omitempty"`
				}
				B *struct {
					A *uint32 `json:"a,omitempty"`
					B *uint32 `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadUint32PtrNilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *uint32 `json:"a,string"`
					B *uint32 `json:"b,string"`
				}
				B *struct {
					A *uint32 `json:"a,string"`
					B *uint32 `json:"b,string"`
				}
			})(nil),
		},

		// AnonymousHeadUint32
		{
			name: "AnonymousHeadUint32",
			data: struct {
				structUint32
				B uint32 `json:"b"`
			}{
				structUint32: structUint32{A: 1},
				B:            2,
			},
		},
		{
			name: "AnonymousHeadUint32OmitEmpty",
			data: struct {
				structUint32OmitEmpty
				B uint32 `json:"b,omitempty"`
			}{
				structUint32OmitEmpty: structUint32OmitEmpty{A: 1},
				B:                     2,
			},
		},
		{
			name: "AnonymousHeadUint32String",
			data: struct {
				structUint32String
				B uint32 `json:"b,string"`
			}{
				structUint32String: structUint32String{A: 1},
				B:                  2,
			},
		},

		// PtrAnonymousHeadUint32
		{
			name: "PtrAnonymousHeadUint32",
			data: struct {
				*structUint32
				B uint32 `json:"b"`
			}{
				structUint32: &structUint32{A: 1},
				B:            2,
			},
		},
		{
			name: "PtrAnonymousHeadUint32OmitEmpty",
			data: struct {
				*structUint32OmitEmpty
				B uint32 `json:"b,omitempty"`
			}{
				structUint32OmitEmpty: &structUint32OmitEmpty{A: 1},
				B:                     2,
			},
		},
		{
			name: "PtrAnonymousHeadUint32String",
			data: struct {
				*structUint32String
				B uint32 `json:"b,string"`
			}{
				structUint32String: &structUint32String{A: 1},
				B:                  2,
			},
		},

		// NilPtrAnonymousHeadUint32
		{
			name: "NilPtrAnonymousHeadUint32",
			data: struct {
				*structUint32
				B uint32 `json:"b"`
			}{
				structUint32: nil,
				B:            2,
			},
		},
		{
			name: "NilPtrAnonymousHeadUint32OmitEmpty",
			data: struct {
				*structUint32OmitEmpty
				B uint32 `json:"b,omitempty"`
			}{
				structUint32OmitEmpty: nil,
				B:                     2,
			},
		},
		{
			name: "NilPtrAnonymousHeadUint32String",
			data: struct {
				*structUint32String
				B uint32 `json:"b,string"`
			}{
				structUint32String: nil,
				B:                  2,
			},
		},

		// AnonymousHeadUint32Ptr
		{
			name: "AnonymousHeadUint32Ptr",
			data: struct {
				structUint32Ptr
				B *uint32 `json:"b"`
			}{
				structUint32Ptr: structUint32Ptr{A: uint32ptr(1)},
				B:               uint32ptr(2),
			},
		},
		{
			name: "AnonymousHeadUint32PtrOmitEmpty",
			data: struct {
				structUint32PtrOmitEmpty
				B *uint32 `json:"b,omitempty"`
			}{
				structUint32PtrOmitEmpty: structUint32PtrOmitEmpty{A: uint32ptr(1)},
				B:                        uint32ptr(2),
			},
		},
		{
			name: "AnonymousHeadUint32PtrString",
			data: struct {
				structUint32PtrString
				B *uint32 `json:"b,string"`
			}{
				structUint32PtrString: structUint32PtrString{A: uint32ptr(1)},
				B:                     uint32ptr(2),
			},
		},

		// AnonymousHeadUint32PtrNil
		{
			name: "AnonymousHeadUint32PtrNil",
			data: struct {
				structUint32Ptr
				B *uint32 `json:"b"`
			}{
				structUint32Ptr: structUint32Ptr{A: nil},
				B:               uint32ptr(2),
			},
		},
		{
			name: "AnonymousHeadUint32PtrNilOmitEmpty",
			data: struct {
				structUint32PtrOmitEmpty
				B *uint32 `json:"b,omitempty"`
			}{
				structUint32PtrOmitEmpty: structUint32PtrOmitEmpty{A: nil},
				B:                        uint32ptr(2),
			},
		},
		{
			name: "AnonymousHeadUint32PtrNilString",
			data: struct {
				structUint32PtrString
				B *uint32 `json:"b,string"`
			}{
				structUint32PtrString: structUint32PtrString{A: nil},
				B:                     uint32ptr(2),
			},
		},

		// PtrAnonymousHeadUint32Ptr
		{
			name: "PtrAnonymousHeadUint32Ptr",
			data: struct {
				*structUint32Ptr
				B *uint32 `json:"b"`
			}{
				structUint32Ptr: &structUint32Ptr{A: uint32ptr(1)},
				B:               uint32ptr(2),
			},
		},
		{
			name: "PtrAnonymousHeadUint32PtrOmitEmpty",
			data: struct {
				*structUint32PtrOmitEmpty
				B *uint32 `json:"b,omitempty"`
			}{
				structUint32PtrOmitEmpty: &structUint32PtrOmitEmpty{A: uint32ptr(1)},
				B:                        uint32ptr(2),
			},
		},
		{
			name: "PtrAnonymousHeadUint32PtrString",
			data: struct {
				*structUint32PtrString
				B *uint32 `json:"b,string"`
			}{
				structUint32PtrString: &structUint32PtrString{A: uint32ptr(1)},
				B:                     uint32ptr(2),
			},
		},

		// NilPtrAnonymousHeadUint32Ptr
		{
			name: "NilPtrAnonymousHeadUint32Ptr",
			data: struct {
				*structUint32Ptr
				B *uint32 `json:"b"`
			}{
				structUint32Ptr: nil,
				B:               uint32ptr(2),
			},
		},
		{
			name: "NilPtrAnonymousHeadUint32PtrOmitEmpty",
			data: struct {
				*structUint32PtrOmitEmpty
				B *uint32 `json:"b,omitempty"`
			}{
				structUint32PtrOmitEmpty: nil,
				B:                        uint32ptr(2),
			},
		},
		{
			name: "NilPtrAnonymousHeadUint32PtrString",
			data: struct {
				*structUint32PtrString
				B *uint32 `json:"b,string"`
			}{
				structUint32PtrString: nil,
				B:                     uint32ptr(2),
			},
		},

		// AnonymousHeadUint32Only
		{
			name: "AnonymousHeadUint32Only",
			data: struct {
				structUint32
			}{
				structUint32: structUint32{A: 1},
			},
		},
		{
			name: "AnonymousHeadUint32OnlyOmitEmpty",
			data: struct {
				structUint32OmitEmpty
			}{
				structUint32OmitEmpty: structUint32OmitEmpty{A: 1},
			},
		},
		{
			name: "AnonymousHeadUint32OnlyString",
			data: struct {
				structUint32String
			}{
				structUint32String: structUint32String{A: 1},
			},
		},

		// PtrAnonymousHeadUint32Only
		{
			name: "PtrAnonymousHeadUint32Only",
			data: struct {
				*structUint32
			}{
				structUint32: &structUint32{A: 1},
			},
		},
		{
			name: "PtrAnonymousHeadUint32OnlyOmitEmpty",
			data: struct {
				*structUint32OmitEmpty
			}{
				structUint32OmitEmpty: &structUint32OmitEmpty{A: 1},
			},
		},
		{
			name: "PtrAnonymousHeadUint32OnlyString",
			data: struct {
				*structUint32String
			}{
				structUint32String: &structUint32String{A: 1},
			},
		},

		// NilPtrAnonymousHeadUint32Only
		{
			name: "NilPtrAnonymousHeadUint32Only",
			data: struct {
				*structUint32
			}{
				structUint32: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadUint32OnlyOmitEmpty",
			data: struct {
				*structUint32OmitEmpty
			}{
				structUint32OmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadUint32OnlyString",
			data: struct {
				*structUint32String
			}{
				structUint32String: nil,
			},
		},

		// AnonymousHeadUint32PtrOnly
		{
			name: "AnonymousHeadUint32PtrOnly",
			data: struct {
				structUint32Ptr
			}{
				structUint32Ptr: structUint32Ptr{A: uint32ptr(1)},
			},
		},
		{
			name: "AnonymousHeadUint32PtrOnlyOmitEmpty",
			data: struct {
				structUint32PtrOmitEmpty
			}{
				structUint32PtrOmitEmpty: structUint32PtrOmitEmpty{A: uint32ptr(1)},
			},
		},
		{
			name: "AnonymousHeadUint32PtrOnlyString",
			data: struct {
				structUint32PtrString
			}{
				structUint32PtrString: structUint32PtrString{A: uint32ptr(1)},
			},
		},

		// AnonymousHeadUint32PtrNilOnly
		{
			name: "AnonymousHeadUint32PtrNilOnly",
			data: struct {
				structUint32Ptr
			}{
				structUint32Ptr: structUint32Ptr{A: nil},
			},
		},
		{
			name: "AnonymousHeadUint32PtrNilOnlyOmitEmpty",
			data: struct {
				structUint32PtrOmitEmpty
			}{
				structUint32PtrOmitEmpty: structUint32PtrOmitEmpty{A: nil},
			},
		},
		{
			name: "AnonymousHeadUint32PtrNilOnlyString",
			data: struct {
				structUint32PtrString
			}{
				structUint32PtrString: structUint32PtrString{A: nil},
			},
		},

		// PtrAnonymousHeadUint32PtrOnly
		{
			name: "PtrAnonymousHeadUint32PtrOnly",
			data: struct {
				*structUint32Ptr
			}{
				structUint32Ptr: &structUint32Ptr{A: uint32ptr(1)},
			},
		},
		{
			name: "PtrAnonymousHeadUint32PtrOnlyOmitEmpty",
			data: struct {
				*structUint32PtrOmitEmpty
			}{
				structUint32PtrOmitEmpty: &structUint32PtrOmitEmpty{A: uint32ptr(1)},
			},
		},
		{
			name: "PtrAnonymousHeadUint32PtrOnlyString",
			data: struct {
				*structUint32PtrString
			}{
				structUint32PtrString: &structUint32PtrString{A: uint32ptr(1)},
			},
		},

		// NilPtrAnonymousHeadUint32PtrOnly
		{
			name: "NilPtrAnonymousHeadUint32PtrOnly",
			data: struct {
				*structUint32Ptr
			}{
				structUint32Ptr: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadUint32PtrOnlyOmitEmpty",
			data: struct {
				*structUint32PtrOmitEmpty
			}{
				structUint32PtrOmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadUint32PtrOnlyString",
			data: struct {
				*structUint32PtrString
			}{
				structUint32PtrString: nil,
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
