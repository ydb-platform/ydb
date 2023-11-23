package json_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/goccy/go-json"
)

func TestCoverUint64(t *testing.T) {
	type structUint64 struct {
		A uint64 `json:"a"`
	}
	type structUint64OmitEmpty struct {
		A uint64 `json:"a,omitempty"`
	}
	type structUint64String struct {
		A uint64 `json:"a,string"`
	}

	type structUint64Ptr struct {
		A *uint64 `json:"a"`
	}
	type structUint64PtrOmitEmpty struct {
		A *uint64 `json:"a,omitempty"`
	}
	type structUint64PtrString struct {
		A *uint64 `json:"a,string"`
	}

	tests := []struct {
		name string
		data interface{}
	}{
		{
			name: "Uint64",
			data: uint64(10),
		},
		{
			name: "Uint64Ptr",
			data: uint64ptr(10),
		},
		{
			name: "Uint64Ptr3",
			data: uint64ptr3(10),
		},
		{
			name: "Uint64PtrNil",
			data: (*uint64)(nil),
		},
		{
			name: "Uint64Ptr3Nil",
			data: (***uint64)(nil),
		},

		// HeadUint64Zero
		{
			name: "HeadUint64Zero",
			data: struct {
				A uint64 `json:"a"`
			}{},
		},
		{
			name: "HeadUint64ZeroOmitEmpty",
			data: struct {
				A uint64 `json:"a,omitempty"`
			}{},
		},
		{
			name: "HeadUint64ZeroString",
			data: struct {
				A uint64 `json:"a,string"`
			}{},
		},

		// HeadUint64
		{
			name: "HeadUint64",
			data: struct {
				A uint64 `json:"a"`
			}{A: 1},
		},
		{
			name: "HeadUint64OmitEmpty",
			data: struct {
				A uint64 `json:"a,omitempty"`
			}{A: 1},
		},
		{
			name: "HeadUint64String",
			data: struct {
				A uint64 `json:"a,string"`
			}{A: 1},
		},

		// HeadUint64Ptr
		{
			name: "HeadUint64Ptr",
			data: struct {
				A *uint64 `json:"a"`
			}{A: uint64ptr(1)},
		},
		{
			name: "HeadUint64PtrOmitEmpty",
			data: struct {
				A *uint64 `json:"a,omitempty"`
			}{A: uint64ptr(1)},
		},
		{
			name: "HeadUint64PtrString",
			data: struct {
				A *uint64 `json:"a,string"`
			}{A: uint64ptr(1)},
		},

		// HeadUint64PtrNil
		{
			name: "HeadUint64PtrNil",
			data: struct {
				A *uint64 `json:"a"`
			}{A: nil},
		},
		{
			name: "HeadUint64PtrNilOmitEmpty",
			data: struct {
				A *uint64 `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "HeadUint64PtrNilString",
			data: struct {
				A *uint64 `json:"a,string"`
			}{A: nil},
		},

		// PtrHeadUint64Zero
		{
			name: "PtrHeadUint64Zero",
			data: &struct {
				A uint64 `json:"a"`
			}{},
		},
		{
			name: "PtrHeadUint64ZeroOmitEmpty",
			data: &struct {
				A uint64 `json:"a,omitempty"`
			}{},
		},
		{
			name: "PtrHeadUint64ZeroString",
			data: &struct {
				A uint64 `json:"a,string"`
			}{},
		},

		// PtrHeadUint64
		{
			name: "PtrHeadUint64",
			data: &struct {
				A uint64 `json:"a"`
			}{A: 1},
		},
		{
			name: "PtrHeadUint64OmitEmpty",
			data: &struct {
				A uint64 `json:"a,omitempty"`
			}{A: 1},
		},
		{
			name: "PtrHeadUint64String",
			data: &struct {
				A uint64 `json:"a,string"`
			}{A: 1},
		},

		// PtrHeadUint64Ptr
		{
			name: "PtrHeadUint64Ptr",
			data: &struct {
				A *uint64 `json:"a"`
			}{A: uint64ptr(1)},
		},
		{
			name: "PtrHeadUint64PtrOmitEmpty",
			data: &struct {
				A *uint64 `json:"a,omitempty"`
			}{A: uint64ptr(1)},
		},
		{
			name: "PtrHeadUint64PtrString",
			data: &struct {
				A *uint64 `json:"a,string"`
			}{A: uint64ptr(1)},
		},

		// PtrHeadUint64PtrNil
		{
			name: "PtrHeadUint64PtrNil",
			data: &struct {
				A *uint64 `json:"a"`
			}{A: nil},
		},
		{
			name: "PtrHeadUint64PtrNilOmitEmpty",
			data: &struct {
				A *uint64 `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadUint64PtrNilString",
			data: &struct {
				A *uint64 `json:"a,string"`
			}{A: nil},
		},

		// PtrHeadUint64Nil
		{
			name: "PtrHeadUint64Nil",
			data: (*struct {
				A *uint64 `json:"a"`
			})(nil),
		},
		{
			name: "PtrHeadUint64NilOmitEmpty",
			data: (*struct {
				A *uint64 `json:"a,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadUint64NilString",
			data: (*struct {
				A *uint64 `json:"a,string"`
			})(nil),
		},

		// HeadUint64ZeroMultiFields
		{
			name: "HeadUint64ZeroMultiFields",
			data: struct {
				A uint64 `json:"a"`
				B uint64 `json:"b"`
				C uint64 `json:"c"`
			}{},
		},
		{
			name: "HeadUint64ZeroMultiFieldsOmitEmpty",
			data: struct {
				A uint64 `json:"a,omitempty"`
				B uint64 `json:"b,omitempty"`
				C uint64 `json:"c,omitempty"`
			}{},
		},
		{
			name: "HeadUint64ZeroMultiFields",
			data: struct {
				A uint64 `json:"a,string"`
				B uint64 `json:"b,string"`
				C uint64 `json:"c,string"`
			}{},
		},

		// HeadUint64MultiFields
		{
			name: "HeadUint64MultiFields",
			data: struct {
				A uint64 `json:"a"`
				B uint64 `json:"b"`
				C uint64 `json:"c"`
			}{A: 1, B: 2, C: 3},
		},
		{
			name: "HeadUint64MultiFieldsOmitEmpty",
			data: struct {
				A uint64 `json:"a,omitempty"`
				B uint64 `json:"b,omitempty"`
				C uint64 `json:"c,omitempty"`
			}{A: 1, B: 2, C: 3},
		},
		{
			name: "HeadUint64MultiFieldsString",
			data: struct {
				A uint64 `json:"a,string"`
				B uint64 `json:"b,string"`
				C uint64 `json:"c,string"`
			}{A: 1, B: 2, C: 3},
		},

		// HeadUint64PtrMultiFields
		{
			name: "HeadUint64PtrMultiFields",
			data: struct {
				A *uint64 `json:"a"`
				B *uint64 `json:"b"`
				C *uint64 `json:"c"`
			}{A: uint64ptr(1), B: uint64ptr(2), C: uint64ptr(3)},
		},
		{
			name: "HeadUint64PtrMultiFieldsOmitEmpty",
			data: struct {
				A *uint64 `json:"a,omitempty"`
				B *uint64 `json:"b,omitempty"`
				C *uint64 `json:"c,omitempty"`
			}{A: uint64ptr(1), B: uint64ptr(2), C: uint64ptr(3)},
		},
		{
			name: "HeadUint64PtrMultiFieldsString",
			data: struct {
				A *uint64 `json:"a,string"`
				B *uint64 `json:"b,string"`
				C *uint64 `json:"c,string"`
			}{A: uint64ptr(1), B: uint64ptr(2), C: uint64ptr(3)},
		},

		// HeadUint64PtrNilMultiFields
		{
			name: "HeadUint64PtrNilMultiFields",
			data: struct {
				A *uint64 `json:"a"`
				B *uint64 `json:"b"`
				C *uint64 `json:"c"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadUint64PtrNilMultiFieldsOmitEmpty",
			data: struct {
				A *uint64 `json:"a,omitempty"`
				B *uint64 `json:"b,omitempty"`
				C *uint64 `json:"c,omitempty"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadUint64PtrNilMultiFieldsString",
			data: struct {
				A *uint64 `json:"a,string"`
				B *uint64 `json:"b,string"`
				C *uint64 `json:"c,string"`
			}{A: nil, B: nil, C: nil},
		},

		// PtrHeadUint64ZeroMultiFields
		{
			name: "PtrHeadUint64ZeroMultiFields",
			data: &struct {
				A uint64 `json:"a"`
				B uint64 `json:"b"`
			}{},
		},
		{
			name: "PtrHeadUint64ZeroMultiFieldsOmitEmpty",
			data: &struct {
				A uint64 `json:"a,omitempty"`
				B uint64 `json:"b,omitempty"`
			}{},
		},
		{
			name: "PtrHeadUint64ZeroMultiFieldsString",
			data: &struct {
				A uint64 `json:"a,string"`
				B uint64 `json:"b,string"`
			}{},
		},

		// PtrHeadUint64MultiFields
		{
			name: "PtrHeadUint64MultiFields",
			data: &struct {
				A uint64 `json:"a"`
				B uint64 `json:"b"`
			}{A: 1, B: 2},
		},
		{
			name: "PtrHeadUint64MultiFieldsOmitEmpty",
			data: &struct {
				A uint64 `json:"a,omitempty"`
				B uint64 `json:"b,omitempty"`
			}{A: 1, B: 2},
		},
		{
			name: "PtrHeadUint64MultiFieldsString",
			data: &struct {
				A uint64 `json:"a,string"`
				B uint64 `json:"b,string"`
			}{A: 1, B: 2},
		},

		// PtrHeadUint64PtrMultiFields
		{
			name: "PtrHeadUint64PtrMultiFields",
			data: &struct {
				A *uint64 `json:"a"`
				B *uint64 `json:"b"`
			}{A: uint64ptr(1), B: uint64ptr(2)},
		},
		{
			name: "PtrHeadUint64PtrMultiFieldsOmitEmpty",
			data: &struct {
				A *uint64 `json:"a,omitempty"`
				B *uint64 `json:"b,omitempty"`
			}{A: uint64ptr(1), B: uint64ptr(2)},
		},
		{
			name: "PtrHeadUint64PtrMultiFieldsString",
			data: &struct {
				A *uint64 `json:"a,string"`
				B *uint64 `json:"b,string"`
			}{A: uint64ptr(1), B: uint64ptr(2)},
		},

		// PtrHeadUint64PtrNilMultiFields
		{
			name: "PtrHeadUint64PtrNilMultiFields",
			data: &struct {
				A *uint64 `json:"a"`
				B *uint64 `json:"b"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUint64PtrNilMultiFieldsOmitEmpty",
			data: &struct {
				A *uint64 `json:"a,omitempty"`
				B *uint64 `json:"b,omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUint64PtrNilMultiFieldsString",
			data: &struct {
				A *uint64 `json:"a,string"`
				B *uint64 `json:"b,string"`
			}{A: nil, B: nil},
		},

		// PtrHeadUint64NilMultiFields
		{
			name: "PtrHeadUint64NilMultiFields",
			data: (*struct {
				A *uint64 `json:"a"`
				B *uint64 `json:"b"`
			})(nil),
		},
		{
			name: "PtrHeadUint64NilMultiFieldsOmitEmpty",
			data: (*struct {
				A *uint64 `json:"a,omitempty"`
				B *uint64 `json:"b,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadUint64NilMultiFieldsString",
			data: (*struct {
				A *uint64 `json:"a,string"`
				B *uint64 `json:"b,string"`
			})(nil),
		},

		// HeadUint64ZeroNotRoot
		{
			name: "HeadUint64ZeroNotRoot",
			data: struct {
				A struct {
					A uint64 `json:"a"`
				}
			}{},
		},
		{
			name: "HeadUint64ZeroNotRootOmitEmpty",
			data: struct {
				A struct {
					A uint64 `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadUint64ZeroNotRootString",
			data: struct {
				A struct {
					A uint64 `json:"a,string"`
				}
			}{},
		},

		// HeadUint64NotRoot
		{
			name: "HeadUint64NotRoot",
			data: struct {
				A struct {
					A uint64 `json:"a"`
				}
			}{A: struct {
				A uint64 `json:"a"`
			}{A: 1}},
		},
		{
			name: "HeadUint64NotRootOmitEmpty",
			data: struct {
				A struct {
					A uint64 `json:"a,omitempty"`
				}
			}{A: struct {
				A uint64 `json:"a,omitempty"`
			}{A: 1}},
		},
		{
			name: "HeadUint64NotRootString",
			data: struct {
				A struct {
					A uint64 `json:"a,string"`
				}
			}{A: struct {
				A uint64 `json:"a,string"`
			}{A: 1}},
		},

		// HeadUint64PtrNotRoot
		{
			name: "HeadUint64PtrNotRoot",
			data: struct {
				A struct {
					A *uint64 `json:"a"`
				}
			}{A: struct {
				A *uint64 `json:"a"`
			}{uint64ptr(1)}},
		},
		{
			name: "HeadUint64PtrNotRootOmitEmpty",
			data: struct {
				A struct {
					A *uint64 `json:"a,omitempty"`
				}
			}{A: struct {
				A *uint64 `json:"a,omitempty"`
			}{uint64ptr(1)}},
		},
		{
			name: "HeadUint64PtrNotRootString",
			data: struct {
				A struct {
					A *uint64 `json:"a,string"`
				}
			}{A: struct {
				A *uint64 `json:"a,string"`
			}{uint64ptr(1)}},
		},

		// HeadUint64PtrNilNotRoot
		{
			name: "HeadUint64PtrNilNotRoot",
			data: struct {
				A struct {
					A *uint64 `json:"a"`
				}
			}{},
		},
		{
			name: "HeadUint64PtrNilNotRootOmitEmpty",
			data: struct {
				A struct {
					A *uint64 `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadUint64PtrNilNotRootString",
			data: struct {
				A struct {
					A *uint64 `json:"a,string"`
				}
			}{},
		},

		// PtrHeadUint64ZeroNotRoot
		{
			name: "PtrHeadUint64ZeroNotRoot",
			data: struct {
				A *struct {
					A uint64 `json:"a"`
				}
			}{A: new(struct {
				A uint64 `json:"a"`
			})},
		},
		{
			name: "PtrHeadUint64ZeroNotRootOmitEmpty",
			data: struct {
				A *struct {
					A uint64 `json:"a,omitempty"`
				}
			}{A: new(struct {
				A uint64 `json:"a,omitempty"`
			})},
		},
		{
			name: "PtrHeadUint64ZeroNotRootString",
			data: struct {
				A *struct {
					A uint64 `json:"a,string"`
				}
			}{A: new(struct {
				A uint64 `json:"a,string"`
			})},
		},

		// PtrHeadUint64NotRoot
		{
			name: "PtrHeadUint64NotRoot",
			data: struct {
				A *struct {
					A uint64 `json:"a"`
				}
			}{A: &(struct {
				A uint64 `json:"a"`
			}{A: 1})},
		},
		{
			name: "PtrHeadUint64NotRootOmitEmpty",
			data: struct {
				A *struct {
					A uint64 `json:"a,omitempty"`
				}
			}{A: &(struct {
				A uint64 `json:"a,omitempty"`
			}{A: 1})},
		},
		{
			name: "PtrHeadUint64NotRootString",
			data: struct {
				A *struct {
					A uint64 `json:"a,string"`
				}
			}{A: &(struct {
				A uint64 `json:"a,string"`
			}{A: 1})},
		},

		// PtrHeadUint64PtrNotRoot
		{
			name: "PtrHeadUint64PtrNotRoot",
			data: struct {
				A *struct {
					A *uint64 `json:"a"`
				}
			}{A: &(struct {
				A *uint64 `json:"a"`
			}{A: uint64ptr(1)})},
		},
		{
			name: "PtrHeadUint64PtrNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *uint64 `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *uint64 `json:"a,omitempty"`
			}{A: uint64ptr(1)})},
		},
		{
			name: "PtrHeadUint64PtrNotRootString",
			data: struct {
				A *struct {
					A *uint64 `json:"a,string"`
				}
			}{A: &(struct {
				A *uint64 `json:"a,string"`
			}{A: uint64ptr(1)})},
		},

		// PtrHeadUint64PtrNilNotRoot
		{
			name: "PtrHeadUint64PtrNilNotRoot",
			data: struct {
				A *struct {
					A *uint64 `json:"a"`
				}
			}{A: &(struct {
				A *uint64 `json:"a"`
			}{A: nil})},
		},
		{
			name: "PtrHeadUint64PtrNilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *uint64 `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *uint64 `json:"a,omitempty"`
			}{A: nil})},
		},
		{
			name: "PtrHeadUint64PtrNilNotRootString",
			data: struct {
				A *struct {
					A *uint64 `json:"a,string"`
				}
			}{A: &(struct {
				A *uint64 `json:"a,string"`
			}{A: nil})},
		},

		// PtrHeadUint64NilNotRoot
		{
			name: "PtrHeadUint64NilNotRoot",
			data: struct {
				A *struct {
					A *uint64 `json:"a"`
				}
			}{A: nil},
		},
		{
			name: "PtrHeadUint64NilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *uint64 `json:"a,omitempty"`
				} `json:",omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadUint64NilNotRootString",
			data: struct {
				A *struct {
					A *uint64 `json:"a,string"`
				} `json:",string"`
			}{A: nil},
		},

		// HeadUint64ZeroMultiFieldsNotRoot
		{
			name: "HeadUint64ZeroMultiFieldsNotRoot",
			data: struct {
				A struct {
					A uint64 `json:"a"`
				}
				B struct {
					B uint64 `json:"b"`
				}
			}{},
		},
		{
			name: "HeadUint64ZeroMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A uint64 `json:"a,omitempty"`
				}
				B struct {
					B uint64 `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "HeadUint64ZeroMultiFieldsNotRootString",
			data: struct {
				A struct {
					A uint64 `json:"a,string"`
				}
				B struct {
					B uint64 `json:"b,string"`
				}
			}{},
		},

		// HeadUint64MultiFieldsNotRoot
		{
			name: "HeadUint64MultiFieldsNotRoot",
			data: struct {
				A struct {
					A uint64 `json:"a"`
				}
				B struct {
					B uint64 `json:"b"`
				}
			}{A: struct {
				A uint64 `json:"a"`
			}{A: 1}, B: struct {
				B uint64 `json:"b"`
			}{B: 2}},
		},
		{
			name: "HeadUint64MultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A uint64 `json:"a,omitempty"`
				}
				B struct {
					B uint64 `json:"b,omitempty"`
				}
			}{A: struct {
				A uint64 `json:"a,omitempty"`
			}{A: 1}, B: struct {
				B uint64 `json:"b,omitempty"`
			}{B: 2}},
		},
		{
			name: "HeadUint64MultiFieldsNotRootString",
			data: struct {
				A struct {
					A uint64 `json:"a,string"`
				}
				B struct {
					B uint64 `json:"b,string"`
				}
			}{A: struct {
				A uint64 `json:"a,string"`
			}{A: 1}, B: struct {
				B uint64 `json:"b,string"`
			}{B: 2}},
		},

		// HeadUint64PtrMultiFieldsNotRoot
		{
			name: "HeadUint64PtrMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *uint64 `json:"a"`
				}
				B struct {
					B *uint64 `json:"b"`
				}
			}{A: struct {
				A *uint64 `json:"a"`
			}{A: uint64ptr(1)}, B: struct {
				B *uint64 `json:"b"`
			}{B: uint64ptr(2)}},
		},
		{
			name: "HeadUint64PtrMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *uint64 `json:"a,omitempty"`
				}
				B struct {
					B *uint64 `json:"b,omitempty"`
				}
			}{A: struct {
				A *uint64 `json:"a,omitempty"`
			}{A: uint64ptr(1)}, B: struct {
				B *uint64 `json:"b,omitempty"`
			}{B: uint64ptr(2)}},
		},
		{
			name: "HeadUint64PtrMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *uint64 `json:"a,string"`
				}
				B struct {
					B *uint64 `json:"b,string"`
				}
			}{A: struct {
				A *uint64 `json:"a,string"`
			}{A: uint64ptr(1)}, B: struct {
				B *uint64 `json:"b,string"`
			}{B: uint64ptr(2)}},
		},

		// HeadUint64PtrNilMultiFieldsNotRoot
		{
			name: "HeadUint64PtrNilMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *uint64 `json:"a"`
				}
				B struct {
					B *uint64 `json:"b"`
				}
			}{A: struct {
				A *uint64 `json:"a"`
			}{A: nil}, B: struct {
				B *uint64 `json:"b"`
			}{B: nil}},
		},
		{
			name: "HeadUint64PtrNilMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *uint64 `json:"a,omitempty"`
				}
				B struct {
					B *uint64 `json:"b,omitempty"`
				}
			}{A: struct {
				A *uint64 `json:"a,omitempty"`
			}{A: nil}, B: struct {
				B *uint64 `json:"b,omitempty"`
			}{B: nil}},
		},
		{
			name: "HeadUint64PtrNilMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *uint64 `json:"a,string"`
				}
				B struct {
					B *uint64 `json:"b,string"`
				}
			}{A: struct {
				A *uint64 `json:"a,string"`
			}{A: nil}, B: struct {
				B *uint64 `json:"b,string"`
			}{B: nil}},
		},

		// PtrHeadUint64ZeroMultiFieldsNotRoot
		{
			name: "PtrHeadUint64ZeroMultiFieldsNotRoot",
			data: &struct {
				A struct {
					A uint64 `json:"a"`
				}
				B struct {
					B uint64 `json:"b"`
				}
			}{},
		},
		{
			name: "PtrHeadUint64ZeroMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A uint64 `json:"a,omitempty"`
				}
				B struct {
					B uint64 `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "PtrHeadUint64ZeroMultiFieldsNotRootString",
			data: &struct {
				A struct {
					A uint64 `json:"a,string"`
				}
				B struct {
					B uint64 `json:"b,string"`
				}
			}{},
		},

		// PtrHeadUint64MultiFieldsNotRoot
		{
			name: "PtrHeadUint64MultiFieldsNotRoot",
			data: &struct {
				A struct {
					A uint64 `json:"a"`
				}
				B struct {
					B uint64 `json:"b"`
				}
			}{A: struct {
				A uint64 `json:"a"`
			}{A: 1}, B: struct {
				B uint64 `json:"b"`
			}{B: 2}},
		},
		{
			name: "PtrHeadUint64MultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A uint64 `json:"a,omitempty"`
				}
				B struct {
					B uint64 `json:"b,omitempty"`
				}
			}{A: struct {
				A uint64 `json:"a,omitempty"`
			}{A: 1}, B: struct {
				B uint64 `json:"b,omitempty"`
			}{B: 2}},
		},
		{
			name: "PtrHeadUint64MultiFieldsNotRootString",
			data: &struct {
				A struct {
					A uint64 `json:"a,string"`
				}
				B struct {
					B uint64 `json:"b,string"`
				}
			}{A: struct {
				A uint64 `json:"a,string"`
			}{A: 1}, B: struct {
				B uint64 `json:"b,string"`
			}{B: 2}},
		},

		// PtrHeadUint64PtrMultiFieldsNotRoot
		{
			name: "PtrHeadUint64PtrMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *uint64 `json:"a"`
				}
				B *struct {
					B *uint64 `json:"b"`
				}
			}{A: &(struct {
				A *uint64 `json:"a"`
			}{A: uint64ptr(1)}), B: &(struct {
				B *uint64 `json:"b"`
			}{B: uint64ptr(2)})},
		},
		{
			name: "PtrHeadUint64PtrMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *uint64 `json:"a,omitempty"`
				}
				B *struct {
					B *uint64 `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *uint64 `json:"a,omitempty"`
			}{A: uint64ptr(1)}), B: &(struct {
				B *uint64 `json:"b,omitempty"`
			}{B: uint64ptr(2)})},
		},
		{
			name: "PtrHeadUint64PtrMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *uint64 `json:"a,string"`
				}
				B *struct {
					B *uint64 `json:"b,string"`
				}
			}{A: &(struct {
				A *uint64 `json:"a,string"`
			}{A: uint64ptr(1)}), B: &(struct {
				B *uint64 `json:"b,string"`
			}{B: uint64ptr(2)})},
		},

		// PtrHeadUint64PtrNilMultiFieldsNotRoot
		{
			name: "PtrHeadUint64PtrNilMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *uint64 `json:"a"`
				}
				B *struct {
					B *uint64 `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUint64PtrNilMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *uint64 `json:"a,omitempty"`
				} `json:",omitempty"`
				B *struct {
					B *uint64 `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUint64PtrNilMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *uint64 `json:"a,string"`
				} `json:",string"`
				B *struct {
					B *uint64 `json:"b,string"`
				} `json:",string"`
			}{A: nil, B: nil},
		},

		// PtrHeadUint64NilMultiFieldsNotRoot
		{
			name: "PtrHeadUint64NilMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *uint64 `json:"a"`
				}
				B *struct {
					B *uint64 `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadUint64NilMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *uint64 `json:"a,omitempty"`
				}
				B *struct {
					B *uint64 `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadUint64NilMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *uint64 `json:"a,string"`
				}
				B *struct {
					B *uint64 `json:"b,string"`
				}
			})(nil),
		},

		// PtrHeadUint64DoubleMultiFieldsNotRoot
		{
			name: "PtrHeadUint64DoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A uint64 `json:"a"`
					B uint64 `json:"b"`
				}
				B *struct {
					A uint64 `json:"a"`
					B uint64 `json:"b"`
				}
			}{A: &(struct {
				A uint64 `json:"a"`
				B uint64 `json:"b"`
			}{A: 1, B: 2}), B: &(struct {
				A uint64 `json:"a"`
				B uint64 `json:"b"`
			}{A: 3, B: 4})},
		},
		{
			name: "PtrHeadUint64DoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A uint64 `json:"a,omitempty"`
					B uint64 `json:"b,omitempty"`
				}
				B *struct {
					A uint64 `json:"a,omitempty"`
					B uint64 `json:"b,omitempty"`
				}
			}{A: &(struct {
				A uint64 `json:"a,omitempty"`
				B uint64 `json:"b,omitempty"`
			}{A: 1, B: 2}), B: &(struct {
				A uint64 `json:"a,omitempty"`
				B uint64 `json:"b,omitempty"`
			}{A: 3, B: 4})},
		},
		{
			name: "PtrHeadUint64DoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A uint64 `json:"a,string"`
					B uint64 `json:"b,string"`
				}
				B *struct {
					A uint64 `json:"a,string"`
					B uint64 `json:"b,string"`
				}
			}{A: &(struct {
				A uint64 `json:"a,string"`
				B uint64 `json:"b,string"`
			}{A: 1, B: 2}), B: &(struct {
				A uint64 `json:"a,string"`
				B uint64 `json:"b,string"`
			}{A: 3, B: 4})},
		},

		// PtrHeadUint64NilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadUint64NilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A uint64 `json:"a"`
					B uint64 `json:"b"`
				}
				B *struct {
					A uint64 `json:"a"`
					B uint64 `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUint64NilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A uint64 `json:"a,omitempty"`
					B uint64 `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A uint64 `json:"a,omitempty"`
					B uint64 `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUint64NilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A uint64 `json:"a,string"`
					B uint64 `json:"b,string"`
				}
				B *struct {
					A uint64 `json:"a,string"`
					B uint64 `json:"b,string"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadUint64NilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadUint64NilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A uint64 `json:"a"`
					B uint64 `json:"b"`
				}
				B *struct {
					A uint64 `json:"a"`
					B uint64 `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadUint64NilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A uint64 `json:"a,omitempty"`
					B uint64 `json:"b,omitempty"`
				}
				B *struct {
					A uint64 `json:"a,omitempty"`
					B uint64 `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadUint64NilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A uint64 `json:"a,string"`
					B uint64 `json:"b,string"`
				}
				B *struct {
					A uint64 `json:"a,string"`
					B uint64 `json:"b,string"`
				}
			})(nil),
		},

		// PtrHeadUint64PtrDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadUint64PtrDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *uint64 `json:"a"`
					B *uint64 `json:"b"`
				}
				B *struct {
					A *uint64 `json:"a"`
					B *uint64 `json:"b"`
				}
			}{A: &(struct {
				A *uint64 `json:"a"`
				B *uint64 `json:"b"`
			}{A: uint64ptr(1), B: uint64ptr(2)}), B: &(struct {
				A *uint64 `json:"a"`
				B *uint64 `json:"b"`
			}{A: uint64ptr(3), B: uint64ptr(4)})},
		},
		{
			name: "PtrHeadUint64PtrDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *uint64 `json:"a,omitempty"`
					B *uint64 `json:"b,omitempty"`
				}
				B *struct {
					A *uint64 `json:"a,omitempty"`
					B *uint64 `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *uint64 `json:"a,omitempty"`
				B *uint64 `json:"b,omitempty"`
			}{A: uint64ptr(1), B: uint64ptr(2)}), B: &(struct {
				A *uint64 `json:"a,omitempty"`
				B *uint64 `json:"b,omitempty"`
			}{A: uint64ptr(3), B: uint64ptr(4)})},
		},
		{
			name: "PtrHeadUint64PtrDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *uint64 `json:"a,string"`
					B *uint64 `json:"b,string"`
				}
				B *struct {
					A *uint64 `json:"a,string"`
					B *uint64 `json:"b,string"`
				}
			}{A: &(struct {
				A *uint64 `json:"a,string"`
				B *uint64 `json:"b,string"`
			}{A: uint64ptr(1), B: uint64ptr(2)}), B: &(struct {
				A *uint64 `json:"a,string"`
				B *uint64 `json:"b,string"`
			}{A: uint64ptr(3), B: uint64ptr(4)})},
		},

		// PtrHeadUint64PtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadUint64PtrNilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *uint64 `json:"a"`
					B *uint64 `json:"b"`
				}
				B *struct {
					A *uint64 `json:"a"`
					B *uint64 `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUint64PtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *uint64 `json:"a,omitempty"`
					B *uint64 `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A *uint64 `json:"a,omitempty"`
					B *uint64 `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUint64PtrNilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *uint64 `json:"a,string"`
					B *uint64 `json:"b,string"`
				}
				B *struct {
					A *uint64 `json:"a,string"`
					B *uint64 `json:"b,string"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadUint64PtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadUint64PtrNilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *uint64 `json:"a"`
					B *uint64 `json:"b"`
				}
				B *struct {
					A *uint64 `json:"a"`
					B *uint64 `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadUint64PtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *uint64 `json:"a,omitempty"`
					B *uint64 `json:"b,omitempty"`
				}
				B *struct {
					A *uint64 `json:"a,omitempty"`
					B *uint64 `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadUint64PtrNilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *uint64 `json:"a,string"`
					B *uint64 `json:"b,string"`
				}
				B *struct {
					A *uint64 `json:"a,string"`
					B *uint64 `json:"b,string"`
				}
			})(nil),
		},

		// AnonymousHeadUint64
		{
			name: "AnonymousHeadUint64",
			data: struct {
				structUint64
				B uint64 `json:"b"`
			}{
				structUint64: structUint64{A: 1},
				B:            2,
			},
		},
		{
			name: "AnonymousHeadUint64OmitEmpty",
			data: struct {
				structUint64OmitEmpty
				B uint64 `json:"b,omitempty"`
			}{
				structUint64OmitEmpty: structUint64OmitEmpty{A: 1},
				B:                     2,
			},
		},
		{
			name: "AnonymousHeadUint64String",
			data: struct {
				structUint64String
				B uint64 `json:"b,string"`
			}{
				structUint64String: structUint64String{A: 1},
				B:                  2,
			},
		},

		// PtrAnonymousHeadUint64
		{
			name: "PtrAnonymousHeadUint64",
			data: struct {
				*structUint64
				B uint64 `json:"b"`
			}{
				structUint64: &structUint64{A: 1},
				B:            2,
			},
		},
		{
			name: "PtrAnonymousHeadUint64OmitEmpty",
			data: struct {
				*structUint64OmitEmpty
				B uint64 `json:"b,omitempty"`
			}{
				structUint64OmitEmpty: &structUint64OmitEmpty{A: 1},
				B:                     2,
			},
		},
		{
			name: "PtrAnonymousHeadUint64String",
			data: struct {
				*structUint64String
				B uint64 `json:"b,string"`
			}{
				structUint64String: &structUint64String{A: 1},
				B:                  2,
			},
		},

		// NilPtrAnonymousHeadUint64
		{
			name: "NilPtrAnonymousHeadUint64",
			data: struct {
				*structUint64
				B uint64 `json:"b"`
			}{
				structUint64: nil,
				B:            2,
			},
		},
		{
			name: "NilPtrAnonymousHeadUint64OmitEmpty",
			data: struct {
				*structUint64OmitEmpty
				B uint64 `json:"b,omitempty"`
			}{
				structUint64OmitEmpty: nil,
				B:                     2,
			},
		},
		{
			name: "NilPtrAnonymousHeadUint64String",
			data: struct {
				*structUint64String
				B uint64 `json:"b,string"`
			}{
				structUint64String: nil,
				B:                  2,
			},
		},

		// AnonymousHeadUint64Ptr
		{
			name: "AnonymousHeadUint64Ptr",
			data: struct {
				structUint64Ptr
				B *uint64 `json:"b"`
			}{
				structUint64Ptr: structUint64Ptr{A: uint64ptr(1)},
				B:               uint64ptr(2),
			},
		},
		{
			name: "AnonymousHeadUint64PtrOmitEmpty",
			data: struct {
				structUint64PtrOmitEmpty
				B *uint64 `json:"b,omitempty"`
			}{
				structUint64PtrOmitEmpty: structUint64PtrOmitEmpty{A: uint64ptr(1)},
				B:                        uint64ptr(2),
			},
		},
		{
			name: "AnonymousHeadUint64PtrString",
			data: struct {
				structUint64PtrString
				B *uint64 `json:"b,string"`
			}{
				structUint64PtrString: structUint64PtrString{A: uint64ptr(1)},
				B:                     uint64ptr(2),
			},
		},

		// AnonymousHeadUint64PtrNil
		{
			name: "AnonymousHeadUint64PtrNil",
			data: struct {
				structUint64Ptr
				B *uint64 `json:"b"`
			}{
				structUint64Ptr: structUint64Ptr{A: nil},
				B:               uint64ptr(2),
			},
		},
		{
			name: "AnonymousHeadUint64PtrNilOmitEmpty",
			data: struct {
				structUint64PtrOmitEmpty
				B *uint64 `json:"b,omitempty"`
			}{
				structUint64PtrOmitEmpty: structUint64PtrOmitEmpty{A: nil},
				B:                        uint64ptr(2),
			},
		},
		{
			name: "AnonymousHeadUint64PtrNilString",
			data: struct {
				structUint64PtrString
				B *uint64 `json:"b,string"`
			}{
				structUint64PtrString: structUint64PtrString{A: nil},
				B:                     uint64ptr(2),
			},
		},

		// PtrAnonymousHeadUint64Ptr
		{
			name: "PtrAnonymousHeadUint64Ptr",
			data: struct {
				*structUint64Ptr
				B *uint64 `json:"b"`
			}{
				structUint64Ptr: &structUint64Ptr{A: uint64ptr(1)},
				B:               uint64ptr(2),
			},
		},
		{
			name: "PtrAnonymousHeadUint64PtrOmitEmpty",
			data: struct {
				*structUint64PtrOmitEmpty
				B *uint64 `json:"b,omitempty"`
			}{
				structUint64PtrOmitEmpty: &structUint64PtrOmitEmpty{A: uint64ptr(1)},
				B:                        uint64ptr(2),
			},
		},
		{
			name: "PtrAnonymousHeadUint64PtrString",
			data: struct {
				*structUint64PtrString
				B *uint64 `json:"b,string"`
			}{
				structUint64PtrString: &structUint64PtrString{A: uint64ptr(1)},
				B:                     uint64ptr(2),
			},
		},

		// NilPtrAnonymousHeadUint64Ptr
		{
			name: "NilPtrAnonymousHeadUint64Ptr",
			data: struct {
				*structUint64Ptr
				B *uint64 `json:"b"`
			}{
				structUint64Ptr: nil,
				B:               uint64ptr(2),
			},
		},
		{
			name: "NilPtrAnonymousHeadUint64PtrOmitEmpty",
			data: struct {
				*structUint64PtrOmitEmpty
				B *uint64 `json:"b,omitempty"`
			}{
				structUint64PtrOmitEmpty: nil,
				B:                        uint64ptr(2),
			},
		},
		{
			name: "NilPtrAnonymousHeadUint64PtrString",
			data: struct {
				*structUint64PtrString
				B *uint64 `json:"b,string"`
			}{
				structUint64PtrString: nil,
				B:                     uint64ptr(2),
			},
		},

		// AnonymousHeadUint64Only
		{
			name: "AnonymousHeadUint64Only",
			data: struct {
				structUint64
			}{
				structUint64: structUint64{A: 1},
			},
		},
		{
			name: "AnonymousHeadUint64OnlyOmitEmpty",
			data: struct {
				structUint64OmitEmpty
			}{
				structUint64OmitEmpty: structUint64OmitEmpty{A: 1},
			},
		},
		{
			name: "AnonymousHeadUint64OnlyString",
			data: struct {
				structUint64String
			}{
				structUint64String: structUint64String{A: 1},
			},
		},

		// PtrAnonymousHeadUint64Only
		{
			name: "PtrAnonymousHeadUint64Only",
			data: struct {
				*structUint64
			}{
				structUint64: &structUint64{A: 1},
			},
		},
		{
			name: "PtrAnonymousHeadUint64OnlyOmitEmpty",
			data: struct {
				*structUint64OmitEmpty
			}{
				structUint64OmitEmpty: &structUint64OmitEmpty{A: 1},
			},
		},
		{
			name: "PtrAnonymousHeadUint64OnlyString",
			data: struct {
				*structUint64String
			}{
				structUint64String: &structUint64String{A: 1},
			},
		},

		// NilPtrAnonymousHeadUint64Only
		{
			name: "NilPtrAnonymousHeadUint64Only",
			data: struct {
				*structUint64
			}{
				structUint64: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadUint64OnlyOmitEmpty",
			data: struct {
				*structUint64OmitEmpty
			}{
				structUint64OmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadUint64OnlyString",
			data: struct {
				*structUint64String
			}{
				structUint64String: nil,
			},
		},

		// AnonymousHeadUint64PtrOnly
		{
			name: "AnonymousHeadUint64PtrOnly",
			data: struct {
				structUint64Ptr
			}{
				structUint64Ptr: structUint64Ptr{A: uint64ptr(1)},
			},
		},
		{
			name: "AnonymousHeadUint64PtrOnlyOmitEmpty",
			data: struct {
				structUint64PtrOmitEmpty
			}{
				structUint64PtrOmitEmpty: structUint64PtrOmitEmpty{A: uint64ptr(1)},
			},
		},
		{
			name: "AnonymousHeadUint64PtrOnlyString",
			data: struct {
				structUint64PtrString
			}{
				structUint64PtrString: structUint64PtrString{A: uint64ptr(1)},
			},
		},

		// AnonymousHeadUint64PtrNilOnly
		{
			name: "AnonymousHeadUint64PtrNilOnly",
			data: struct {
				structUint64Ptr
			}{
				structUint64Ptr: structUint64Ptr{A: nil},
			},
		},
		{
			name: "AnonymousHeadUint64PtrNilOnlyOmitEmpty",
			data: struct {
				structUint64PtrOmitEmpty
			}{
				structUint64PtrOmitEmpty: structUint64PtrOmitEmpty{A: nil},
			},
		},
		{
			name: "AnonymousHeadUint64PtrNilOnlyString",
			data: struct {
				structUint64PtrString
			}{
				structUint64PtrString: structUint64PtrString{A: nil},
			},
		},

		// PtrAnonymousHeadUint64PtrOnly
		{
			name: "PtrAnonymousHeadUint64PtrOnly",
			data: struct {
				*structUint64Ptr
			}{
				structUint64Ptr: &structUint64Ptr{A: uint64ptr(1)},
			},
		},
		{
			name: "PtrAnonymousHeadUint64PtrOnlyOmitEmpty",
			data: struct {
				*structUint64PtrOmitEmpty
			}{
				structUint64PtrOmitEmpty: &structUint64PtrOmitEmpty{A: uint64ptr(1)},
			},
		},
		{
			name: "PtrAnonymousHeadUint64PtrOnlyString",
			data: struct {
				*structUint64PtrString
			}{
				structUint64PtrString: &structUint64PtrString{A: uint64ptr(1)},
			},
		},

		// NilPtrAnonymousHeadUint64PtrOnly
		{
			name: "NilPtrAnonymousHeadUint64PtrOnly",
			data: struct {
				*structUint64Ptr
			}{
				structUint64Ptr: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadUint64PtrOnlyOmitEmpty",
			data: struct {
				*structUint64PtrOmitEmpty
			}{
				structUint64PtrOmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadUint64PtrOnlyString",
			data: struct {
				*structUint64PtrString
			}{
				structUint64PtrString: nil,
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
