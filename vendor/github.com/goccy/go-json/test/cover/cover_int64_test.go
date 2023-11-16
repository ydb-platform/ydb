package json_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/goccy/go-json"
)

func TestCoverInt64(t *testing.T) {
	type structInt64 struct {
		A int64 `json:"a"`
	}
	type structInt64OmitEmpty struct {
		A int64 `json:"a,omitempty"`
	}
	type structInt64String struct {
		A int64 `json:"a,string"`
	}

	type structInt64Ptr struct {
		A *int64 `json:"a"`
	}
	type structInt64PtrOmitEmpty struct {
		A *int64 `json:"a,omitempty"`
	}
	type structInt64PtrString struct {
		A *int64 `json:"a,string"`
	}

	tests := []struct {
		name string
		data interface{}
	}{
		{
			name: "Int64",
			data: int64(10),
		},
		{
			name: "Int64Ptr",
			data: int64ptr(10),
		},
		{
			name: "Int64Ptr3",
			data: int64ptr3(10),
		},
		{
			name: "Int64PtrNil",
			data: (*int64)(nil),
		},
		{
			name: "Int64Ptr3Nil",
			data: (***int64)(nil),
		},

		// HeadInt64Zero
		{
			name: "HeadInt64Zero",
			data: struct {
				A int64 `json:"a"`
			}{},
		},
		{
			name: "HeadInt64ZeroOmitEmpty",
			data: struct {
				A int64 `json:"a,omitempty"`
			}{},
		},
		{
			name: "HeadInt64ZeroString",
			data: struct {
				A int64 `json:"a,string"`
			}{},
		},

		// HeadInt64
		{
			name: "HeadInt64",
			data: struct {
				A int64 `json:"a"`
			}{A: -1},
		},
		{
			name: "HeadInt64OmitEmpty",
			data: struct {
				A int64 `json:"a,omitempty"`
			}{A: -1},
		},
		{
			name: "HeadInt64String",
			data: struct {
				A int64 `json:"a,string"`
			}{A: -1},
		},

		// HeadInt64Ptr
		{
			name: "HeadInt64Ptr",
			data: struct {
				A *int64 `json:"a"`
			}{A: int64ptr(-1)},
		},
		{
			name: "HeadInt64PtrOmitEmpty",
			data: struct {
				A *int64 `json:"a,omitempty"`
			}{A: int64ptr(-1)},
		},
		{
			name: "HeadInt64PtrString",
			data: struct {
				A *int64 `json:"a,string"`
			}{A: int64ptr(-1)},
		},

		// HeadInt64PtrNil
		{
			name: "HeadInt64PtrNil",
			data: struct {
				A *int64 `json:"a"`
			}{A: nil},
		},
		{
			name: "HeadInt64PtrNilOmitEmpty",
			data: struct {
				A *int64 `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "HeadInt64PtrNilString",
			data: struct {
				A *int64 `json:"a,string"`
			}{A: nil},
		},

		// PtrHeadInt64Zero
		{
			name: "PtrHeadInt64Zero",
			data: &struct {
				A int64 `json:"a"`
			}{},
		},
		{
			name: "PtrHeadInt64ZeroOmitEmpty",
			data: &struct {
				A int64 `json:"a,omitempty"`
			}{},
		},
		{
			name: "PtrHeadInt64ZeroString",
			data: &struct {
				A int64 `json:"a,string"`
			}{},
		},

		// PtrHeadInt64
		{
			name: "PtrHeadInt64",
			data: &struct {
				A int64 `json:"a"`
			}{A: -1},
		},
		{
			name: "PtrHeadInt64OmitEmpty",
			data: &struct {
				A int64 `json:"a,omitempty"`
			}{A: -1},
		},
		{
			name: "PtrHeadInt64String",
			data: &struct {
				A int64 `json:"a,string"`
			}{A: -1},
		},

		// PtrHeadInt64Ptr
		{
			name: "PtrHeadInt64Ptr",
			data: &struct {
				A *int64 `json:"a"`
			}{A: int64ptr(-1)},
		},
		{
			name: "PtrHeadInt64PtrOmitEmpty",
			data: &struct {
				A *int64 `json:"a,omitempty"`
			}{A: int64ptr(-1)},
		},
		{
			name: "PtrHeadInt64PtrString",
			data: &struct {
				A *int64 `json:"a,string"`
			}{A: int64ptr(-1)},
		},

		// PtrHeadInt64PtrNil
		{
			name: "PtrHeadInt64PtrNil",
			data: &struct {
				A *int64 `json:"a"`
			}{A: nil},
		},
		{
			name: "PtrHeadInt64PtrNilOmitEmpty",
			data: &struct {
				A *int64 `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadInt64PtrNilString",
			data: &struct {
				A *int64 `json:"a,string"`
			}{A: nil},
		},

		// PtrHeadInt64Nil
		{
			name: "PtrHeadInt64Nil",
			data: (*struct {
				A *int64 `json:"a"`
			})(nil),
		},
		{
			name: "PtrHeadInt64NilOmitEmpty",
			data: (*struct {
				A *int64 `json:"a,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadInt64NilString",
			data: (*struct {
				A *int64 `json:"a,string"`
			})(nil),
		},

		// HeadInt64ZeroMultiFields
		{
			name: "HeadInt64ZeroMultiFields",
			data: struct {
				A int64 `json:"a"`
				B int64 `json:"b"`
				C int64 `json:"c"`
			}{},
		},
		{
			name: "HeadInt64ZeroMultiFieldsOmitEmpty",
			data: struct {
				A int64 `json:"a,omitempty"`
				B int64 `json:"b,omitempty"`
				C int64 `json:"c,omitempty"`
			}{},
		},
		{
			name: "HeadInt64ZeroMultiFields",
			data: struct {
				A int64 `json:"a,string"`
				B int64 `json:"b,string"`
				C int64 `json:"c,string"`
			}{},
		},

		// HeadInt64MultiFields
		{
			name: "HeadInt64MultiFields",
			data: struct {
				A int64 `json:"a"`
				B int64 `json:"b"`
				C int64 `json:"c"`
			}{A: -1, B: 2, C: 3},
		},
		{
			name: "HeadInt64MultiFieldsOmitEmpty",
			data: struct {
				A int64 `json:"a,omitempty"`
				B int64 `json:"b,omitempty"`
				C int64 `json:"c,omitempty"`
			}{A: -1, B: 2, C: 3},
		},
		{
			name: "HeadInt64MultiFieldsString",
			data: struct {
				A int64 `json:"a,string"`
				B int64 `json:"b,string"`
				C int64 `json:"c,string"`
			}{A: -1, B: 2, C: 3},
		},

		// HeadInt64PtrMultiFields
		{
			name: "HeadInt64PtrMultiFields",
			data: struct {
				A *int64 `json:"a"`
				B *int64 `json:"b"`
				C *int64 `json:"c"`
			}{A: int64ptr(-1), B: int64ptr(2), C: int64ptr(3)},
		},
		{
			name: "HeadInt64PtrMultiFieldsOmitEmpty",
			data: struct {
				A *int64 `json:"a,omitempty"`
				B *int64 `json:"b,omitempty"`
				C *int64 `json:"c,omitempty"`
			}{A: int64ptr(-1), B: int64ptr(2), C: int64ptr(3)},
		},
		{
			name: "HeadInt64PtrMultiFieldsString",
			data: struct {
				A *int64 `json:"a,string"`
				B *int64 `json:"b,string"`
				C *int64 `json:"c,string"`
			}{A: int64ptr(-1), B: int64ptr(2), C: int64ptr(3)},
		},

		// HeadInt64PtrNilMultiFields
		{
			name: "HeadInt64PtrNilMultiFields",
			data: struct {
				A *int64 `json:"a"`
				B *int64 `json:"b"`
				C *int64 `json:"c"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadInt64PtrNilMultiFieldsOmitEmpty",
			data: struct {
				A *int64 `json:"a,omitempty"`
				B *int64 `json:"b,omitempty"`
				C *int64 `json:"c,omitempty"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadInt64PtrNilMultiFieldsString",
			data: struct {
				A *int64 `json:"a,string"`
				B *int64 `json:"b,string"`
				C *int64 `json:"c,string"`
			}{A: nil, B: nil, C: nil},
		},

		// PtrHeadInt64ZeroMultiFields
		{
			name: "PtrHeadInt64ZeroMultiFields",
			data: &struct {
				A int64 `json:"a"`
				B int64 `json:"b"`
			}{},
		},
		{
			name: "PtrHeadInt64ZeroMultiFieldsOmitEmpty",
			data: &struct {
				A int64 `json:"a,omitempty"`
				B int64 `json:"b,omitempty"`
			}{},
		},
		{
			name: "PtrHeadInt64ZeroMultiFieldsString",
			data: &struct {
				A int64 `json:"a,string"`
				B int64 `json:"b,string"`
			}{},
		},

		// PtrHeadInt64MultiFields
		{
			name: "PtrHeadInt64MultiFields",
			data: &struct {
				A int64 `json:"a"`
				B int64 `json:"b"`
			}{A: -1, B: 2},
		},
		{
			name: "PtrHeadInt64MultiFieldsOmitEmpty",
			data: &struct {
				A int64 `json:"a,omitempty"`
				B int64 `json:"b,omitempty"`
			}{A: -1, B: 2},
		},
		{
			name: "PtrHeadInt64MultiFieldsString",
			data: &struct {
				A int64 `json:"a,string"`
				B int64 `json:"b,string"`
			}{A: -1, B: 2},
		},

		// PtrHeadInt64PtrMultiFields
		{
			name: "PtrHeadInt64PtrMultiFields",
			data: &struct {
				A *int64 `json:"a"`
				B *int64 `json:"b"`
			}{A: int64ptr(-1), B: int64ptr(2)},
		},
		{
			name: "PtrHeadInt64PtrMultiFieldsOmitEmpty",
			data: &struct {
				A *int64 `json:"a,omitempty"`
				B *int64 `json:"b,omitempty"`
			}{A: int64ptr(-1), B: int64ptr(2)},
		},
		{
			name: "PtrHeadInt64PtrMultiFieldsString",
			data: &struct {
				A *int64 `json:"a,string"`
				B *int64 `json:"b,string"`
			}{A: int64ptr(-1), B: int64ptr(2)},
		},

		// PtrHeadInt64PtrNilMultiFields
		{
			name: "PtrHeadInt64PtrNilMultiFields",
			data: &struct {
				A *int64 `json:"a"`
				B *int64 `json:"b"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadInt64PtrNilMultiFieldsOmitEmpty",
			data: &struct {
				A *int64 `json:"a,omitempty"`
				B *int64 `json:"b,omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadInt64PtrNilMultiFieldsString",
			data: &struct {
				A *int64 `json:"a,string"`
				B *int64 `json:"b,string"`
			}{A: nil, B: nil},
		},

		// PtrHeadInt64NilMultiFields
		{
			name: "PtrHeadInt64NilMultiFields",
			data: (*struct {
				A *int64 `json:"a"`
				B *int64 `json:"b"`
			})(nil),
		},
		{
			name: "PtrHeadInt64NilMultiFieldsOmitEmpty",
			data: (*struct {
				A *int64 `json:"a,omitempty"`
				B *int64 `json:"b,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadInt64NilMultiFieldsString",
			data: (*struct {
				A *int64 `json:"a,string"`
				B *int64 `json:"b,string"`
			})(nil),
		},

		// HeadInt64ZeroNotRoot
		{
			name: "HeadInt64ZeroNotRoot",
			data: struct {
				A struct {
					A int64 `json:"a"`
				}
			}{},
		},
		{
			name: "HeadInt64ZeroNotRootOmitEmpty",
			data: struct {
				A struct {
					A int64 `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadInt64ZeroNotRootString",
			data: struct {
				A struct {
					A int64 `json:"a,string"`
				}
			}{},
		},

		// HeadInt64NotRoot
		{
			name: "HeadInt64NotRoot",
			data: struct {
				A struct {
					A int64 `json:"a"`
				}
			}{A: struct {
				A int64 `json:"a"`
			}{A: -1}},
		},
		{
			name: "HeadInt64NotRootOmitEmpty",
			data: struct {
				A struct {
					A int64 `json:"a,omitempty"`
				}
			}{A: struct {
				A int64 `json:"a,omitempty"`
			}{A: -1}},
		},
		{
			name: "HeadInt64NotRootString",
			data: struct {
				A struct {
					A int64 `json:"a,string"`
				}
			}{A: struct {
				A int64 `json:"a,string"`
			}{A: -1}},
		},

		// HeadInt64PtrNotRoot
		{
			name: "HeadInt64PtrNotRoot",
			data: struct {
				A struct {
					A *int64 `json:"a"`
				}
			}{A: struct {
				A *int64 `json:"a"`
			}{int64ptr(-1)}},
		},
		{
			name: "HeadInt64PtrNotRootOmitEmpty",
			data: struct {
				A struct {
					A *int64 `json:"a,omitempty"`
				}
			}{A: struct {
				A *int64 `json:"a,omitempty"`
			}{int64ptr(-1)}},
		},
		{
			name: "HeadInt64PtrNotRootString",
			data: struct {
				A struct {
					A *int64 `json:"a,string"`
				}
			}{A: struct {
				A *int64 `json:"a,string"`
			}{int64ptr(-1)}},
		},

		// HeadInt64PtrNilNotRoot
		{
			name: "HeadInt64PtrNilNotRoot",
			data: struct {
				A struct {
					A *int64 `json:"a"`
				}
			}{},
		},
		{
			name: "HeadInt64PtrNilNotRootOmitEmpty",
			data: struct {
				A struct {
					A *int64 `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadInt64PtrNilNotRootString",
			data: struct {
				A struct {
					A *int64 `json:"a,string"`
				}
			}{},
		},

		// PtrHeadInt64ZeroNotRoot
		{
			name: "PtrHeadInt64ZeroNotRoot",
			data: struct {
				A *struct {
					A int64 `json:"a"`
				}
			}{A: new(struct {
				A int64 `json:"a"`
			})},
		},
		{
			name: "PtrHeadInt64ZeroNotRootOmitEmpty",
			data: struct {
				A *struct {
					A int64 `json:"a,omitempty"`
				}
			}{A: new(struct {
				A int64 `json:"a,omitempty"`
			})},
		},
		{
			name: "PtrHeadInt64ZeroNotRootString",
			data: struct {
				A *struct {
					A int64 `json:"a,string"`
				}
			}{A: new(struct {
				A int64 `json:"a,string"`
			})},
		},

		// PtrHeadInt64NotRoot
		{
			name: "PtrHeadInt64NotRoot",
			data: struct {
				A *struct {
					A int64 `json:"a"`
				}
			}{A: &(struct {
				A int64 `json:"a"`
			}{A: -1})},
		},
		{
			name: "PtrHeadInt64NotRootOmitEmpty",
			data: struct {
				A *struct {
					A int64 `json:"a,omitempty"`
				}
			}{A: &(struct {
				A int64 `json:"a,omitempty"`
			}{A: -1})},
		},
		{
			name: "PtrHeadInt64NotRootString",
			data: struct {
				A *struct {
					A int64 `json:"a,string"`
				}
			}{A: &(struct {
				A int64 `json:"a,string"`
			}{A: -1})},
		},

		// PtrHeadInt64PtrNotRoot
		{
			name: "PtrHeadInt64PtrNotRoot",
			data: struct {
				A *struct {
					A *int64 `json:"a"`
				}
			}{A: &(struct {
				A *int64 `json:"a"`
			}{A: int64ptr(-1)})},
		},
		{
			name: "PtrHeadInt64PtrNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *int64 `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *int64 `json:"a,omitempty"`
			}{A: int64ptr(-1)})},
		},
		{
			name: "PtrHeadInt64PtrNotRootString",
			data: struct {
				A *struct {
					A *int64 `json:"a,string"`
				}
			}{A: &(struct {
				A *int64 `json:"a,string"`
			}{A: int64ptr(-1)})},
		},

		// PtrHeadInt64PtrNilNotRoot
		{
			name: "PtrHeadInt64PtrNilNotRoot",
			data: struct {
				A *struct {
					A *int64 `json:"a"`
				}
			}{A: &(struct {
				A *int64 `json:"a"`
			}{A: nil})},
		},
		{
			name: "PtrHeadInt64PtrNilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *int64 `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *int64 `json:"a,omitempty"`
			}{A: nil})},
		},
		{
			name: "PtrHeadInt64PtrNilNotRootString",
			data: struct {
				A *struct {
					A *int64 `json:"a,string"`
				}
			}{A: &(struct {
				A *int64 `json:"a,string"`
			}{A: nil})},
		},

		// PtrHeadInt64NilNotRoot
		{
			name: "PtrHeadInt64NilNotRoot",
			data: struct {
				A *struct {
					A *int64 `json:"a"`
				}
			}{A: nil},
		},
		{
			name: "PtrHeadInt64NilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *int64 `json:"a,omitempty"`
				} `json:",omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadInt64NilNotRootString",
			data: struct {
				A *struct {
					A *int64 `json:"a,string"`
				} `json:",string"`
			}{A: nil},
		},

		// HeadInt64ZeroMultiFieldsNotRoot
		{
			name: "HeadInt64ZeroMultiFieldsNotRoot",
			data: struct {
				A struct {
					A int64 `json:"a"`
				}
				B struct {
					B int64 `json:"b"`
				}
			}{},
		},
		{
			name: "HeadInt64ZeroMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A int64 `json:"a,omitempty"`
				}
				B struct {
					B int64 `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "HeadInt64ZeroMultiFieldsNotRootString",
			data: struct {
				A struct {
					A int64 `json:"a,string"`
				}
				B struct {
					B int64 `json:"b,string"`
				}
			}{},
		},

		// HeadInt64MultiFieldsNotRoot
		{
			name: "HeadInt64MultiFieldsNotRoot",
			data: struct {
				A struct {
					A int64 `json:"a"`
				}
				B struct {
					B int64 `json:"b"`
				}
			}{A: struct {
				A int64 `json:"a"`
			}{A: -1}, B: struct {
				B int64 `json:"b"`
			}{B: 2}},
		},
		{
			name: "HeadInt64MultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A int64 `json:"a,omitempty"`
				}
				B struct {
					B int64 `json:"b,omitempty"`
				}
			}{A: struct {
				A int64 `json:"a,omitempty"`
			}{A: -1}, B: struct {
				B int64 `json:"b,omitempty"`
			}{B: 2}},
		},
		{
			name: "HeadInt64MultiFieldsNotRootString",
			data: struct {
				A struct {
					A int64 `json:"a,string"`
				}
				B struct {
					B int64 `json:"b,string"`
				}
			}{A: struct {
				A int64 `json:"a,string"`
			}{A: -1}, B: struct {
				B int64 `json:"b,string"`
			}{B: 2}},
		},

		// HeadInt64PtrMultiFieldsNotRoot
		{
			name: "HeadInt64PtrMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *int64 `json:"a"`
				}
				B struct {
					B *int64 `json:"b"`
				}
			}{A: struct {
				A *int64 `json:"a"`
			}{A: int64ptr(-1)}, B: struct {
				B *int64 `json:"b"`
			}{B: int64ptr(2)}},
		},
		{
			name: "HeadInt64PtrMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *int64 `json:"a,omitempty"`
				}
				B struct {
					B *int64 `json:"b,omitempty"`
				}
			}{A: struct {
				A *int64 `json:"a,omitempty"`
			}{A: int64ptr(-1)}, B: struct {
				B *int64 `json:"b,omitempty"`
			}{B: int64ptr(2)}},
		},
		{
			name: "HeadInt64PtrMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *int64 `json:"a,string"`
				}
				B struct {
					B *int64 `json:"b,string"`
				}
			}{A: struct {
				A *int64 `json:"a,string"`
			}{A: int64ptr(-1)}, B: struct {
				B *int64 `json:"b,string"`
			}{B: int64ptr(2)}},
		},

		// HeadInt64PtrNilMultiFieldsNotRoot
		{
			name: "HeadInt64PtrNilMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *int64 `json:"a"`
				}
				B struct {
					B *int64 `json:"b"`
				}
			}{A: struct {
				A *int64 `json:"a"`
			}{A: nil}, B: struct {
				B *int64 `json:"b"`
			}{B: nil}},
		},
		{
			name: "HeadInt64PtrNilMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *int64 `json:"a,omitempty"`
				}
				B struct {
					B *int64 `json:"b,omitempty"`
				}
			}{A: struct {
				A *int64 `json:"a,omitempty"`
			}{A: nil}, B: struct {
				B *int64 `json:"b,omitempty"`
			}{B: nil}},
		},
		{
			name: "HeadInt64PtrNilMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *int64 `json:"a,string"`
				}
				B struct {
					B *int64 `json:"b,string"`
				}
			}{A: struct {
				A *int64 `json:"a,string"`
			}{A: nil}, B: struct {
				B *int64 `json:"b,string"`
			}{B: nil}},
		},

		// PtrHeadInt64ZeroMultiFieldsNotRoot
		{
			name: "PtrHeadInt64ZeroMultiFieldsNotRoot",
			data: &struct {
				A struct {
					A int64 `json:"a"`
				}
				B struct {
					B int64 `json:"b"`
				}
			}{},
		},
		{
			name: "PtrHeadInt64ZeroMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A int64 `json:"a,omitempty"`
				}
				B struct {
					B int64 `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "PtrHeadInt64ZeroMultiFieldsNotRootString",
			data: &struct {
				A struct {
					A int64 `json:"a,string"`
				}
				B struct {
					B int64 `json:"b,string"`
				}
			}{},
		},

		// PtrHeadInt64MultiFieldsNotRoot
		{
			name: "PtrHeadInt64MultiFieldsNotRoot",
			data: &struct {
				A struct {
					A int64 `json:"a"`
				}
				B struct {
					B int64 `json:"b"`
				}
			}{A: struct {
				A int64 `json:"a"`
			}{A: -1}, B: struct {
				B int64 `json:"b"`
			}{B: 2}},
		},
		{
			name: "PtrHeadInt64MultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A int64 `json:"a,omitempty"`
				}
				B struct {
					B int64 `json:"b,omitempty"`
				}
			}{A: struct {
				A int64 `json:"a,omitempty"`
			}{A: -1}, B: struct {
				B int64 `json:"b,omitempty"`
			}{B: 2}},
		},
		{
			name: "PtrHeadInt64MultiFieldsNotRootString",
			data: &struct {
				A struct {
					A int64 `json:"a,string"`
				}
				B struct {
					B int64 `json:"b,string"`
				}
			}{A: struct {
				A int64 `json:"a,string"`
			}{A: -1}, B: struct {
				B int64 `json:"b,string"`
			}{B: 2}},
		},

		// PtrHeadInt64PtrMultiFieldsNotRoot
		{
			name: "PtrHeadInt64PtrMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *int64 `json:"a"`
				}
				B *struct {
					B *int64 `json:"b"`
				}
			}{A: &(struct {
				A *int64 `json:"a"`
			}{A: int64ptr(-1)}), B: &(struct {
				B *int64 `json:"b"`
			}{B: int64ptr(2)})},
		},
		{
			name: "PtrHeadInt64PtrMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *int64 `json:"a,omitempty"`
				}
				B *struct {
					B *int64 `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *int64 `json:"a,omitempty"`
			}{A: int64ptr(-1)}), B: &(struct {
				B *int64 `json:"b,omitempty"`
			}{B: int64ptr(2)})},
		},
		{
			name: "PtrHeadInt64PtrMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *int64 `json:"a,string"`
				}
				B *struct {
					B *int64 `json:"b,string"`
				}
			}{A: &(struct {
				A *int64 `json:"a,string"`
			}{A: int64ptr(-1)}), B: &(struct {
				B *int64 `json:"b,string"`
			}{B: int64ptr(2)})},
		},

		// PtrHeadInt64PtrNilMultiFieldsNotRoot
		{
			name: "PtrHeadInt64PtrNilMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *int64 `json:"a"`
				}
				B *struct {
					B *int64 `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadInt64PtrNilMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *int64 `json:"a,omitempty"`
				} `json:",omitempty"`
				B *struct {
					B *int64 `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadInt64PtrNilMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *int64 `json:"a,string"`
				} `json:",string"`
				B *struct {
					B *int64 `json:"b,string"`
				} `json:",string"`
			}{A: nil, B: nil},
		},

		// PtrHeadInt64NilMultiFieldsNotRoot
		{
			name: "PtrHeadInt64NilMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *int64 `json:"a"`
				}
				B *struct {
					B *int64 `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadInt64NilMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *int64 `json:"a,omitempty"`
				}
				B *struct {
					B *int64 `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadInt64NilMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *int64 `json:"a,string"`
				}
				B *struct {
					B *int64 `json:"b,string"`
				}
			})(nil),
		},

		// PtrHeadInt64DoubleMultiFieldsNotRoot
		{
			name: "PtrHeadInt64DoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A int64 `json:"a"`
					B int64 `json:"b"`
				}
				B *struct {
					A int64 `json:"a"`
					B int64 `json:"b"`
				}
			}{A: &(struct {
				A int64 `json:"a"`
				B int64 `json:"b"`
			}{A: -1, B: 2}), B: &(struct {
				A int64 `json:"a"`
				B int64 `json:"b"`
			}{A: 3, B: 4})},
		},
		{
			name: "PtrHeadInt64DoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A int64 `json:"a,omitempty"`
					B int64 `json:"b,omitempty"`
				}
				B *struct {
					A int64 `json:"a,omitempty"`
					B int64 `json:"b,omitempty"`
				}
			}{A: &(struct {
				A int64 `json:"a,omitempty"`
				B int64 `json:"b,omitempty"`
			}{A: -1, B: 2}), B: &(struct {
				A int64 `json:"a,omitempty"`
				B int64 `json:"b,omitempty"`
			}{A: 3, B: 4})},
		},
		{
			name: "PtrHeadInt64DoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A int64 `json:"a,string"`
					B int64 `json:"b,string"`
				}
				B *struct {
					A int64 `json:"a,string"`
					B int64 `json:"b,string"`
				}
			}{A: &(struct {
				A int64 `json:"a,string"`
				B int64 `json:"b,string"`
			}{A: -1, B: 2}), B: &(struct {
				A int64 `json:"a,string"`
				B int64 `json:"b,string"`
			}{A: 3, B: 4})},
		},

		// PtrHeadInt64NilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadInt64NilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A int64 `json:"a"`
					B int64 `json:"b"`
				}
				B *struct {
					A int64 `json:"a"`
					B int64 `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadInt64NilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A int64 `json:"a,omitempty"`
					B int64 `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A int64 `json:"a,omitempty"`
					B int64 `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadInt64NilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A int64 `json:"a,string"`
					B int64 `json:"b,string"`
				}
				B *struct {
					A int64 `json:"a,string"`
					B int64 `json:"b,string"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadInt64NilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadInt64NilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A int64 `json:"a"`
					B int64 `json:"b"`
				}
				B *struct {
					A int64 `json:"a"`
					B int64 `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadInt64NilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A int64 `json:"a,omitempty"`
					B int64 `json:"b,omitempty"`
				}
				B *struct {
					A int64 `json:"a,omitempty"`
					B int64 `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadInt64NilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A int64 `json:"a,string"`
					B int64 `json:"b,string"`
				}
				B *struct {
					A int64 `json:"a,string"`
					B int64 `json:"b,string"`
				}
			})(nil),
		},

		// PtrHeadInt64PtrDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadInt64PtrDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *int64 `json:"a"`
					B *int64 `json:"b"`
				}
				B *struct {
					A *int64 `json:"a"`
					B *int64 `json:"b"`
				}
			}{A: &(struct {
				A *int64 `json:"a"`
				B *int64 `json:"b"`
			}{A: int64ptr(-1), B: int64ptr(2)}), B: &(struct {
				A *int64 `json:"a"`
				B *int64 `json:"b"`
			}{A: int64ptr(3), B: int64ptr(4)})},
		},
		{
			name: "PtrHeadInt64PtrDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *int64 `json:"a,omitempty"`
					B *int64 `json:"b,omitempty"`
				}
				B *struct {
					A *int64 `json:"a,omitempty"`
					B *int64 `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *int64 `json:"a,omitempty"`
				B *int64 `json:"b,omitempty"`
			}{A: int64ptr(-1), B: int64ptr(2)}), B: &(struct {
				A *int64 `json:"a,omitempty"`
				B *int64 `json:"b,omitempty"`
			}{A: int64ptr(3), B: int64ptr(4)})},
		},
		{
			name: "PtrHeadInt64PtrDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *int64 `json:"a,string"`
					B *int64 `json:"b,string"`
				}
				B *struct {
					A *int64 `json:"a,string"`
					B *int64 `json:"b,string"`
				}
			}{A: &(struct {
				A *int64 `json:"a,string"`
				B *int64 `json:"b,string"`
			}{A: int64ptr(-1), B: int64ptr(2)}), B: &(struct {
				A *int64 `json:"a,string"`
				B *int64 `json:"b,string"`
			}{A: int64ptr(3), B: int64ptr(4)})},
		},

		// PtrHeadInt64PtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadInt64PtrNilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *int64 `json:"a"`
					B *int64 `json:"b"`
				}
				B *struct {
					A *int64 `json:"a"`
					B *int64 `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadInt64PtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *int64 `json:"a,omitempty"`
					B *int64 `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A *int64 `json:"a,omitempty"`
					B *int64 `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadInt64PtrNilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *int64 `json:"a,string"`
					B *int64 `json:"b,string"`
				}
				B *struct {
					A *int64 `json:"a,string"`
					B *int64 `json:"b,string"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadInt64PtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadInt64PtrNilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *int64 `json:"a"`
					B *int64 `json:"b"`
				}
				B *struct {
					A *int64 `json:"a"`
					B *int64 `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadInt64PtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *int64 `json:"a,omitempty"`
					B *int64 `json:"b,omitempty"`
				}
				B *struct {
					A *int64 `json:"a,omitempty"`
					B *int64 `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadInt64PtrNilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *int64 `json:"a,string"`
					B *int64 `json:"b,string"`
				}
				B *struct {
					A *int64 `json:"a,string"`
					B *int64 `json:"b,string"`
				}
			})(nil),
		},

		// AnonymousHeadInt64
		{
			name: "AnonymousHeadInt64",
			data: struct {
				structInt64
				B int64 `json:"b"`
			}{
				structInt64: structInt64{A: -1},
				B:           2,
			},
		},
		{
			name: "AnonymousHeadInt64OmitEmpty",
			data: struct {
				structInt64OmitEmpty
				B int64 `json:"b,omitempty"`
			}{
				structInt64OmitEmpty: structInt64OmitEmpty{A: -1},
				B:                    2,
			},
		},
		{
			name: "AnonymousHeadInt64String",
			data: struct {
				structInt64String
				B int64 `json:"b,string"`
			}{
				structInt64String: structInt64String{A: -1},
				B:                 2,
			},
		},

		// PtrAnonymousHeadInt64
		{
			name: "PtrAnonymousHeadInt64",
			data: struct {
				*structInt64
				B int64 `json:"b"`
			}{
				structInt64: &structInt64{A: -1},
				B:           2,
			},
		},
		{
			name: "PtrAnonymousHeadInt64OmitEmpty",
			data: struct {
				*structInt64OmitEmpty
				B int64 `json:"b,omitempty"`
			}{
				structInt64OmitEmpty: &structInt64OmitEmpty{A: -1},
				B:                    2,
			},
		},
		{
			name: "PtrAnonymousHeadInt64String",
			data: struct {
				*structInt64String
				B int64 `json:"b,string"`
			}{
				structInt64String: &structInt64String{A: -1},
				B:                 2,
			},
		},

		// NilPtrAnonymousHeadInt64
		{
			name: "NilPtrAnonymousHeadInt64",
			data: struct {
				*structInt64
				B int64 `json:"b"`
			}{
				structInt64: nil,
				B:           2,
			},
		},
		{
			name: "NilPtrAnonymousHeadInt64OmitEmpty",
			data: struct {
				*structInt64OmitEmpty
				B int64 `json:"b,omitempty"`
			}{
				structInt64OmitEmpty: nil,
				B:                    2,
			},
		},
		{
			name: "NilPtrAnonymousHeadInt64String",
			data: struct {
				*structInt64String
				B int64 `json:"b,string"`
			}{
				structInt64String: nil,
				B:                 2,
			},
		},

		// AnonymousHeadInt64Ptr
		{
			name: "AnonymousHeadInt64Ptr",
			data: struct {
				structInt64Ptr
				B *int64 `json:"b"`
			}{
				structInt64Ptr: structInt64Ptr{A: int64ptr(-1)},
				B:              int64ptr(2),
			},
		},
		{
			name: "AnonymousHeadInt64PtrOmitEmpty",
			data: struct {
				structInt64PtrOmitEmpty
				B *int64 `json:"b,omitempty"`
			}{
				structInt64PtrOmitEmpty: structInt64PtrOmitEmpty{A: int64ptr(-1)},
				B:                       int64ptr(2),
			},
		},
		{
			name: "AnonymousHeadInt64PtrString",
			data: struct {
				structInt64PtrString
				B *int64 `json:"b,string"`
			}{
				structInt64PtrString: structInt64PtrString{A: int64ptr(-1)},
				B:                    int64ptr(2),
			},
		},

		// AnonymousHeadInt64PtrNil
		{
			name: "AnonymousHeadInt64PtrNil",
			data: struct {
				structInt64Ptr
				B *int64 `json:"b"`
			}{
				structInt64Ptr: structInt64Ptr{A: nil},
				B:              int64ptr(2),
			},
		},
		{
			name: "AnonymousHeadInt64PtrNilOmitEmpty",
			data: struct {
				structInt64PtrOmitEmpty
				B *int64 `json:"b,omitempty"`
			}{
				structInt64PtrOmitEmpty: structInt64PtrOmitEmpty{A: nil},
				B:                       int64ptr(2),
			},
		},
		{
			name: "AnonymousHeadInt64PtrNilString",
			data: struct {
				structInt64PtrString
				B *int64 `json:"b,string"`
			}{
				structInt64PtrString: structInt64PtrString{A: nil},
				B:                    int64ptr(2),
			},
		},

		// PtrAnonymousHeadInt64Ptr
		{
			name: "PtrAnonymousHeadInt64Ptr",
			data: struct {
				*structInt64Ptr
				B *int64 `json:"b"`
			}{
				structInt64Ptr: &structInt64Ptr{A: int64ptr(-1)},
				B:              int64ptr(2),
			},
		},
		{
			name: "PtrAnonymousHeadInt64PtrOmitEmpty",
			data: struct {
				*structInt64PtrOmitEmpty
				B *int64 `json:"b,omitempty"`
			}{
				structInt64PtrOmitEmpty: &structInt64PtrOmitEmpty{A: int64ptr(-1)},
				B:                       int64ptr(2),
			},
		},
		{
			name: "PtrAnonymousHeadInt64PtrString",
			data: struct {
				*structInt64PtrString
				B *int64 `json:"b,string"`
			}{
				structInt64PtrString: &structInt64PtrString{A: int64ptr(-1)},
				B:                    int64ptr(2),
			},
		},

		// NilPtrAnonymousHeadInt64Ptr
		{
			name: "NilPtrAnonymousHeadInt64Ptr",
			data: struct {
				*structInt64Ptr
				B *int64 `json:"b"`
			}{
				structInt64Ptr: nil,
				B:              int64ptr(2),
			},
		},
		{
			name: "NilPtrAnonymousHeadInt64PtrOmitEmpty",
			data: struct {
				*structInt64PtrOmitEmpty
				B *int64 `json:"b,omitempty"`
			}{
				structInt64PtrOmitEmpty: nil,
				B:                       int64ptr(2),
			},
		},
		{
			name: "NilPtrAnonymousHeadInt64PtrString",
			data: struct {
				*structInt64PtrString
				B *int64 `json:"b,string"`
			}{
				structInt64PtrString: nil,
				B:                    int64ptr(2),
			},
		},

		// AnonymousHeadInt64Only
		{
			name: "AnonymousHeadInt64Only",
			data: struct {
				structInt64
			}{
				structInt64: structInt64{A: -1},
			},
		},
		{
			name: "AnonymousHeadInt64OnlyOmitEmpty",
			data: struct {
				structInt64OmitEmpty
			}{
				structInt64OmitEmpty: structInt64OmitEmpty{A: -1},
			},
		},
		{
			name: "AnonymousHeadInt64OnlyString",
			data: struct {
				structInt64String
			}{
				structInt64String: structInt64String{A: -1},
			},
		},

		// PtrAnonymousHeadInt64Only
		{
			name: "PtrAnonymousHeadInt64Only",
			data: struct {
				*structInt64
			}{
				structInt64: &structInt64{A: -1},
			},
		},
		{
			name: "PtrAnonymousHeadInt64OnlyOmitEmpty",
			data: struct {
				*structInt64OmitEmpty
			}{
				structInt64OmitEmpty: &structInt64OmitEmpty{A: -1},
			},
		},
		{
			name: "PtrAnonymousHeadInt64OnlyString",
			data: struct {
				*structInt64String
			}{
				structInt64String: &structInt64String{A: -1},
			},
		},

		// NilPtrAnonymousHeadInt64Only
		{
			name: "NilPtrAnonymousHeadInt64Only",
			data: struct {
				*structInt64
			}{
				structInt64: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadInt64OnlyOmitEmpty",
			data: struct {
				*structInt64OmitEmpty
			}{
				structInt64OmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadInt64OnlyString",
			data: struct {
				*structInt64String
			}{
				structInt64String: nil,
			},
		},

		// AnonymousHeadInt64PtrOnly
		{
			name: "AnonymousHeadInt64PtrOnly",
			data: struct {
				structInt64Ptr
			}{
				structInt64Ptr: structInt64Ptr{A: int64ptr(-1)},
			},
		},
		{
			name: "AnonymousHeadInt64PtrOnlyOmitEmpty",
			data: struct {
				structInt64PtrOmitEmpty
			}{
				structInt64PtrOmitEmpty: structInt64PtrOmitEmpty{A: int64ptr(-1)},
			},
		},
		{
			name: "AnonymousHeadInt64PtrOnlyString",
			data: struct {
				structInt64PtrString
			}{
				structInt64PtrString: structInt64PtrString{A: int64ptr(-1)},
			},
		},

		// AnonymousHeadInt64PtrNilOnly
		{
			name: "AnonymousHeadInt64PtrNilOnly",
			data: struct {
				structInt64Ptr
			}{
				structInt64Ptr: structInt64Ptr{A: nil},
			},
		},
		{
			name: "AnonymousHeadInt64PtrNilOnlyOmitEmpty",
			data: struct {
				structInt64PtrOmitEmpty
			}{
				structInt64PtrOmitEmpty: structInt64PtrOmitEmpty{A: nil},
			},
		},
		{
			name: "AnonymousHeadInt64PtrNilOnlyString",
			data: struct {
				structInt64PtrString
			}{
				structInt64PtrString: structInt64PtrString{A: nil},
			},
		},

		// PtrAnonymousHeadInt64PtrOnly
		{
			name: "PtrAnonymousHeadInt64PtrOnly",
			data: struct {
				*structInt64Ptr
			}{
				structInt64Ptr: &structInt64Ptr{A: int64ptr(-1)},
			},
		},
		{
			name: "PtrAnonymousHeadInt64PtrOnlyOmitEmpty",
			data: struct {
				*structInt64PtrOmitEmpty
			}{
				structInt64PtrOmitEmpty: &structInt64PtrOmitEmpty{A: int64ptr(-1)},
			},
		},
		{
			name: "PtrAnonymousHeadInt64PtrOnlyString",
			data: struct {
				*structInt64PtrString
			}{
				structInt64PtrString: &structInt64PtrString{A: int64ptr(-1)},
			},
		},

		// NilPtrAnonymousHeadInt64PtrOnly
		{
			name: "NilPtrAnonymousHeadInt64PtrOnly",
			data: struct {
				*structInt64Ptr
			}{
				structInt64Ptr: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadInt64PtrOnlyOmitEmpty",
			data: struct {
				*structInt64PtrOmitEmpty
			}{
				structInt64PtrOmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadInt64PtrOnlyString",
			data: struct {
				*structInt64PtrString
			}{
				structInt64PtrString: nil,
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
