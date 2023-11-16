package json_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/goccy/go-json"
)

func TestCoverInt16(t *testing.T) {
	type structInt16 struct {
		A int16 `json:"a"`
	}
	type structInt16OmitEmpty struct {
		A int16 `json:"a,omitempty"`
	}
	type structInt16String struct {
		A int16 `json:"a,string"`
	}

	type structInt16Ptr struct {
		A *int16 `json:"a"`
	}
	type structInt16PtrOmitEmpty struct {
		A *int16 `json:"a,omitempty"`
	}
	type structInt16PtrString struct {
		A *int16 `json:"a,string"`
	}

	tests := []struct {
		name string
		data interface{}
	}{
		{
			name: "Int16",
			data: int16(10),
		},
		{
			name: "Int16Ptr",
			data: int16ptr(10),
		},
		{
			name: "Int16Ptr3",
			data: int16ptr3(10),
		},
		{
			name: "Int16PtrNil",
			data: (*int16)(nil),
		},
		{
			name: "Int16Ptr3Nil",
			data: (***int16)(nil),
		},

		// HeadInt16Zero
		{
			name: "HeadInt16Zero",
			data: struct {
				A int16 `json:"a"`
			}{},
		},
		{
			name: "HeadInt16ZeroOmitEmpty",
			data: struct {
				A int16 `json:"a,omitempty"`
			}{},
		},
		{
			name: "HeadInt16ZeroString",
			data: struct {
				A int16 `json:"a,string"`
			}{},
		},

		// HeadInt16
		{
			name: "HeadInt16",
			data: struct {
				A int16 `json:"a"`
			}{A: 1},
		},
		{
			name: "HeadInt16OmitEmpty",
			data: struct {
				A int16 `json:"a,omitempty"`
			}{A: 1},
		},
		{
			name: "HeadInt16String",
			data: struct {
				A int16 `json:"a,string"`
			}{A: 1},
		},

		// HeadInt16Ptr
		{
			name: "HeadInt16Ptr",
			data: struct {
				A *int16 `json:"a"`
			}{A: int16ptr(1)},
		},
		{
			name: "HeadInt16PtrOmitEmpty",
			data: struct {
				A *int16 `json:"a,omitempty"`
			}{A: int16ptr(1)},
		},
		{
			name: "HeadInt16PtrString",
			data: struct {
				A *int16 `json:"a,string"`
			}{A: int16ptr(1)},
		},

		// HeadInt16PtrNil
		{
			name: "HeadInt16PtrNil",
			data: struct {
				A *int16 `json:"a"`
			}{A: nil},
		},
		{
			name: "HeadInt16PtrNilOmitEmpty",
			data: struct {
				A *int16 `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "HeadInt16PtrNilString",
			data: struct {
				A *int16 `json:"a,string"`
			}{A: nil},
		},

		// PtrHeadInt16Zero
		{
			name: "PtrHeadInt16Zero",
			data: &struct {
				A int16 `json:"a"`
			}{},
		},
		{
			name: "PtrHeadInt16ZeroOmitEmpty",
			data: &struct {
				A int16 `json:"a,omitempty"`
			}{},
		},
		{
			name: "PtrHeadInt16ZeroString",
			data: &struct {
				A int16 `json:"a,string"`
			}{},
		},

		// PtrHeadInt16
		{
			name: "PtrHeadInt16",
			data: &struct {
				A int16 `json:"a"`
			}{A: 1},
		},
		{
			name: "PtrHeadInt16OmitEmpty",
			data: &struct {
				A int16 `json:"a,omitempty"`
			}{A: 1},
		},
		{
			name: "PtrHeadInt16String",
			data: &struct {
				A int16 `json:"a,string"`
			}{A: 1},
		},

		// PtrHeadInt16Ptr
		{
			name: "PtrHeadInt16Ptr",
			data: &struct {
				A *int16 `json:"a"`
			}{A: int16ptr(1)},
		},
		{
			name: "PtrHeadInt16PtrOmitEmpty",
			data: &struct {
				A *int16 `json:"a,omitempty"`
			}{A: int16ptr(1)},
		},
		{
			name: "PtrHeadInt16PtrString",
			data: &struct {
				A *int16 `json:"a,string"`
			}{A: int16ptr(1)},
		},

		// PtrHeadInt16PtrNil
		{
			name: "PtrHeadInt16PtrNil",
			data: &struct {
				A *int16 `json:"a"`
			}{A: nil},
		},
		{
			name: "PtrHeadInt16PtrNilOmitEmpty",
			data: &struct {
				A *int16 `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadInt16PtrNilString",
			data: &struct {
				A *int16 `json:"a,string"`
			}{A: nil},
		},

		// PtrHeadInt16Nil
		{
			name: "PtrHeadInt16Nil",
			data: (*struct {
				A *int16 `json:"a"`
			})(nil),
		},
		{
			name: "PtrHeadInt16NilOmitEmpty",
			data: (*struct {
				A *int16 `json:"a,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadInt16NilString",
			data: (*struct {
				A *int16 `json:"a,string"`
			})(nil),
		},

		// HeadInt16ZeroMultiFields
		{
			name: "HeadInt16ZeroMultiFields",
			data: struct {
				A int16 `json:"a"`
				B int16 `json:"b"`
				C int16 `json:"c"`
			}{},
		},
		{
			name: "HeadInt16ZeroMultiFieldsOmitEmpty",
			data: struct {
				A int16 `json:"a,omitempty"`
				B int16 `json:"b,omitempty"`
				C int16 `json:"c,omitempty"`
			}{},
		},
		{
			name: "HeadInt16ZeroMultiFields",
			data: struct {
				A int16 `json:"a,string"`
				B int16 `json:"b,string"`
				C int16 `json:"c,string"`
			}{},
		},

		// HeadInt16MultiFields
		{
			name: "HeadInt16MultiFields",
			data: struct {
				A int16 `json:"a"`
				B int16 `json:"b"`
				C int16 `json:"c"`
			}{A: 1, B: -2, C: 3},
		},
		{
			name: "HeadInt16MultiFieldsOmitEmpty",
			data: struct {
				A int16 `json:"a,omitempty"`
				B int16 `json:"b,omitempty"`
				C int16 `json:"c,omitempty"`
			}{A: 1, B: -2, C: 3},
		},
		{
			name: "HeadInt16MultiFieldsString",
			data: struct {
				A int16 `json:"a,string"`
				B int16 `json:"b,string"`
				C int16 `json:"c,string"`
			}{A: 1, B: -2, C: 3},
		},

		// HeadInt16PtrMultiFields
		{
			name: "HeadInt16PtrMultiFields",
			data: struct {
				A *int16 `json:"a"`
				B *int16 `json:"b"`
				C *int16 `json:"c"`
			}{A: int16ptr(1), B: int16ptr(-2), C: int16ptr(3)},
		},
		{
			name: "HeadInt16PtrMultiFieldsOmitEmpty",
			data: struct {
				A *int16 `json:"a,omitempty"`
				B *int16 `json:"b,omitempty"`
				C *int16 `json:"c,omitempty"`
			}{A: int16ptr(1), B: int16ptr(-2), C: int16ptr(3)},
		},
		{
			name: "HeadInt16PtrMultiFieldsString",
			data: struct {
				A *int16 `json:"a,string"`
				B *int16 `json:"b,string"`
				C *int16 `json:"c,string"`
			}{A: int16ptr(1), B: int16ptr(-2), C: int16ptr(3)},
		},

		// HeadInt16PtrNilMultiFields
		{
			name: "HeadInt16PtrNilMultiFields",
			data: struct {
				A *int16 `json:"a"`
				B *int16 `json:"b"`
				C *int16 `json:"c"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadInt16PtrNilMultiFieldsOmitEmpty",
			data: struct {
				A *int16 `json:"a,omitempty"`
				B *int16 `json:"b,omitempty"`
				C *int16 `json:"c,omitempty"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadInt16PtrNilMultiFieldsString",
			data: struct {
				A *int16 `json:"a,string"`
				B *int16 `json:"b,string"`
				C *int16 `json:"c,string"`
			}{A: nil, B: nil, C: nil},
		},

		// PtrHeadInt16ZeroMultiFields
		{
			name: "PtrHeadInt16ZeroMultiFields",
			data: &struct {
				A int16 `json:"a"`
				B int16 `json:"b"`
			}{},
		},
		{
			name: "PtrHeadInt16ZeroMultiFieldsOmitEmpty",
			data: &struct {
				A int16 `json:"a,omitempty"`
				B int16 `json:"b,omitempty"`
			}{},
		},
		{
			name: "PtrHeadInt16ZeroMultiFieldsString",
			data: &struct {
				A int16 `json:"a,string"`
				B int16 `json:"b,string"`
			}{},
		},

		// PtrHeadInt16MultiFields
		{
			name: "PtrHeadInt16MultiFields",
			data: &struct {
				A int16 `json:"a"`
				B int16 `json:"b"`
			}{A: 1, B: -2},
		},
		{
			name: "PtrHeadInt16MultiFieldsOmitEmpty",
			data: &struct {
				A int16 `json:"a,omitempty"`
				B int16 `json:"b,omitempty"`
			}{A: 1, B: -2},
		},
		{
			name: "PtrHeadInt16MultiFieldsString",
			data: &struct {
				A int16 `json:"a,string"`
				B int16 `json:"b,string"`
			}{A: 1, B: -2},
		},

		// PtrHeadInt16PtrMultiFields
		{
			name: "PtrHeadInt16PtrMultiFields",
			data: &struct {
				A *int16 `json:"a"`
				B *int16 `json:"b"`
			}{A: int16ptr(1), B: int16ptr(-2)},
		},
		{
			name: "PtrHeadInt16PtrMultiFieldsOmitEmpty",
			data: &struct {
				A *int16 `json:"a,omitempty"`
				B *int16 `json:"b,omitempty"`
			}{A: int16ptr(1), B: int16ptr(-2)},
		},
		{
			name: "PtrHeadInt16PtrMultiFieldsString",
			data: &struct {
				A *int16 `json:"a,string"`
				B *int16 `json:"b,string"`
			}{A: int16ptr(1), B: int16ptr(-2)},
		},

		// PtrHeadInt16PtrNilMultiFields
		{
			name: "PtrHeadInt16PtrNilMultiFields",
			data: &struct {
				A *int16 `json:"a"`
				B *int16 `json:"b"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadInt16PtrNilMultiFieldsOmitEmpty",
			data: &struct {
				A *int16 `json:"a,omitempty"`
				B *int16 `json:"b,omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadInt16PtrNilMultiFieldsString",
			data: &struct {
				A *int16 `json:"a,string"`
				B *int16 `json:"b,string"`
			}{A: nil, B: nil},
		},

		// PtrHeadInt16NilMultiFields
		{
			name: "PtrHeadInt16NilMultiFields",
			data: (*struct {
				A int16 `json:"a"`
				B int16 `json:"b"`
			})(nil),
		},
		{
			name: "PtrHeadInt16NilMultiFieldsOmitEmpty",
			data: (*struct {
				A int16 `json:"a,omitempty"`
				B int16 `json:"b,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadInt16NilMultiFieldsString",
			data: (*struct {
				A int16 `json:"a,string"`
				B int16 `json:"b,string"`
			})(nil),
		},

		// PtrHeadInt16NilMultiFields
		{
			name: "PtrHeadInt16NilMultiFields",
			data: (*struct {
				A *int16 `json:"a"`
				B *int16 `json:"b"`
			})(nil),
		},
		{
			name: "PtrHeadInt16NilMultiFieldsOmitEmpty",
			data: (*struct {
				A *int16 `json:"a,omitempty"`
				B *int16 `json:"b,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadInt16NilMultiFieldsString",
			data: (*struct {
				A *int16 `json:"a,string"`
				B *int16 `json:"b,string"`
			})(nil),
		},

		// HeadInt16ZeroNotRoot
		{
			name: "HeadInt16ZeroNotRoot",
			data: struct {
				A struct {
					A int16 `json:"a"`
				}
			}{},
		},
		{
			name: "HeadInt16ZeroNotRootOmitEmpty",
			data: struct {
				A struct {
					A int16 `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadInt16ZeroNotRootString",
			data: struct {
				A struct {
					A int16 `json:"a,string"`
				}
			}{},
		},

		// HeadInt16NotRoot
		{
			name: "HeadInt16NotRoot",
			data: struct {
				A struct {
					A int16 `json:"a"`
				}
			}{A: struct {
				A int16 `json:"a"`
			}{A: 1}},
		},
		{
			name: "HeadInt16NotRootOmitEmpty",
			data: struct {
				A struct {
					A int16 `json:"a,omitempty"`
				}
			}{A: struct {
				A int16 `json:"a,omitempty"`
			}{A: 1}},
		},
		{
			name: "HeadInt16NotRootString",
			data: struct {
				A struct {
					A int16 `json:"a,string"`
				}
			}{A: struct {
				A int16 `json:"a,string"`
			}{A: 1}},
		},

		// HeadInt16PtrNotRoot
		{
			name: "HeadInt16PtrNotRoot",
			data: struct {
				A struct {
					A *int16 `json:"a"`
				}
			}{A: struct {
				A *int16 `json:"a"`
			}{int16ptr(1)}},
		},
		{
			name: "HeadInt16PtrNotRootOmitEmpty",
			data: struct {
				A struct {
					A *int16 `json:"a,omitempty"`
				}
			}{A: struct {
				A *int16 `json:"a,omitempty"`
			}{int16ptr(1)}},
		},
		{
			name: "HeadInt16PtrNotRootString",
			data: struct {
				A struct {
					A *int16 `json:"a,string"`
				}
			}{A: struct {
				A *int16 `json:"a,string"`
			}{int16ptr(1)}},
		},

		// HeadInt16PtrNilNotRoot
		{
			name: "HeadInt16PtrNilNotRoot",
			data: struct {
				A struct {
					A *int16 `json:"a"`
				}
			}{},
		},
		{
			name: "HeadInt16PtrNilNotRootOmitEmpty",
			data: struct {
				A struct {
					A *int16 `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadInt16PtrNilNotRootString",
			data: struct {
				A struct {
					A *int16 `json:"a,string"`
				}
			}{},
		},

		// PtrHeadInt16ZeroNotRoot
		{
			name: "PtrHeadInt16ZeroNotRoot",
			data: struct {
				A *struct {
					A int16 `json:"a"`
				}
			}{A: new(struct {
				A int16 `json:"a"`
			})},
		},
		{
			name: "PtrHeadInt16ZeroNotRootOmitEmpty",
			data: struct {
				A *struct {
					A int16 `json:"a,omitempty"`
				}
			}{A: new(struct {
				A int16 `json:"a,omitempty"`
			})},
		},
		{
			name: "PtrHeadInt16ZeroNotRootString",
			data: struct {
				A *struct {
					A int16 `json:"a,string"`
				}
			}{A: new(struct {
				A int16 `json:"a,string"`
			})},
		},

		// PtrHeadInt16NotRoot
		{
			name: "PtrHeadInt16NotRoot",
			data: struct {
				A *struct {
					A int16 `json:"a"`
				}
			}{A: &(struct {
				A int16 `json:"a"`
			}{A: 1})},
		},
		{
			name: "PtrHeadInt16NotRootOmitEmpty",
			data: struct {
				A *struct {
					A int16 `json:"a,omitempty"`
				}
			}{A: &(struct {
				A int16 `json:"a,omitempty"`
			}{A: 1})},
		},
		{
			name: "PtrHeadInt16NotRootString",
			data: struct {
				A *struct {
					A int16 `json:"a,string"`
				}
			}{A: &(struct {
				A int16 `json:"a,string"`
			}{A: 1})},
		},

		// PtrHeadInt16PtrNotRoot
		{
			name: "PtrHeadInt16PtrNotRoot",
			data: struct {
				A *struct {
					A *int16 `json:"a"`
				}
			}{A: &(struct {
				A *int16 `json:"a"`
			}{A: int16ptr(1)})},
		},
		{
			name: "PtrHeadInt16PtrNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *int16 `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *int16 `json:"a,omitempty"`
			}{A: int16ptr(1)})},
		},
		{
			name: "PtrHeadInt16PtrNotRootString",
			data: struct {
				A *struct {
					A *int16 `json:"a,string"`
				}
			}{A: &(struct {
				A *int16 `json:"a,string"`
			}{A: int16ptr(1)})},
		},

		// PtrHeadInt16PtrNilNotRoot
		{
			name: "PtrHeadInt16PtrNilNotRoot",
			data: struct {
				A *struct {
					A *int16 `json:"a"`
				}
			}{A: &(struct {
				A *int16 `json:"a"`
			}{A: nil})},
		},
		{
			name: "PtrHeadInt16PtrNilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *int16 `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *int16 `json:"a,omitempty"`
			}{A: nil})},
		},
		{
			name: "PtrHeadInt16PtrNilNotRootString",
			data: struct {
				A *struct {
					A *int16 `json:"a,string"`
				}
			}{A: &(struct {
				A *int16 `json:"a,string"`
			}{A: nil})},
		},

		// PtrHeadInt16NilNotRoot
		{
			name: "PtrHeadInt16NilNotRoot",
			data: struct {
				A *struct {
					A *int16 `json:"a"`
				}
			}{A: nil},
		},
		{
			name: "PtrHeadInt16NilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *int16 `json:"a,omitempty"`
				} `json:",omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadInt16NilNotRootString",
			data: struct {
				A *struct {
					A *int16 `json:"a,string"`
				} `json:",string"`
			}{A: nil},
		},

		// HeadInt16ZeroMultiFieldsNotRoot
		{
			name: "HeadInt16ZeroMultiFieldsNotRoot",
			data: struct {
				A struct {
					A int16 `json:"a"`
				}
				B struct {
					B int16 `json:"b"`
				}
			}{},
		},
		{
			name: "HeadInt16ZeroMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A int16 `json:"a,omitempty"`
				}
				B struct {
					B int16 `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "HeadInt16ZeroMultiFieldsNotRootString",
			data: struct {
				A struct {
					A int16 `json:"a,string"`
				}
				B struct {
					B int16 `json:"b,string"`
				}
			}{},
		},

		// HeadInt16MultiFieldsNotRoot
		{
			name: "HeadInt16MultiFieldsNotRoot",
			data: struct {
				A struct {
					A int16 `json:"a"`
				}
				B struct {
					B int16 `json:"b"`
				}
			}{A: struct {
				A int16 `json:"a"`
			}{A: 1}, B: struct {
				B int16 `json:"b"`
			}{B: -2}},
		},
		{
			name: "HeadInt16MultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A int16 `json:"a,omitempty"`
				}
				B struct {
					B int16 `json:"b,omitempty"`
				}
			}{A: struct {
				A int16 `json:"a,omitempty"`
			}{A: 1}, B: struct {
				B int16 `json:"b,omitempty"`
			}{B: -2}},
		},
		{
			name: "HeadInt16MultiFieldsNotRootString",
			data: struct {
				A struct {
					A int16 `json:"a,string"`
				}
				B struct {
					B int16 `json:"b,string"`
				}
			}{A: struct {
				A int16 `json:"a,string"`
			}{A: 1}, B: struct {
				B int16 `json:"b,string"`
			}{B: -2}},
		},

		// HeadInt16PtrMultiFieldsNotRoot
		{
			name: "HeadInt16PtrMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *int16 `json:"a"`
				}
				B struct {
					B *int16 `json:"b"`
				}
			}{A: struct {
				A *int16 `json:"a"`
			}{A: int16ptr(1)}, B: struct {
				B *int16 `json:"b"`
			}{B: int16ptr(-2)}},
		},
		{
			name: "HeadInt16PtrMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *int16 `json:"a,omitempty"`
				}
				B struct {
					B *int16 `json:"b,omitempty"`
				}
			}{A: struct {
				A *int16 `json:"a,omitempty"`
			}{A: int16ptr(1)}, B: struct {
				B *int16 `json:"b,omitempty"`
			}{B: int16ptr(-2)}},
		},
		{
			name: "HeadInt16PtrMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *int16 `json:"a,string"`
				}
				B struct {
					B *int16 `json:"b,string"`
				}
			}{A: struct {
				A *int16 `json:"a,string"`
			}{A: int16ptr(1)}, B: struct {
				B *int16 `json:"b,string"`
			}{B: int16ptr(-2)}},
		},

		// HeadInt16PtrNilMultiFieldsNotRoot
		{
			name: "HeadInt16PtrNilMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *int16 `json:"a"`
				}
				B struct {
					B *int16 `json:"b"`
				}
			}{A: struct {
				A *int16 `json:"a"`
			}{A: nil}, B: struct {
				B *int16 `json:"b"`
			}{B: nil}},
		},
		{
			name: "HeadInt16PtrNilMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *int16 `json:"a,omitempty"`
				}
				B struct {
					B *int16 `json:"b,omitempty"`
				}
			}{A: struct {
				A *int16 `json:"a,omitempty"`
			}{A: nil}, B: struct {
				B *int16 `json:"b,omitempty"`
			}{B: nil}},
		},
		{
			name: "HeadInt16PtrNilMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *int16 `json:"a,string"`
				}
				B struct {
					B *int16 `json:"b,string"`
				}
			}{A: struct {
				A *int16 `json:"a,string"`
			}{A: nil}, B: struct {
				B *int16 `json:"b,string"`
			}{B: nil}},
		},

		// PtrHeadInt16ZeroMultiFieldsNotRoot
		{
			name: "PtrHeadInt16ZeroMultiFieldsNotRoot",
			data: &struct {
				A struct {
					A int16 `json:"a"`
				}
				B struct {
					B int16 `json:"b"`
				}
			}{},
		},
		{
			name: "PtrHeadInt16ZeroMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A int16 `json:"a,omitempty"`
				}
				B struct {
					B int16 `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "PtrHeadInt16ZeroMultiFieldsNotRootString",
			data: &struct {
				A struct {
					A int16 `json:"a,string"`
				}
				B struct {
					B int16 `json:"b,string"`
				}
			}{},
		},

		// PtrHeadInt16MultiFieldsNotRoot
		{
			name: "PtrHeadInt16MultiFieldsNotRoot",
			data: &struct {
				A struct {
					A int16 `json:"a"`
				}
				B struct {
					B int16 `json:"b"`
				}
			}{A: struct {
				A int16 `json:"a"`
			}{A: 1}, B: struct {
				B int16 `json:"b"`
			}{B: -2}},
		},
		{
			name: "PtrHeadInt16MultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A int16 `json:"a,omitempty"`
				}
				B struct {
					B int16 `json:"b,omitempty"`
				}
			}{A: struct {
				A int16 `json:"a,omitempty"`
			}{A: 1}, B: struct {
				B int16 `json:"b,omitempty"`
			}{B: -2}},
		},
		{
			name: "PtrHeadInt16MultiFieldsNotRootString",
			data: &struct {
				A struct {
					A int16 `json:"a,string"`
				}
				B struct {
					B int16 `json:"b,string"`
				}
			}{A: struct {
				A int16 `json:"a,string"`
			}{A: 1}, B: struct {
				B int16 `json:"b,string"`
			}{B: -2}},
		},

		// PtrHeadInt16PtrMultiFieldsNotRoot
		{
			name: "PtrHeadInt16PtrMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *int16 `json:"a"`
				}
				B *struct {
					B *int16 `json:"b"`
				}
			}{A: &(struct {
				A *int16 `json:"a"`
			}{A: int16ptr(1)}), B: &(struct {
				B *int16 `json:"b"`
			}{B: int16ptr(-2)})},
		},
		{
			name: "PtrHeadInt16PtrMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *int16 `json:"a,omitempty"`
				}
				B *struct {
					B *int16 `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *int16 `json:"a,omitempty"`
			}{A: int16ptr(1)}), B: &(struct {
				B *int16 `json:"b,omitempty"`
			}{B: int16ptr(-2)})},
		},
		{
			name: "PtrHeadInt16PtrMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *int16 `json:"a,string"`
				}
				B *struct {
					B *int16 `json:"b,string"`
				}
			}{A: &(struct {
				A *int16 `json:"a,string"`
			}{A: int16ptr(1)}), B: &(struct {
				B *int16 `json:"b,string"`
			}{B: int16ptr(-2)})},
		},

		// PtrHeadInt16PtrNilMultiFieldsNotRoot
		{
			name: "PtrHeadInt16PtrNilMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *int16 `json:"a"`
				}
				B *struct {
					B *int16 `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadInt16PtrNilMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *int16 `json:"a,omitempty"`
				} `json:",omitempty"`
				B *struct {
					B *int16 `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadInt16PtrNilMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *int16 `json:"a,string"`
				} `json:",string"`
				B *struct {
					B *int16 `json:"b,string"`
				} `json:",string"`
			}{A: nil, B: nil},
		},

		// PtrHeadInt16NilMultiFieldsNotRoot
		{
			name: "PtrHeadInt16NilMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *int16 `json:"a"`
				}
				B *struct {
					B *int16 `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadInt16NilMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *int16 `json:"a,omitempty"`
				}
				B *struct {
					B *int16 `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadInt16NilMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *int16 `json:"a,string"`
				}
				B *struct {
					B *int16 `json:"b,string"`
				}
			})(nil),
		},

		// PtrHeadInt16DoubleMultiFieldsNotRoot
		{
			name: "PtrHeadInt16DoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A int16 `json:"a"`
					B int16 `json:"b"`
				}
				B *struct {
					A int16 `json:"a"`
					B int16 `json:"b"`
				}
			}{A: &(struct {
				A int16 `json:"a"`
				B int16 `json:"b"`
			}{A: 1, B: -2}), B: &(struct {
				A int16 `json:"a"`
				B int16 `json:"b"`
			}{A: 3, B: 4})},
		},
		{
			name: "PtrHeadInt16DoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A int16 `json:"a,omitempty"`
					B int16 `json:"b,omitempty"`
				}
				B *struct {
					A int16 `json:"a,omitempty"`
					B int16 `json:"b,omitempty"`
				}
			}{A: &(struct {
				A int16 `json:"a,omitempty"`
				B int16 `json:"b,omitempty"`
			}{A: 1, B: -2}), B: &(struct {
				A int16 `json:"a,omitempty"`
				B int16 `json:"b,omitempty"`
			}{A: 3, B: 4})},
		},
		{
			name: "PtrHeadInt16DoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A int16 `json:"a,string"`
					B int16 `json:"b,string"`
				}
				B *struct {
					A int16 `json:"a,string"`
					B int16 `json:"b,string"`
				}
			}{A: &(struct {
				A int16 `json:"a,string"`
				B int16 `json:"b,string"`
			}{A: 1, B: -2}), B: &(struct {
				A int16 `json:"a,string"`
				B int16 `json:"b,string"`
			}{A: 3, B: 4})},
		},

		// PtrHeadInt16NilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadInt16NilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A int16 `json:"a"`
					B int16 `json:"b"`
				}
				B *struct {
					A int16 `json:"a"`
					B int16 `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadInt16NilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A int16 `json:"a,omitempty"`
					B int16 `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A int16 `json:"a,omitempty"`
					B int16 `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadInt16NilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A int16 `json:"a,string"`
					B int16 `json:"b,string"`
				}
				B *struct {
					A int16 `json:"a,string"`
					B int16 `json:"b,string"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadInt16NilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadInt16NilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A int16 `json:"a"`
					B int16 `json:"b"`
				}
				B *struct {
					A int16 `json:"a"`
					B int16 `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadInt16NilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A int16 `json:"a,omitempty"`
					B int16 `json:"b,omitempty"`
				}
				B *struct {
					A int16 `json:"a,omitempty"`
					B int16 `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadInt16NilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A int16 `json:"a,string"`
					B int16 `json:"b,string"`
				}
				B *struct {
					A int16 `json:"a,string"`
					B int16 `json:"b,string"`
				}
			})(nil),
		},

		// PtrHeadInt16PtrDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadInt16PtrDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *int16 `json:"a"`
					B *int16 `json:"b"`
				}
				B *struct {
					A *int16 `json:"a"`
					B *int16 `json:"b"`
				}
			}{A: &(struct {
				A *int16 `json:"a"`
				B *int16 `json:"b"`
			}{A: int16ptr(1), B: int16ptr(-2)}), B: &(struct {
				A *int16 `json:"a"`
				B *int16 `json:"b"`
			}{A: int16ptr(3), B: int16ptr(4)})},
		},
		{
			name: "PtrHeadInt16PtrDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *int16 `json:"a,omitempty"`
					B *int16 `json:"b,omitempty"`
				}
				B *struct {
					A *int16 `json:"a,omitempty"`
					B *int16 `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *int16 `json:"a,omitempty"`
				B *int16 `json:"b,omitempty"`
			}{A: int16ptr(1), B: int16ptr(-2)}), B: &(struct {
				A *int16 `json:"a,omitempty"`
				B *int16 `json:"b,omitempty"`
			}{A: int16ptr(3), B: int16ptr(4)})},
		},
		{
			name: "PtrHeadInt16PtrDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *int16 `json:"a,string"`
					B *int16 `json:"b,string"`
				}
				B *struct {
					A *int16 `json:"a,string"`
					B *int16 `json:"b,string"`
				}
			}{A: &(struct {
				A *int16 `json:"a,string"`
				B *int16 `json:"b,string"`
			}{A: int16ptr(1), B: int16ptr(-2)}), B: &(struct {
				A *int16 `json:"a,string"`
				B *int16 `json:"b,string"`
			}{A: int16ptr(3), B: int16ptr(4)})},
		},

		// PtrHeadInt16PtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadInt16PtrNilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *int16 `json:"a"`
					B *int16 `json:"b"`
				}
				B *struct {
					A *int16 `json:"a"`
					B *int16 `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadInt16PtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *int16 `json:"a,omitempty"`
					B *int16 `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A *int16 `json:"a,omitempty"`
					B *int16 `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadInt16PtrNilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *int16 `json:"a,string"`
					B *int16 `json:"b,string"`
				}
				B *struct {
					A *int16 `json:"a,string"`
					B *int16 `json:"b,string"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadInt16PtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadInt16PtrNilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *int16 `json:"a"`
					B *int16 `json:"b"`
				}
				B *struct {
					A *int16 `json:"a"`
					B *int16 `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadInt16PtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *int16 `json:"a,omitempty"`
					B *int16 `json:"b,omitempty"`
				}
				B *struct {
					A *int16 `json:"a,omitempty"`
					B *int16 `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadInt16PtrNilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *int16 `json:"a,string"`
					B *int16 `json:"b,string"`
				}
				B *struct {
					A *int16 `json:"a,string"`
					B *int16 `json:"b,string"`
				}
			})(nil),
		},

		// AnonymousHeadInt16
		{
			name: "AnonymousHeadInt16",
			data: struct {
				structInt16
				B int16 `json:"b"`
			}{
				structInt16: structInt16{A: 1},
				B:           -2,
			},
		},
		{
			name: "AnonymousHeadInt16OmitEmpty",
			data: struct {
				structInt16OmitEmpty
				B int16 `json:"b,omitempty"`
			}{
				structInt16OmitEmpty: structInt16OmitEmpty{A: 1},
				B:                    -2,
			},
		},
		{
			name: "AnonymousHeadInt16String",
			data: struct {
				structInt16String
				B int16 `json:"b,string"`
			}{
				structInt16String: structInt16String{A: 1},
				B:                 -2,
			},
		},

		// PtrAnonymousHeadInt16
		{
			name: "PtrAnonymousHeadInt16",
			data: struct {
				*structInt16
				B int16 `json:"b"`
			}{
				structInt16: &structInt16{A: 1},
				B:           -2,
			},
		},
		{
			name: "PtrAnonymousHeadInt16OmitEmpty",
			data: struct {
				*structInt16OmitEmpty
				B int16 `json:"b,omitempty"`
			}{
				structInt16OmitEmpty: &structInt16OmitEmpty{A: 1},
				B:                    -2,
			},
		},
		{
			name: "PtrAnonymousHeadInt16String",
			data: struct {
				*structInt16String
				B int16 `json:"b,string"`
			}{
				structInt16String: &structInt16String{A: 1},
				B:                 -2,
			},
		},

		// NilPtrAnonymousHeadInt16
		{
			name: "NilPtrAnonymousHeadInt16",
			data: struct {
				*structInt16
				B int16 `json:"b"`
			}{
				structInt16: nil,
				B:           -2,
			},
		},
		{
			name: "NilPtrAnonymousHeadInt16OmitEmpty",
			data: struct {
				*structInt16OmitEmpty
				B int16 `json:"b,omitempty"`
			}{
				structInt16OmitEmpty: nil,
				B:                    -2,
			},
		},
		{
			name: "NilPtrAnonymousHeadInt16String",
			data: struct {
				*structInt16String
				B int16 `json:"b,string"`
			}{
				structInt16String: nil,
				B:                 -2,
			},
		},

		// AnonymousHeadInt16Ptr
		{
			name: "AnonymousHeadInt16Ptr",
			data: struct {
				structInt16Ptr
				B *int16 `json:"b"`
			}{
				structInt16Ptr: structInt16Ptr{A: int16ptr(1)},
				B:              int16ptr(-2),
			},
		},
		{
			name: "AnonymousHeadInt16PtrOmitEmpty",
			data: struct {
				structInt16PtrOmitEmpty
				B *int16 `json:"b,omitempty"`
			}{
				structInt16PtrOmitEmpty: structInt16PtrOmitEmpty{A: int16ptr(1)},
				B:                       int16ptr(-2),
			},
		},
		{
			name: "AnonymousHeadInt16PtrString",
			data: struct {
				structInt16PtrString
				B *int16 `json:"b,string"`
			}{
				structInt16PtrString: structInt16PtrString{A: int16ptr(1)},
				B:                    int16ptr(-2),
			},
		},

		// AnonymousHeadInt16PtrNil
		{
			name: "AnonymousHeadInt16PtrNil",
			data: struct {
				structInt16Ptr
				B *int16 `json:"b"`
			}{
				structInt16Ptr: structInt16Ptr{A: nil},
				B:              int16ptr(-2),
			},
		},
		{
			name: "AnonymousHeadInt16PtrNilOmitEmpty",
			data: struct {
				structInt16PtrOmitEmpty
				B *int16 `json:"b,omitempty"`
			}{
				structInt16PtrOmitEmpty: structInt16PtrOmitEmpty{A: nil},
				B:                       int16ptr(-2),
			},
		},
		{
			name: "AnonymousHeadInt16PtrNilString",
			data: struct {
				structInt16PtrString
				B *int16 `json:"b,string"`
			}{
				structInt16PtrString: structInt16PtrString{A: nil},
				B:                    int16ptr(-2),
			},
		},

		// PtrAnonymousHeadInt16Ptr
		{
			name: "PtrAnonymousHeadInt16Ptr",
			data: struct {
				*structInt16Ptr
				B *int16 `json:"b"`
			}{
				structInt16Ptr: &structInt16Ptr{A: int16ptr(1)},
				B:              int16ptr(-2),
			},
		},
		{
			name: "PtrAnonymousHeadInt16PtrOmitEmpty",
			data: struct {
				*structInt16PtrOmitEmpty
				B *int16 `json:"b,omitempty"`
			}{
				structInt16PtrOmitEmpty: &structInt16PtrOmitEmpty{A: int16ptr(1)},
				B:                       int16ptr(-2),
			},
		},
		{
			name: "PtrAnonymousHeadInt16PtrString",
			data: struct {
				*structInt16PtrString
				B *int16 `json:"b,string"`
			}{
				structInt16PtrString: &structInt16PtrString{A: int16ptr(1)},
				B:                    int16ptr(-2),
			},
		},

		// NilPtrAnonymousHeadInt16Ptr
		{
			name: "NilPtrAnonymousHeadInt16Ptr",
			data: struct {
				*structInt16Ptr
				B *int16 `json:"b"`
			}{
				structInt16Ptr: nil,
				B:              int16ptr(-2),
			},
		},
		{
			name: "NilPtrAnonymousHeadInt16PtrOmitEmpty",
			data: struct {
				*structInt16PtrOmitEmpty
				B *int16 `json:"b,omitempty"`
			}{
				structInt16PtrOmitEmpty: nil,
				B:                       int16ptr(-2),
			},
		},
		{
			name: "NilPtrAnonymousHeadInt16PtrString",
			data: struct {
				*structInt16PtrString
				B *int16 `json:"b,string"`
			}{
				structInt16PtrString: nil,
				B:                    int16ptr(-2),
			},
		},

		// AnonymousHeadInt16Only
		{
			name: "AnonymousHeadInt16Only",
			data: struct {
				structInt16
			}{
				structInt16: structInt16{A: 1},
			},
		},
		{
			name: "AnonymousHeadInt16OnlyOmitEmpty",
			data: struct {
				structInt16OmitEmpty
			}{
				structInt16OmitEmpty: structInt16OmitEmpty{A: 1},
			},
		},
		{
			name: "AnonymousHeadInt16OnlyString",
			data: struct {
				structInt16String
			}{
				structInt16String: structInt16String{A: 1},
			},
		},

		// PtrAnonymousHeadInt16Only
		{
			name: "PtrAnonymousHeadInt16Only",
			data: struct {
				*structInt16
			}{
				structInt16: &structInt16{A: 1},
			},
		},
		{
			name: "PtrAnonymousHeadInt16OnlyOmitEmpty",
			data: struct {
				*structInt16OmitEmpty
			}{
				structInt16OmitEmpty: &structInt16OmitEmpty{A: 1},
			},
		},
		{
			name: "PtrAnonymousHeadInt16OnlyString",
			data: struct {
				*structInt16String
			}{
				structInt16String: &structInt16String{A: 1},
			},
		},

		// NilPtrAnonymousHeadInt16Only
		{
			name: "NilPtrAnonymousHeadInt16Only",
			data: struct {
				*structInt16
			}{
				structInt16: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadInt16OnlyOmitEmpty",
			data: struct {
				*structInt16OmitEmpty
			}{
				structInt16OmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadInt16OnlyString",
			data: struct {
				*structInt16String
			}{
				structInt16String: nil,
			},
		},

		// AnonymousHeadInt16PtrOnly
		{
			name: "AnonymousHeadInt16PtrOnly",
			data: struct {
				structInt16Ptr
			}{
				structInt16Ptr: structInt16Ptr{A: int16ptr(1)},
			},
		},
		{
			name: "AnonymousHeadInt16PtrOnlyOmitEmpty",
			data: struct {
				structInt16PtrOmitEmpty
			}{
				structInt16PtrOmitEmpty: structInt16PtrOmitEmpty{A: int16ptr(1)},
			},
		},
		{
			name: "AnonymousHeadInt16PtrOnlyString",
			data: struct {
				structInt16PtrString
			}{
				structInt16PtrString: structInt16PtrString{A: int16ptr(1)},
			},
		},

		// AnonymousHeadInt16PtrNilOnly
		{
			name: "AnonymousHeadInt16PtrNilOnly",
			data: struct {
				structInt16Ptr
			}{
				structInt16Ptr: structInt16Ptr{A: nil},
			},
		},
		{
			name: "AnonymousHeadInt16PtrNilOnlyOmitEmpty",
			data: struct {
				structInt16PtrOmitEmpty
			}{
				structInt16PtrOmitEmpty: structInt16PtrOmitEmpty{A: nil},
			},
		},
		{
			name: "AnonymousHeadInt16PtrNilOnlyString",
			data: struct {
				structInt16PtrString
			}{
				structInt16PtrString: structInt16PtrString{A: nil},
			},
		},

		// PtrAnonymousHeadInt16PtrOnly
		{
			name: "PtrAnonymousHeadInt16PtrOnly",
			data: struct {
				*structInt16Ptr
			}{
				structInt16Ptr: &structInt16Ptr{A: int16ptr(1)},
			},
		},
		{
			name: "PtrAnonymousHeadInt16PtrOnlyOmitEmpty",
			data: struct {
				*structInt16PtrOmitEmpty
			}{
				structInt16PtrOmitEmpty: &structInt16PtrOmitEmpty{A: int16ptr(1)},
			},
		},
		{
			name: "PtrAnonymousHeadInt16PtrOnlyString",
			data: struct {
				*structInt16PtrString
			}{
				structInt16PtrString: &structInt16PtrString{A: int16ptr(1)},
			},
		},

		// NilPtrAnonymousHeadInt16PtrOnly
		{
			name: "NilPtrAnonymousHeadInt16PtrOnly",
			data: struct {
				*structInt16Ptr
			}{
				structInt16Ptr: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadInt16PtrOnlyOmitEmpty",
			data: struct {
				*structInt16PtrOmitEmpty
			}{
				structInt16PtrOmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadInt16PtrOnlyString",
			data: struct {
				*structInt16PtrString
			}{
				structInt16PtrString: nil,
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
