package json_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/goccy/go-json"
)

func TestCoverInt(t *testing.T) {
	type structInt struct {
		A int `json:"a"`
	}
	type structIntOmitEmpty struct {
		A int `json:"a,omitempty"`
	}
	type structIntString struct {
		A int `json:"a,string"`
	}
	type structIntStringOmitEmpty struct {
		A int `json:"a,omitempty,string"`
	}

	type structIntPtr struct {
		A *int `json:"a"`
	}
	type structIntPtrOmitEmpty struct {
		A *int `json:"a,omitempty"`
	}
	type structIntPtrString struct {
		A *int `json:"a,string"`
	}
	type structIntPtrStringOmitEmpty struct {
		A *int `json:"a,omitempty,string"`
	}

	tests := []struct {
		name string
		data interface{}
	}{
		{
			name: "Int",
			data: 10,
		},
		{
			name: "IntPtr",
			data: intptr(10),
		},
		{
			name: "IntPtr3",
			data: intptr3(10),
		},
		{
			name: "IntPtrNil",
			data: (*int)(nil),
		},
		{
			name: "IntPtr3Nil",
			data: (***int)(nil),
		},

		// HeadIntZero
		{
			name: "HeadIntZero",
			data: struct {
				A int `json:"a"`
			}{},
		},
		{
			name: "HeadIntZeroOmitEmpty",
			data: struct {
				A int `json:"a,omitempty"`
			}{},
		},
		{
			name: "HeadIntZeroString",
			data: struct {
				A int `json:"a,string"`
			}{},
		},

		// HeadInt
		{
			name: "HeadInt",
			data: struct {
				A int `json:"a"`
			}{A: -1},
		},
		{
			name: "HeadIntOmitEmpty",
			data: struct {
				A int `json:"a,omitempty"`
			}{A: -1},
		},
		{
			name: "HeadIntString",
			data: struct {
				A int `json:"a,string"`
			}{A: -1},
		},

		// HeadIntPtr
		{
			name: "HeadIntPtr",
			data: struct {
				A *int `json:"a"`
			}{A: intptr(-1)},
		},
		{
			name: "HeadIntPtrOmitEmpty",
			data: struct {
				A *int `json:"a,omitempty"`
			}{A: intptr(-1)},
		},
		{
			name: "HeadIntPtrString",
			data: struct {
				A *int `json:"a,string"`
			}{A: intptr(-1)},
		},

		// HeadIntPtrNil
		{
			name: "HeadIntPtrNil",
			data: struct {
				A *int `json:"a"`
			}{A: nil},
		},
		{
			name: "HeadIntPtrNilOmitEmpty",
			data: struct {
				A *int `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "HeadIntPtrNilString",
			data: struct {
				A *int `json:"a,string"`
			}{A: nil},
		},

		// PtrHeadIntZero
		{
			name: "PtrHeadIntZero",
			data: &struct {
				A int `json:"a"`
			}{},
		},
		{
			name: "PtrHeadIntZeroOmitEmpty",
			data: &struct {
				A int `json:"a,omitempty"`
			}{},
		},
		{
			name: "PtrHeadIntZeroString",
			data: &struct {
				A int `json:"a,string"`
			}{},
		},
		{
			name: "PtrHeadIntZeroStringOmitEmpty",
			data: &struct {
				A int `json:"a,string,omitempty"`
			}{},
		},

		// PtrHeadInt
		{
			name: "PtrHeadInt",
			data: &struct {
				A int `json:"a"`
			}{A: -1},
		},
		{
			name: "PtrHeadIntOmitEmpty",
			data: &struct {
				A int `json:"a,omitempty"`
			}{A: -1},
		},
		{
			name: "PtrHeadIntString",
			data: &struct {
				A int `json:"a,string"`
			}{A: -1},
		},
		{
			name: "PtrHeadIntStringOmitEmpty",
			data: &struct {
				A int `json:"a,string,omitempty"`
			}{A: -1},
		},

		// PtrHeadIntPtr
		{
			name: "PtrHeadIntPtr",
			data: &struct {
				A *int `json:"a"`
			}{A: intptr(-1)},
		},
		{
			name: "PtrHeadIntPtrOmitEmpty",
			data: &struct {
				A *int `json:"a,omitempty"`
			}{A: intptr(-1)},
		},
		{
			name: "PtrHeadIntPtrString",
			data: &struct {
				A *int `json:"a,string"`
			}{A: intptr(-1)},
		},
		{
			name: "PtrHeadIntPtrStringOmitEmpty",
			data: &struct {
				A *int `json:"a,string,omitempty"`
			}{A: intptr(-1)},
		},

		// PtrHeadIntPtrNil
		{
			name: "PtrHeadIntPtrNil",
			data: &struct {
				A *int `json:"a"`
			}{A: nil},
		},
		{
			name: "PtrHeadIntPtrNilOmitEmpty",
			data: &struct {
				A *int `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadIntPtrNilString",
			data: &struct {
				A *int `json:"a,string"`
			}{A: nil},
		},
		{
			name: "PtrHeadIntPtrNilStringOmitEmpty",
			data: &struct {
				A *int `json:"a,string,omitempty"`
			}{A: nil},
		},

		// PtrHeadIntNil
		{
			name: "PtrHeadIntNil",
			data: (*struct {
				A *int `json:"a"`
			})(nil),
		},
		{
			name: "PtrHeadIntNilOmitEmpty",
			data: (*struct {
				A *int `json:"a,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadIntNilString",
			data: (*struct {
				A *int `json:"a,string"`
			})(nil),
		},
		{
			name: "PtrHeadIntNilStringOmitEmpty",
			data: (*struct {
				A *int `json:"a,string,omitempty"`
			})(nil),
		},

		// HeadIntZeroMultiFields
		{
			name: "HeadIntZeroMultiFields",
			data: struct {
				A int `json:"a"`
				B int `json:"b"`
				C int `json:"c"`
			}{},
		},
		{
			name: "HeadIntZeroMultiFieldsOmitEmpty",
			data: struct {
				A int `json:"a,omitempty"`
				B int `json:"b,omitempty"`
				C int `json:"c,omitempty"`
			}{},
		},
		{
			name: "HeadIntZeroMultiFieldsString",
			data: struct {
				A int `json:"a,string"`
				B int `json:"b,string"`
				C int `json:"c,string"`
			}{},
		},
		{
			name: "HeadIntZeroMultiFieldsStringOmitEmpty",
			data: struct {
				A int `json:"a,string,omitempty"`
				B int `json:"b,string,omitempty"`
				C int `json:"c,string,omitempty"`
			}{},
		},

		// HeadIntMultiFields
		{
			name: "HeadIntMultiFields",
			data: struct {
				A int `json:"a"`
				B int `json:"b"`
				C int `json:"c"`
			}{A: -1, B: 2, C: 3},
		},
		{
			name: "HeadIntMultiFieldsOmitEmpty",
			data: struct {
				A int `json:"a,omitempty"`
				B int `json:"b,omitempty"`
				C int `json:"c,omitempty"`
			}{A: -1, B: 2, C: 3},
		},
		{
			name: "HeadIntMultiFieldsString",
			data: struct {
				A int `json:"a,string"`
				B int `json:"b,string"`
				C int `json:"c,string"`
			}{A: -1, B: 2, C: 3},
		},
		{
			name: "HeadIntMultiFieldsStringOmitEmpty",
			data: struct {
				A int `json:"a,string,omitempty"`
				B int `json:"b,string,omitempty"`
				C int `json:"c,string,omitempty"`
			}{A: -1, B: 2, C: 3},
		},

		// HeadIntPtrMultiFields
		{
			name: "HeadIntPtrMultiFields",
			data: struct {
				A *int `json:"a"`
				B *int `json:"b"`
				C *int `json:"c"`
			}{A: intptr(-1), B: intptr(2), C: intptr(3)},
		},
		{
			name: "HeadIntPtrMultiFieldsOmitEmpty",
			data: struct {
				A *int `json:"a,omitempty"`
				B *int `json:"b,omitempty"`
				C *int `json:"c,omitempty"`
			}{A: intptr(-1), B: intptr(2), C: intptr(3)},
		},
		{
			name: "HeadIntPtrMultiFieldsString",
			data: struct {
				A *int `json:"a,string"`
				B *int `json:"b,string"`
				C *int `json:"c,string"`
			}{A: intptr(-1), B: intptr(2), C: intptr(3)},
		},
		{
			name: "HeadIntPtrMultiFieldsStringOmitEmpty",
			data: struct {
				A *int `json:"a,string,omitempty"`
				B *int `json:"b,string,omitempty"`
				C *int `json:"c,string,omitempty"`
			}{A: intptr(-1), B: intptr(2), C: intptr(3)},
		},

		// HeadIntPtrNilMultiFields
		{
			name: "HeadIntPtrNilMultiFields",
			data: struct {
				A *int `json:"a"`
				B *int `json:"b"`
				C *int `json:"c"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadIntPtrNilMultiFieldsOmitEmpty",
			data: struct {
				A *int `json:"a,omitempty"`
				B *int `json:"b,omitempty"`
				C *int `json:"c,omitempty"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadIntPtrNilMultiFieldsString",
			data: struct {
				A *int `json:"a,string"`
				B *int `json:"b,string"`
				C *int `json:"c,string"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadIntPtrNilMultiFieldsStringOmitEmpty",
			data: struct {
				A *int `json:"a,string,omitempty"`
				B *int `json:"b,string,omitempty"`
				C *int `json:"c,string,omitempty"`
			}{A: nil, B: nil, C: nil},
		},

		// PtrHeadIntZeroMultiFields
		{
			name: "PtrHeadIntZeroMultiFields",
			data: &struct {
				A int `json:"a"`
				B int `json:"b"`
			}{},
		},
		{
			name: "PtrHeadIntZeroMultiFieldsOmitEmpty",
			data: &struct {
				A int `json:"a,omitempty"`
				B int `json:"b,omitempty"`
			}{},
		},
		{
			name: "PtrHeadIntZeroMultiFieldsString",
			data: &struct {
				A int `json:"a,string"`
				B int `json:"b,string"`
			}{},
		},
		{
			name: "PtrHeadIntZeroMultiFieldsStringOmitEmpty",
			data: &struct {
				A int `json:"a,string,omitempty"`
				B int `json:"b,string,omitempty"`
			}{},
		},

		// PtrHeadIntMultiFields
		{
			name: "PtrHeadIntMultiFields",
			data: &struct {
				A int `json:"a"`
				B int `json:"b"`
			}{A: -1, B: 2},
		},
		{
			name: "PtrHeadIntMultiFieldsOmitEmpty",
			data: &struct {
				A int `json:"a,omitempty"`
				B int `json:"b,omitempty"`
			}{A: -1, B: 2},
		},
		{
			name: "PtrHeadIntMultiFieldsString",
			data: &struct {
				A int `json:"a,string"`
				B int `json:"b,string"`
			}{A: -1, B: 2},
		},
		{
			name: "PtrHeadIntMultiFieldsStringOmitEmpty",
			data: &struct {
				A int `json:"a,string,omitempty"`
				B int `json:"b,string,omitempty"`
			}{A: -1, B: 2},
		},

		// PtrHeadIntPtrMultiFields
		{
			name: "PtrHeadIntPtrMultiFields",
			data: &struct {
				A *int `json:"a"`
				B *int `json:"b"`
			}{A: intptr(-1), B: intptr(2)},
		},
		{
			name: "PtrHeadIntPtrMultiFieldsOmitEmpty",
			data: &struct {
				A *int `json:"a,omitempty"`
				B *int `json:"b,omitempty"`
			}{A: intptr(-1), B: intptr(2)},
		},
		{
			name: "PtrHeadIntPtrMultiFieldsString",
			data: &struct {
				A *int `json:"a,string"`
				B *int `json:"b,string"`
			}{A: intptr(-1), B: intptr(2)},
		},
		{
			name: "PtrHeadIntPtrMultiFieldsStringOmitEmpty",
			data: &struct {
				A *int `json:"a,string,omitempty"`
				B *int `json:"b,string,omitempty"`
			}{A: intptr(-1), B: intptr(2)},
		},

		// PtrHeadIntPtrNilMultiFields
		{
			name: "PtrHeadIntPtrNilMultiFields",
			data: &struct {
				A *int `json:"a"`
				B *int `json:"b"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadIntPtrNilMultiFieldsOmitEmpty",
			data: &struct {
				A *int `json:"a,omitempty"`
				B *int `json:"b,omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadIntPtrNilMultiFieldsString",
			data: &struct {
				A *int `json:"a,string"`
				B *int `json:"b,string"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadIntPtrNilMultiFieldsStringOmitEmpty",
			data: &struct {
				A *int `json:"a,string,omitempty"`
				B *int `json:"b,string,omitempty"`
			}{A: nil, B: nil},
		},

		// PtrHeadIntNilMultiFields
		{
			name: "PtrHeadIntNilMultiFields",
			data: (*struct {
				A *int `json:"a"`
				B *int `json:"b"`
			})(nil),
		},
		{
			name: "PtrHeadIntNilMultiFieldsOmitEmpty",
			data: (*struct {
				A *int `json:"a,omitempty"`
				B *int `json:"b,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadIntNilMultiFieldsString",
			data: (*struct {
				A *int `json:"a,string"`
				B *int `json:"b,string"`
			})(nil),
		},
		{
			name: "PtrHeadIntNilMultiFieldsStringOmitEmpty",
			data: (*struct {
				A *int `json:"a,string,omitempty"`
				B *int `json:"b,string,omitempty"`
			})(nil),
		},

		// HeadIntZeroNotRoot
		{
			name: "HeadIntZeroNotRoot",
			data: struct {
				A struct {
					A int `json:"a"`
				}
			}{},
		},
		{
			name: "HeadIntZeroNotRootOmitEmpty",
			data: struct {
				A struct {
					A int `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadIntZeroNotRootString",
			data: struct {
				A struct {
					A int `json:"a,string"`
				}
			}{},
		},
		{
			name: "HeadIntZeroNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A int `json:"a,string,omitempty"`
				}
			}{},
		},

		// HeadIntNotRoot
		{
			name: "HeadIntNotRoot",
			data: struct {
				A struct {
					A int `json:"a"`
				}
			}{A: struct {
				A int `json:"a"`
			}{A: -1}},
		},
		{
			name: "HeadIntNotRootOmitEmpty",
			data: struct {
				A struct {
					A int `json:"a,omitempty"`
				}
			}{A: struct {
				A int `json:"a,omitempty"`
			}{A: -1}},
		},
		{
			name: "HeadIntNotRootString",
			data: struct {
				A struct {
					A int `json:"a,string"`
				}
			}{A: struct {
				A int `json:"a,string"`
			}{A: -1}},
		},
		{
			name: "HeadIntNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A int `json:"a,string,omitempty"`
				}
			}{A: struct {
				A int `json:"a,string,omitempty"`
			}{A: -1}},
		},

		// HeadIntNotRootMultiFields
		{
			name: "HeadIntNotRootMultiFields",
			data: struct {
				A struct {
					A int `json:"a"`
					B int `json:"b"`
				}
			}{A: struct {
				A int `json:"a"`
				B int `json:"b"`
			}{A: -1, B: 1}},
		},
		{
			name: "HeadIntNotRootOmitEmptyMultiFields",
			data: struct {
				A struct {
					A int `json:"a,omitempty"`
					B int `json:"b,omitempty"`
				}
			}{A: struct {
				A int `json:"a,omitempty"`
				B int `json:"b,omitempty"`
			}{A: -1, B: 1}},
		},
		{
			name: "HeadIntNotRootStringMultiFields",
			data: struct {
				A struct {
					A int `json:"a,string"`
					B int `json:"b,string"`
				}
			}{A: struct {
				A int `json:"a,string"`
				B int `json:"b,string"`
			}{A: -1, B: 1}},
		},
		{
			name: "HeadIntNotRootStringOmitEmptyMultiFields",
			data: struct {
				A struct {
					A int `json:"a,string,omitempty"`
					B int `json:"b,string,omitempty"`
				}
			}{A: struct {
				A int `json:"a,string,omitempty"`
				B int `json:"b,string,omitempty"`
			}{A: -1, B: 1}},
		},

		// HeadIntPtrNotRoot
		{
			name: "HeadIntPtrNotRoot",
			data: struct {
				A struct {
					A *int `json:"a"`
				}
			}{A: struct {
				A *int `json:"a"`
			}{intptr(-1)}},
		},
		{
			name: "HeadIntPtrNotRootOmitEmpty",
			data: struct {
				A struct {
					A *int `json:"a,omitempty"`
				}
			}{A: struct {
				A *int `json:"a,omitempty"`
			}{intptr(-1)}},
		},
		{
			name: "HeadIntPtrNotRootString",
			data: struct {
				A struct {
					A *int `json:"a,string"`
				}
			}{A: struct {
				A *int `json:"a,string"`
			}{intptr(-1)}},
		},
		{
			name: "HeadIntPtrNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A *int `json:"a,string,omitempty"`
				}
			}{A: struct {
				A *int `json:"a,string,omitempty"`
			}{intptr(-1)}},
		},

		// HeadIntPtrNilNotRoot
		{
			name: "HeadIntPtrNilNotRoot",
			data: struct {
				A struct {
					A *int `json:"a"`
				}
			}{},
		},
		{
			name: "HeadIntPtrNilNotRootOmitEmpty",
			data: struct {
				A struct {
					A *int `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadIntPtrNilNotRootString",
			data: struct {
				A struct {
					A *int `json:"a,string"`
				}
			}{},
		},
		{
			name: "HeadIntPtrNilNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A *int `json:"a,string,omitempty"`
				}
			}{},
		},

		// PtrHeadIntZeroNotRoot
		{
			name: "PtrHeadIntZeroNotRoot",
			data: struct {
				A *struct {
					A int `json:"a"`
				}
			}{A: new(struct {
				A int `json:"a"`
			})},
		},
		{
			name: "PtrHeadIntZeroNotRootOmitEmpty",
			data: struct {
				A *struct {
					A int `json:"a,omitempty"`
				}
			}{A: new(struct {
				A int `json:"a,omitempty"`
			})},
		},
		{
			name: "PtrHeadIntZeroNotRootString",
			data: struct {
				A *struct {
					A int `json:"a,string"`
				}
			}{A: new(struct {
				A int `json:"a,string"`
			})},
		},
		{
			name: "PtrHeadIntZeroNotRootStringOmitEmpty",
			data: struct {
				A *struct {
					A int `json:"a,string,omitempty"`
				}
			}{A: new(struct {
				A int `json:"a,string,omitempty"`
			})},
		},

		// PtrHeadIntNotRoot
		{
			name: "PtrHeadIntNotRoot",
			data: struct {
				A *struct {
					A int `json:"a"`
				}
			}{A: &(struct {
				A int `json:"a"`
			}{A: -1})},
		},
		{
			name: "PtrHeadIntNotRootOmitEmpty",
			data: struct {
				A *struct {
					A int `json:"a,omitempty"`
				}
			}{A: &(struct {
				A int `json:"a,omitempty"`
			}{A: -1})},
		},
		{
			name: "PtrHeadIntNotRootString",
			data: struct {
				A *struct {
					A int `json:"a,string"`
				}
			}{A: &(struct {
				A int `json:"a,string"`
			}{A: -1})},
		},
		{
			name: "PtrHeadIntNotRootStringOmitEmpty",
			data: struct {
				A *struct {
					A int `json:"a,string,omitempty"`
				}
			}{A: &(struct {
				A int `json:"a,string,omitempty"`
			}{A: -1})},
		},

		// PtrHeadIntNotRootMultiFields
		{
			name: "PtrHeadIntNotRootMultiFields",
			data: struct {
				A *struct {
					A int `json:"a"`
					B int `json:"b"`
				}
			}{A: &(struct {
				A int `json:"a"`
				B int `json:"b"`
			}{A: -1, B: 1})},
		},
		{
			name: "PtrHeadIntNotRootOmitEmptyMultiFields",
			data: struct {
				A *struct {
					A int `json:"a,omitempty"`
					B int `json:"b,omitempty"`
				}
			}{A: &(struct {
				A int `json:"a,omitempty"`
				B int `json:"b,omitempty"`
			}{A: -1, B: 1})},
		},
		{
			name: "PtrHeadIntNotRootStringMultiFields",
			data: struct {
				A *struct {
					A int `json:"a,string"`
					B int `json:"b,string"`
				}
			}{A: &(struct {
				A int `json:"a,string"`
				B int `json:"b,string"`
			}{A: -1, B: 1})},
		},
		{
			name: "PtrHeadIntNotRootStringOmitEmptyMultiFields",
			data: struct {
				A *struct {
					A int `json:"a,string,omitempty"`
					B int `json:"b,string,omitempty"`
				}
			}{A: &(struct {
				A int `json:"a,string,omitempty"`
				B int `json:"b,string,omitempty"`
			}{A: -1, B: 1})},
		},

		// PtrHeadIntPtrNotRoot
		{
			name: "PtrHeadIntPtrNotRoot",
			data: struct {
				A *struct {
					A *int `json:"a"`
				}
			}{A: &(struct {
				A *int `json:"a"`
			}{A: intptr(-1)})},
		},
		{
			name: "PtrHeadIntPtrNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *int `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *int `json:"a,omitempty"`
			}{A: intptr(-1)})},
		},
		{
			name: "PtrHeadIntPtrNotRootString",
			data: struct {
				A *struct {
					A *int `json:"a,string"`
				}
			}{A: &(struct {
				A *int `json:"a,string"`
			}{A: intptr(-1)})},
		},
		{
			name: "PtrHeadIntPtrNotRootStringOmitEmpty",
			data: struct {
				A *struct {
					A *int `json:"a,string,omitempty"`
				}
			}{A: &(struct {
				A *int `json:"a,string,omitempty"`
			}{A: intptr(-1)})},
		},

		// PtrHeadIntPtrNilNotRoot
		{
			name: "PtrHeadIntPtrNilNotRoot",
			data: struct {
				A *struct {
					A *int `json:"a"`
				}
			}{A: &(struct {
				A *int `json:"a"`
			}{A: nil})},
		},
		{
			name: "PtrHeadIntPtrNilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *int `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *int `json:"a,omitempty"`
			}{A: nil})},
		},
		{
			name: "PtrHeadIntPtrNilNotRootString",
			data: struct {
				A *struct {
					A *int `json:"a,string"`
				}
			}{A: &(struct {
				A *int `json:"a,string"`
			}{A: nil})},
		},
		{
			name: "PtrHeadIntPtrNilNotRootStringOmitEmpty",
			data: struct {
				A *struct {
					A *int `json:"a,string,omitempty"`
				}
			}{A: &(struct {
				A *int `json:"a,string,omitempty"`
			}{A: nil})},
		},

		// PtrHeadIntNilNotRoot
		{
			name: "PtrHeadIntNilNotRoot",
			data: struct {
				A *struct {
					A *int `json:"a"`
				}
			}{A: nil},
		},
		{
			name: "PtrHeadIntNilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *int `json:"a,omitempty"`
				} `json:",omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadIntNilNotRootString",
			data: struct {
				A *struct {
					A *int `json:"a,string"`
				} `json:",string"`
			}{A: nil},
		},
		{
			name: "PtrHeadIntNilNotRootStringOmitEmpty",
			data: struct {
				A *struct {
					A *int `json:"a,string,omitempty"`
				} `json:",string,omitempty"`
			}{A: nil},
		},

		// HeadIntZeroMultiFieldsNotRoot
		{
			name: "HeadIntZeroMultiFieldsNotRoot",
			data: struct {
				A struct {
					A int `json:"a"`
				}
				B struct {
					B int `json:"b"`
				}
			}{},
		},
		{
			name: "HeadIntZeroMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A int `json:"a,omitempty"`
				}
				B struct {
					B int `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "HeadIntZeroMultiFieldsNotRootString",
			data: struct {
				A struct {
					A int `json:"a,string"`
				}
				B struct {
					B int `json:"b,string"`
				}
			}{},
		},
		{
			name: "HeadIntZeroMultiFieldsNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A int `json:"a,string,omitempty"`
				}
				B struct {
					B int `json:"b,string,omitempty"`
				}
			}{},
		},

		// HeadIntMultiFieldsNotRoot
		{
			name: "HeadIntMultiFieldsNotRoot",
			data: struct {
				A struct {
					A int `json:"a"`
				}
				B struct {
					B int `json:"b"`
				}
			}{A: struct {
				A int `json:"a"`
			}{A: -1}, B: struct {
				B int `json:"b"`
			}{B: 2}},
		},
		{
			name: "HeadIntMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A int `json:"a,omitempty"`
				}
				B struct {
					B int `json:"b,omitempty"`
				}
			}{A: struct {
				A int `json:"a,omitempty"`
			}{A: -1}, B: struct {
				B int `json:"b,omitempty"`
			}{B: 2}},
		},
		{
			name: "HeadIntMultiFieldsNotRootString",
			data: struct {
				A struct {
					A int `json:"a,string"`
				}
				B struct {
					B int `json:"b,string"`
				}
			}{A: struct {
				A int `json:"a,string"`
			}{A: -1}, B: struct {
				B int `json:"b,string"`
			}{B: 2}},
		},
		{
			name: "HeadIntMultiFieldsNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A int `json:"a,string,omitempty"`
				}
				B struct {
					B int `json:"b,string,omitempty"`
				}
			}{A: struct {
				A int `json:"a,string,omitempty"`
			}{A: -1}, B: struct {
				B int `json:"b,string,omitempty"`
			}{B: 2}},
		},

		// HeadIntPtrMultiFieldsNotRoot
		{
			name: "HeadIntPtrMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *int `json:"a"`
				}
				B struct {
					B *int `json:"b"`
				}
			}{A: struct {
				A *int `json:"a"`
			}{A: intptr(-1)}, B: struct {
				B *int `json:"b"`
			}{B: intptr(2)}},
		},
		{
			name: "HeadIntPtrMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *int `json:"a,omitempty"`
				}
				B struct {
					B *int `json:"b,omitempty"`
				}
			}{A: struct {
				A *int `json:"a,omitempty"`
			}{A: intptr(-1)}, B: struct {
				B *int `json:"b,omitempty"`
			}{B: intptr(2)}},
		},
		{
			name: "HeadIntPtrMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *int `json:"a,string"`
				}
				B struct {
					B *int `json:"b,string"`
				}
			}{A: struct {
				A *int `json:"a,string"`
			}{A: intptr(-1)}, B: struct {
				B *int `json:"b,string"`
			}{B: intptr(2)}},
		},
		{
			name: "HeadIntPtrMultiFieldsNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A *int `json:"a,string,omitempty"`
				}
				B struct {
					B *int `json:"b,string,omitempty"`
				}
			}{A: struct {
				A *int `json:"a,string,omitempty"`
			}{A: intptr(-1)}, B: struct {
				B *int `json:"b,string,omitempty"`
			}{B: intptr(2)}},
		},

		// HeadIntPtrNilMultiFieldsNotRoot
		{
			name: "HeadIntPtrNilMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *int `json:"a"`
				}
				B struct {
					B *int `json:"b"`
				}
			}{A: struct {
				A *int `json:"a"`
			}{A: nil}, B: struct {
				B *int `json:"b"`
			}{B: nil}},
		},
		{
			name: "HeadIntPtrNilMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *int `json:"a,omitempty"`
				}
				B struct {
					B *int `json:"b,omitempty"`
				}
			}{A: struct {
				A *int `json:"a,omitempty"`
			}{A: nil}, B: struct {
				B *int `json:"b,omitempty"`
			}{B: nil}},
		},
		{
			name: "HeadIntPtrNilMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *int `json:"a,string"`
				}
				B struct {
					B *int `json:"b,string"`
				}
			}{A: struct {
				A *int `json:"a,string"`
			}{A: nil}, B: struct {
				B *int `json:"b,string"`
			}{B: nil}},
		},
		{
			name: "HeadIntPtrNilMultiFieldsNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A *int `json:"a,string,omitempty"`
				}
				B struct {
					B *int `json:"b,string,omitempty"`
				}
			}{A: struct {
				A *int `json:"a,string,omitempty"`
			}{A: nil}, B: struct {
				B *int `json:"b,string,omitempty"`
			}{B: nil}},
		},

		// PtrHeadIntZeroMultiFieldsNotRoot
		{
			name: "PtrHeadIntZeroMultiFieldsNotRoot",
			data: &struct {
				A struct {
					A int `json:"a"`
				}
				B struct {
					B int `json:"b"`
				}
			}{},
		},
		{
			name: "PtrHeadIntZeroMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A int `json:"a,omitempty"`
				}
				B struct {
					B int `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "PtrHeadIntZeroMultiFieldsNotRootString",
			data: &struct {
				A struct {
					A int `json:"a,string"`
				}
				B struct {
					B int `json:"b,string"`
				}
			}{},
		},
		{
			name: "PtrHeadIntZeroMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A struct {
					A int `json:"a,string,omitempty"`
				}
				B struct {
					B int `json:"b,string,omitempty"`
				}
			}{},
		},

		// PtrHeadIntMultiFieldsNotRoot
		{
			name: "PtrHeadIntMultiFieldsNotRoot",
			data: &struct {
				A struct {
					A int `json:"a"`
				}
				B struct {
					B int `json:"b"`
				}
			}{A: struct {
				A int `json:"a"`
			}{A: -1}, B: struct {
				B int `json:"b"`
			}{B: 2}},
		},
		{
			name: "PtrHeadIntMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A int `json:"a,omitempty"`
				}
				B struct {
					B int `json:"b,omitempty"`
				}
			}{A: struct {
				A int `json:"a,omitempty"`
			}{A: -1}, B: struct {
				B int `json:"b,omitempty"`
			}{B: 2}},
		},
		{
			name: "PtrHeadIntMultiFieldsNotRootString",
			data: &struct {
				A struct {
					A int `json:"a,string"`
				}
				B struct {
					B int `json:"b,string"`
				}
			}{A: struct {
				A int `json:"a,string"`
			}{A: -1}, B: struct {
				B int `json:"b,string"`
			}{B: 2}},
		},
		{
			name: "PtrHeadIntMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A struct {
					A int `json:"a,string,omitempty"`
				}
				B struct {
					B int `json:"b,string,omitempty"`
				}
			}{A: struct {
				A int `json:"a,string,omitempty"`
			}{A: -1}, B: struct {
				B int `json:"b,string,omitempty"`
			}{B: 2}},
		},

		// PtrHeadIntPtrMultiFieldsNotRoot
		{
			name: "PtrHeadIntPtrMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *int `json:"a"`
				}
				B *struct {
					B *int `json:"b"`
				}
			}{A: &(struct {
				A *int `json:"a"`
			}{A: intptr(-1)}), B: &(struct {
				B *int `json:"b"`
			}{B: intptr(2)})},
		},
		{
			name: "PtrHeadIntPtrMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *int `json:"a,omitempty"`
				}
				B *struct {
					B *int `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *int `json:"a,omitempty"`
			}{A: intptr(-1)}), B: &(struct {
				B *int `json:"b,omitempty"`
			}{B: intptr(2)})},
		},
		{
			name: "PtrHeadIntPtrMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *int `json:"a,string"`
				}
				B *struct {
					B *int `json:"b,string"`
				}
			}{A: &(struct {
				A *int `json:"a,string"`
			}{A: intptr(-1)}), B: &(struct {
				B *int `json:"b,string"`
			}{B: intptr(2)})},
		},
		{
			name: "PtrHeadIntPtrMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A *struct {
					A *int `json:"a,string,omitempty"`
				}
				B *struct {
					B *int `json:"b,string,omitempty"`
				}
			}{A: &(struct {
				A *int `json:"a,string,omitempty"`
			}{A: intptr(-1)}), B: &(struct {
				B *int `json:"b,string,omitempty"`
			}{B: intptr(2)})},
		},

		// PtrHeadIntPtrNilMultiFieldsNotRoot
		{
			name: "PtrHeadIntPtrNilMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *int `json:"a"`
				}
				B *struct {
					B *int `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadIntPtrNilMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *int `json:"a,omitempty"`
				} `json:",omitempty"`
				B *struct {
					B *int `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadIntPtrNilMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *int `json:"a,string"`
				} `json:",string"`
				B *struct {
					B *int `json:"b,string"`
				} `json:",string"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadIntPtrNilMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A *struct {
					A *int `json:"a,string,omitempty"`
				} `json:",string,omitempty"`
				B *struct {
					B *int `json:"b,string,omitempty"`
				} `json:",string,omitempty"`
			}{A: nil, B: nil},
		},

		// PtrHeadIntNilMultiFieldsNotRoot
		{
			name: "PtrHeadIntNilMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *int `json:"a"`
				}
				B *struct {
					B *int `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadIntNilMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *int `json:"a,omitempty"`
				}
				B *struct {
					B *int `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadIntNilMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *int `json:"a,string"`
				}
				B *struct {
					B *int `json:"b,string"`
				}
			})(nil),
		},
		{
			name: "PtrHeadIntNilMultiFieldsNotRootStringOmitEmpty",
			data: (*struct {
				A *struct {
					A *int `json:"a,string,omitempty"`
				}
				B *struct {
					B *int `json:"b,string,omitempty"`
				}
			})(nil),
		},

		// PtrHeadIntDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadIntDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A int `json:"a"`
					B int `json:"b"`
				}
				B *struct {
					A int `json:"a"`
					B int `json:"b"`
				}
			}{A: &(struct {
				A int `json:"a"`
				B int `json:"b"`
			}{A: -1, B: 2}), B: &(struct {
				A int `json:"a"`
				B int `json:"b"`
			}{A: 3, B: 4})},
		},
		{
			name: "PtrHeadIntDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A int `json:"a,omitempty"`
					B int `json:"b,omitempty"`
				}
				B *struct {
					A int `json:"a,omitempty"`
					B int `json:"b,omitempty"`
				}
			}{A: &(struct {
				A int `json:"a,omitempty"`
				B int `json:"b,omitempty"`
			}{A: -1, B: 2}), B: &(struct {
				A int `json:"a,omitempty"`
				B int `json:"b,omitempty"`
			}{A: 3, B: 4})},
		},
		{
			name: "PtrHeadIntDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A int `json:"a,string"`
					B int `json:"b,string"`
				}
				B *struct {
					A int `json:"a,string"`
					B int `json:"b,string"`
				}
			}{A: &(struct {
				A int `json:"a,string"`
				B int `json:"b,string"`
			}{A: -1, B: 2}), B: &(struct {
				A int `json:"a,string"`
				B int `json:"b,string"`
			}{A: 3, B: 4})},
		},
		{
			name: "PtrHeadIntDoubleMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A *struct {
					A int `json:"a,string,omitempty"`
					B int `json:"b,string,omitempty"`
				}
				B *struct {
					A int `json:"a,string,omitempty"`
					B int `json:"b,string,omitempty"`
				}
			}{A: &(struct {
				A int `json:"a,string,omitempty"`
				B int `json:"b,string,omitempty"`
			}{A: -1, B: 2}), B: &(struct {
				A int `json:"a,string,omitempty"`
				B int `json:"b,string,omitempty"`
			}{A: 3, B: 4})},
		},

		// PtrHeadIntNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadIntNilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A int `json:"a"`
					B int `json:"b"`
				}
				B *struct {
					A int `json:"a"`
					B int `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadIntNilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A int `json:"a,omitempty"`
					B int `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A int `json:"a,omitempty"`
					B int `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadIntNilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A int `json:"a,string"`
					B int `json:"b,string"`
				}
				B *struct {
					A int `json:"a,string"`
					B int `json:"b,string"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadIntNilDoubleMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A *struct {
					A int `json:"a,string,omitempty"`
					B int `json:"b,string,omitempty"`
				}
				B *struct {
					A int `json:"a,string,omitempty"`
					B int `json:"b,string,omitempty"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadIntNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadIntNilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A int `json:"a"`
					B int `json:"b"`
				}
				B *struct {
					A int `json:"a"`
					B int `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadIntNilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A int `json:"a,omitempty"`
					B int `json:"b,omitempty"`
				}
				B *struct {
					A int `json:"a,omitempty"`
					B int `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadIntNilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A int `json:"a,string"`
					B int `json:"b,string"`
				}
				B *struct {
					A int `json:"a,string"`
					B int `json:"b,string"`
				}
			})(nil),
		},
		{
			name: "PtrHeadIntNilDoubleMultiFieldsNotRootStringOmitEmpty",
			data: (*struct {
				A *struct {
					A int `json:"a,string,omitempty"`
					B int `json:"b,string,omitempty"`
				}
				B *struct {
					A int `json:"a,string,omitempty"`
					B int `json:"b,string,omitempty"`
				}
			})(nil),
		},

		// PtrHeadIntPtrDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadIntPtrDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *int `json:"a"`
					B *int `json:"b"`
				}
				B *struct {
					A *int `json:"a"`
					B *int `json:"b"`
				}
			}{A: &(struct {
				A *int `json:"a"`
				B *int `json:"b"`
			}{A: intptr(-1), B: intptr(2)}), B: &(struct {
				A *int `json:"a"`
				B *int `json:"b"`
			}{A: intptr(3), B: intptr(4)})},
		},
		{
			name: "PtrHeadIntPtrDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *int `json:"a,omitempty"`
					B *int `json:"b,omitempty"`
				}
				B *struct {
					A *int `json:"a,omitempty"`
					B *int `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *int `json:"a,omitempty"`
				B *int `json:"b,omitempty"`
			}{A: intptr(-1), B: intptr(2)}), B: &(struct {
				A *int `json:"a,omitempty"`
				B *int `json:"b,omitempty"`
			}{A: intptr(3), B: intptr(4)})},
		},
		{
			name: "PtrHeadIntPtrDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *int `json:"a,string"`
					B *int `json:"b,string"`
				}
				B *struct {
					A *int `json:"a,string"`
					B *int `json:"b,string"`
				}
			}{A: &(struct {
				A *int `json:"a,string"`
				B *int `json:"b,string"`
			}{A: intptr(-1), B: intptr(2)}), B: &(struct {
				A *int `json:"a,string"`
				B *int `json:"b,string"`
			}{A: intptr(3), B: intptr(4)})},
		},
		{
			name: "PtrHeadIntPtrDoubleMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A *struct {
					A *int `json:"a,string,omitempty"`
					B *int `json:"b,string,omitempty"`
				}
				B *struct {
					A *int `json:"a,string,omitempty"`
					B *int `json:"b,string,omitempty"`
				}
			}{A: &(struct {
				A *int `json:"a,string,omitempty"`
				B *int `json:"b,string,omitempty"`
			}{A: intptr(-1), B: intptr(2)}), B: &(struct {
				A *int `json:"a,string,omitempty"`
				B *int `json:"b,string,omitempty"`
			}{A: intptr(3), B: intptr(4)})},
		},

		// PtrHeadIntPtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadIntPtrNilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *int `json:"a"`
					B *int `json:"b"`
				}
				B *struct {
					A *int `json:"a"`
					B *int `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadIntPtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *int `json:"a,omitempty"`
					B *int `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A *int `json:"a,omitempty"`
					B *int `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadIntPtrNilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *int `json:"a,string"`
					B *int `json:"b,string"`
				}
				B *struct {
					A *int `json:"a,string"`
					B *int `json:"b,string"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadIntPtrNilDoubleMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A *struct {
					A *int `json:"a,string,omitempty"`
					B *int `json:"b,string,omitempty"`
				}
				B *struct {
					A *int `json:"a,string,omitempty"`
					B *int `json:"b,string,omitempty"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadIntPtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadIntPtrNilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *int `json:"a"`
					B *int `json:"b"`
				}
				B *struct {
					A *int `json:"a"`
					B *int `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadIntPtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *int `json:"a,omitempty"`
					B *int `json:"b,omitempty"`
				}
				B *struct {
					A *int `json:"a,omitempty"`
					B *int `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadIntPtrNilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *int `json:"a,string"`
					B *int `json:"b,string"`
				}
				B *struct {
					A *int `json:"a,string"`
					B *int `json:"b,string"`
				}
			})(nil),
		},
		{
			name: "PtrHeadIntPtrNilDoubleMultiFieldsNotRootStringOmitEmpty",
			data: (*struct {
				A *struct {
					A *int `json:"a,string,omitempty"`
					B *int `json:"b,string,omitempty"`
				}
				B *struct {
					A *int `json:"a,string,omitempty"`
					B *int `json:"b,string,omitempty"`
				}
			})(nil),
		},

		// AnonymousHeadInt
		{
			name: "AnonymousHeadInt",
			data: struct {
				structInt
				B int `json:"b"`
			}{
				structInt: structInt{A: -1},
				B:         2,
			},
		},
		{
			name: "AnonymousHeadIntOmitEmpty",
			data: struct {
				structIntOmitEmpty
				B int `json:"b,omitempty"`
			}{
				structIntOmitEmpty: structIntOmitEmpty{A: -1},
				B:                  2,
			},
		},
		{
			name: "AnonymousHeadIntString",
			data: struct {
				structIntString
				B int `json:"b,string"`
			}{
				structIntString: structIntString{A: -1},
				B:               2,
			},
		},
		{
			name: "AnonymousHeadIntStringOmitEmpty",
			data: struct {
				structIntStringOmitEmpty
				B int `json:"b,string,omitempty"`
			}{
				structIntStringOmitEmpty: structIntStringOmitEmpty{A: -1},
				B:                        2,
			},
		},

		// PtrAnonymousHeadInt
		{
			name: "PtrAnonymousHeadInt",
			data: struct {
				*structInt
				B int `json:"b"`
			}{
				structInt: &structInt{A: -1},
				B:         2,
			},
		},
		{
			name: "PtrAnonymousHeadIntOmitEmpty",
			data: struct {
				*structIntOmitEmpty
				B int `json:"b,omitempty"`
			}{
				structIntOmitEmpty: &structIntOmitEmpty{A: -1},
				B:                  2,
			},
		},
		{
			name: "PtrAnonymousHeadIntString",
			data: struct {
				*structIntString
				B int `json:"b,string"`
			}{
				structIntString: &structIntString{A: -1},
				B:               2,
			},
		},
		{
			name: "PtrAnonymousHeadIntStringOmitEmpty",
			data: struct {
				*structIntStringOmitEmpty
				B int `json:"b,string,omitempty"`
			}{
				structIntStringOmitEmpty: &structIntStringOmitEmpty{A: -1},
				B:                        2,
			},
		},

		// NilPtrAnonymousHeadInt
		{
			name: "NilPtrAnonymousHeadInt",
			data: struct {
				*structInt
				B int `json:"b"`
			}{
				structInt: nil,
				B:         2,
			},
		},
		{
			name: "NilPtrAnonymousHeadIntOmitEmpty",
			data: struct {
				*structIntOmitEmpty
				B int `json:"b,omitempty"`
			}{
				structIntOmitEmpty: nil,
				B:                  2,
			},
		},
		{
			name: "NilPtrAnonymousHeadIntString",
			data: struct {
				*structIntString
				B int `json:"b,string"`
			}{
				structIntString: nil,
				B:               2,
			},
		},
		{
			name: "NilPtrAnonymousHeadIntStringOmitEmpty",
			data: struct {
				*structIntStringOmitEmpty
				B int `json:"b,string,omitempty"`
			}{
				structIntStringOmitEmpty: nil,
				B:                        2,
			},
		},

		// AnonymousHeadIntPtr
		{
			name: "AnonymousHeadIntPtr",
			data: struct {
				structIntPtr
				B *int `json:"b"`
			}{
				structIntPtr: structIntPtr{A: intptr(-1)},
				B:            intptr(2),
			},
		},
		{
			name: "AnonymousHeadIntPtrOmitEmpty",
			data: struct {
				structIntPtrOmitEmpty
				B *int `json:"b,omitempty"`
			}{
				structIntPtrOmitEmpty: structIntPtrOmitEmpty{A: intptr(-1)},
				B:                     intptr(2),
			},
		},
		{
			name: "AnonymousHeadIntPtrString",
			data: struct {
				structIntPtrString
				B *int `json:"b,string"`
			}{
				structIntPtrString: structIntPtrString{A: intptr(-1)},
				B:                  intptr(2),
			},
		},
		{
			name: "AnonymousHeadIntPtrStringOmitEmpty",
			data: struct {
				structIntPtrStringOmitEmpty
				B *int `json:"b,string,omitempty"`
			}{
				structIntPtrStringOmitEmpty: structIntPtrStringOmitEmpty{A: intptr(-1)},
				B:                           intptr(2),
			},
		},

		// AnonymousHeadIntPtrNil
		{
			name: "AnonymousHeadIntPtrNil",
			data: struct {
				structIntPtr
				B *int `json:"b"`
			}{
				structIntPtr: structIntPtr{A: nil},
				B:            intptr(2),
			},
		},
		{
			name: "AnonymousHeadIntPtrNilOmitEmpty",
			data: struct {
				structIntPtrOmitEmpty
				B *int `json:"b,omitempty"`
			}{
				structIntPtrOmitEmpty: structIntPtrOmitEmpty{A: nil},
				B:                     intptr(2),
			},
		},
		{
			name: "AnonymousHeadIntPtrNilString",
			data: struct {
				structIntPtrString
				B *int `json:"b,string"`
			}{
				structIntPtrString: structIntPtrString{A: nil},
				B:                  intptr(2),
			},
		},
		{
			name: "AnonymousHeadIntPtrNilStringOmitEmpty",
			data: struct {
				structIntPtrStringOmitEmpty
				B *int `json:"b,string,omitempty"`
			}{
				structIntPtrStringOmitEmpty: structIntPtrStringOmitEmpty{A: nil},
				B:                           intptr(2),
			},
		},

		// PtrAnonymousHeadIntPtr
		{
			name: "PtrAnonymousHeadIntPtr",
			data: struct {
				*structIntPtr
				B *int `json:"b"`
			}{
				structIntPtr: &structIntPtr{A: intptr(-1)},
				B:            intptr(2),
			},
		},
		{
			name: "PtrAnonymousHeadIntPtrOmitEmpty",
			data: struct {
				*structIntPtrOmitEmpty
				B *int `json:"b,omitempty"`
			}{
				structIntPtrOmitEmpty: &structIntPtrOmitEmpty{A: intptr(-1)},
				B:                     intptr(2),
			},
		},
		{
			name: "PtrAnonymousHeadIntPtrString",
			data: struct {
				*structIntPtrString
				B *int `json:"b,string"`
			}{
				structIntPtrString: &structIntPtrString{A: intptr(-1)},
				B:                  intptr(2),
			},
		},
		{
			name: "PtrAnonymousHeadIntPtrStringOmitEmpty",
			data: struct {
				*structIntPtrStringOmitEmpty
				B *int `json:"b,string,omitempty"`
			}{
				structIntPtrStringOmitEmpty: &structIntPtrStringOmitEmpty{A: intptr(-1)},
				B:                           intptr(2),
			},
		},

		// NilPtrAnonymousHeadIntPtr
		{
			name: "NilPtrAnonymousHeadIntPtr",
			data: struct {
				*structIntPtr
				B *int `json:"b"`
			}{
				structIntPtr: nil,
				B:            intptr(2),
			},
		},
		{
			name: "NilPtrAnonymousHeadIntPtrOmitEmpty",
			data: struct {
				*structIntPtrOmitEmpty
				B *int `json:"b,omitempty"`
			}{
				structIntPtrOmitEmpty: nil,
				B:                     intptr(2),
			},
		},
		{
			name: "NilPtrAnonymousHeadIntPtrString",
			data: struct {
				*structIntPtrString
				B *int `json:"b,string"`
			}{
				structIntPtrString: nil,
				B:                  intptr(2),
			},
		},
		{
			name: "NilPtrAnonymousHeadIntPtrStringOmitEmpty",
			data: struct {
				*structIntPtrStringOmitEmpty
				B *int `json:"b,string,omitempty"`
			}{
				structIntPtrStringOmitEmpty: nil,
				B:                           intptr(2),
			},
		},

		// AnonymousHeadIntOnly
		{
			name: "AnonymousHeadIntOnly",
			data: struct {
				structInt
			}{
				structInt: structInt{A: -1},
			},
		},
		{
			name: "AnonymousHeadIntOnlyOmitEmpty",
			data: struct {
				structIntOmitEmpty
			}{
				structIntOmitEmpty: structIntOmitEmpty{A: -1},
			},
		},
		{
			name: "AnonymousHeadIntOnlyString",
			data: struct {
				structIntString
			}{
				structIntString: structIntString{A: -1},
			},
		},
		{
			name: "AnonymousHeadIntOnlyStringOmitEmpty",
			data: struct {
				structIntStringOmitEmpty
			}{
				structIntStringOmitEmpty: structIntStringOmitEmpty{A: -1},
			},
		},

		// PtrAnonymousHeadIntOnly
		{
			name: "PtrAnonymousHeadIntOnly",
			data: struct {
				*structInt
			}{
				structInt: &structInt{A: -1},
			},
		},
		{
			name: "PtrAnonymousHeadIntOnlyOmitEmpty",
			data: struct {
				*structIntOmitEmpty
			}{
				structIntOmitEmpty: &structIntOmitEmpty{A: -1},
			},
		},
		{
			name: "PtrAnonymousHeadIntOnlyString",
			data: struct {
				*structIntString
			}{
				structIntString: &structIntString{A: -1},
			},
		},
		{
			name: "PtrAnonymousHeadIntOnlyStringOmitEmpty",
			data: struct {
				*structIntStringOmitEmpty
			}{
				structIntStringOmitEmpty: &structIntStringOmitEmpty{A: -1},
			},
		},

		// NilPtrAnonymousHeadIntOnly
		{
			name: "NilPtrAnonymousHeadIntOnly",
			data: struct {
				*structInt
			}{
				structInt: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadIntOnlyOmitEmpty",
			data: struct {
				*structIntOmitEmpty
			}{
				structIntOmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadIntOnlyString",
			data: struct {
				*structIntString
			}{
				structIntString: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadIntOnlyStringOmitEmpty",
			data: struct {
				*structIntStringOmitEmpty
			}{
				structIntStringOmitEmpty: nil,
			},
		},

		// AnonymousHeadIntPtrOnly
		{
			name: "AnonymousHeadIntPtrOnly",
			data: struct {
				structIntPtr
			}{
				structIntPtr: structIntPtr{A: intptr(-1)},
			},
		},
		{
			name: "AnonymousHeadIntPtrOnlyOmitEmpty",
			data: struct {
				structIntPtrOmitEmpty
			}{
				structIntPtrOmitEmpty: structIntPtrOmitEmpty{A: intptr(-1)},
			},
		},
		{
			name: "AnonymousHeadIntPtrOnlyString",
			data: struct {
				structIntPtrString
			}{
				structIntPtrString: structIntPtrString{A: intptr(-1)},
			},
		},
		{
			name: "AnonymousHeadIntPtrOnlyStringOmitEmpty",
			data: struct {
				structIntPtrStringOmitEmpty
			}{
				structIntPtrStringOmitEmpty: structIntPtrStringOmitEmpty{A: intptr(-1)},
			},
		},

		// AnonymousHeadIntPtrNilOnly
		{
			name: "AnonymousHeadIntPtrNilOnly",
			data: struct {
				structIntPtr
			}{
				structIntPtr: structIntPtr{A: nil},
			},
		},
		{
			name: "AnonymousHeadIntPtrNilOnlyOmitEmpty",
			data: struct {
				structIntPtrOmitEmpty
			}{
				structIntPtrOmitEmpty: structIntPtrOmitEmpty{A: nil},
			},
		},
		{
			name: "AnonymousHeadIntPtrNilOnlyString",
			data: struct {
				structIntPtrString
			}{
				structIntPtrString: structIntPtrString{A: nil},
			},
		},
		{
			name: "AnonymousHeadIntPtrNilOnlyStringOmitEmpty",
			data: struct {
				structIntPtrStringOmitEmpty
			}{
				structIntPtrStringOmitEmpty: structIntPtrStringOmitEmpty{A: nil},
			},
		},

		// PtrAnonymousHeadIntPtrOnly
		{
			name: "PtrAnonymousHeadIntPtrOnly",
			data: struct {
				*structIntPtr
			}{
				structIntPtr: &structIntPtr{A: intptr(-1)},
			},
		},
		{
			name: "PtrAnonymousHeadIntPtrOnlyOmitEmpty",
			data: struct {
				*structIntPtrOmitEmpty
			}{
				structIntPtrOmitEmpty: &structIntPtrOmitEmpty{A: intptr(-1)},
			},
		},
		{
			name: "PtrAnonymousHeadIntPtrOnlyString",
			data: struct {
				*structIntPtrString
			}{
				structIntPtrString: &structIntPtrString{A: intptr(-1)},
			},
		},
		{
			name: "PtrAnonymousHeadIntPtrOnlyStringOmitEmpty",
			data: struct {
				*structIntPtrStringOmitEmpty
			}{
				structIntPtrStringOmitEmpty: &structIntPtrStringOmitEmpty{A: intptr(-1)},
			},
		},

		// NilPtrAnonymousHeadIntPtrOnly
		{
			name: "NilPtrAnonymousHeadIntPtrOnly",
			data: struct {
				*structIntPtr
			}{
				structIntPtr: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadIntPtrOnlyOmitEmpty",
			data: struct {
				*structIntPtrOmitEmpty
			}{
				structIntPtrOmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadIntPtrOnlyString",
			data: struct {
				*structIntPtrString
			}{
				structIntPtrString: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadIntPtrOnlyStringOmitEmpty",
			data: struct {
				*structIntPtrStringOmitEmpty
			}{
				structIntPtrStringOmitEmpty: nil,
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
