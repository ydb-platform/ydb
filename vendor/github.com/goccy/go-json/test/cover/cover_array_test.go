package json_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/goccy/go-json"
)

func TestCoverArray(t *testing.T) {
	type structArray struct {
		A [2]int `json:"a"`
	}
	type structArrayOmitEmpty struct {
		A [2]int `json:"a,omitempty"`
	}
	type structArrayString struct {
		A [2]int `json:"a,string"`
	}
	type structArrayPtr struct {
		A *[2]int `json:"a"`
	}
	type structArrayPtrOmitEmpty struct {
		A *[2]int `json:"a,omitempty"`
	}
	type structArrayPtrString struct {
		A *[2]int `json:"a,string"`
	}

	type structArrayPtrContent struct {
		A [2]*int `json:"a"`
	}
	type structArrayOmitEmptyPtrContent struct {
		A [2]*int `json:"a,omitempty"`
	}
	type structArrayStringPtrContent struct {
		A [2]*int `json:"a,string"`
	}
	type structArrayPtrPtrContent struct {
		A *[2]*int `json:"a"`
	}
	type structArrayPtrOmitEmptyPtrContent struct {
		A *[2]*int `json:"a,omitempty"`
	}
	type structArrayPtrStringPtrContent struct {
		A *[2]*int `json:"a,string"`
	}

	tests := []struct {
		name string
		data interface{}
	}{
		// HeadArrayZero
		{
			name: "HeadArrayZero",
			data: struct {
				A [2]int `json:"a"`
			}{},
		},
		{
			name: "HeadArrayZeroOmitEmpty",
			data: struct {
				A [2]int `json:"a,omitempty"`
			}{},
		},
		{
			name: "HeadArrayZeroString",
			data: struct {
				A [2]int `json:"a,string"`
			}{},
		},

		// HeadArray
		{
			name: "HeadArray",
			data: struct {
				A [2]int `json:"a"`
			}{A: [2]int{-1}},
		},
		{
			name: "HeadArrayOmitEmpty",
			data: struct {
				A [2]int `json:"a,omitempty"`
			}{A: [2]int{-1}},
		},
		{
			name: "HeadArrayString",
			data: struct {
				A [2]int `json:"a,string"`
			}{A: [2]int{-1}},
		},

		// HeadArrayPtr
		{
			name: "HeadArrayPtr",
			data: struct {
				A *[2]int `json:"a"`
			}{A: arrayptr([2]int{-1})},
		},
		{
			name: "HeadArrayPtrOmitEmpty",
			data: struct {
				A *[2]int `json:"a,omitempty"`
			}{A: arrayptr([2]int{-1})},
		},
		{
			name: "HeadArrayPtrString",
			data: struct {
				A *[2]int `json:"a,string"`
			}{A: arrayptr([2]int{-1})},
		},

		// HeadArrayPtrNil
		{
			name: "HeadArrayPtrNil",
			data: struct {
				A *[2]int `json:"a"`
			}{A: nil},
		},
		{
			name: "HeadArrayPtrNilOmitEmpty",
			data: struct {
				A *[2]int `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "HeadArrayPtrNilString",
			data: struct {
				A *[2]int `json:"a,string"`
			}{A: nil},
		},

		// PtrHeadArrayZero
		{
			name: "PtrHeadArrayZero",
			data: &struct {
				A [2]int `json:"a"`
			}{},
		},
		{
			name: "PtrHeadArrayZeroOmitEmpty",
			data: &struct {
				A [2]int `json:"a,omitempty"`
			}{},
		},
		{
			name: "PtrHeadArrayZeroString",
			data: &struct {
				A [2]int `json:"a,string"`
			}{},
		},

		// PtrHeadArray
		{
			name: "PtrHeadArray",
			data: &struct {
				A [2]int `json:"a"`
			}{A: [2]int{-1}},
		},
		{
			name: "PtrHeadArrayOmitEmpty",
			data: &struct {
				A [2]int `json:"a,omitempty"`
			}{A: [2]int{-1}},
		},
		{
			name: "PtrHeadArrayString",
			data: &struct {
				A [2]int `json:"a,string"`
			}{A: [2]int{-1}},
		},

		// PtrHeadArrayPtr
		{
			name: "PtrHeadArrayPtr",
			data: &struct {
				A *[2]int `json:"a"`
			}{A: arrayptr([2]int{-1})},
		},
		{
			name: "PtrHeadArrayPtrOmitEmpty",
			data: &struct {
				A *[2]int `json:"a,omitempty"`
			}{A: arrayptr([2]int{-1})},
		},
		{
			name: "PtrHeadArrayPtrString",
			data: &struct {
				A *[2]int `json:"a,string"`
			}{A: arrayptr([2]int{-1})},
		},

		// PtrHeadArrayPtrNil
		{
			name: "PtrHeadArrayPtrNil",
			data: &struct {
				A *[2]int `json:"a"`
			}{A: nil},
		},
		{
			name: "PtrHeadArrayPtrNilOmitEmpty",
			data: &struct {
				A *[2]int `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadArrayPtrNilString",
			data: &struct {
				A *[2]int `json:"a,string"`
			}{A: nil},
		},

		// PtrHeadArrayNil
		{
			name: "PtrHeadArrayNil",
			data: (*struct {
				A *[2]int `json:"a"`
			})(nil),
		},
		{
			name: "PtrHeadArrayNilOmitEmpty",
			data: (*struct {
				A *[2]int `json:"a,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadArrayNilString",
			data: (*struct {
				A *[2]int `json:"a,string"`
			})(nil),
		},

		// HeadArrayZeroMultiFields
		{
			name: "HeadArrayZeroMultiFields",
			data: struct {
				A [2]int `json:"a"`
				B [2]int `json:"b"`
				C [2]int `json:"c"`
			}{},
		},
		{
			name: "HeadArrayZeroMultiFieldsOmitEmpty",
			data: struct {
				A [2]int `json:"a,omitempty"`
				B [2]int `json:"b,omitempty"`
				C [2]int `json:"c,omitempty"`
			}{},
		},
		{
			name: "HeadArrayZeroMultiFields",
			data: struct {
				A [2]int `json:"a,string"`
				B [2]int `json:"b,string"`
				C [2]int `json:"c,string"`
			}{},
		},

		// HeadArrayMultiFields
		{
			name: "HeadArrayMultiFields",
			data: struct {
				A [2]int `json:"a"`
				B [2]int `json:"b"`
				C [2]int `json:"c"`
			}{A: [2]int{-1}, B: [2]int{-2}, C: [2]int{-3}},
		},
		{
			name: "HeadArrayMultiFieldsOmitEmpty",
			data: struct {
				A [2]int `json:"a,omitempty"`
				B [2]int `json:"b,omitempty"`
				C [2]int `json:"c,omitempty"`
			}{A: [2]int{-1}, B: [2]int{-2}, C: [2]int{-3}},
		},
		{
			name: "HeadArrayMultiFieldsString",
			data: struct {
				A [2]int `json:"a,string"`
				B [2]int `json:"b,string"`
				C [2]int `json:"c,string"`
			}{A: [2]int{-1}, B: [2]int{-2}, C: [2]int{-3}},
		},

		// HeadArrayPtrMultiFields
		{
			name: "HeadArrayPtrMultiFields",
			data: struct {
				A *[2]int `json:"a"`
				B *[2]int `json:"b"`
				C *[2]int `json:"c"`
			}{A: arrayptr([2]int{-1}), B: arrayptr([2]int{-2}), C: arrayptr([2]int{-3})},
		},
		{
			name: "HeadArrayPtrMultiFieldsOmitEmpty",
			data: struct {
				A *[2]int `json:"a,omitempty"`
				B *[2]int `json:"b,omitempty"`
				C *[2]int `json:"c,omitempty"`
			}{A: arrayptr([2]int{-1}), B: arrayptr([2]int{-2}), C: arrayptr([2]int{-3})},
		},
		{
			name: "HeadArrayPtrMultiFieldsString",
			data: struct {
				A *[2]int `json:"a,string"`
				B *[2]int `json:"b,string"`
				C *[2]int `json:"c,string"`
			}{A: arrayptr([2]int{-1}), B: arrayptr([2]int{-2}), C: arrayptr([2]int{-3})},
		},

		// HeadArrayPtrNilMultiFields
		{
			name: "HeadArrayPtrNilMultiFields",
			data: struct {
				A *[2]int `json:"a"`
				B *[2]int `json:"b"`
				C *[2]int `json:"c"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadArrayPtrNilMultiFieldsOmitEmpty",
			data: struct {
				A *[2]int `json:"a,omitempty"`
				B *[2]int `json:"b,omitempty"`
				C *[2]int `json:"c,omitempty"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadArrayPtrNilMultiFieldsString",
			data: struct {
				A *[2]int `json:"a,string"`
				B *[2]int `json:"b,string"`
				C *[2]int `json:"c,string"`
			}{A: nil, B: nil, C: nil},
		},

		// PtrHeadArrayZeroMultiFields
		{
			name: "PtrHeadArrayZeroMultiFields",
			data: &struct {
				A [2]int `json:"a"`
				B [2]int `json:"b"`
			}{},
		},
		{
			name: "PtrHeadArrayZeroMultiFieldsOmitEmpty",
			data: &struct {
				A [2]int `json:"a,omitempty"`
				B [2]int `json:"b,omitempty"`
			}{},
		},
		{
			name: "PtrHeadArrayZeroMultiFieldsString",
			data: &struct {
				A [2]int `json:"a,string"`
				B [2]int `json:"b,string"`
			}{},
		},

		// PtrHeadArrayMultiFields
		{
			name: "PtrHeadArrayMultiFields",
			data: &struct {
				A [2]int `json:"a"`
				B [2]int `json:"b"`
			}{A: [2]int{-1}, B: [2]int{1}},
		},
		{
			name: "PtrHeadArrayMultiFieldsOmitEmpty",
			data: &struct {
				A [2]int `json:"a,omitempty"`
				B [2]int `json:"b,omitempty"`
			}{A: [2]int{-1}, B: [2]int{1}},
		},
		{
			name: "PtrHeadArrayMultiFieldsString",
			data: &struct {
				A [2]int `json:"a,string"`
				B [2]int `json:"b,string"`
			}{A: [2]int{-1}, B: [2]int{1}},
		},

		// PtrHeadArrayPtrMultiFields
		{
			name: "PtrHeadArrayPtrMultiFields",
			data: &struct {
				A *[2]int `json:"a"`
				B *[2]int `json:"b"`
			}{A: arrayptr([2]int{-1}), B: arrayptr([2]int{-2})},
		},
		{
			name: "PtrHeadArrayPtrMultiFieldsOmitEmpty",
			data: &struct {
				A *[2]int `json:"a,omitempty"`
				B *[2]int `json:"b,omitempty"`
			}{A: arrayptr([2]int{-1}), B: arrayptr([2]int{-2})},
		},
		{
			name: "PtrHeadArrayPtrMultiFieldsString",
			data: &struct {
				A *[2]int `json:"a,string"`
				B *[2]int `json:"b,string"`
			}{A: arrayptr([2]int{-1}), B: arrayptr([2]int{-2})},
		},

		// PtrHeadArrayPtrNilMultiFields
		{
			name: "PtrHeadArrayPtrNilMultiFields",
			data: &struct {
				A *[2]int `json:"a"`
				B *[2]int `json:"b"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadArrayPtrNilMultiFieldsOmitEmpty",
			data: &struct {
				A *[2]int `json:"a,omitempty"`
				B *[2]int `json:"b,omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadArrayPtrNilMultiFieldsString",
			data: &struct {
				A *[2]int `json:"a,string"`
				B *[2]int `json:"b,string"`
			}{A: nil, B: nil},
		},

		// PtrHeadArrayNilMultiFields
		{
			name: "PtrHeadArrayNilMultiFields",
			data: (*struct {
				A [2]int `json:"a"`
				B [2]int `json:"b"`
			})(nil),
		},
		{
			name: "PtrHeadArrayNilMultiFieldsOmitEmpty",
			data: (*struct {
				A [2]int `json:"a,omitempty"`
				B [2]int `json:"b,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadArrayNilMultiFieldsString",
			data: (*struct {
				A [2]int `json:"a,string"`
				B [2]int `json:"b,string"`
			})(nil),
		},

		// PtrHeadArrayNilMultiFields
		{
			name: "PtrHeadArrayNilMultiFields",
			data: (*struct {
				A *[2]int `json:"a"`
				B *[2]int `json:"b"`
			})(nil),
		},
		{
			name: "PtrHeadArrayNilMultiFieldsOmitEmpty",
			data: (*struct {
				A *[2]int `json:"a,omitempty"`
				B *[2]int `json:"b,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadArrayNilMultiFieldsString",
			data: (*struct {
				A *[2]int `json:"a,string"`
				B *[2]int `json:"b,string"`
			})(nil),
		},

		// HeadArrayZeroNotRoot
		{
			name: "HeadArrayZeroNotRoot",
			data: struct {
				A struct {
					A [2]int `json:"a"`
				}
			}{},
		},
		{
			name: "HeadArrayZeroNotRootOmitEmpty",
			data: struct {
				A struct {
					A [2]int `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadArrayZeroNotRootString",
			data: struct {
				A struct {
					A [2]int `json:"a,string"`
				}
			}{},
		},

		// HeadArrayNotRoot
		{
			name: "HeadArrayNotRoot",
			data: struct {
				A struct {
					A [2]int `json:"a"`
				}
			}{A: struct {
				A [2]int `json:"a"`
			}{A: [2]int{-1}}},
		},
		{
			name: "HeadArrayNotRootOmitEmpty",
			data: struct {
				A struct {
					A [2]int `json:"a,omitempty"`
				}
			}{A: struct {
				A [2]int `json:"a,omitempty"`
			}{A: [2]int{-1}}},
		},
		{
			name: "HeadArrayNotRootString",
			data: struct {
				A struct {
					A [2]int `json:"a,string"`
				}
			}{A: struct {
				A [2]int `json:"a,string"`
			}{A: [2]int{-1}}},
		},

		// HeadArrayPtrNotRoot
		{
			name: "HeadArrayPtrNotRoot",
			data: struct {
				A struct {
					A *[2]int `json:"a"`
				}
			}{A: struct {
				A *[2]int `json:"a"`
			}{arrayptr([2]int{-1})}},
		},
		{
			name: "HeadArrayPtrNotRootOmitEmpty",
			data: struct {
				A struct {
					A *[2]int `json:"a,omitempty"`
				}
			}{A: struct {
				A *[2]int `json:"a,omitempty"`
			}{arrayptr([2]int{-1})}},
		},
		{
			name: "HeadArrayPtrNotRootString",
			data: struct {
				A struct {
					A *[2]int `json:"a,string"`
				}
			}{A: struct {
				A *[2]int `json:"a,string"`
			}{arrayptr([2]int{-1})}},
		},

		// HeadArrayPtrNilNotRoot
		{
			name: "HeadArrayPtrNilNotRoot",
			data: struct {
				A struct {
					A *[2]int `json:"a"`
				}
			}{},
		},
		{
			name: "HeadArrayPtrNilNotRootOmitEmpty",
			data: struct {
				A struct {
					A *[2]int `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadArrayPtrNilNotRootString",
			data: struct {
				A struct {
					A *[2]int `json:"a,string"`
				}
			}{},
		},

		// PtrHeadArrayZeroNotRoot
		{
			name: "PtrHeadArrayZeroNotRoot",
			data: struct {
				A *struct {
					A [2]int `json:"a"`
				}
			}{A: new(struct {
				A [2]int `json:"a"`
			})},
		},
		{
			name: "PtrHeadArrayZeroNotRootOmitEmpty",
			data: struct {
				A *struct {
					A [2]int `json:"a,omitempty"`
				}
			}{A: new(struct {
				A [2]int `json:"a,omitempty"`
			})},
		},
		{
			name: "PtrHeadArrayZeroNotRootString",
			data: struct {
				A *struct {
					A [2]int `json:"a,string"`
				}
			}{A: new(struct {
				A [2]int `json:"a,string"`
			})},
		},

		// PtrHeadArrayNotRoot
		{
			name: "PtrHeadArrayNotRoot",
			data: struct {
				A *struct {
					A [2]int `json:"a"`
				}
			}{A: &(struct {
				A [2]int `json:"a"`
			}{A: [2]int{-1}})},
		},
		{
			name: "PtrHeadArrayNotRootOmitEmpty",
			data: struct {
				A *struct {
					A [2]int `json:"a,omitempty"`
				}
			}{A: &(struct {
				A [2]int `json:"a,omitempty"`
			}{A: [2]int{-1}})},
		},
		{
			name: "PtrHeadArrayNotRootString",
			data: struct {
				A *struct {
					A [2]int `json:"a,string"`
				}
			}{A: &(struct {
				A [2]int `json:"a,string"`
			}{A: [2]int{-1}})},
		},

		// PtrHeadArrayPtrNotRoot
		{
			name: "PtrHeadArrayPtrNotRoot",
			data: struct {
				A *struct {
					A *[2]int `json:"a"`
				}
			}{A: &(struct {
				A *[2]int `json:"a"`
			}{A: arrayptr([2]int{-1})})},
		},
		{
			name: "PtrHeadArrayPtrNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *[2]int `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *[2]int `json:"a,omitempty"`
			}{A: arrayptr([2]int{-1})})},
		},
		{
			name: "PtrHeadArrayPtrNotRootString",
			data: struct {
				A *struct {
					A *[2]int `json:"a,string"`
				}
			}{A: &(struct {
				A *[2]int `json:"a,string"`
			}{A: arrayptr([2]int{-1})})},
		},

		// PtrHeadArrayPtrNilNotRoot
		{
			name: "PtrHeadArrayPtrNilNotRoot",
			data: struct {
				A *struct {
					A *[2]int `json:"a"`
				}
			}{A: &(struct {
				A *[2]int `json:"a"`
			}{A: nil})},
		},
		{
			name: "PtrHeadArrayPtrNilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *[2]int `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *[2]int `json:"a,omitempty"`
			}{A: nil})},
		},
		{
			name: "PtrHeadArrayPtrNilNotRootString",
			data: struct {
				A *struct {
					A *[2]int `json:"a,string"`
				}
			}{A: &(struct {
				A *[2]int `json:"a,string"`
			}{A: nil})},
		},

		// PtrHeadArrayNilNotRoot
		{
			name: "PtrHeadArrayNilNotRoot",
			data: struct {
				A *struct {
					A *[2]int `json:"a"`
				}
			}{A: nil},
		},
		{
			name: "PtrHeadArrayNilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *[2]int `json:"a,omitempty"`
				} `json:",omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadArrayNilNotRootString",
			data: struct {
				A *struct {
					A *[2]int `json:"a,string"`
				} `json:",string"`
			}{A: nil},
		},

		// HeadArrayZeroMultiFieldsNotRoot
		{
			name: "HeadArrayZeroMultiFieldsNotRoot",
			data: struct {
				A struct {
					A [2]int `json:"a"`
				}
				B struct {
					B [2]int `json:"b"`
				}
			}{},
		},
		{
			name: "HeadArrayZeroMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A [2]int `json:"a,omitempty"`
				}
				B struct {
					B [2]int `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "HeadArrayZeroMultiFieldsNotRootString",
			data: struct {
				A struct {
					A [2]int `json:"a,string"`
				}
				B struct {
					B [2]int `json:"b,string"`
				}
			}{},
		},

		// HeadArrayMultiFieldsNotRoot
		{
			name: "HeadArrayMultiFieldsNotRoot",
			data: struct {
				A struct {
					A [2]int `json:"a"`
				}
				B struct {
					B [2]int `json:"b"`
				}
			}{A: struct {
				A [2]int `json:"a"`
			}{A: [2]int{-1}}, B: struct {
				B [2]int `json:"b"`
			}{B: [2]int{0}}},
		},
		{
			name: "HeadArrayMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A [2]int `json:"a,omitempty"`
				}
				B struct {
					B [2]int `json:"b,omitempty"`
				}
			}{A: struct {
				A [2]int `json:"a,omitempty"`
			}{A: [2]int{-1}}, B: struct {
				B [2]int `json:"b,omitempty"`
			}{B: [2]int{1}}},
		},
		{
			name: "HeadArrayMultiFieldsNotRootString",
			data: struct {
				A struct {
					A [2]int `json:"a,string"`
				}
				B struct {
					B [2]int `json:"b,string"`
				}
			}{A: struct {
				A [2]int `json:"a,string"`
			}{A: [2]int{-1}}, B: struct {
				B [2]int `json:"b,string"`
			}{B: [2]int{1}}},
		},

		// HeadArrayPtrMultiFieldsNotRoot
		{
			name: "HeadArrayPtrMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *[2]int `json:"a"`
				}
				B struct {
					B *[2]int `json:"b"`
				}
			}{A: struct {
				A *[2]int `json:"a"`
			}{A: arrayptr([2]int{-1})}, B: struct {
				B *[2]int `json:"b"`
			}{B: arrayptr([2]int{1})}},
		},
		{
			name: "HeadArrayPtrMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *[2]int `json:"a,omitempty"`
				}
				B struct {
					B *[2]int `json:"b,omitempty"`
				}
			}{A: struct {
				A *[2]int `json:"a,omitempty"`
			}{A: arrayptr([2]int{-1})}, B: struct {
				B *[2]int `json:"b,omitempty"`
			}{B: arrayptr([2]int{1})}},
		},
		{
			name: "HeadArrayPtrMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *[2]int `json:"a,string"`
				}
				B struct {
					B *[2]int `json:"b,string"`
				}
			}{A: struct {
				A *[2]int `json:"a,string"`
			}{A: arrayptr([2]int{-1})}, B: struct {
				B *[2]int `json:"b,string"`
			}{B: arrayptr([2]int{1})}},
		},

		// HeadArrayPtrNilMultiFieldsNotRoot
		{
			name: "HeadArrayPtrNilMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *[2]int `json:"a"`
				}
				B struct {
					B *[2]int `json:"b"`
				}
			}{A: struct {
				A *[2]int `json:"a"`
			}{A: nil}, B: struct {
				B *[2]int `json:"b"`
			}{B: nil}},
		},
		{
			name: "HeadArrayPtrNilMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *[2]int `json:"a,omitempty"`
				}
				B struct {
					B *[2]int `json:"b,omitempty"`
				}
			}{A: struct {
				A *[2]int `json:"a,omitempty"`
			}{A: nil}, B: struct {
				B *[2]int `json:"b,omitempty"`
			}{B: nil}},
		},
		{
			name: "HeadArrayPtrNilMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *[2]int `json:"a,string"`
				}
				B struct {
					B *[2]int `json:"b,string"`
				}
			}{A: struct {
				A *[2]int `json:"a,string"`
			}{A: nil}, B: struct {
				B *[2]int `json:"b,string"`
			}{B: nil}},
		},

		// PtrHeadArrayZeroMultiFieldsNotRoot
		{
			name: "PtrHeadArrayZeroMultiFieldsNotRoot",
			data: &struct {
				A struct {
					A [2]int `json:"a"`
				}
				B struct {
					B [2]int `json:"b"`
				}
			}{},
		},
		{
			name: "PtrHeadArrayZeroMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A [2]int `json:"a,omitempty"`
				}
				B struct {
					B [2]int `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "PtrHeadArrayZeroMultiFieldsNotRootString",
			data: &struct {
				A struct {
					A [2]int `json:"a,string"`
				}
				B struct {
					B [2]int `json:"b,string"`
				}
			}{},
		},

		// PtrHeadArrayMultiFieldsNotRoot
		{
			name: "PtrHeadArrayMultiFieldsNotRoot",
			data: &struct {
				A struct {
					A [2]int `json:"a"`
				}
				B struct {
					B [2]int `json:"b"`
				}
			}{A: struct {
				A [2]int `json:"a"`
			}{A: [2]int{-1}}, B: struct {
				B [2]int `json:"b"`
			}{B: [2]int{1}}},
		},
		{
			name: "PtrHeadArrayMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A [2]int `json:"a,omitempty"`
				}
				B struct {
					B [2]int `json:"b,omitempty"`
				}
			}{A: struct {
				A [2]int `json:"a,omitempty"`
			}{A: [2]int{-1}}, B: struct {
				B [2]int `json:"b,omitempty"`
			}{B: [2]int{1}}},
		},
		{
			name: "PtrHeadArrayMultiFieldsNotRootString",
			data: &struct {
				A struct {
					A [2]int `json:"a,string"`
				}
				B struct {
					B [2]int `json:"b,string"`
				}
			}{A: struct {
				A [2]int `json:"a,string"`
			}{A: [2]int{-1}}, B: struct {
				B [2]int `json:"b,string"`
			}{B: [2]int{1}}},
		},

		// PtrHeadArrayPtrMultiFieldsNotRoot
		{
			name: "PtrHeadArrayPtrMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *[2]int `json:"a"`
				}
				B *struct {
					B *[2]int `json:"b"`
				}
			}{A: &(struct {
				A *[2]int `json:"a"`
			}{A: arrayptr([2]int{-1})}), B: &(struct {
				B *[2]int `json:"b"`
			}{B: arrayptr([2]int{1})})},
		},
		{
			name: "PtrHeadArrayPtrMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *[2]int `json:"a,omitempty"`
				}
				B *struct {
					B *[2]int `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *[2]int `json:"a,omitempty"`
			}{A: arrayptr([2]int{-1})}), B: &(struct {
				B *[2]int `json:"b,omitempty"`
			}{B: arrayptr([2]int{1})})},
		},
		{
			name: "PtrHeadArrayPtrMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *[2]int `json:"a,string"`
				}
				B *struct {
					B *[2]int `json:"b,string"`
				}
			}{A: &(struct {
				A *[2]int `json:"a,string"`
			}{A: arrayptr([2]int{-1})}), B: &(struct {
				B *[2]int `json:"b,string"`
			}{B: arrayptr([2]int{1})})},
		},

		// PtrHeadArrayPtrNilMultiFieldsNotRoot
		{
			name: "PtrHeadArrayPtrNilMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *[2]int `json:"a"`
				}
				B *struct {
					B *[2]int `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadArrayPtrNilMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *[2]int `json:"a,omitempty"`
				} `json:",omitempty"`
				B *struct {
					B *[2]int `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadArrayPtrNilMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *[2]int `json:"a,string"`
				} `json:",string"`
				B *struct {
					B *[2]int `json:"b,string"`
				} `json:",string"`
			}{A: nil, B: nil},
		},

		// PtrHeadArrayNilMultiFieldsNotRoot
		{
			name: "PtrHeadArrayNilMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *[2]int `json:"a"`
				}
				B *struct {
					B *[2]int `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadArrayNilMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *[2]int `json:"a,omitempty"`
				}
				B *struct {
					B *[2]int `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadArrayNilMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *[2]int `json:"a,string"`
				}
				B *struct {
					B *[2]int `json:"b,string"`
				}
			})(nil),
		},

		// PtrHeadArrayDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadArrayDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A [2]int `json:"a"`
					B [2]int `json:"b"`
				}
				B *struct {
					A [2]int `json:"a"`
					B [2]int `json:"b"`
				}
			}{A: &(struct {
				A [2]int `json:"a"`
				B [2]int `json:"b"`
			}{A: [2]int{-1}, B: [2]int{1}}), B: &(struct {
				A [2]int `json:"a"`
				B [2]int `json:"b"`
			}{A: [2]int{-1}, B: [2]int{1}})},
		},
		{
			name: "PtrHeadArrayDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A [2]int `json:"a,omitempty"`
					B [2]int `json:"b,omitempty"`
				}
				B *struct {
					A [2]int `json:"a,omitempty"`
					B [2]int `json:"b,omitempty"`
				}
			}{A: &(struct {
				A [2]int `json:"a,omitempty"`
				B [2]int `json:"b,omitempty"`
			}{A: [2]int{-1}, B: [2]int{1}}), B: &(struct {
				A [2]int `json:"a,omitempty"`
				B [2]int `json:"b,omitempty"`
			}{A: [2]int{-1}, B: [2]int{1}})},
		},
		{
			name: "PtrHeadArrayDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A [2]int `json:"a,string"`
					B [2]int `json:"b,string"`
				}
				B *struct {
					A [2]int `json:"a,string"`
					B [2]int `json:"b,string"`
				}
			}{A: &(struct {
				A [2]int `json:"a,string"`
				B [2]int `json:"b,string"`
			}{A: [2]int{-1}, B: [2]int{1}}), B: &(struct {
				A [2]int `json:"a,string"`
				B [2]int `json:"b,string"`
			}{A: [2]int{-1}, B: [2]int{1}})},
		},

		// PtrHeadArrayNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadArrayNilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A [2]int `json:"a"`
					B [2]int `json:"b"`
				}
				B *struct {
					A [2]int `json:"a"`
					B [2]int `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadArrayNilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A [2]int `json:"a,omitempty"`
					B [2]int `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A [2]int `json:"a,omitempty"`
					B [2]int `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadArrayNilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A [2]int `json:"a,string"`
					B [2]int `json:"b,string"`
				}
				B *struct {
					A [2]int `json:"a,string"`
					B [2]int `json:"b,string"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadArrayNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadArrayNilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A [2]int `json:"a"`
					B [2]int `json:"b"`
				}
				B *struct {
					A [2]int `json:"a"`
					B [2]int `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadArrayNilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A [2]int `json:"a,omitempty"`
					B [2]int `json:"b,omitempty"`
				}
				B *struct {
					A [2]int `json:"a,omitempty"`
					B [2]int `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadArrayNilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A [2]int `json:"a,string"`
					B [2]int `json:"b,string"`
				}
				B *struct {
					A [2]int `json:"a,string"`
					B [2]int `json:"b,string"`
				}
			})(nil),
		},

		// PtrHeadArrayPtrDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadArrayPtrDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *[2]int `json:"a"`
					B *[2]int `json:"b"`
				}
				B *struct {
					A *[2]int `json:"a"`
					B *[2]int `json:"b"`
				}
			}{A: &(struct {
				A *[2]int `json:"a"`
				B *[2]int `json:"b"`
			}{A: arrayptr([2]int{-1}), B: arrayptr([2]int{1})}), B: &(struct {
				A *[2]int `json:"a"`
				B *[2]int `json:"b"`
			}{A: nil, B: nil})},
		},
		{
			name: "PtrHeadArrayPtrDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *[2]int `json:"a,omitempty"`
					B *[2]int `json:"b,omitempty"`
				}
				B *struct {
					A *[2]int `json:"a,omitempty"`
					B *[2]int `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *[2]int `json:"a,omitempty"`
				B *[2]int `json:"b,omitempty"`
			}{A: arrayptr([2]int{-1}), B: arrayptr([2]int{1})}), B: &(struct {
				A *[2]int `json:"a,omitempty"`
				B *[2]int `json:"b,omitempty"`
			}{A: nil, B: nil})},
		},
		{
			name: "PtrHeadArrayPtrDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *[2]int `json:"a,string"`
					B *[2]int `json:"b,string"`
				}
				B *struct {
					A *[2]int `json:"a,string"`
					B *[2]int `json:"b,string"`
				}
			}{A: &(struct {
				A *[2]int `json:"a,string"`
				B *[2]int `json:"b,string"`
			}{A: arrayptr([2]int{-1}), B: arrayptr([2]int{1})}), B: &(struct {
				A *[2]int `json:"a,string"`
				B *[2]int `json:"b,string"`
			}{A: nil, B: nil})},
		},

		// PtrHeadArrayPtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadArrayPtrNilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *[2]int `json:"a"`
					B *[2]int `json:"b"`
				}
				B *struct {
					A *[2]int `json:"a"`
					B *[2]int `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadArrayPtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *[2]int `json:"a,omitempty"`
					B *[2]int `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A *[2]int `json:"a,omitempty"`
					B *[2]int `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadArrayPtrNilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *[2]int `json:"a,string"`
					B *[2]int `json:"b,string"`
				}
				B *struct {
					A *[2]int `json:"a,string"`
					B *[2]int `json:"b,string"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadArrayPtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadArrayPtrNilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *[2]int `json:"a"`
					B *[2]int `json:"b"`
				}
				B *struct {
					A *[2]int `json:"a"`
					B *[2]int `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadArrayPtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *[2]int `json:"a,omitempty"`
					B *[2]int `json:"b,omitempty"`
				}
				B *struct {
					A *[2]int `json:"a,omitempty"`
					B *[2]int `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadArrayPtrNilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *[2]int `json:"a,string"`
					B *[2]int `json:"b,string"`
				}
				B *struct {
					A *[2]int `json:"a,string"`
					B *[2]int `json:"b,string"`
				}
			})(nil),
		},

		// AnonymousHeadArray
		{
			name: "AnonymousHeadArray",
			data: struct {
				structArray
				B [2]int `json:"b"`
			}{
				structArray: structArray{A: [2]int{-1}},
				B:           [2]int{1},
			},
		},
		{
			name: "AnonymousHeadArrayOmitEmpty",
			data: struct {
				structArrayOmitEmpty
				B [2]int `json:"b,omitempty"`
			}{
				structArrayOmitEmpty: structArrayOmitEmpty{A: [2]int{-1}},
				B:                    [2]int{1},
			},
		},
		{
			name: "AnonymousHeadArrayString",
			data: struct {
				structArrayString
				B [2]int `json:"b,string"`
			}{
				structArrayString: structArrayString{A: [2]int{-1}},
				B:                 [2]int{1},
			},
		},

		// PtrAnonymousHeadArray
		{
			name: "PtrAnonymousHeadArray",
			data: struct {
				*structArray
				B [2]int `json:"b"`
			}{
				structArray: &structArray{A: [2]int{-1}},
				B:           [2]int{1},
			},
		},
		{
			name: "PtrAnonymousHeadArrayOmitEmpty",
			data: struct {
				*structArrayOmitEmpty
				B [2]int `json:"b,omitempty"`
			}{
				structArrayOmitEmpty: &structArrayOmitEmpty{A: [2]int{-1}},
				B:                    [2]int{1},
			},
		},
		{
			name: "PtrAnonymousHeadArrayString",
			data: struct {
				*structArrayString
				B [2]int `json:"b,string"`
			}{
				structArrayString: &structArrayString{A: [2]int{-1}},
				B:                 [2]int{1},
			},
		},

		// PtrAnonymousHeadArrayNil
		{
			name: "PtrAnonymousHeadArrayNil",
			data: struct {
				*structArray
				B [2]int `json:"b"`
			}{
				structArray: &structArray{A: [2]int{1}},
				B:           [2]int{1},
			},
		},
		{
			name: "PtrAnonymousHeadArrayNilOmitEmpty",
			data: struct {
				*structArrayOmitEmpty
				B [2]int `json:"b,omitempty"`
			}{
				structArrayOmitEmpty: &structArrayOmitEmpty{A: [2]int{1}},
				B:                    [2]int{1},
			},
		},
		{
			name: "PtrAnonymousHeadArrayNilString",
			data: struct {
				*structArrayString
				B [2]int `json:"b,string"`
			}{
				structArrayString: &structArrayString{A: [2]int{1}},
				B:                 [2]int{1},
			},
		},

		// NilPtrAnonymousHeadArray
		{
			name: "NilPtrAnonymousHeadArray",
			data: struct {
				*structArray
				B [2]int `json:"b"`
			}{
				structArray: nil,
				B:           [2]int{-1},
			},
		},
		{
			name: "NilPtrAnonymousHeadArrayOmitEmpty",
			data: struct {
				*structArrayOmitEmpty
				B [2]int `json:"b,omitempty"`
			}{
				structArrayOmitEmpty: nil,
				B:                    [2]int{-1},
			},
		},
		{
			name: "NilPtrAnonymousHeadArrayString",
			data: struct {
				*structArrayString
				B [2]int `json:"b,string"`
			}{
				structArrayString: nil,
				B:                 [2]int{-1},
			},
		},

		// AnonymousHeadArrayPtr
		{
			name: "AnonymousHeadArrayPtr",
			data: struct {
				structArrayPtr
				B *[2]int `json:"b"`
			}{
				structArrayPtr: structArrayPtr{A: arrayptr([2]int{-1})},
				B:              nil,
			},
		},
		{
			name: "AnonymousHeadArrayPtrOmitEmpty",
			data: struct {
				structArrayPtrOmitEmpty
				B *[2]int `json:"b,omitempty"`
			}{
				structArrayPtrOmitEmpty: structArrayPtrOmitEmpty{A: arrayptr([2]int{-1})},
				B:                       nil,
			},
		},
		{
			name: "AnonymousHeadArrayPtrString",
			data: struct {
				structArrayPtrString
				B *[2]int `json:"b,string"`
			}{
				structArrayPtrString: structArrayPtrString{A: arrayptr([2]int{-1})},
				B:                    nil,
			},
		},

		// AnonymousHeadArrayPtrNil
		{
			name: "AnonymousHeadArrayPtrNil",
			data: struct {
				structArrayPtr
				B *[2]int `json:"b"`
			}{
				structArrayPtr: structArrayPtr{A: nil},
				B:              arrayptr([2]int{-1}),
			},
		},
		{
			name: "AnonymousHeadArrayPtrNilOmitEmpty",
			data: struct {
				structArrayPtrOmitEmpty
				B *[2]int `json:"b,omitempty"`
			}{
				structArrayPtrOmitEmpty: structArrayPtrOmitEmpty{A: nil},
				B:                       arrayptr([2]int{-1}),
			},
		},
		{
			name: "AnonymousHeadArrayPtrNilString",
			data: struct {
				structArrayPtrString
				B *[2]int `json:"b,string"`
			}{
				structArrayPtrString: structArrayPtrString{A: nil},
				B:                    arrayptr([2]int{-1}),
			},
		},

		// PtrAnonymousHeadArrayPtr
		{
			name: "PtrAnonymousHeadArrayPtr",
			data: struct {
				*structArrayPtr
				B *[2]int `json:"b"`
			}{
				structArrayPtr: &structArrayPtr{A: arrayptr([2]int{-1})},
				B:              nil,
			},
		},
		{
			name: "PtrAnonymousHeadArrayPtrOmitEmpty",
			data: struct {
				*structArrayPtrOmitEmpty
				B *[2]int `json:"b,omitempty"`
			}{
				structArrayPtrOmitEmpty: &structArrayPtrOmitEmpty{A: arrayptr([2]int{-1})},
				B:                       nil,
			},
		},
		{
			name: "PtrAnonymousHeadArrayPtrString",
			data: struct {
				*structArrayPtrString
				B *[2]int `json:"b,string"`
			}{
				structArrayPtrString: &structArrayPtrString{A: arrayptr([2]int{-1})},
				B:                    nil,
			},
		},

		// NilPtrAnonymousHeadArrayPtr
		{
			name: "NilPtrAnonymousHeadArrayPtr",
			data: struct {
				*structArrayPtr
				B *[2]int `json:"b"`
			}{
				structArrayPtr: nil,
				B:              arrayptr([2]int{-1}),
			},
		},
		{
			name: "NilPtrAnonymousHeadArrayPtrOmitEmpty",
			data: struct {
				*structArrayPtrOmitEmpty
				B *[2]int `json:"b,omitempty"`
			}{
				structArrayPtrOmitEmpty: nil,
				B:                       arrayptr([2]int{-1}),
			},
		},
		{
			name: "NilPtrAnonymousHeadArrayPtrString",
			data: struct {
				*structArrayPtrString
				B *[2]int `json:"b,string"`
			}{
				structArrayPtrString: nil,
				B:                    arrayptr([2]int{-1}),
			},
		},

		// AnonymousHeadArrayOnly
		{
			name: "AnonymousHeadArrayOnly",
			data: struct {
				structArray
			}{
				structArray: structArray{A: [2]int{-1}},
			},
		},
		{
			name: "AnonymousHeadArrayOnlyOmitEmpty",
			data: struct {
				structArrayOmitEmpty
			}{
				structArrayOmitEmpty: structArrayOmitEmpty{A: [2]int{-1}},
			},
		},
		{
			name: "AnonymousHeadArrayOnlyString",
			data: struct {
				structArrayString
			}{
				structArrayString: structArrayString{A: [2]int{-1}},
			},
		},

		// PtrAnonymousHeadArrayOnly
		{
			name: "PtrAnonymousHeadArrayOnly",
			data: struct {
				*structArray
			}{
				structArray: &structArray{A: [2]int{-1}},
			},
		},
		{
			name: "PtrAnonymousHeadArrayOnlyOmitEmpty",
			data: struct {
				*structArrayOmitEmpty
			}{
				structArrayOmitEmpty: &structArrayOmitEmpty{A: [2]int{-1}},
			},
		},
		{
			name: "PtrAnonymousHeadArrayOnlyString",
			data: struct {
				*structArrayString
			}{
				structArrayString: &structArrayString{A: [2]int{-1}},
			},
		},

		// NilPtrAnonymousHeadArrayOnly
		{
			name: "NilPtrAnonymousHeadArrayOnly",
			data: struct {
				*structArray
			}{
				structArray: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadArrayOnlyOmitEmpty",
			data: struct {
				*structArrayOmitEmpty
			}{
				structArrayOmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadArrayOnlyString",
			data: struct {
				*structArrayString
			}{
				structArrayString: nil,
			},
		},

		// AnonymousHeadArrayPtrOnly
		{
			name: "AnonymousHeadArrayPtrOnly",
			data: struct {
				structArrayPtr
			}{
				structArrayPtr: structArrayPtr{A: arrayptr([2]int{-1})},
			},
		},
		{
			name: "AnonymousHeadArrayPtrOnlyOmitEmpty",
			data: struct {
				structArrayPtrOmitEmpty
			}{
				structArrayPtrOmitEmpty: structArrayPtrOmitEmpty{A: arrayptr([2]int{-1})},
			},
		},
		{
			name: "AnonymousHeadArrayPtrOnlyString",
			data: struct {
				structArrayPtrString
			}{
				structArrayPtrString: structArrayPtrString{A: arrayptr([2]int{-1})},
			},
		},

		// AnonymousHeadArrayPtrNilOnly
		{
			name: "AnonymousHeadArrayPtrNilOnly",
			data: struct {
				structArrayPtr
			}{
				structArrayPtr: structArrayPtr{A: nil},
			},
		},
		{
			name: "AnonymousHeadArrayPtrNilOnlyOmitEmpty",
			data: struct {
				structArrayPtrOmitEmpty
			}{
				structArrayPtrOmitEmpty: structArrayPtrOmitEmpty{A: nil},
			},
		},
		{
			name: "AnonymousHeadArrayPtrNilOnlyString",
			data: struct {
				structArrayPtrString
			}{
				structArrayPtrString: structArrayPtrString{A: nil},
			},
		},

		// PtrAnonymousHeadArrayPtrOnly
		{
			name: "PtrAnonymousHeadArrayPtrOnly",
			data: struct {
				*structArrayPtr
			}{
				structArrayPtr: &structArrayPtr{A: arrayptr([2]int{-1})},
			},
		},
		{
			name: "PtrAnonymousHeadArrayPtrOnlyOmitEmpty",
			data: struct {
				*structArrayPtrOmitEmpty
			}{
				structArrayPtrOmitEmpty: &structArrayPtrOmitEmpty{A: arrayptr([2]int{-1})},
			},
		},
		{
			name: "PtrAnonymousHeadArrayPtrOnlyString",
			data: struct {
				*structArrayPtrString
			}{
				structArrayPtrString: &structArrayPtrString{A: arrayptr([2]int{-1})},
			},
		},

		// NilPtrAnonymousHeadArrayPtrOnly
		{
			name: "NilPtrAnonymousHeadArrayPtrOnly",
			data: struct {
				*structArrayPtr
			}{
				structArrayPtr: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadArrayPtrOnlyOmitEmpty",
			data: struct {
				*structArrayPtrOmitEmpty
			}{
				structArrayPtrOmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadArrayPtrOnlyString",
			data: struct {
				*structArrayPtrString
			}{
				structArrayPtrString: nil,
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
