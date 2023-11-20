package json_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/goccy/go-json"
)

func TestCoverFloat64(t *testing.T) {
	type structFloat64 struct {
		A float64 `json:"a"`
	}
	type structFloat64OmitEmpty struct {
		A float64 `json:"a,omitempty"`
	}
	type structFloat64String struct {
		A float64 `json:"a,string"`
	}
	type structFloat64StringOmitEmpty struct {
		A float64 `json:"a,string,omitempty"`
	}

	type structFloat64Ptr struct {
		A *float64 `json:"a"`
	}
	type structFloat64PtrOmitEmpty struct {
		A *float64 `json:"a,omitempty"`
	}
	type structFloat64PtrString struct {
		A *float64 `json:"a,string"`
	}
	type structFloat64PtrStringOmitEmpty struct {
		A *float64 `json:"a,string,omitempty"`
	}

	tests := []struct {
		name string
		data interface{}
	}{
		{
			name: "Float64",
			data: float64(10),
		},
		{
			name: "Float64Ptr",
			data: float64ptr(10),
		},
		{
			name: "Float64Ptr3",
			data: float64ptr3(10),
		},
		{
			name: "Float64PtrNil",
			data: (*float64)(nil),
		},
		{
			name: "Float64Ptr3Nil",
			data: (***float64)(nil),
		},

		// HeadFloat64Zero
		{
			name: "HeadFloat64Zero",
			data: struct {
				A float64 `json:"a"`
			}{},
		},
		{
			name: "HeadFloat64ZeroOmitEmpty",
			data: struct {
				A float64 `json:"a,omitempty"`
			}{},
		},
		{
			name: "HeadFloat64ZeroString",
			data: struct {
				A float64 `json:"a,string"`
			}{},
		},
		{
			name: "HeadFloat64ZeroStringOmitEmpty",
			data: struct {
				A float64 `json:"a,string,omitempty"`
			}{},
		},

		// HeadFloat64
		{
			name: "HeadFloat64",
			data: struct {
				A float64 `json:"a"`
			}{A: 1},
		},
		{
			name: "HeadFloat64OmitEmpty",
			data: struct {
				A float64 `json:"a,omitempty"`
			}{A: 1},
		},
		{
			name: "HeadFloat64String",
			data: struct {
				A float64 `json:"a,string"`
			}{A: 1},
		},
		{
			name: "HeadFloat64StringOmitEmpty",
			data: struct {
				A float64 `json:"a,string,omitempty"`
			}{A: 1},
		},

		// HeadFloat64Ptr
		{
			name: "HeadFloat64Ptr",
			data: struct {
				A *float64 `json:"a"`
			}{A: float64ptr(1)},
		},
		{
			name: "HeadFloat64PtrOmitEmpty",
			data: struct {
				A *float64 `json:"a,omitempty"`
			}{A: float64ptr(1)},
		},
		{
			name: "HeadFloat64PtrString",
			data: struct {
				A *float64 `json:"a,string"`
			}{A: float64ptr(1)},
		},
		{
			name: "HeadFloat64PtrStringOmitEmpty",
			data: struct {
				A *float64 `json:"a,string,omitempty"`
			}{A: float64ptr(1)},
		},

		// HeadFloat64PtrNil
		{
			name: "HeadFloat64PtrNil",
			data: struct {
				A *float64 `json:"a"`
			}{A: nil},
		},
		{
			name: "HeadFloat64PtrNilOmitEmpty",
			data: struct {
				A *float64 `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "HeadFloat64PtrNilString",
			data: struct {
				A *float64 `json:"a,string"`
			}{A: nil},
		},
		{
			name: "HeadFloat64PtrNilStringOmitEmpty",
			data: struct {
				A *float64 `json:"a,string,omitempty"`
			}{A: nil},
		},

		// PtrHeadFloat64Zero
		{
			name: "PtrHeadFloat64Zero",
			data: &struct {
				A float64 `json:"a"`
			}{},
		},
		{
			name: "PtrHeadFloat64ZeroOmitEmpty",
			data: &struct {
				A float64 `json:"a,omitempty"`
			}{},
		},
		{
			name: "PtrHeadFloat64ZeroString",
			data: &struct {
				A float64 `json:"a,string"`
			}{},
		},
		{
			name: "PtrHeadFloat64ZeroStringOmitEmpty",
			data: &struct {
				A float64 `json:"a,string,omitempty"`
			}{},
		},

		// PtrHeadFloat64
		{
			name: "PtrHeadFloat64",
			data: &struct {
				A float64 `json:"a"`
			}{A: 1},
		},
		{
			name: "PtrHeadFloat64OmitEmpty",
			data: &struct {
				A float64 `json:"a,omitempty"`
			}{A: 1},
		},
		{
			name: "PtrHeadFloat64String",
			data: &struct {
				A float64 `json:"a,string"`
			}{A: 1},
		},
		{
			name: "PtrHeadFloat64StringOmitEmpty",
			data: &struct {
				A float64 `json:"a,string,omitempty"`
			}{A: 1},
		},

		// PtrHeadFloat64Ptr
		{
			name: "PtrHeadFloat64Ptr",
			data: &struct {
				A *float64 `json:"a"`
			}{A: float64ptr(1)},
		},
		{
			name: "PtrHeadFloat64PtrOmitEmpty",
			data: &struct {
				A *float64 `json:"a,omitempty"`
			}{A: float64ptr(1)},
		},
		{
			name: "PtrHeadFloat64PtrString",
			data: &struct {
				A *float64 `json:"a,string"`
			}{A: float64ptr(1)},
		},
		{
			name: "PtrHeadFloat64PtrStringOmitEmpty",
			data: &struct {
				A *float64 `json:"a,string,omitempty"`
			}{A: float64ptr(1)},
		},

		// PtrHeadFloat64PtrNil
		{
			name: "PtrHeadFloat64PtrNil",
			data: &struct {
				A *float64 `json:"a"`
			}{A: nil},
		},
		{
			name: "PtrHeadFloat64PtrNilOmitEmpty",
			data: &struct {
				A *float64 `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadFloat64PtrNilString",
			data: &struct {
				A *float64 `json:"a,string"`
			}{A: nil},
		},
		{
			name: "PtrHeadFloat64PtrNilStringOmitEmpty",
			data: &struct {
				A *float64 `json:"a,string,omitempty"`
			}{A: nil},
		},

		// PtrHeadFloat64Nil
		{
			name: "PtrHeadFloat64Nil",
			data: (*struct {
				A *float64 `json:"a"`
			})(nil),
		},
		{
			name: "PtrHeadFloat64NilOmitEmpty",
			data: (*struct {
				A *float64 `json:"a,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadFloat64NilString",
			data: (*struct {
				A *float64 `json:"a,string"`
			})(nil),
		},
		{
			name: "PtrHeadFloat64NilStringOmitEmpty",
			data: (*struct {
				A *float64 `json:"a,string,omitempty"`
			})(nil),
		},

		// HeadFloat64ZeroMultiFields
		{
			name: "HeadFloat64ZeroMultiFields",
			data: struct {
				A float64 `json:"a"`
				B float64 `json:"b"`
				C float64 `json:"c"`
			}{},
		},
		{
			name: "HeadFloat64ZeroMultiFieldsOmitEmpty",
			data: struct {
				A float64 `json:"a,omitempty"`
				B float64 `json:"b,omitempty"`
				C float64 `json:"c,omitempty"`
			}{},
		},
		{
			name: "HeadFloat64ZeroMultiFieldsString",
			data: struct {
				A float64 `json:"a,string"`
				B float64 `json:"b,string"`
				C float64 `json:"c,string"`
			}{},
		},
		{
			name: "HeadFloat64ZeroMultiFieldsStringOmitEmpty",
			data: struct {
				A float64 `json:"a,string,omitempty"`
				B float64 `json:"b,string,omitempty"`
				C float64 `json:"c,string,omitempty"`
			}{},
		},

		// HeadFloat64MultiFields
		{
			name: "HeadFloat64MultiFields",
			data: struct {
				A float64 `json:"a"`
				B float64 `json:"b"`
				C float64 `json:"c"`
			}{A: 1, B: 2, C: 3},
		},
		{
			name: "HeadFloat64MultiFieldsOmitEmpty",
			data: struct {
				A float64 `json:"a,omitempty"`
				B float64 `json:"b,omitempty"`
				C float64 `json:"c,omitempty"`
			}{A: 1, B: 2, C: 3},
		},
		{
			name: "HeadFloat64MultiFieldsString",
			data: struct {
				A float64 `json:"a,string"`
				B float64 `json:"b,string"`
				C float64 `json:"c,string"`
			}{A: 1, B: 2, C: 3},
		},
		{
			name: "HeadFloat64MultiFieldsStringOmitEmpty",
			data: struct {
				A float64 `json:"a,string,omitempty"`
				B float64 `json:"b,string,omitempty"`
				C float64 `json:"c,string,omitempty"`
			}{A: 1, B: 2, C: 3},
		},

		// HeadFloat64PtrMultiFields
		{
			name: "HeadFloat64PtrMultiFields",
			data: struct {
				A *float64 `json:"a"`
				B *float64 `json:"b"`
				C *float64 `json:"c"`
			}{A: float64ptr(1), B: float64ptr(2), C: float64ptr(3)},
		},
		{
			name: "HeadFloat64PtrMultiFieldsOmitEmpty",
			data: struct {
				A *float64 `json:"a,omitempty"`
				B *float64 `json:"b,omitempty"`
				C *float64 `json:"c,omitempty"`
			}{A: float64ptr(1), B: float64ptr(2), C: float64ptr(3)},
		},
		{
			name: "HeadFloat64PtrMultiFieldsString",
			data: struct {
				A *float64 `json:"a,string"`
				B *float64 `json:"b,string"`
				C *float64 `json:"c,string"`
			}{A: float64ptr(1), B: float64ptr(2), C: float64ptr(3)},
		},
		{
			name: "HeadFloat64PtrMultiFieldsStringOmitEmpty",
			data: struct {
				A *float64 `json:"a,string,omitempty"`
				B *float64 `json:"b,string,omitempty"`
				C *float64 `json:"c,string,omitempty"`
			}{A: float64ptr(1), B: float64ptr(2), C: float64ptr(3)},
		},

		// HeadFloat64PtrNilMultiFields
		{
			name: "HeadFloat64PtrNilMultiFields",
			data: struct {
				A *float64 `json:"a"`
				B *float64 `json:"b"`
				C *float64 `json:"c"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadFloat64PtrNilMultiFieldsOmitEmpty",
			data: struct {
				A *float64 `json:"a,omitempty"`
				B *float64 `json:"b,omitempty"`
				C *float64 `json:"c,omitempty"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadFloat64PtrNilMultiFieldsString",
			data: struct {
				A *float64 `json:"a,string"`
				B *float64 `json:"b,string"`
				C *float64 `json:"c,string"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadFloat64PtrNilMultiFieldsStringOmitEmpty",
			data: struct {
				A *float64 `json:"a,string,omitempty"`
				B *float64 `json:"b,string,omitempty"`
				C *float64 `json:"c,string,omitempty"`
			}{A: nil, B: nil, C: nil},
		},

		// PtrHeadFloat64ZeroMultiFields
		{
			name: "PtrHeadFloat64ZeroMultiFields",
			data: &struct {
				A float64 `json:"a"`
				B float64 `json:"b"`
			}{},
		},
		{
			name: "PtrHeadFloat64ZeroMultiFieldsOmitEmpty",
			data: &struct {
				A float64 `json:"a,omitempty"`
				B float64 `json:"b,omitempty"`
			}{},
		},
		{
			name: "PtrHeadFloat64ZeroMultiFieldsString",
			data: &struct {
				A float64 `json:"a,string"`
				B float64 `json:"b,string"`
			}{},
		},
		{
			name: "PtrHeadFloat64ZeroMultiFieldsStringOmitEmpty",
			data: &struct {
				A float64 `json:"a,string,omitempty"`
				B float64 `json:"b,string,omitempty"`
			}{},
		},

		// PtrHeadFloat64MultiFields
		{
			name: "PtrHeadFloat64MultiFields",
			data: &struct {
				A float64 `json:"a"`
				B float64 `json:"b"`
			}{A: 1, B: 2},
		},
		{
			name: "PtrHeadFloat64MultiFieldsOmitEmpty",
			data: &struct {
				A float64 `json:"a,omitempty"`
				B float64 `json:"b,omitempty"`
			}{A: 1, B: 2},
		},
		{
			name: "PtrHeadFloat64MultiFieldsString",
			data: &struct {
				A float64 `json:"a,string"`
				B float64 `json:"b,string"`
			}{A: 1, B: 2},
		},
		{
			name: "PtrHeadFloat64MultiFieldsStringOmitEmpty",
			data: &struct {
				A float64 `json:"a,string,omitempty"`
				B float64 `json:"b,string,omitempty"`
			}{A: 1, B: 2},
		},

		// PtrHeadFloat64PtrMultiFields
		{
			name: "PtrHeadFloat64PtrMultiFields",
			data: &struct {
				A *float64 `json:"a"`
				B *float64 `json:"b"`
			}{A: float64ptr(1), B: float64ptr(2)},
		},
		{
			name: "PtrHeadFloat64PtrMultiFieldsOmitEmpty",
			data: &struct {
				A *float64 `json:"a,omitempty"`
				B *float64 `json:"b,omitempty"`
			}{A: float64ptr(1), B: float64ptr(2)},
		},
		{
			name: "PtrHeadFloat64PtrMultiFieldsString",
			data: &struct {
				A *float64 `json:"a,string"`
				B *float64 `json:"b,string"`
			}{A: float64ptr(1), B: float64ptr(2)},
		},
		{
			name: "PtrHeadFloat64PtrMultiFieldsStringOmitEmpty",
			data: &struct {
				A *float64 `json:"a,string,omitempty"`
				B *float64 `json:"b,string,omitempty"`
			}{A: float64ptr(1), B: float64ptr(2)},
		},

		// PtrHeadFloat64PtrNilMultiFields
		{
			name: "PtrHeadFloat64PtrNilMultiFields",
			data: &struct {
				A *float64 `json:"a"`
				B *float64 `json:"b"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadFloat64PtrNilMultiFieldsOmitEmpty",
			data: &struct {
				A *float64 `json:"a,omitempty"`
				B *float64 `json:"b,omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadFloat64PtrNilMultiFieldsString",
			data: &struct {
				A *float64 `json:"a,string"`
				B *float64 `json:"b,string"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadFloat64PtrNilMultiFieldsStringOmitEmpty",
			data: &struct {
				A *float64 `json:"a,string,omitempty"`
				B *float64 `json:"b,string,omitempty"`
			}{A: nil, B: nil},
		},

		// PtrHeadFloat64NilMultiFields
		{
			name: "PtrHeadFloat64NilMultiFields",
			data: (*struct {
				A *float64 `json:"a"`
				B *float64 `json:"b"`
			})(nil),
		},
		{
			name: "PtrHeadFloat64NilMultiFieldsOmitEmpty",
			data: (*struct {
				A *float64 `json:"a,omitempty"`
				B *float64 `json:"b,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadFloat64NilMultiFieldsString",
			data: (*struct {
				A *float64 `json:"a,string"`
				B *float64 `json:"b,string"`
			})(nil),
		},
		{
			name: "PtrHeadFloat64NilMultiFieldsStringOmitEmpty",
			data: (*struct {
				A *float64 `json:"a,string,omitempty"`
				B *float64 `json:"b,string,omitempty"`
			})(nil),
		},

		// HeadFloat64ZeroNotRoot
		{
			name: "HeadFloat64ZeroNotRoot",
			data: struct {
				A struct {
					A float64 `json:"a"`
				}
			}{},
		},
		{
			name: "HeadFloat64ZeroNotRootOmitEmpty",
			data: struct {
				A struct {
					A float64 `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadFloat64ZeroNotRootString",
			data: struct {
				A struct {
					A float64 `json:"a,string"`
				}
			}{},
		},
		{
			name: "HeadFloat64ZeroNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A float64 `json:"a,string,omitempty"`
				}
			}{},
		},

		// HeadFloat64NotRoot
		{
			name: "HeadFloat64NotRoot",
			data: struct {
				A struct {
					A float64 `json:"a"`
				}
			}{A: struct {
				A float64 `json:"a"`
			}{A: 1}},
		},
		{
			name: "HeadFloat64NotRootOmitEmpty",
			data: struct {
				A struct {
					A float64 `json:"a,omitempty"`
				}
			}{A: struct {
				A float64 `json:"a,omitempty"`
			}{A: 1}},
		},
		{
			name: "HeadFloat64NotRootString",
			data: struct {
				A struct {
					A float64 `json:"a,string"`
				}
			}{A: struct {
				A float64 `json:"a,string"`
			}{A: 1}},
		},
		{
			name: "HeadFloat64NotRootStringOmitEmpty",
			data: struct {
				A struct {
					A float64 `json:"a,string,omitempty"`
				}
			}{A: struct {
				A float64 `json:"a,string,omitempty"`
			}{A: 1}},
		},

		// HeadFloat64PtrNotRoot
		{
			name: "HeadFloat64PtrNotRoot",
			data: struct {
				A struct {
					A *float64 `json:"a"`
				}
			}{A: struct {
				A *float64 `json:"a"`
			}{float64ptr(1)}},
		},
		{
			name: "HeadFloat64PtrNotRootOmitEmpty",
			data: struct {
				A struct {
					A *float64 `json:"a,omitempty"`
				}
			}{A: struct {
				A *float64 `json:"a,omitempty"`
			}{float64ptr(1)}},
		},
		{
			name: "HeadFloat64PtrNotRootString",
			data: struct {
				A struct {
					A *float64 `json:"a,string"`
				}
			}{A: struct {
				A *float64 `json:"a,string"`
			}{float64ptr(1)}},
		},
		{
			name: "HeadFloat64PtrNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A *float64 `json:"a,string,omitempty"`
				}
			}{A: struct {
				A *float64 `json:"a,string,omitempty"`
			}{float64ptr(1)}},
		},

		// HeadFloat64PtrNilNotRoot
		{
			name: "HeadFloat64PtrNilNotRoot",
			data: struct {
				A struct {
					A *float64 `json:"a"`
				}
			}{},
		},
		{
			name: "HeadFloat64PtrNilNotRootOmitEmpty",
			data: struct {
				A struct {
					A *float64 `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadFloat64PtrNilNotRootString",
			data: struct {
				A struct {
					A *float64 `json:"a,string"`
				}
			}{},
		},
		{
			name: "HeadFloat64PtrNilNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A *float64 `json:"a,string,omitempty"`
				}
			}{},
		},

		// PtrHeadFloat64ZeroNotRoot
		{
			name: "PtrHeadFloat64ZeroNotRoot",
			data: struct {
				A *struct {
					A float64 `json:"a"`
				}
			}{A: new(struct {
				A float64 `json:"a"`
			})},
		},
		{
			name: "PtrHeadFloat64ZeroNotRootOmitEmpty",
			data: struct {
				A *struct {
					A float64 `json:"a,omitempty"`
				}
			}{A: new(struct {
				A float64 `json:"a,omitempty"`
			})},
		},
		{
			name: "PtrHeadFloat64ZeroNotRootString",
			data: struct {
				A *struct {
					A float64 `json:"a,string"`
				}
			}{A: new(struct {
				A float64 `json:"a,string"`
			})},
		},
		{
			name: "PtrHeadFloat64ZeroNotRootStringOmitEmpty",
			data: struct {
				A *struct {
					A float64 `json:"a,string,omitempty"`
				}
			}{A: new(struct {
				A float64 `json:"a,string,omitempty"`
			})},
		},

		// PtrHeadFloat64NotRoot
		{
			name: "PtrHeadFloat64NotRoot",
			data: struct {
				A *struct {
					A float64 `json:"a"`
				}
			}{A: &(struct {
				A float64 `json:"a"`
			}{A: 1})},
		},
		{
			name: "PtrHeadFloat64NotRootOmitEmpty",
			data: struct {
				A *struct {
					A float64 `json:"a,omitempty"`
				}
			}{A: &(struct {
				A float64 `json:"a,omitempty"`
			}{A: 1})},
		},
		{
			name: "PtrHeadFloat64NotRootString",
			data: struct {
				A *struct {
					A float64 `json:"a,string"`
				}
			}{A: &(struct {
				A float64 `json:"a,string"`
			}{A: 1})},
		},
		{
			name: "PtrHeadFloat64NotRootStringOmitEmpty",
			data: struct {
				A *struct {
					A float64 `json:"a,string,omitempty"`
				}
			}{A: &(struct {
				A float64 `json:"a,string,omitempty"`
			}{A: 1})},
		},

		// PtrHeadFloat64PtrNotRoot
		{
			name: "PtrHeadFloat64PtrNotRoot",
			data: struct {
				A *struct {
					A *float64 `json:"a"`
				}
			}{A: &(struct {
				A *float64 `json:"a"`
			}{A: float64ptr(1)})},
		},
		{
			name: "PtrHeadFloat64PtrNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *float64 `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *float64 `json:"a,omitempty"`
			}{A: float64ptr(1)})},
		},
		{
			name: "PtrHeadFloat64PtrNotRootString",
			data: struct {
				A *struct {
					A *float64 `json:"a,string"`
				}
			}{A: &(struct {
				A *float64 `json:"a,string"`
			}{A: float64ptr(1)})},
		},
		{
			name: "PtrHeadFloat64PtrNotRootStringOmitEmpty",
			data: struct {
				A *struct {
					A *float64 `json:"a,string,omitempty"`
				}
			}{A: &(struct {
				A *float64 `json:"a,string,omitempty"`
			}{A: float64ptr(1)})},
		},

		// PtrHeadFloat64PtrNilNotRoot
		{
			name: "PtrHeadFloat64PtrNilNotRoot",
			data: struct {
				A *struct {
					A *float64 `json:"a"`
				}
			}{A: &(struct {
				A *float64 `json:"a"`
			}{A: nil})},
		},
		{
			name: "PtrHeadFloat64PtrNilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *float64 `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *float64 `json:"a,omitempty"`
			}{A: nil})},
		},
		{
			name: "PtrHeadFloat64PtrNilNotRootString",
			data: struct {
				A *struct {
					A *float64 `json:"a,string"`
				}
			}{A: &(struct {
				A *float64 `json:"a,string"`
			}{A: nil})},
		},
		{
			name: "PtrHeadFloat64PtrNilNotRootStringOmitEmpty",
			data: struct {
				A *struct {
					A *float64 `json:"a,string,omitempty"`
				}
			}{A: &(struct {
				A *float64 `json:"a,string,omitempty"`
			}{A: nil})},
		},

		// PtrHeadFloat64NilNotRoot
		{
			name: "PtrHeadFloat64NilNotRoot",
			data: struct {
				A *struct {
					A *float64 `json:"a"`
				}
			}{A: nil},
		},
		{
			name: "PtrHeadFloat64NilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *float64 `json:"a,omitempty"`
				} `json:",omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadFloat64NilNotRootString",
			data: struct {
				A *struct {
					A *float64 `json:"a,string"`
				} `json:",string"`
			}{A: nil},
		},
		{
			name: "PtrHeadFloat64NilNotRootStringOmitEmpty",
			data: struct {
				A *struct {
					A *float64 `json:"a,string,omitempty"`
				} `json:",string,omitempty"`
			}{A: nil},
		},

		// HeadFloat64ZeroMultiFieldsNotRoot
		{
			name: "HeadFloat64ZeroMultiFieldsNotRoot",
			data: struct {
				A struct {
					A float64 `json:"a"`
				}
				B struct {
					B float64 `json:"b"`
				}
			}{},
		},
		{
			name: "HeadFloat64ZeroMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A float64 `json:"a,omitempty"`
				}
				B struct {
					B float64 `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "HeadFloat64ZeroMultiFieldsNotRootString",
			data: struct {
				A struct {
					A float64 `json:"a,string"`
				}
				B struct {
					B float64 `json:"b,string"`
				}
			}{},
		},
		{
			name: "HeadFloat64ZeroMultiFieldsNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A float64 `json:"a,string,omitempty"`
				}
				B struct {
					B float64 `json:"b,string,omitempty"`
				}
			}{},
		},

		// HeadFloat64MultiFieldsNotRoot
		{
			name: "HeadFloat64MultiFieldsNotRoot",
			data: struct {
				A struct {
					A float64 `json:"a"`
				}
				B struct {
					B float64 `json:"b"`
				}
			}{A: struct {
				A float64 `json:"a"`
			}{A: 1}, B: struct {
				B float64 `json:"b"`
			}{B: 2}},
		},
		{
			name: "HeadFloat64MultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A float64 `json:"a,omitempty"`
				}
				B struct {
					B float64 `json:"b,omitempty"`
				}
			}{A: struct {
				A float64 `json:"a,omitempty"`
			}{A: 1}, B: struct {
				B float64 `json:"b,omitempty"`
			}{B: 2}},
		},
		{
			name: "HeadFloat64MultiFieldsNotRootString",
			data: struct {
				A struct {
					A float64 `json:"a,string"`
				}
				B struct {
					B float64 `json:"b,string"`
				}
			}{A: struct {
				A float64 `json:"a,string"`
			}{A: 1}, B: struct {
				B float64 `json:"b,string"`
			}{B: 2}},
		},
		{
			name: "HeadFloat64MultiFieldsNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A float64 `json:"a,string,omitempty"`
				}
				B struct {
					B float64 `json:"b,string,omitempty"`
				}
			}{A: struct {
				A float64 `json:"a,string,omitempty"`
			}{A: 1}, B: struct {
				B float64 `json:"b,string,omitempty"`
			}{B: 2}},
		},

		// HeadFloat64PtrMultiFieldsNotRoot
		{
			name: "HeadFloat64PtrMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *float64 `json:"a"`
				}
				B struct {
					B *float64 `json:"b"`
				}
			}{A: struct {
				A *float64 `json:"a"`
			}{A: float64ptr(1)}, B: struct {
				B *float64 `json:"b"`
			}{B: float64ptr(2)}},
		},
		{
			name: "HeadFloat64PtrMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *float64 `json:"a,omitempty"`
				}
				B struct {
					B *float64 `json:"b,omitempty"`
				}
			}{A: struct {
				A *float64 `json:"a,omitempty"`
			}{A: float64ptr(1)}, B: struct {
				B *float64 `json:"b,omitempty"`
			}{B: float64ptr(2)}},
		},
		{
			name: "HeadFloat64PtrMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *float64 `json:"a,string"`
				}
				B struct {
					B *float64 `json:"b,string"`
				}
			}{A: struct {
				A *float64 `json:"a,string"`
			}{A: float64ptr(1)}, B: struct {
				B *float64 `json:"b,string"`
			}{B: float64ptr(2)}},
		},
		{
			name: "HeadFloat64PtrMultiFieldsNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A *float64 `json:"a,string,omitempty"`
				}
				B struct {
					B *float64 `json:"b,string,omitempty"`
				}
			}{A: struct {
				A *float64 `json:"a,string,omitempty"`
			}{A: float64ptr(1)}, B: struct {
				B *float64 `json:"b,string,omitempty"`
			}{B: float64ptr(2)}},
		},

		// HeadFloat64PtrNilMultiFieldsNotRoot
		{
			name: "HeadFloat64PtrNilMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *float64 `json:"a"`
				}
				B struct {
					B *float64 `json:"b"`
				}
			}{A: struct {
				A *float64 `json:"a"`
			}{A: nil}, B: struct {
				B *float64 `json:"b"`
			}{B: nil}},
		},
		{
			name: "HeadFloat64PtrNilMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *float64 `json:"a,omitempty"`
				}
				B struct {
					B *float64 `json:"b,omitempty"`
				}
			}{A: struct {
				A *float64 `json:"a,omitempty"`
			}{A: nil}, B: struct {
				B *float64 `json:"b,omitempty"`
			}{B: nil}},
		},
		{
			name: "HeadFloat64PtrNilMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *float64 `json:"a,string"`
				}
				B struct {
					B *float64 `json:"b,string"`
				}
			}{A: struct {
				A *float64 `json:"a,string"`
			}{A: nil}, B: struct {
				B *float64 `json:"b,string"`
			}{B: nil}},
		},
		{
			name: "HeadFloat64PtrNilMultiFieldsNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A *float64 `json:"a,string,omitempty"`
				}
				B struct {
					B *float64 `json:"b,string,omitempty"`
				}
			}{A: struct {
				A *float64 `json:"a,string,omitempty"`
			}{A: nil}, B: struct {
				B *float64 `json:"b,string,omitempty"`
			}{B: nil}},
		},

		// PtrHeadFloat64ZeroMultiFieldsNotRoot
		{
			name: "PtrHeadFloat64ZeroMultiFieldsNotRoot",
			data: &struct {
				A struct {
					A float64 `json:"a"`
				}
				B struct {
					B float64 `json:"b"`
				}
			}{},
		},
		{
			name: "PtrHeadFloat64ZeroMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A float64 `json:"a,omitempty"`
				}
				B struct {
					B float64 `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "PtrHeadFloat64ZeroMultiFieldsNotRootString",
			data: &struct {
				A struct {
					A float64 `json:"a,string"`
				}
				B struct {
					B float64 `json:"b,string"`
				}
			}{},
		},
		{
			name: "PtrHeadFloat64ZeroMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A struct {
					A float64 `json:"a,string,omitempty"`
				}
				B struct {
					B float64 `json:"b,string,omitempty"`
				}
			}{},
		},

		// PtrHeadFloat64MultiFieldsNotRoot
		{
			name: "PtrHeadFloat64MultiFieldsNotRoot",
			data: &struct {
				A struct {
					A float64 `json:"a"`
				}
				B struct {
					B float64 `json:"b"`
				}
			}{A: struct {
				A float64 `json:"a"`
			}{A: 1}, B: struct {
				B float64 `json:"b"`
			}{B: 2}},
		},
		{
			name: "PtrHeadFloat64MultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A float64 `json:"a,omitempty"`
				}
				B struct {
					B float64 `json:"b,omitempty"`
				}
			}{A: struct {
				A float64 `json:"a,omitempty"`
			}{A: 1}, B: struct {
				B float64 `json:"b,omitempty"`
			}{B: 2}},
		},
		{
			name: "PtrHeadFloat64MultiFieldsNotRootString",
			data: &struct {
				A struct {
					A float64 `json:"a,string"`
				}
				B struct {
					B float64 `json:"b,string"`
				}
			}{A: struct {
				A float64 `json:"a,string"`
			}{A: 1}, B: struct {
				B float64 `json:"b,string"`
			}{B: 2}},
		},
		{
			name: "PtrHeadFloat64MultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A struct {
					A float64 `json:"a,string,omitempty"`
				}
				B struct {
					B float64 `json:"b,string,omitempty"`
				}
			}{A: struct {
				A float64 `json:"a,string,omitempty"`
			}{A: 1}, B: struct {
				B float64 `json:"b,string,omitempty"`
			}{B: 2}},
		},

		// PtrHeadFloat64PtrMultiFieldsNotRoot
		{
			name: "PtrHeadFloat64PtrMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *float64 `json:"a"`
				}
				B *struct {
					B *float64 `json:"b"`
				}
			}{A: &(struct {
				A *float64 `json:"a"`
			}{A: float64ptr(1)}), B: &(struct {
				B *float64 `json:"b"`
			}{B: float64ptr(2)})},
		},
		{
			name: "PtrHeadFloat64PtrMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *float64 `json:"a,omitempty"`
				}
				B *struct {
					B *float64 `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *float64 `json:"a,omitempty"`
			}{A: float64ptr(1)}), B: &(struct {
				B *float64 `json:"b,omitempty"`
			}{B: float64ptr(2)})},
		},
		{
			name: "PtrHeadFloat64PtrMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *float64 `json:"a,string"`
				}
				B *struct {
					B *float64 `json:"b,string"`
				}
			}{A: &(struct {
				A *float64 `json:"a,string"`
			}{A: float64ptr(1)}), B: &(struct {
				B *float64 `json:"b,string"`
			}{B: float64ptr(2)})},
		},
		{
			name: "PtrHeadFloat64PtrMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A *struct {
					A *float64 `json:"a,string,omitempty"`
				}
				B *struct {
					B *float64 `json:"b,string,omitempty"`
				}
			}{A: &(struct {
				A *float64 `json:"a,string,omitempty"`
			}{A: float64ptr(1)}), B: &(struct {
				B *float64 `json:"b,string,omitempty"`
			}{B: float64ptr(2)})},
		},

		// PtrHeadFloat64PtrNilMultiFieldsNotRoot
		{
			name: "PtrHeadFloat64PtrNilMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *float64 `json:"a"`
				}
				B *struct {
					B *float64 `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadFloat64PtrNilMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *float64 `json:"a,omitempty"`
				} `json:",omitempty"`
				B *struct {
					B *float64 `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadFloat64PtrNilMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *float64 `json:"a,string"`
				} `json:",string"`
				B *struct {
					B *float64 `json:"b,string"`
				} `json:",string"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadFloat64PtrNilMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A *struct {
					A *float64 `json:"a,string,omitempty"`
				} `json:",string,omitempty"`
				B *struct {
					B *float64 `json:"b,string,omitempty"`
				} `json:",string,omitempty"`
			}{A: nil, B: nil},
		},

		// PtrHeadFloat64NilMultiFieldsNotRoot
		{
			name: "PtrHeadFloat64NilMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *float64 `json:"a"`
				}
				B *struct {
					B *float64 `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadFloat64NilMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *float64 `json:"a,omitempty"`
				}
				B *struct {
					B *float64 `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadFloat64NilMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *float64 `json:"a,string"`
				}
				B *struct {
					B *float64 `json:"b,string"`
				}
			})(nil),
		},
		{
			name: "PtrHeadFloat64NilMultiFieldsNotRootStringOmitEmpty",
			data: (*struct {
				A *struct {
					A *float64 `json:"a,string,omitempty"`
				}
				B *struct {
					B *float64 `json:"b,string,omitempty"`
				}
			})(nil),
		},

		// PtrHeadFloat64DoubleMultiFieldsNotRoot
		{
			name: "PtrHeadFloat64DoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A float64 `json:"a"`
					B float64 `json:"b"`
				}
				B *struct {
					A float64 `json:"a"`
					B float64 `json:"b"`
				}
			}{A: &(struct {
				A float64 `json:"a"`
				B float64 `json:"b"`
			}{A: 1, B: 2}), B: &(struct {
				A float64 `json:"a"`
				B float64 `json:"b"`
			}{A: 3, B: 4})},
		},
		{
			name: "PtrHeadFloat64DoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A float64 `json:"a,omitempty"`
					B float64 `json:"b,omitempty"`
				}
				B *struct {
					A float64 `json:"a,omitempty"`
					B float64 `json:"b,omitempty"`
				}
			}{A: &(struct {
				A float64 `json:"a,omitempty"`
				B float64 `json:"b,omitempty"`
			}{A: 1, B: 2}), B: &(struct {
				A float64 `json:"a,omitempty"`
				B float64 `json:"b,omitempty"`
			}{A: 3, B: 4})},
		},
		{
			name: "PtrHeadFloat64DoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A float64 `json:"a,string"`
					B float64 `json:"b,string"`
				}
				B *struct {
					A float64 `json:"a,string"`
					B float64 `json:"b,string"`
				}
			}{A: &(struct {
				A float64 `json:"a,string"`
				B float64 `json:"b,string"`
			}{A: 1, B: 2}), B: &(struct {
				A float64 `json:"a,string"`
				B float64 `json:"b,string"`
			}{A: 3, B: 4})},
		},
		{
			name: "PtrHeadFloat64DoubleMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A *struct {
					A float64 `json:"a,string,omitempty"`
					B float64 `json:"b,string,omitempty"`
				}
				B *struct {
					A float64 `json:"a,string,omitempty"`
					B float64 `json:"b,string,omitempty"`
				}
			}{A: &(struct {
				A float64 `json:"a,string,omitempty"`
				B float64 `json:"b,string,omitempty"`
			}{A: 1, B: 2}), B: &(struct {
				A float64 `json:"a,string,omitempty"`
				B float64 `json:"b,string,omitempty"`
			}{A: 3, B: 4})},
		},

		// PtrHeadFloat64NilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadFloat64NilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A float64 `json:"a"`
					B float64 `json:"b"`
				}
				B *struct {
					A float64 `json:"a"`
					B float64 `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadFloat64NilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A float64 `json:"a,omitempty"`
					B float64 `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A float64 `json:"a,omitempty"`
					B float64 `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadFloat64NilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A float64 `json:"a,string"`
					B float64 `json:"b,string"`
				}
				B *struct {
					A float64 `json:"a,string"`
					B float64 `json:"b,string"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadFloat64NilDoubleMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A *struct {
					A float64 `json:"a,string,omitempty"`
					B float64 `json:"b,string,omitempty"`
				}
				B *struct {
					A float64 `json:"a,string,omitempty"`
					B float64 `json:"b,string,omitempty"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadFloat64NilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadFloat64NilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A float64 `json:"a"`
					B float64 `json:"b"`
				}
				B *struct {
					A float64 `json:"a"`
					B float64 `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadFloat64NilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A float64 `json:"a,omitempty"`
					B float64 `json:"b,omitempty"`
				}
				B *struct {
					A float64 `json:"a,omitempty"`
					B float64 `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadFloat64NilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A float64 `json:"a,string"`
					B float64 `json:"b,string"`
				}
				B *struct {
					A float64 `json:"a,string"`
					B float64 `json:"b,string"`
				}
			})(nil),
		},
		{
			name: "PtrHeadFloat64NilDoubleMultiFieldsNotRootStringOmitEmpty",
			data: (*struct {
				A *struct {
					A float64 `json:"a,string,omitempty"`
					B float64 `json:"b,string,omitempty"`
				}
				B *struct {
					A float64 `json:"a,string,omitempty"`
					B float64 `json:"b,string,omitempty"`
				}
			})(nil),
		},

		// PtrHeadFloat64PtrDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadFloat64PtrDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *float64 `json:"a"`
					B *float64 `json:"b"`
				}
				B *struct {
					A *float64 `json:"a"`
					B *float64 `json:"b"`
				}
			}{A: &(struct {
				A *float64 `json:"a"`
				B *float64 `json:"b"`
			}{A: float64ptr(1), B: float64ptr(2)}), B: &(struct {
				A *float64 `json:"a"`
				B *float64 `json:"b"`
			}{A: float64ptr(3), B: float64ptr(4)})},
		},
		{
			name: "PtrHeadFloat64PtrDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *float64 `json:"a,omitempty"`
					B *float64 `json:"b,omitempty"`
				}
				B *struct {
					A *float64 `json:"a,omitempty"`
					B *float64 `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *float64 `json:"a,omitempty"`
				B *float64 `json:"b,omitempty"`
			}{A: float64ptr(1), B: float64ptr(2)}), B: &(struct {
				A *float64 `json:"a,omitempty"`
				B *float64 `json:"b,omitempty"`
			}{A: float64ptr(3), B: float64ptr(4)})},
		},
		{
			name: "PtrHeadFloat64PtrDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *float64 `json:"a,string"`
					B *float64 `json:"b,string"`
				}
				B *struct {
					A *float64 `json:"a,string"`
					B *float64 `json:"b,string"`
				}
			}{A: &(struct {
				A *float64 `json:"a,string"`
				B *float64 `json:"b,string"`
			}{A: float64ptr(1), B: float64ptr(2)}), B: &(struct {
				A *float64 `json:"a,string"`
				B *float64 `json:"b,string"`
			}{A: float64ptr(3), B: float64ptr(4)})},
		},
		{
			name: "PtrHeadFloat64PtrDoubleMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A *struct {
					A *float64 `json:"a,string,omitempty"`
					B *float64 `json:"b,string,omitempty"`
				}
				B *struct {
					A *float64 `json:"a,string,omitempty"`
					B *float64 `json:"b,string,omitempty"`
				}
			}{A: &(struct {
				A *float64 `json:"a,string,omitempty"`
				B *float64 `json:"b,string,omitempty"`
			}{A: float64ptr(1), B: float64ptr(2)}), B: &(struct {
				A *float64 `json:"a,string,omitempty"`
				B *float64 `json:"b,string,omitempty"`
			}{A: float64ptr(3), B: float64ptr(4)})},
		},

		// PtrHeadFloat64PtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadFloat64PtrNilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *float64 `json:"a"`
					B *float64 `json:"b"`
				}
				B *struct {
					A *float64 `json:"a"`
					B *float64 `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadFloat64PtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *float64 `json:"a,omitempty"`
					B *float64 `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A *float64 `json:"a,omitempty"`
					B *float64 `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadFloat64PtrNilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *float64 `json:"a,string"`
					B *float64 `json:"b,string"`
				}
				B *struct {
					A *float64 `json:"a,string"`
					B *float64 `json:"b,string"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadFloat64PtrNilDoubleMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A *struct {
					A *float64 `json:"a,string,omitempty"`
					B *float64 `json:"b,string,omitempty"`
				}
				B *struct {
					A *float64 `json:"a,string,omitempty"`
					B *float64 `json:"b,string,omitempty"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadFloat64PtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadFloat64PtrNilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *float64 `json:"a"`
					B *float64 `json:"b"`
				}
				B *struct {
					A *float64 `json:"a"`
					B *float64 `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadFloat64PtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *float64 `json:"a,omitempty"`
					B *float64 `json:"b,omitempty"`
				}
				B *struct {
					A *float64 `json:"a,omitempty"`
					B *float64 `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadFloat64PtrNilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *float64 `json:"a,string"`
					B *float64 `json:"b,string"`
				}
				B *struct {
					A *float64 `json:"a,string"`
					B *float64 `json:"b,string"`
				}
			})(nil),
		},
		{
			name: "PtrHeadFloat64PtrNilDoubleMultiFieldsNotRootStringOmitEmpty",
			data: (*struct {
				A *struct {
					A *float64 `json:"a,string,omitempty"`
					B *float64 `json:"b,string,omitempty"`
				}
				B *struct {
					A *float64 `json:"a,string,omitempty"`
					B *float64 `json:"b,string,omitempty"`
				}
			})(nil),
		},

		// AnonymousHeadFloat64
		{
			name: "AnonymousHeadFloat64",
			data: struct {
				structFloat64
				B float64 `json:"b"`
			}{
				structFloat64: structFloat64{A: 1},
				B:             2,
			},
		},
		{
			name: "AnonymousHeadFloat64OmitEmpty",
			data: struct {
				structFloat64OmitEmpty
				B float64 `json:"b,omitempty"`
			}{
				structFloat64OmitEmpty: structFloat64OmitEmpty{A: 1},
				B:                      2,
			},
		},
		{
			name: "AnonymousHeadFloat64String",
			data: struct {
				structFloat64String
				B float64 `json:"b,string"`
			}{
				structFloat64String: structFloat64String{A: 1},
				B:                   2,
			},
		},
		{
			name: "AnonymousHeadFloat64StringOmitEmpty",
			data: struct {
				structFloat64StringOmitEmpty
				B float64 `json:"b,string,omitempty"`
			}{
				structFloat64StringOmitEmpty: structFloat64StringOmitEmpty{A: 1},
				B:                            2,
			},
		},

		// PtrAnonymousHeadFloat64
		{
			name: "PtrAnonymousHeadFloat64",
			data: struct {
				*structFloat64
				B float64 `json:"b"`
			}{
				structFloat64: &structFloat64{A: 1},
				B:             2,
			},
		},
		{
			name: "PtrAnonymousHeadFloat64OmitEmpty",
			data: struct {
				*structFloat64OmitEmpty
				B float64 `json:"b,omitempty"`
			}{
				structFloat64OmitEmpty: &structFloat64OmitEmpty{A: 1},
				B:                      2,
			},
		},
		{
			name: "PtrAnonymousHeadFloat64String",
			data: struct {
				*structFloat64String
				B float64 `json:"b,string"`
			}{
				structFloat64String: &structFloat64String{A: 1},
				B:                   2,
			},
		},
		{
			name: "PtrAnonymousHeadFloat64StringOmitEmpty",
			data: struct {
				*structFloat64StringOmitEmpty
				B float64 `json:"b,string,omitempty"`
			}{
				structFloat64StringOmitEmpty: &structFloat64StringOmitEmpty{A: 1},
				B:                            2,
			},
		},

		// NilPtrAnonymousHeadFloat64
		{
			name: "NilPtrAnonymousHeadFloat64",
			data: struct {
				*structFloat64
				B float64 `json:"b"`
			}{
				structFloat64: nil,
				B:             2,
			},
		},
		{
			name: "NilPtrAnonymousHeadFloat64OmitEmpty",
			data: struct {
				*structFloat64OmitEmpty
				B float64 `json:"b,omitempty"`
			}{
				structFloat64OmitEmpty: nil,
				B:                      2,
			},
		},
		{
			name: "NilPtrAnonymousHeadFloat64String",
			data: struct {
				*structFloat64String
				B float64 `json:"b,string"`
			}{
				structFloat64String: nil,
				B:                   2,
			},
		},
		{
			name: "NilPtrAnonymousHeadFloat64StringOmitEmpty",
			data: struct {
				*structFloat64StringOmitEmpty
				B float64 `json:"b,string,omitempty"`
			}{
				structFloat64StringOmitEmpty: nil,
				B:                            2,
			},
		},

		// AnonymousHeadFloat64Ptr
		{
			name: "AnonymousHeadFloat64Ptr",
			data: struct {
				structFloat64Ptr
				B *float64 `json:"b"`
			}{
				structFloat64Ptr: structFloat64Ptr{A: float64ptr(1)},
				B:                float64ptr(2),
			},
		},
		{
			name: "AnonymousHeadFloat64PtrOmitEmpty",
			data: struct {
				structFloat64PtrOmitEmpty
				B *float64 `json:"b,omitempty"`
			}{
				structFloat64PtrOmitEmpty: structFloat64PtrOmitEmpty{A: float64ptr(1)},
				B:                         float64ptr(2),
			},
		},
		{
			name: "AnonymousHeadFloat64PtrString",
			data: struct {
				structFloat64PtrString
				B *float64 `json:"b,string"`
			}{
				structFloat64PtrString: structFloat64PtrString{A: float64ptr(1)},
				B:                      float64ptr(2),
			},
		},
		{
			name: "AnonymousHeadFloat64PtrStringOmitEmpty",
			data: struct {
				structFloat64PtrStringOmitEmpty
				B *float64 `json:"b,string,omitempty"`
			}{
				structFloat64PtrStringOmitEmpty: structFloat64PtrStringOmitEmpty{A: float64ptr(1)},
				B:                               float64ptr(2),
			},
		},

		// AnonymousHeadFloat64PtrNil
		{
			name: "AnonymousHeadFloat64PtrNil",
			data: struct {
				structFloat64Ptr
				B *float64 `json:"b"`
			}{
				structFloat64Ptr: structFloat64Ptr{A: nil},
				B:                float64ptr(2),
			},
		},
		{
			name: "AnonymousHeadFloat64PtrNilOmitEmpty",
			data: struct {
				structFloat64PtrOmitEmpty
				B *float64 `json:"b,omitempty"`
			}{
				structFloat64PtrOmitEmpty: structFloat64PtrOmitEmpty{A: nil},
				B:                         float64ptr(2),
			},
		},
		{
			name: "AnonymousHeadFloat64PtrNilString",
			data: struct {
				structFloat64PtrString
				B *float64 `json:"b,string"`
			}{
				structFloat64PtrString: structFloat64PtrString{A: nil},
				B:                      float64ptr(2),
			},
		},
		{
			name: "AnonymousHeadFloat64PtrNilStringOmitEmpty",
			data: struct {
				structFloat64PtrStringOmitEmpty
				B *float64 `json:"b,string,omitempty"`
			}{
				structFloat64PtrStringOmitEmpty: structFloat64PtrStringOmitEmpty{A: nil},
				B:                               float64ptr(2),
			},
		},

		// PtrAnonymousHeadFloat64Ptr
		{
			name: "PtrAnonymousHeadFloat64Ptr",
			data: struct {
				*structFloat64Ptr
				B *float64 `json:"b"`
			}{
				structFloat64Ptr: &structFloat64Ptr{A: float64ptr(1)},
				B:                float64ptr(2),
			},
		},
		{
			name: "PtrAnonymousHeadFloat64PtrOmitEmpty",
			data: struct {
				*structFloat64PtrOmitEmpty
				B *float64 `json:"b,omitempty"`
			}{
				structFloat64PtrOmitEmpty: &structFloat64PtrOmitEmpty{A: float64ptr(1)},
				B:                         float64ptr(2),
			},
		},
		{
			name: "PtrAnonymousHeadFloat64PtrString",
			data: struct {
				*structFloat64PtrString
				B *float64 `json:"b,string"`
			}{
				structFloat64PtrString: &structFloat64PtrString{A: float64ptr(1)},
				B:                      float64ptr(2),
			},
		},
		{
			name: "PtrAnonymousHeadFloat64PtrStringOmitEmpty",
			data: struct {
				*structFloat64PtrStringOmitEmpty
				B *float64 `json:"b,string,omitempty"`
			}{
				structFloat64PtrStringOmitEmpty: &structFloat64PtrStringOmitEmpty{A: float64ptr(1)},
				B:                               float64ptr(2),
			},
		},

		// NilPtrAnonymousHeadFloat64Ptr
		{
			name: "NilPtrAnonymousHeadFloat64Ptr",
			data: struct {
				*structFloat64Ptr
				B *float64 `json:"b"`
			}{
				structFloat64Ptr: nil,
				B:                float64ptr(2),
			},
		},
		{
			name: "NilPtrAnonymousHeadFloat64PtrOmitEmpty",
			data: struct {
				*structFloat64PtrOmitEmpty
				B *float64 `json:"b,omitempty"`
			}{
				structFloat64PtrOmitEmpty: nil,
				B:                         float64ptr(2),
			},
		},
		{
			name: "NilPtrAnonymousHeadFloat64PtrString",
			data: struct {
				*structFloat64PtrString
				B *float64 `json:"b,string"`
			}{
				structFloat64PtrString: nil,
				B:                      float64ptr(2),
			},
		},
		{
			name: "NilPtrAnonymousHeadFloat64PtrStringOmitEmpty",
			data: struct {
				*structFloat64PtrStringOmitEmpty
				B *float64 `json:"b,string,omitempty"`
			}{
				structFloat64PtrStringOmitEmpty: nil,
				B:                               float64ptr(2),
			},
		},

		// AnonymousHeadFloat64Only
		{
			name: "AnonymousHeadFloat64Only",
			data: struct {
				structFloat64
			}{
				structFloat64: structFloat64{A: 1},
			},
		},
		{
			name: "AnonymousHeadFloat64OnlyOmitEmpty",
			data: struct {
				structFloat64OmitEmpty
			}{
				structFloat64OmitEmpty: structFloat64OmitEmpty{A: 1},
			},
		},
		{
			name: "AnonymousHeadFloat64OnlyString",
			data: struct {
				structFloat64String
			}{
				structFloat64String: structFloat64String{A: 1},
			},
		},
		{
			name: "AnonymousHeadFloat64OnlyStringOmitEmpty",
			data: struct {
				structFloat64StringOmitEmpty
			}{
				structFloat64StringOmitEmpty: structFloat64StringOmitEmpty{A: 1},
			},
		},

		// PtrAnonymousHeadFloat64Only
		{
			name: "PtrAnonymousHeadFloat64Only",
			data: struct {
				*structFloat64
			}{
				structFloat64: &structFloat64{A: 1},
			},
		},
		{
			name: "PtrAnonymousHeadFloat64OnlyOmitEmpty",
			data: struct {
				*structFloat64OmitEmpty
			}{
				structFloat64OmitEmpty: &structFloat64OmitEmpty{A: 1},
			},
		},
		{
			name: "PtrAnonymousHeadFloat64OnlyString",
			data: struct {
				*structFloat64String
			}{
				structFloat64String: &structFloat64String{A: 1},
			},
		},
		{
			name: "PtrAnonymousHeadFloat64OnlyStringOmitEmpty",
			data: struct {
				*structFloat64StringOmitEmpty
			}{
				structFloat64StringOmitEmpty: &structFloat64StringOmitEmpty{A: 1},
			},
		},

		// NilPtrAnonymousHeadFloat64Only
		{
			name: "NilPtrAnonymousHeadFloat64Only",
			data: struct {
				*structFloat64
			}{
				structFloat64: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadFloat64OnlyOmitEmpty",
			data: struct {
				*structFloat64OmitEmpty
			}{
				structFloat64OmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadFloat64OnlyString",
			data: struct {
				*structFloat64String
			}{
				structFloat64String: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadFloat64OnlyStringOmitEmpty",
			data: struct {
				*structFloat64StringOmitEmpty
			}{
				structFloat64StringOmitEmpty: nil,
			},
		},

		// AnonymousHeadFloat64PtrOnly
		{
			name: "AnonymousHeadFloat64PtrOnly",
			data: struct {
				structFloat64Ptr
			}{
				structFloat64Ptr: structFloat64Ptr{A: float64ptr(1)},
			},
		},
		{
			name: "AnonymousHeadFloat64PtrOnlyOmitEmpty",
			data: struct {
				structFloat64PtrOmitEmpty
			}{
				structFloat64PtrOmitEmpty: structFloat64PtrOmitEmpty{A: float64ptr(1)},
			},
		},
		{
			name: "AnonymousHeadFloat64PtrOnlyString",
			data: struct {
				structFloat64PtrString
			}{
				structFloat64PtrString: structFloat64PtrString{A: float64ptr(1)},
			},
		},
		{
			name: "AnonymousHeadFloat64PtrOnlyStringOmitEmpty",
			data: struct {
				structFloat64PtrStringOmitEmpty
			}{
				structFloat64PtrStringOmitEmpty: structFloat64PtrStringOmitEmpty{A: float64ptr(1)},
			},
		},

		// AnonymousHeadFloat64PtrNilOnly
		{
			name: "AnonymousHeadFloat64PtrNilOnly",
			data: struct {
				structFloat64Ptr
			}{
				structFloat64Ptr: structFloat64Ptr{A: nil},
			},
		},
		{
			name: "AnonymousHeadFloat64PtrNilOnlyOmitEmpty",
			data: struct {
				structFloat64PtrOmitEmpty
			}{
				structFloat64PtrOmitEmpty: structFloat64PtrOmitEmpty{A: nil},
			},
		},
		{
			name: "AnonymousHeadFloat64PtrNilOnlyString",
			data: struct {
				structFloat64PtrString
			}{
				structFloat64PtrString: structFloat64PtrString{A: nil},
			},
		},
		{
			name: "AnonymousHeadFloat64PtrNilOnlyStringOmitEmpty",
			data: struct {
				structFloat64PtrStringOmitEmpty
			}{
				structFloat64PtrStringOmitEmpty: structFloat64PtrStringOmitEmpty{A: nil},
			},
		},

		// PtrAnonymousHeadFloat64PtrOnly
		{
			name: "PtrAnonymousHeadFloat64PtrOnly",
			data: struct {
				*structFloat64Ptr
			}{
				structFloat64Ptr: &structFloat64Ptr{A: float64ptr(1)},
			},
		},
		{
			name: "PtrAnonymousHeadFloat64PtrOnlyOmitEmpty",
			data: struct {
				*structFloat64PtrOmitEmpty
			}{
				structFloat64PtrOmitEmpty: &structFloat64PtrOmitEmpty{A: float64ptr(1)},
			},
		},
		{
			name: "PtrAnonymousHeadFloat64PtrOnlyString",
			data: struct {
				*structFloat64PtrString
			}{
				structFloat64PtrString: &structFloat64PtrString{A: float64ptr(1)},
			},
		},
		{
			name: "PtrAnonymousHeadFloat64PtrOnlyStringOmitEmpty",
			data: struct {
				*structFloat64PtrStringOmitEmpty
			}{
				structFloat64PtrStringOmitEmpty: &structFloat64PtrStringOmitEmpty{A: float64ptr(1)},
			},
		},

		// NilPtrAnonymousHeadFloat64PtrOnly
		{
			name: "NilPtrAnonymousHeadFloat64PtrOnly",
			data: struct {
				*structFloat64Ptr
			}{
				structFloat64Ptr: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadFloat64PtrOnlyOmitEmpty",
			data: struct {
				*structFloat64PtrOmitEmpty
			}{
				structFloat64PtrOmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadFloat64PtrOnlyString",
			data: struct {
				*structFloat64PtrString
			}{
				structFloat64PtrString: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadFloat64PtrOnlyStringOmitEmpty",
			data: struct {
				*structFloat64PtrStringOmitEmpty
			}{
				structFloat64PtrStringOmitEmpty: nil,
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
