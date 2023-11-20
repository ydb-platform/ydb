package json_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/goccy/go-json"
)

func TestCoverFloat32(t *testing.T) {
	type structFloat32 struct {
		A float32 `json:"a"`
	}
	type structFloat32OmitEmpty struct {
		A float32 `json:"a,omitempty"`
	}
	type structFloat32String struct {
		A float32 `json:"a,string"`
	}
	type structFloat32StringOmitEmpty struct {
		A float32 `json:"a,string,omitempty"`
	}

	type structFloat32Ptr struct {
		A *float32 `json:"a"`
	}
	type structFloat32PtrOmitEmpty struct {
		A *float32 `json:"a,omitempty"`
	}
	type structFloat32PtrString struct {
		A *float32 `json:"a,string"`
	}
	type structFloat32PtrStringOmitEmpty struct {
		A *float32 `json:"a,string,omitempty"`
	}

	tests := []struct {
		name string
		data interface{}
	}{
		{
			name: "Float32",
			data: float32(10),
		},
		{
			name: "Float32Ptr",
			data: float32ptr(10),
		},
		{
			name: "Float32Ptr3",
			data: float32ptr3(10),
		},
		{
			name: "Float32PtrNil",
			data: (*float32)(nil),
		},
		{
			name: "Float32Ptr3Nil",
			data: (***float32)(nil),
		},

		// HeadFloat32Zero
		{
			name: "HeadFloat32Zero",
			data: struct {
				A float32 `json:"a"`
			}{},
		},
		{
			name: "HeadFloat32ZeroOmitEmpty",
			data: struct {
				A float32 `json:"a,omitempty"`
			}{},
		},
		{
			name: "HeadFloat32ZeroString",
			data: struct {
				A float32 `json:"a,string"`
			}{},
		},
		{
			name: "HeadFloat32ZeroStringOmitEmpty",
			data: struct {
				A float32 `json:"a,string,omitempty"`
			}{},
		},

		// HeadFloat32
		{
			name: "HeadFloat32",
			data: struct {
				A float32 `json:"a"`
			}{A: 1},
		},
		{
			name: "HeadFloat32OmitEmpty",
			data: struct {
				A float32 `json:"a,omitempty"`
			}{A: 1},
		},
		{
			name: "HeadFloat32String",
			data: struct {
				A float32 `json:"a,string"`
			}{A: 1},
		},
		{
			name: "HeadFloat32StringOmitEmpty",
			data: struct {
				A float32 `json:"a,string,omitempty"`
			}{A: 1},
		},

		// HeadFloat32Ptr
		{
			name: "HeadFloat32Ptr",
			data: struct {
				A *float32 `json:"a"`
			}{A: float32ptr(1)},
		},
		{
			name: "HeadFloat32PtrOmitEmpty",
			data: struct {
				A *float32 `json:"a,omitempty"`
			}{A: float32ptr(1)},
		},
		{
			name: "HeadFloat32PtrString",
			data: struct {
				A *float32 `json:"a,string"`
			}{A: float32ptr(1)},
		},
		{
			name: "HeadFloat32PtrStringOmitEmpty",
			data: struct {
				A *float32 `json:"a,string,omitempty"`
			}{A: float32ptr(1)},
		},

		// HeadFloat32PtrNil
		{
			name: "HeadFloat32PtrNil",
			data: struct {
				A *float32 `json:"a"`
			}{A: nil},
		},
		{
			name: "HeadFloat32PtrNilOmitEmpty",
			data: struct {
				A *float32 `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "HeadFloat32PtrNilString",
			data: struct {
				A *float32 `json:"a,string"`
			}{A: nil},
		},
		{
			name: "HeadFloat32PtrNilStringOmitEmpty",
			data: struct {
				A *float32 `json:"a,string,omitempty"`
			}{A: nil},
		},

		// PtrHeadFloat32Zero
		{
			name: "PtrHeadFloat32Zero",
			data: &struct {
				A float32 `json:"a"`
			}{},
		},
		{
			name: "PtrHeadFloat32ZeroOmitEmpty",
			data: &struct {
				A float32 `json:"a,omitempty"`
			}{},
		},
		{
			name: "PtrHeadFloat32ZeroString",
			data: &struct {
				A float32 `json:"a,string"`
			}{},
		},
		{
			name: "PtrHeadFloat32ZeroStringOmitEmpty",
			data: &struct {
				A float32 `json:"a,string,omitempty"`
			}{},
		},

		// PtrHeadFloat32
		{
			name: "PtrHeadFloat32",
			data: &struct {
				A float32 `json:"a"`
			}{A: 1},
		},
		{
			name: "PtrHeadFloat32OmitEmpty",
			data: &struct {
				A float32 `json:"a,omitempty"`
			}{A: 1},
		},
		{
			name: "PtrHeadFloat32String",
			data: &struct {
				A float32 `json:"a,string"`
			}{A: 1},
		},
		{
			name: "PtrHeadFloat32StringOmitEmpty",
			data: &struct {
				A float32 `json:"a,string,omitempty"`
			}{A: 1},
		},

		// PtrHeadFloat32Ptr
		{
			name: "PtrHeadFloat32Ptr",
			data: &struct {
				A *float32 `json:"a"`
			}{A: float32ptr(1)},
		},
		{
			name: "PtrHeadFloat32PtrOmitEmpty",
			data: &struct {
				A *float32 `json:"a,omitempty"`
			}{A: float32ptr(1)},
		},
		{
			name: "PtrHeadFloat32PtrString",
			data: &struct {
				A *float32 `json:"a,string"`
			}{A: float32ptr(1)},
		},
		{
			name: "PtrHeadFloat32PtrStringOmitEmpty",
			data: &struct {
				A *float32 `json:"a,string,omitempty"`
			}{A: float32ptr(1)},
		},

		// PtrHeadFloat32PtrNil
		{
			name: "PtrHeadFloat32PtrNil",
			data: &struct {
				A *float32 `json:"a"`
			}{A: nil},
		},
		{
			name: "PtrHeadFloat32PtrNilOmitEmpty",
			data: &struct {
				A *float32 `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadFloat32PtrNilString",
			data: &struct {
				A *float32 `json:"a,string"`
			}{A: nil},
		},
		{
			name: "PtrHeadFloat32PtrNilStringOmitEmpty",
			data: &struct {
				A *float32 `json:"a,string,omitempty"`
			}{A: nil},
		},

		// PtrHeadFloat32Nil
		{
			name: "PtrHeadFloat32Nil",
			data: (*struct {
				A *float32 `json:"a"`
			})(nil),
		},
		{
			name: "PtrHeadFloat32NilOmitEmpty",
			data: (*struct {
				A *float32 `json:"a,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadFloat32NilString",
			data: (*struct {
				A *float32 `json:"a,string"`
			})(nil),
		},
		{
			name: "PtrHeadFloat32NilStringOmitEmpty",
			data: (*struct {
				A *float32 `json:"a,string,omitempty"`
			})(nil),
		},

		// HeadFloat32ZeroMultiFields
		{
			name: "HeadFloat32ZeroMultiFields",
			data: struct {
				A float32 `json:"a"`
				B float32 `json:"b"`
				C float32 `json:"c"`
			}{},
		},
		{
			name: "HeadFloat32ZeroMultiFieldsOmitEmpty",
			data: struct {
				A float32 `json:"a,omitempty"`
				B float32 `json:"b,omitempty"`
				C float32 `json:"c,omitempty"`
			}{},
		},
		{
			name: "HeadFloat32ZeroMultiFieldsString",
			data: struct {
				A float32 `json:"a,string"`
				B float32 `json:"b,string"`
				C float32 `json:"c,string"`
			}{},
		},
		{
			name: "HeadFloat32ZeroMultiFieldsStringOmitEmpty",
			data: struct {
				A float32 `json:"a,string,omitempty"`
				B float32 `json:"b,string,omitempty"`
				C float32 `json:"c,string,omitempty"`
			}{},
		},

		// HeadFloat32MultiFields
		{
			name: "HeadFloat32MultiFields",
			data: struct {
				A float32 `json:"a"`
				B float32 `json:"b"`
				C float32 `json:"c"`
			}{A: 1, B: 2, C: 3},
		},
		{
			name: "HeadFloat32MultiFieldsOmitEmpty",
			data: struct {
				A float32 `json:"a,omitempty"`
				B float32 `json:"b,omitempty"`
				C float32 `json:"c,omitempty"`
			}{A: 1, B: 2, C: 3},
		},
		{
			name: "HeadFloat32MultiFieldsString",
			data: struct {
				A float32 `json:"a,string"`
				B float32 `json:"b,string"`
				C float32 `json:"c,string"`
			}{A: 1, B: 2, C: 3},
		},
		{
			name: "HeadFloat32MultiFieldsStringOmitEmpty",
			data: struct {
				A float32 `json:"a,string,omitempty"`
				B float32 `json:"b,string,omitempty"`
				C float32 `json:"c,string,omitempty"`
			}{A: 1, B: 2, C: 3},
		},

		// HeadFloat32PtrMultiFields
		{
			name: "HeadFloat32PtrMultiFields",
			data: struct {
				A *float32 `json:"a"`
				B *float32 `json:"b"`
				C *float32 `json:"c"`
			}{A: float32ptr(1), B: float32ptr(2), C: float32ptr(3)},
		},
		{
			name: "HeadFloat32PtrMultiFieldsOmitEmpty",
			data: struct {
				A *float32 `json:"a,omitempty"`
				B *float32 `json:"b,omitempty"`
				C *float32 `json:"c,omitempty"`
			}{A: float32ptr(1), B: float32ptr(2), C: float32ptr(3)},
		},
		{
			name: "HeadFloat32PtrMultiFieldsString",
			data: struct {
				A *float32 `json:"a,string"`
				B *float32 `json:"b,string"`
				C *float32 `json:"c,string"`
			}{A: float32ptr(1), B: float32ptr(2), C: float32ptr(3)},
		},
		{
			name: "HeadFloat32PtrMultiFieldsStringOmitEmpty",
			data: struct {
				A *float32 `json:"a,string,omitempty"`
				B *float32 `json:"b,string,omitempty"`
				C *float32 `json:"c,string,omitempty"`
			}{A: float32ptr(1), B: float32ptr(2), C: float32ptr(3)},
		},

		// HeadFloat32PtrNilMultiFields
		{
			name: "HeadFloat32PtrNilMultiFields",
			data: struct {
				A *float32 `json:"a"`
				B *float32 `json:"b"`
				C *float32 `json:"c"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadFloat32PtrNilMultiFieldsOmitEmpty",
			data: struct {
				A *float32 `json:"a,omitempty"`
				B *float32 `json:"b,omitempty"`
				C *float32 `json:"c,omitempty"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadFloat32PtrNilMultiFieldsString",
			data: struct {
				A *float32 `json:"a,string"`
				B *float32 `json:"b,string"`
				C *float32 `json:"c,string"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadFloat32PtrNilMultiFieldsStringOmitEmpty",
			data: struct {
				A *float32 `json:"a,string,omitempty"`
				B *float32 `json:"b,string,omitempty"`
				C *float32 `json:"c,string,omitempty"`
			}{A: nil, B: nil, C: nil},
		},

		// PtrHeadFloat32ZeroMultiFields
		{
			name: "PtrHeadFloat32ZeroMultiFields",
			data: &struct {
				A float32 `json:"a"`
				B float32 `json:"b"`
			}{},
		},
		{
			name: "PtrHeadFloat32ZeroMultiFieldsOmitEmpty",
			data: &struct {
				A float32 `json:"a,omitempty"`
				B float32 `json:"b,omitempty"`
			}{},
		},
		{
			name: "PtrHeadFloat32ZeroMultiFieldsString",
			data: &struct {
				A float32 `json:"a,string"`
				B float32 `json:"b,string"`
			}{},
		},
		{
			name: "PtrHeadFloat32ZeroMultiFieldsStringOmitEmpty",
			data: &struct {
				A float32 `json:"a,string,omitempty"`
				B float32 `json:"b,string,omitempty"`
			}{},
		},

		// PtrHeadFloat32MultiFields
		{
			name: "PtrHeadFloat32MultiFields",
			data: &struct {
				A float32 `json:"a"`
				B float32 `json:"b"`
			}{A: 1, B: 2},
		},
		{
			name: "PtrHeadFloat32MultiFieldsOmitEmpty",
			data: &struct {
				A float32 `json:"a,omitempty"`
				B float32 `json:"b,omitempty"`
			}{A: 1, B: 2},
		},
		{
			name: "PtrHeadFloat32MultiFieldsString",
			data: &struct {
				A float32 `json:"a,string"`
				B float32 `json:"b,string"`
			}{A: 1, B: 2},
		},
		{
			name: "PtrHeadFloat32MultiFieldsStringOmitEmpty",
			data: &struct {
				A float32 `json:"a,string,omitempty"`
				B float32 `json:"b,string,omitempty"`
			}{A: 1, B: 2},
		},

		// PtrHeadFloat32PtrMultiFields
		{
			name: "PtrHeadFloat32PtrMultiFields",
			data: &struct {
				A *float32 `json:"a"`
				B *float32 `json:"b"`
			}{A: float32ptr(1), B: float32ptr(2)},
		},
		{
			name: "PtrHeadFloat32PtrMultiFieldsOmitEmpty",
			data: &struct {
				A *float32 `json:"a,omitempty"`
				B *float32 `json:"b,omitempty"`
			}{A: float32ptr(1), B: float32ptr(2)},
		},
		{
			name: "PtrHeadFloat32PtrMultiFieldsString",
			data: &struct {
				A *float32 `json:"a,string"`
				B *float32 `json:"b,string"`
			}{A: float32ptr(1), B: float32ptr(2)},
		},
		{
			name: "PtrHeadFloat32PtrMultiFieldsStringOmitEmpty",
			data: &struct {
				A *float32 `json:"a,string,omitempty"`
				B *float32 `json:"b,string,omitempty"`
			}{A: float32ptr(1), B: float32ptr(2)},
		},

		// PtrHeadFloat32PtrNilMultiFields
		{
			name: "PtrHeadFloat32PtrNilMultiFields",
			data: &struct {
				A *float32 `json:"a"`
				B *float32 `json:"b"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadFloat32PtrNilMultiFieldsOmitEmpty",
			data: &struct {
				A *float32 `json:"a,omitempty"`
				B *float32 `json:"b,omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadFloat32PtrNilMultiFieldsString",
			data: &struct {
				A *float32 `json:"a,string"`
				B *float32 `json:"b,string"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadFloat32PtrNilMultiFieldsStringOmitEmpty",
			data: &struct {
				A *float32 `json:"a,string,omitempty"`
				B *float32 `json:"b,string,omitempty"`
			}{A: nil, B: nil},
		},

		// PtrHeadFloat32NilMultiFields
		{
			name: "PtrHeadFloat32NilMultiFields",
			data: (*struct {
				A *float32 `json:"a"`
				B *float32 `json:"b"`
			})(nil),
		},
		{
			name: "PtrHeadFloat32NilMultiFieldsOmitEmpty",
			data: (*struct {
				A *float32 `json:"a,omitempty"`
				B *float32 `json:"b,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadFloat32NilMultiFieldsString",
			data: (*struct {
				A *float32 `json:"a,string"`
				B *float32 `json:"b,string"`
			})(nil),
		},
		{
			name: "PtrHeadFloat32NilMultiFieldsStringOmitEmpty",
			data: (*struct {
				A *float32 `json:"a,string,omitempty"`
				B *float32 `json:"b,string,omitempty"`
			})(nil),
		},

		// HeadFloat32ZeroNotRoot
		{
			name: "HeadFloat32ZeroNotRoot",
			data: struct {
				A struct {
					A float32 `json:"a"`
				}
			}{},
		},
		{
			name: "HeadFloat32ZeroNotRootOmitEmpty",
			data: struct {
				A struct {
					A float32 `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadFloat32ZeroNotRootString",
			data: struct {
				A struct {
					A float32 `json:"a,string"`
				}
			}{},
		},
		{
			name: "HeadFloat32ZeroNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A float32 `json:"a,string,omitempty"`
				}
			}{},
		},

		// HeadFloat32NotRoot
		{
			name: "HeadFloat32NotRoot",
			data: struct {
				A struct {
					A float32 `json:"a"`
				}
			}{A: struct {
				A float32 `json:"a"`
			}{A: 1}},
		},
		{
			name: "HeadFloat32NotRootOmitEmpty",
			data: struct {
				A struct {
					A float32 `json:"a,omitempty"`
				}
			}{A: struct {
				A float32 `json:"a,omitempty"`
			}{A: 1}},
		},
		{
			name: "HeadFloat32NotRootString",
			data: struct {
				A struct {
					A float32 `json:"a,string"`
				}
			}{A: struct {
				A float32 `json:"a,string"`
			}{A: 1}},
		},
		{
			name: "HeadFloat32NotRootStringOmitEmpty",
			data: struct {
				A struct {
					A float32 `json:"a,string,omitempty"`
				}
			}{A: struct {
				A float32 `json:"a,string,omitempty"`
			}{A: 1}},
		},

		// HeadFloat32PtrNotRoot
		{
			name: "HeadFloat32PtrNotRoot",
			data: struct {
				A struct {
					A *float32 `json:"a"`
				}
			}{A: struct {
				A *float32 `json:"a"`
			}{float32ptr(1)}},
		},
		{
			name: "HeadFloat32PtrNotRootOmitEmpty",
			data: struct {
				A struct {
					A *float32 `json:"a,omitempty"`
				}
			}{A: struct {
				A *float32 `json:"a,omitempty"`
			}{float32ptr(1)}},
		},
		{
			name: "HeadFloat32PtrNotRootString",
			data: struct {
				A struct {
					A *float32 `json:"a,string"`
				}
			}{A: struct {
				A *float32 `json:"a,string"`
			}{float32ptr(1)}},
		},
		{
			name: "HeadFloat32PtrNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A *float32 `json:"a,string,omitempty"`
				}
			}{A: struct {
				A *float32 `json:"a,string,omitempty"`
			}{float32ptr(1)}},
		},

		// HeadFloat32PtrNilNotRoot
		{
			name: "HeadFloat32PtrNilNotRoot",
			data: struct {
				A struct {
					A *float32 `json:"a"`
				}
			}{},
		},
		{
			name: "HeadFloat32PtrNilNotRootOmitEmpty",
			data: struct {
				A struct {
					A *float32 `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadFloat32PtrNilNotRootString",
			data: struct {
				A struct {
					A *float32 `json:"a,string"`
				}
			}{},
		},
		{
			name: "HeadFloat32PtrNilNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A *float32 `json:"a,string,omitempty"`
				}
			}{},
		},

		// PtrHeadFloat32ZeroNotRoot
		{
			name: "PtrHeadFloat32ZeroNotRoot",
			data: struct {
				A *struct {
					A float32 `json:"a"`
				}
			}{A: new(struct {
				A float32 `json:"a"`
			})},
		},
		{
			name: "PtrHeadFloat32ZeroNotRootOmitEmpty",
			data: struct {
				A *struct {
					A float32 `json:"a,omitempty"`
				}
			}{A: new(struct {
				A float32 `json:"a,omitempty"`
			})},
		},
		{
			name: "PtrHeadFloat32ZeroNotRootString",
			data: struct {
				A *struct {
					A float32 `json:"a,string"`
				}
			}{A: new(struct {
				A float32 `json:"a,string"`
			})},
		},
		{
			name: "PtrHeadFloat32ZeroNotRootStringOmitEmpty",
			data: struct {
				A *struct {
					A float32 `json:"a,string,omitempty"`
				}
			}{A: new(struct {
				A float32 `json:"a,string,omitempty"`
			})},
		},

		// PtrHeadFloat32NotRoot
		{
			name: "PtrHeadFloat32NotRoot",
			data: struct {
				A *struct {
					A float32 `json:"a"`
				}
			}{A: &(struct {
				A float32 `json:"a"`
			}{A: 1})},
		},
		{
			name: "PtrHeadFloat32NotRootOmitEmpty",
			data: struct {
				A *struct {
					A float32 `json:"a,omitempty"`
				}
			}{A: &(struct {
				A float32 `json:"a,omitempty"`
			}{A: 1})},
		},
		{
			name: "PtrHeadFloat32NotRootString",
			data: struct {
				A *struct {
					A float32 `json:"a,string"`
				}
			}{A: &(struct {
				A float32 `json:"a,string"`
			}{A: 1})},
		},
		{
			name: "PtrHeadFloat32NotRootStringOmitEmpty",
			data: struct {
				A *struct {
					A float32 `json:"a,string,omitempty"`
				}
			}{A: &(struct {
				A float32 `json:"a,string,omitempty"`
			}{A: 1})},
		},

		// PtrHeadFloat32PtrNotRoot
		{
			name: "PtrHeadFloat32PtrNotRoot",
			data: struct {
				A *struct {
					A *float32 `json:"a"`
				}
			}{A: &(struct {
				A *float32 `json:"a"`
			}{A: float32ptr(1)})},
		},
		{
			name: "PtrHeadFloat32PtrNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *float32 `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *float32 `json:"a,omitempty"`
			}{A: float32ptr(1)})},
		},
		{
			name: "PtrHeadFloat32PtrNotRootString",
			data: struct {
				A *struct {
					A *float32 `json:"a,string"`
				}
			}{A: &(struct {
				A *float32 `json:"a,string"`
			}{A: float32ptr(1)})},
		},
		{
			name: "PtrHeadFloat32PtrNotRootStringOmitEmpty",
			data: struct {
				A *struct {
					A *float32 `json:"a,string,omitempty"`
				}
			}{A: &(struct {
				A *float32 `json:"a,string,omitempty"`
			}{A: float32ptr(1)})},
		},

		// PtrHeadFloat32PtrNilNotRoot
		{
			name: "PtrHeadFloat32PtrNilNotRoot",
			data: struct {
				A *struct {
					A *float32 `json:"a"`
				}
			}{A: &(struct {
				A *float32 `json:"a"`
			}{A: nil})},
		},
		{
			name: "PtrHeadFloat32PtrNilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *float32 `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *float32 `json:"a,omitempty"`
			}{A: nil})},
		},
		{
			name: "PtrHeadFloat32PtrNilNotRootString",
			data: struct {
				A *struct {
					A *float32 `json:"a,string"`
				}
			}{A: &(struct {
				A *float32 `json:"a,string"`
			}{A: nil})},
		},
		{
			name: "PtrHeadFloat32PtrNilNotRootStringOmitEmpty",
			data: struct {
				A *struct {
					A *float32 `json:"a,string,omitempty"`
				}
			}{A: &(struct {
				A *float32 `json:"a,string,omitempty"`
			}{A: nil})},
		},

		// PtrHeadFloat32NilNotRoot
		{
			name: "PtrHeadFloat32NilNotRoot",
			data: struct {
				A *struct {
					A *float32 `json:"a"`
				}
			}{A: nil},
		},
		{
			name: "PtrHeadFloat32NilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *float32 `json:"a,omitempty"`
				} `json:",omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadFloat32NilNotRootString",
			data: struct {
				A *struct {
					A *float32 `json:"a,string"`
				} `json:",string"`
			}{A: nil},
		},
		{
			name: "PtrHeadFloat32NilNotRootStringOmitEmpty",
			data: struct {
				A *struct {
					A *float32 `json:"a,string,omitempty"`
				} `json:",string,omitempty"`
			}{A: nil},
		},

		// HeadFloat32ZeroMultiFieldsNotRoot
		{
			name: "HeadFloat32ZeroMultiFieldsNotRoot",
			data: struct {
				A struct {
					A float32 `json:"a"`
				}
				B struct {
					B float32 `json:"b"`
				}
			}{},
		},
		{
			name: "HeadFloat32ZeroMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A float32 `json:"a,omitempty"`
				}
				B struct {
					B float32 `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "HeadFloat32ZeroMultiFieldsNotRootString",
			data: struct {
				A struct {
					A float32 `json:"a,string"`
				}
				B struct {
					B float32 `json:"b,string"`
				}
			}{},
		},
		{
			name: "HeadFloat32ZeroMultiFieldsNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A float32 `json:"a,string,omitempty"`
				}
				B struct {
					B float32 `json:"b,string,omitempty"`
				}
			}{},
		},

		// HeadFloat32MultiFieldsNotRoot
		{
			name: "HeadFloat32MultiFieldsNotRoot",
			data: struct {
				A struct {
					A float32 `json:"a"`
				}
				B struct {
					B float32 `json:"b"`
				}
			}{A: struct {
				A float32 `json:"a"`
			}{A: 1}, B: struct {
				B float32 `json:"b"`
			}{B: 2}},
		},
		{
			name: "HeadFloat32MultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A float32 `json:"a,omitempty"`
				}
				B struct {
					B float32 `json:"b,omitempty"`
				}
			}{A: struct {
				A float32 `json:"a,omitempty"`
			}{A: 1}, B: struct {
				B float32 `json:"b,omitempty"`
			}{B: 2}},
		},
		{
			name: "HeadFloat32MultiFieldsNotRootString",
			data: struct {
				A struct {
					A float32 `json:"a,string"`
				}
				B struct {
					B float32 `json:"b,string"`
				}
			}{A: struct {
				A float32 `json:"a,string"`
			}{A: 1}, B: struct {
				B float32 `json:"b,string"`
			}{B: 2}},
		},
		{
			name: "HeadFloat32MultiFieldsNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A float32 `json:"a,string,omitempty"`
				}
				B struct {
					B float32 `json:"b,string,omitempty"`
				}
			}{A: struct {
				A float32 `json:"a,string,omitempty"`
			}{A: 1}, B: struct {
				B float32 `json:"b,string,omitempty"`
			}{B: 2}},
		},

		// HeadFloat32PtrMultiFieldsNotRoot
		{
			name: "HeadFloat32PtrMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *float32 `json:"a"`
				}
				B struct {
					B *float32 `json:"b"`
				}
			}{A: struct {
				A *float32 `json:"a"`
			}{A: float32ptr(1)}, B: struct {
				B *float32 `json:"b"`
			}{B: float32ptr(2)}},
		},
		{
			name: "HeadFloat32PtrMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *float32 `json:"a,omitempty"`
				}
				B struct {
					B *float32 `json:"b,omitempty"`
				}
			}{A: struct {
				A *float32 `json:"a,omitempty"`
			}{A: float32ptr(1)}, B: struct {
				B *float32 `json:"b,omitempty"`
			}{B: float32ptr(2)}},
		},
		{
			name: "HeadFloat32PtrMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *float32 `json:"a,string"`
				}
				B struct {
					B *float32 `json:"b,string"`
				}
			}{A: struct {
				A *float32 `json:"a,string"`
			}{A: float32ptr(1)}, B: struct {
				B *float32 `json:"b,string"`
			}{B: float32ptr(2)}},
		},
		{
			name: "HeadFloat32PtrMultiFieldsNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A *float32 `json:"a,string,omitempty"`
				}
				B struct {
					B *float32 `json:"b,string,omitempty"`
				}
			}{A: struct {
				A *float32 `json:"a,string,omitempty"`
			}{A: float32ptr(1)}, B: struct {
				B *float32 `json:"b,string,omitempty"`
			}{B: float32ptr(2)}},
		},

		// HeadFloat32PtrNilMultiFieldsNotRoot
		{
			name: "HeadFloat32PtrNilMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *float32 `json:"a"`
				}
				B struct {
					B *float32 `json:"b"`
				}
			}{A: struct {
				A *float32 `json:"a"`
			}{A: nil}, B: struct {
				B *float32 `json:"b"`
			}{B: nil}},
		},
		{
			name: "HeadFloat32PtrNilMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *float32 `json:"a,omitempty"`
				}
				B struct {
					B *float32 `json:"b,omitempty"`
				}
			}{A: struct {
				A *float32 `json:"a,omitempty"`
			}{A: nil}, B: struct {
				B *float32 `json:"b,omitempty"`
			}{B: nil}},
		},
		{
			name: "HeadFloat32PtrNilMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *float32 `json:"a,string"`
				}
				B struct {
					B *float32 `json:"b,string"`
				}
			}{A: struct {
				A *float32 `json:"a,string"`
			}{A: nil}, B: struct {
				B *float32 `json:"b,string"`
			}{B: nil}},
		},
		{
			name: "HeadFloat32PtrNilMultiFieldsNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A *float32 `json:"a,string,omitempty"`
				}
				B struct {
					B *float32 `json:"b,string,omitempty"`
				}
			}{A: struct {
				A *float32 `json:"a,string,omitempty"`
			}{A: nil}, B: struct {
				B *float32 `json:"b,string,omitempty"`
			}{B: nil}},
		},

		// PtrHeadFloat32ZeroMultiFieldsNotRoot
		{
			name: "PtrHeadFloat32ZeroMultiFieldsNotRoot",
			data: &struct {
				A struct {
					A float32 `json:"a"`
				}
				B struct {
					B float32 `json:"b"`
				}
			}{},
		},
		{
			name: "PtrHeadFloat32ZeroMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A float32 `json:"a,omitempty"`
				}
				B struct {
					B float32 `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "PtrHeadFloat32ZeroMultiFieldsNotRootString",
			data: &struct {
				A struct {
					A float32 `json:"a,string"`
				}
				B struct {
					B float32 `json:"b,string"`
				}
			}{},
		},
		{
			name: "PtrHeadFloat32ZeroMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A struct {
					A float32 `json:"a,string,omitempty"`
				}
				B struct {
					B float32 `json:"b,string,omitempty"`
				}
			}{},
		},

		// PtrHeadFloat32MultiFieldsNotRoot
		{
			name: "PtrHeadFloat32MultiFieldsNotRoot",
			data: &struct {
				A struct {
					A float32 `json:"a"`
				}
				B struct {
					B float32 `json:"b"`
				}
			}{A: struct {
				A float32 `json:"a"`
			}{A: 1}, B: struct {
				B float32 `json:"b"`
			}{B: 2}},
		},
		{
			name: "PtrHeadFloat32MultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A float32 `json:"a,omitempty"`
				}
				B struct {
					B float32 `json:"b,omitempty"`
				}
			}{A: struct {
				A float32 `json:"a,omitempty"`
			}{A: 1}, B: struct {
				B float32 `json:"b,omitempty"`
			}{B: 2}},
		},
		{
			name: "PtrHeadFloat32MultiFieldsNotRootString",
			data: &struct {
				A struct {
					A float32 `json:"a,string"`
				}
				B struct {
					B float32 `json:"b,string"`
				}
			}{A: struct {
				A float32 `json:"a,string"`
			}{A: 1}, B: struct {
				B float32 `json:"b,string"`
			}{B: 2}},
		},
		{
			name: "PtrHeadFloat32MultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A struct {
					A float32 `json:"a,string,omitempty"`
				}
				B struct {
					B float32 `json:"b,string,omitempty"`
				}
			}{A: struct {
				A float32 `json:"a,string,omitempty"`
			}{A: 1}, B: struct {
				B float32 `json:"b,string,omitempty"`
			}{B: 2}},
		},

		// PtrHeadFloat32PtrMultiFieldsNotRoot
		{
			name: "PtrHeadFloat32PtrMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *float32 `json:"a"`
				}
				B *struct {
					B *float32 `json:"b"`
				}
			}{A: &(struct {
				A *float32 `json:"a"`
			}{A: float32ptr(1)}), B: &(struct {
				B *float32 `json:"b"`
			}{B: float32ptr(2)})},
		},
		{
			name: "PtrHeadFloat32PtrMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *float32 `json:"a,omitempty"`
				}
				B *struct {
					B *float32 `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *float32 `json:"a,omitempty"`
			}{A: float32ptr(1)}), B: &(struct {
				B *float32 `json:"b,omitempty"`
			}{B: float32ptr(2)})},
		},
		{
			name: "PtrHeadFloat32PtrMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *float32 `json:"a,string"`
				}
				B *struct {
					B *float32 `json:"b,string"`
				}
			}{A: &(struct {
				A *float32 `json:"a,string"`
			}{A: float32ptr(1)}), B: &(struct {
				B *float32 `json:"b,string"`
			}{B: float32ptr(2)})},
		},
		{
			name: "PtrHeadFloat32PtrMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A *struct {
					A *float32 `json:"a,string,omitempty"`
				}
				B *struct {
					B *float32 `json:"b,string,omitempty"`
				}
			}{A: &(struct {
				A *float32 `json:"a,string,omitempty"`
			}{A: float32ptr(1)}), B: &(struct {
				B *float32 `json:"b,string,omitempty"`
			}{B: float32ptr(2)})},
		},

		// PtrHeadFloat32PtrNilMultiFieldsNotRoot
		{
			name: "PtrHeadFloat32PtrNilMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *float32 `json:"a"`
				}
				B *struct {
					B *float32 `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadFloat32PtrNilMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *float32 `json:"a,omitempty"`
				} `json:",omitempty"`
				B *struct {
					B *float32 `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadFloat32PtrNilMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *float32 `json:"a,string"`
				} `json:",string"`
				B *struct {
					B *float32 `json:"b,string"`
				} `json:",string"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadFloat32PtrNilMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A *struct {
					A *float32 `json:"a,string,omitempty"`
				} `json:",string,omitempty"`
				B *struct {
					B *float32 `json:"b,string,omitempty"`
				} `json:",string,omitempty"`
			}{A: nil, B: nil},
		},

		// PtrHeadFloat32NilMultiFieldsNotRoot
		{
			name: "PtrHeadFloat32NilMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *float32 `json:"a"`
				}
				B *struct {
					B *float32 `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadFloat32NilMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *float32 `json:"a,omitempty"`
				}
				B *struct {
					B *float32 `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadFloat32NilMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *float32 `json:"a,string"`
				}
				B *struct {
					B *float32 `json:"b,string"`
				}
			})(nil),
		},
		{
			name: "PtrHeadFloat32NilMultiFieldsNotRootStringOmitEmpty",
			data: (*struct {
				A *struct {
					A *float32 `json:"a,string,omitempty"`
				}
				B *struct {
					B *float32 `json:"b,string,omitempty"`
				}
			})(nil),
		},

		// PtrHeadFloat32DoubleMultiFieldsNotRoot
		{
			name: "PtrHeadFloat32DoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A float32 `json:"a"`
					B float32 `json:"b"`
				}
				B *struct {
					A float32 `json:"a"`
					B float32 `json:"b"`
				}
			}{A: &(struct {
				A float32 `json:"a"`
				B float32 `json:"b"`
			}{A: 1, B: 2}), B: &(struct {
				A float32 `json:"a"`
				B float32 `json:"b"`
			}{A: 3, B: 4})},
		},
		{
			name: "PtrHeadFloat32DoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A float32 `json:"a,omitempty"`
					B float32 `json:"b,omitempty"`
				}
				B *struct {
					A float32 `json:"a,omitempty"`
					B float32 `json:"b,omitempty"`
				}
			}{A: &(struct {
				A float32 `json:"a,omitempty"`
				B float32 `json:"b,omitempty"`
			}{A: 1, B: 2}), B: &(struct {
				A float32 `json:"a,omitempty"`
				B float32 `json:"b,omitempty"`
			}{A: 3, B: 4})},
		},
		{
			name: "PtrHeadFloat32DoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A float32 `json:"a,string"`
					B float32 `json:"b,string"`
				}
				B *struct {
					A float32 `json:"a,string"`
					B float32 `json:"b,string"`
				}
			}{A: &(struct {
				A float32 `json:"a,string"`
				B float32 `json:"b,string"`
			}{A: 1, B: 2}), B: &(struct {
				A float32 `json:"a,string"`
				B float32 `json:"b,string"`
			}{A: 3, B: 4})},
		},
		{
			name: "PtrHeadFloat32DoubleMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A *struct {
					A float32 `json:"a,string,omitempty"`
					B float32 `json:"b,string,omitempty"`
				}
				B *struct {
					A float32 `json:"a,string,omitempty"`
					B float32 `json:"b,string,omitempty"`
				}
			}{A: &(struct {
				A float32 `json:"a,string,omitempty"`
				B float32 `json:"b,string,omitempty"`
			}{A: 1, B: 2}), B: &(struct {
				A float32 `json:"a,string,omitempty"`
				B float32 `json:"b,string,omitempty"`
			}{A: 3, B: 4})},
		},

		// PtrHeadFloat32NilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadFloat32NilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A float32 `json:"a"`
					B float32 `json:"b"`
				}
				B *struct {
					A float32 `json:"a"`
					B float32 `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadFloat32NilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A float32 `json:"a,omitempty"`
					B float32 `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A float32 `json:"a,omitempty"`
					B float32 `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadFloat32NilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A float32 `json:"a,string"`
					B float32 `json:"b,string"`
				}
				B *struct {
					A float32 `json:"a,string"`
					B float32 `json:"b,string"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadFloat32NilDoubleMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A *struct {
					A float32 `json:"a,string,omitempty"`
					B float32 `json:"b,string,omitempty"`
				}
				B *struct {
					A float32 `json:"a,string,omitempty"`
					B float32 `json:"b,string,omitempty"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadFloat32NilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadFloat32NilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A float32 `json:"a"`
					B float32 `json:"b"`
				}
				B *struct {
					A float32 `json:"a"`
					B float32 `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadFloat32NilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A float32 `json:"a,omitempty"`
					B float32 `json:"b,omitempty"`
				}
				B *struct {
					A float32 `json:"a,omitempty"`
					B float32 `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadFloat32NilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A float32 `json:"a,string"`
					B float32 `json:"b,string"`
				}
				B *struct {
					A float32 `json:"a,string"`
					B float32 `json:"b,string"`
				}
			})(nil),
		},
		{
			name: "PtrHeadFloat32NilDoubleMultiFieldsNotRootStringOmitEmpty",
			data: (*struct {
				A *struct {
					A float32 `json:"a,string,omitempty"`
					B float32 `json:"b,string,omitempty"`
				}
				B *struct {
					A float32 `json:"a,string,omitempty"`
					B float32 `json:"b,string,omitempty"`
				}
			})(nil),
		},

		// PtrHeadFloat32PtrDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadFloat32PtrDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *float32 `json:"a"`
					B *float32 `json:"b"`
				}
				B *struct {
					A *float32 `json:"a"`
					B *float32 `json:"b"`
				}
			}{A: &(struct {
				A *float32 `json:"a"`
				B *float32 `json:"b"`
			}{A: float32ptr(1), B: float32ptr(2)}), B: &(struct {
				A *float32 `json:"a"`
				B *float32 `json:"b"`
			}{A: float32ptr(3), B: float32ptr(4)})},
		},
		{
			name: "PtrHeadFloat32PtrDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *float32 `json:"a,omitempty"`
					B *float32 `json:"b,omitempty"`
				}
				B *struct {
					A *float32 `json:"a,omitempty"`
					B *float32 `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *float32 `json:"a,omitempty"`
				B *float32 `json:"b,omitempty"`
			}{A: float32ptr(1), B: float32ptr(2)}), B: &(struct {
				A *float32 `json:"a,omitempty"`
				B *float32 `json:"b,omitempty"`
			}{A: float32ptr(3), B: float32ptr(4)})},
		},
		{
			name: "PtrHeadFloat32PtrDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *float32 `json:"a,string"`
					B *float32 `json:"b,string"`
				}
				B *struct {
					A *float32 `json:"a,string"`
					B *float32 `json:"b,string"`
				}
			}{A: &(struct {
				A *float32 `json:"a,string"`
				B *float32 `json:"b,string"`
			}{A: float32ptr(1), B: float32ptr(2)}), B: &(struct {
				A *float32 `json:"a,string"`
				B *float32 `json:"b,string"`
			}{A: float32ptr(3), B: float32ptr(4)})},
		},
		{
			name: "PtrHeadFloat32PtrDoubleMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A *struct {
					A *float32 `json:"a,string,omitempty"`
					B *float32 `json:"b,string,omitempty"`
				}
				B *struct {
					A *float32 `json:"a,string,omitempty"`
					B *float32 `json:"b,string,omitempty"`
				}
			}{A: &(struct {
				A *float32 `json:"a,string,omitempty"`
				B *float32 `json:"b,string,omitempty"`
			}{A: float32ptr(1), B: float32ptr(2)}), B: &(struct {
				A *float32 `json:"a,string,omitempty"`
				B *float32 `json:"b,string,omitempty"`
			}{A: float32ptr(3), B: float32ptr(4)})},
		},

		// PtrHeadFloat32PtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadFloat32PtrNilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *float32 `json:"a"`
					B *float32 `json:"b"`
				}
				B *struct {
					A *float32 `json:"a"`
					B *float32 `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadFloat32PtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *float32 `json:"a,omitempty"`
					B *float32 `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A *float32 `json:"a,omitempty"`
					B *float32 `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadFloat32PtrNilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *float32 `json:"a,string"`
					B *float32 `json:"b,string"`
				}
				B *struct {
					A *float32 `json:"a,string"`
					B *float32 `json:"b,string"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadFloat32PtrNilDoubleMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A *struct {
					A *float32 `json:"a,string,omitempty"`
					B *float32 `json:"b,string,omitempty"`
				}
				B *struct {
					A *float32 `json:"a,string,omitempty"`
					B *float32 `json:"b,string,omitempty"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadFloat32PtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadFloat32PtrNilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *float32 `json:"a"`
					B *float32 `json:"b"`
				}
				B *struct {
					A *float32 `json:"a"`
					B *float32 `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadFloat32PtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *float32 `json:"a,omitempty"`
					B *float32 `json:"b,omitempty"`
				}
				B *struct {
					A *float32 `json:"a,omitempty"`
					B *float32 `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadFloat32PtrNilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *float32 `json:"a,string"`
					B *float32 `json:"b,string"`
				}
				B *struct {
					A *float32 `json:"a,string"`
					B *float32 `json:"b,string"`
				}
			})(nil),
		},
		{
			name: "PtrHeadFloat32PtrNilDoubleMultiFieldsNotRootStringOmitEmpty",
			data: (*struct {
				A *struct {
					A *float32 `json:"a,string,omitempty"`
					B *float32 `json:"b,string,omitempty"`
				}
				B *struct {
					A *float32 `json:"a,string,omitempty"`
					B *float32 `json:"b,string,omitempty"`
				}
			})(nil),
		},

		// AnonymousHeadFloat32
		{
			name: "AnonymousHeadFloat32",
			data: struct {
				structFloat32
				B float32 `json:"b"`
			}{
				structFloat32: structFloat32{A: 1},
				B:             2,
			},
		},
		{
			name: "AnonymousHeadFloat32OmitEmpty",
			data: struct {
				structFloat32OmitEmpty
				B float32 `json:"b,omitempty"`
			}{
				structFloat32OmitEmpty: structFloat32OmitEmpty{A: 1},
				B:                      2,
			},
		},
		{
			name: "AnonymousHeadFloat32String",
			data: struct {
				structFloat32String
				B float32 `json:"b,string"`
			}{
				structFloat32String: structFloat32String{A: 1},
				B:                   2,
			},
		},
		{
			name: "AnonymousHeadFloat32StringOmitEmpty",
			data: struct {
				structFloat32StringOmitEmpty
				B float32 `json:"b,string,omitempty"`
			}{
				structFloat32StringOmitEmpty: structFloat32StringOmitEmpty{A: 1},
				B:                            2,
			},
		},

		// PtrAnonymousHeadFloat32
		{
			name: "PtrAnonymousHeadFloat32",
			data: struct {
				*structFloat32
				B float32 `json:"b"`
			}{
				structFloat32: &structFloat32{A: 1},
				B:             2,
			},
		},
		{
			name: "PtrAnonymousHeadFloat32OmitEmpty",
			data: struct {
				*structFloat32OmitEmpty
				B float32 `json:"b,omitempty"`
			}{
				structFloat32OmitEmpty: &structFloat32OmitEmpty{A: 1},
				B:                      2,
			},
		},
		{
			name: "PtrAnonymousHeadFloat32String",
			data: struct {
				*structFloat32String
				B float32 `json:"b,string"`
			}{
				structFloat32String: &structFloat32String{A: 1},
				B:                   2,
			},
		},
		{
			name: "PtrAnonymousHeadFloat32StringOmitEmpty",
			data: struct {
				*structFloat32StringOmitEmpty
				B float32 `json:"b,string,omitempty"`
			}{
				structFloat32StringOmitEmpty: &structFloat32StringOmitEmpty{A: 1},
				B:                            2,
			},
		},

		// NilPtrAnonymousHeadFloat32
		{
			name: "NilPtrAnonymousHeadFloat32",
			data: struct {
				*structFloat32
				B float32 `json:"b"`
			}{
				structFloat32: nil,
				B:             2,
			},
		},
		{
			name: "NilPtrAnonymousHeadFloat32OmitEmpty",
			data: struct {
				*structFloat32OmitEmpty
				B float32 `json:"b,omitempty"`
			}{
				structFloat32OmitEmpty: nil,
				B:                      2,
			},
		},
		{
			name: "NilPtrAnonymousHeadFloat32String",
			data: struct {
				*structFloat32String
				B float32 `json:"b,string"`
			}{
				structFloat32String: nil,
				B:                   2,
			},
		},
		{
			name: "NilPtrAnonymousHeadFloat32StringOmitEmpty",
			data: struct {
				*structFloat32StringOmitEmpty
				B float32 `json:"b,string,omitempty"`
			}{
				structFloat32StringOmitEmpty: nil,
				B:                            2,
			},
		},

		// AnonymousHeadFloat32Ptr
		{
			name: "AnonymousHeadFloat32Ptr",
			data: struct {
				structFloat32Ptr
				B *float32 `json:"b"`
			}{
				structFloat32Ptr: structFloat32Ptr{A: float32ptr(1)},
				B:                float32ptr(2),
			},
		},
		{
			name: "AnonymousHeadFloat32PtrOmitEmpty",
			data: struct {
				structFloat32PtrOmitEmpty
				B *float32 `json:"b,omitempty"`
			}{
				structFloat32PtrOmitEmpty: structFloat32PtrOmitEmpty{A: float32ptr(1)},
				B:                         float32ptr(2),
			},
		},
		{
			name: "AnonymousHeadFloat32PtrString",
			data: struct {
				structFloat32PtrString
				B *float32 `json:"b,string"`
			}{
				structFloat32PtrString: structFloat32PtrString{A: float32ptr(1)},
				B:                      float32ptr(2),
			},
		},
		{
			name: "AnonymousHeadFloat32PtrStringOmitEmpty",
			data: struct {
				structFloat32PtrStringOmitEmpty
				B *float32 `json:"b,string,omitempty"`
			}{
				structFloat32PtrStringOmitEmpty: structFloat32PtrStringOmitEmpty{A: float32ptr(1)},
				B:                               float32ptr(2),
			},
		},

		// AnonymousHeadFloat32PtrNil
		{
			name: "AnonymousHeadFloat32PtrNil",
			data: struct {
				structFloat32Ptr
				B *float32 `json:"b"`
			}{
				structFloat32Ptr: structFloat32Ptr{A: nil},
				B:                float32ptr(2),
			},
		},
		{
			name: "AnonymousHeadFloat32PtrNilOmitEmpty",
			data: struct {
				structFloat32PtrOmitEmpty
				B *float32 `json:"b,omitempty"`
			}{
				structFloat32PtrOmitEmpty: structFloat32PtrOmitEmpty{A: nil},
				B:                         float32ptr(2),
			},
		},
		{
			name: "AnonymousHeadFloat32PtrNilString",
			data: struct {
				structFloat32PtrString
				B *float32 `json:"b,string"`
			}{
				structFloat32PtrString: structFloat32PtrString{A: nil},
				B:                      float32ptr(2),
			},
		},
		{
			name: "AnonymousHeadFloat32PtrNilStringOmitEmpty",
			data: struct {
				structFloat32PtrStringOmitEmpty
				B *float32 `json:"b,string,omitempty"`
			}{
				structFloat32PtrStringOmitEmpty: structFloat32PtrStringOmitEmpty{A: nil},
				B:                               float32ptr(2),
			},
		},

		// PtrAnonymousHeadFloat32Ptr
		{
			name: "PtrAnonymousHeadFloat32Ptr",
			data: struct {
				*structFloat32Ptr
				B *float32 `json:"b"`
			}{
				structFloat32Ptr: &structFloat32Ptr{A: float32ptr(1)},
				B:                float32ptr(2),
			},
		},
		{
			name: "PtrAnonymousHeadFloat32PtrOmitEmpty",
			data: struct {
				*structFloat32PtrOmitEmpty
				B *float32 `json:"b,omitempty"`
			}{
				structFloat32PtrOmitEmpty: &structFloat32PtrOmitEmpty{A: float32ptr(1)},
				B:                         float32ptr(2),
			},
		},
		{
			name: "PtrAnonymousHeadFloat32PtrString",
			data: struct {
				*structFloat32PtrString
				B *float32 `json:"b,string"`
			}{
				structFloat32PtrString: &structFloat32PtrString{A: float32ptr(1)},
				B:                      float32ptr(2),
			},
		},
		{
			name: "PtrAnonymousHeadFloat32PtrStringOmitEmpty",
			data: struct {
				*structFloat32PtrStringOmitEmpty
				B *float32 `json:"b,string,omitempty"`
			}{
				structFloat32PtrStringOmitEmpty: &structFloat32PtrStringOmitEmpty{A: float32ptr(1)},
				B:                               float32ptr(2),
			},
		},

		// NilPtrAnonymousHeadFloat32Ptr
		{
			name: "NilPtrAnonymousHeadFloat32Ptr",
			data: struct {
				*structFloat32Ptr
				B *float32 `json:"b"`
			}{
				structFloat32Ptr: nil,
				B:                float32ptr(2),
			},
		},
		{
			name: "NilPtrAnonymousHeadFloat32PtrOmitEmpty",
			data: struct {
				*structFloat32PtrOmitEmpty
				B *float32 `json:"b,omitempty"`
			}{
				structFloat32PtrOmitEmpty: nil,
				B:                         float32ptr(2),
			},
		},
		{
			name: "NilPtrAnonymousHeadFloat32PtrString",
			data: struct {
				*structFloat32PtrString
				B *float32 `json:"b,string"`
			}{
				structFloat32PtrString: nil,
				B:                      float32ptr(2),
			},
		},
		{
			name: "NilPtrAnonymousHeadFloat32PtrStringOmitEmpty",
			data: struct {
				*structFloat32PtrStringOmitEmpty
				B *float32 `json:"b,string,omitempty"`
			}{
				structFloat32PtrStringOmitEmpty: nil,
				B:                               float32ptr(2),
			},
		},

		// AnonymousHeadFloat32Only
		{
			name: "AnonymousHeadFloat32Only",
			data: struct {
				structFloat32
			}{
				structFloat32: structFloat32{A: 1},
			},
		},
		{
			name: "AnonymousHeadFloat32OnlyOmitEmpty",
			data: struct {
				structFloat32OmitEmpty
			}{
				structFloat32OmitEmpty: structFloat32OmitEmpty{A: 1},
			},
		},
		{
			name: "AnonymousHeadFloat32OnlyString",
			data: struct {
				structFloat32String
			}{
				structFloat32String: structFloat32String{A: 1},
			},
		},
		{
			name: "AnonymousHeadFloat32OnlyStringOmitEmpty",
			data: struct {
				structFloat32StringOmitEmpty
			}{
				structFloat32StringOmitEmpty: structFloat32StringOmitEmpty{A: 1},
			},
		},

		// PtrAnonymousHeadFloat32Only
		{
			name: "PtrAnonymousHeadFloat32Only",
			data: struct {
				*structFloat32
			}{
				structFloat32: &structFloat32{A: 1},
			},
		},
		{
			name: "PtrAnonymousHeadFloat32OnlyOmitEmpty",
			data: struct {
				*structFloat32OmitEmpty
			}{
				structFloat32OmitEmpty: &structFloat32OmitEmpty{A: 1},
			},
		},
		{
			name: "PtrAnonymousHeadFloat32OnlyString",
			data: struct {
				*structFloat32String
			}{
				structFloat32String: &structFloat32String{A: 1},
			},
		},
		{
			name: "PtrAnonymousHeadFloat32OnlyStringOmitEmpty",
			data: struct {
				*structFloat32StringOmitEmpty
			}{
				structFloat32StringOmitEmpty: &structFloat32StringOmitEmpty{A: 1},
			},
		},

		// NilPtrAnonymousHeadFloat32Only
		{
			name: "NilPtrAnonymousHeadFloat32Only",
			data: struct {
				*structFloat32
			}{
				structFloat32: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadFloat32OnlyOmitEmpty",
			data: struct {
				*structFloat32OmitEmpty
			}{
				structFloat32OmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadFloat32OnlyString",
			data: struct {
				*structFloat32String
			}{
				structFloat32String: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadFloat32OnlyStringOmitEmpty",
			data: struct {
				*structFloat32StringOmitEmpty
			}{
				structFloat32StringOmitEmpty: nil,
			},
		},

		// AnonymousHeadFloat32PtrOnly
		{
			name: "AnonymousHeadFloat32PtrOnly",
			data: struct {
				structFloat32Ptr
			}{
				structFloat32Ptr: structFloat32Ptr{A: float32ptr(1)},
			},
		},
		{
			name: "AnonymousHeadFloat32PtrOnlyOmitEmpty",
			data: struct {
				structFloat32PtrOmitEmpty
			}{
				structFloat32PtrOmitEmpty: structFloat32PtrOmitEmpty{A: float32ptr(1)},
			},
		},
		{
			name: "AnonymousHeadFloat32PtrOnlyString",
			data: struct {
				structFloat32PtrString
			}{
				structFloat32PtrString: structFloat32PtrString{A: float32ptr(1)},
			},
		},
		{
			name: "AnonymousHeadFloat32PtrOnlyStringOmitEmpty",
			data: struct {
				structFloat32PtrStringOmitEmpty
			}{
				structFloat32PtrStringOmitEmpty: structFloat32PtrStringOmitEmpty{A: float32ptr(1)},
			},
		},

		// AnonymousHeadFloat32PtrNilOnly
		{
			name: "AnonymousHeadFloat32PtrNilOnly",
			data: struct {
				structFloat32Ptr
			}{
				structFloat32Ptr: structFloat32Ptr{A: nil},
			},
		},
		{
			name: "AnonymousHeadFloat32PtrNilOnlyOmitEmpty",
			data: struct {
				structFloat32PtrOmitEmpty
			}{
				structFloat32PtrOmitEmpty: structFloat32PtrOmitEmpty{A: nil},
			},
		},
		{
			name: "AnonymousHeadFloat32PtrNilOnlyString",
			data: struct {
				structFloat32PtrString
			}{
				structFloat32PtrString: structFloat32PtrString{A: nil},
			},
		},
		{
			name: "AnonymousHeadFloat32PtrNilOnlyStringOmitEmpty",
			data: struct {
				structFloat32PtrStringOmitEmpty
			}{
				structFloat32PtrStringOmitEmpty: structFloat32PtrStringOmitEmpty{A: nil},
			},
		},

		// PtrAnonymousHeadFloat32PtrOnly
		{
			name: "PtrAnonymousHeadFloat32PtrOnly",
			data: struct {
				*structFloat32Ptr
			}{
				structFloat32Ptr: &structFloat32Ptr{A: float32ptr(1)},
			},
		},
		{
			name: "PtrAnonymousHeadFloat32PtrOnlyOmitEmpty",
			data: struct {
				*structFloat32PtrOmitEmpty
			}{
				structFloat32PtrOmitEmpty: &structFloat32PtrOmitEmpty{A: float32ptr(1)},
			},
		},
		{
			name: "PtrAnonymousHeadFloat32PtrOnlyString",
			data: struct {
				*structFloat32PtrString
			}{
				structFloat32PtrString: &structFloat32PtrString{A: float32ptr(1)},
			},
		},
		{
			name: "PtrAnonymousHeadFloat32PtrOnlyStringOmitEmpty",
			data: struct {
				*structFloat32PtrStringOmitEmpty
			}{
				structFloat32PtrStringOmitEmpty: &structFloat32PtrStringOmitEmpty{A: float32ptr(1)},
			},
		},

		// NilPtrAnonymousHeadFloat32PtrOnly
		{
			name: "NilPtrAnonymousHeadFloat32PtrOnly",
			data: struct {
				*structFloat32Ptr
			}{
				structFloat32Ptr: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadFloat32PtrOnlyOmitEmpty",
			data: struct {
				*structFloat32PtrOmitEmpty
			}{
				structFloat32PtrOmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadFloat32PtrOnlyString",
			data: struct {
				*structFloat32PtrString
			}{
				structFloat32PtrString: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadFloat32PtrOnlyStringOmitEmpty",
			data: struct {
				*structFloat32PtrStringOmitEmpty
			}{
				structFloat32PtrStringOmitEmpty: nil,
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
