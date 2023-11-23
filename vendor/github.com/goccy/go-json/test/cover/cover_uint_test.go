package json_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/goccy/go-json"
)

func TestCoverUint(t *testing.T) {
	type structUint struct {
		A uint `json:"a"`
	}
	type structUintOmitEmpty struct {
		A uint `json:"a,omitempty"`
	}
	type structUintString struct {
		A uint `json:"a,string"`
	}
	type structUintStringOmitEmpty struct {
		A uint `json:"a,string,omitempty"`
	}

	type structUintPtr struct {
		A *uint `json:"a"`
	}
	type structUintPtrOmitEmpty struct {
		A *uint `json:"a,omitempty"`
	}
	type structUintPtrString struct {
		A *uint `json:"a,string"`
	}
	type structUintPtrStringOmitEmpty struct {
		A *uint `json:"a,string,omitempty"`
	}

	tests := []struct {
		name string
		data interface{}
	}{
		{
			name: "Uint",
			data: uint(10),
		},
		{
			name: "UintPtr",
			data: uptr(10),
		},
		{
			name: "UintPtr3",
			data: uintptr3(10),
		},
		{
			name: "UintPtrNil",
			data: (*uint)(nil),
		},
		{
			name: "UintPtr3Nil",
			data: (***uint)(nil),
		},

		// HeadUintZero
		{
			name: "HeadUintZero",
			data: struct {
				A uint `json:"a"`
			}{},
		},
		{
			name: "HeadUintZeroOmitEmpty",
			data: struct {
				A uint `json:"a,omitempty"`
			}{},
		},
		{
			name: "HeadUintZeroString",
			data: struct {
				A uint `json:"a,string"`
			}{},
		},
		{
			name: "HeadUintZeroStringOmitEmpty",
			data: struct {
				A uint `json:"a,string,omitempty"`
			}{},
		},

		// HeadUint
		{
			name: "HeadUint",
			data: struct {
				A uint `json:"a"`
			}{A: 1},
		},
		{
			name: "HeadUintOmitEmpty",
			data: struct {
				A uint `json:"a,omitempty"`
			}{A: 1},
		},
		{
			name: "HeadUintString",
			data: struct {
				A uint `json:"a,string"`
			}{A: 1},
		},
		{
			name: "HeadUintStringOmitEmpty",
			data: struct {
				A uint `json:"a,string,omitempty"`
			}{A: 1},
		},

		// HeadUintPtr
		{
			name: "HeadUintPtr",
			data: struct {
				A *uint `json:"a"`
			}{A: uptr(1)},
		},
		{
			name: "HeadUintPtrOmitEmpty",
			data: struct {
				A *uint `json:"a,omitempty"`
			}{A: uptr(1)},
		},
		{
			name: "HeadUintPtrString",
			data: struct {
				A *uint `json:"a,string"`
			}{A: uptr(1)},
		},
		{
			name: "HeadUintPtrStringOmitEmpty",
			data: struct {
				A *uint `json:"a,string,omitempty"`
			}{A: uptr(1)},
		},

		// HeadUintPtrNil
		{
			name: "HeadUintPtrNil",
			data: struct {
				A *uint `json:"a"`
			}{A: nil},
		},
		{
			name: "HeadUintPtrNilOmitEmpty",
			data: struct {
				A *uint `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "HeadUintPtrNilString",
			data: struct {
				A *uint `json:"a,string"`
			}{A: nil},
		},
		{
			name: "HeadUintPtrNilStringOmitEmpty",
			data: struct {
				A *uint `json:"a,string,omitempty"`
			}{A: nil},
		},

		// PtrHeadUintZero
		{
			name: "PtrHeadUintZero",
			data: &struct {
				A uint `json:"a"`
			}{},
		},
		{
			name: "PtrHeadUintZeroOmitEmpty",
			data: &struct {
				A uint `json:"a,omitempty"`
			}{},
		},
		{
			name: "PtrHeadUintZeroString",
			data: &struct {
				A uint `json:"a,string"`
			}{},
		},
		{
			name: "PtrHeadUintZeroStringOmitEmpty",
			data: &struct {
				A uint `json:"a,string,omitempty"`
			}{},
		},

		// PtrHeadUint
		{
			name: "PtrHeadUint",
			data: &struct {
				A uint `json:"a"`
			}{A: 1},
		},
		{
			name: "PtrHeadUintOmitEmpty",
			data: &struct {
				A uint `json:"a,omitempty"`
			}{A: 1},
		},
		{
			name: "PtrHeadUintString",
			data: &struct {
				A uint `json:"a,string"`
			}{A: 1},
		},
		{
			name: "PtrHeadUintStringOmitEmpty",
			data: &struct {
				A uint `json:"a,string,omitempty"`
			}{A: 1},
		},

		// PtrHeadUintPtr
		{
			name: "PtrHeadUintPtr",
			data: &struct {
				A *uint `json:"a"`
			}{A: uptr(1)},
		},
		{
			name: "PtrHeadUintPtrOmitEmpty",
			data: &struct {
				A *uint `json:"a,omitempty"`
			}{A: uptr(1)},
		},
		{
			name: "PtrHeadUintPtrString",
			data: &struct {
				A *uint `json:"a,string"`
			}{A: uptr(1)},
		},
		{
			name: "PtrHeadUintPtrStringOmitEmpty",
			data: &struct {
				A *uint `json:"a,string,omitempty"`
			}{A: uptr(1)},
		},

		// PtrHeadUintPtrNil
		{
			name: "PtrHeadUintPtrNil",
			data: &struct {
				A *uint `json:"a"`
			}{A: nil},
		},
		{
			name: "PtrHeadUintPtrNilOmitEmpty",
			data: &struct {
				A *uint `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadUintPtrNilString",
			data: &struct {
				A *uint `json:"a,string"`
			}{A: nil},
		},
		{
			name: "PtrHeadUintPtrNilStringOmitEmpty",
			data: &struct {
				A *uint `json:"a,string,omitempty"`
			}{A: nil},
		},

		// PtrHeadUintNil
		{
			name: "PtrHeadUintNil",
			data: (*struct {
				A *uint `json:"a"`
			})(nil),
		},
		{
			name: "PtrHeadUintNilOmitEmpty",
			data: (*struct {
				A *uint `json:"a,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadUintNilString",
			data: (*struct {
				A *uint `json:"a,string"`
			})(nil),
		},
		{
			name: "PtrHeadUintNilStringOmitEmpty",
			data: (*struct {
				A *uint `json:"a,string,omitempty"`
			})(nil),
		},

		// HeadUintZeroMultiFields
		{
			name: "HeadUintZeroMultiFields",
			data: struct {
				A uint `json:"a"`
				B uint `json:"b"`
				C uint `json:"c"`
			}{},
		},
		{
			name: "HeadUintZeroMultiFieldsOmitEmpty",
			data: struct {
				A uint `json:"a,omitempty"`
				B uint `json:"b,omitempty"`
				C uint `json:"c,omitempty"`
			}{},
		},
		{
			name: "HeadUintZeroMultiFieldsString",
			data: struct {
				A uint `json:"a,string"`
				B uint `json:"b,string"`
				C uint `json:"c,string"`
			}{},
		},
		{
			name: "HeadUintZeroMultiFieldsStringOmitEmpty",
			data: struct {
				A uint `json:"a,string,omitempty"`
				B uint `json:"b,string,omitempty"`
				C uint `json:"c,string,omitempty"`
			}{},
		},

		// HeadUintMultiFields
		{
			name: "HeadUintMultiFields",
			data: struct {
				A uint `json:"a"`
				B uint `json:"b"`
				C uint `json:"c"`
			}{A: 1, B: 2, C: 3},
		},
		{
			name: "HeadUintMultiFieldsOmitEmpty",
			data: struct {
				A uint `json:"a,omitempty"`
				B uint `json:"b,omitempty"`
				C uint `json:"c,omitempty"`
			}{A: 1, B: 2, C: 3},
		},
		{
			name: "HeadUintMultiFieldsString",
			data: struct {
				A uint `json:"a,string"`
				B uint `json:"b,string"`
				C uint `json:"c,string"`
			}{A: 1, B: 2, C: 3},
		},
		{
			name: "HeadUintMultiFieldsStringOmitEmpty",
			data: struct {
				A uint `json:"a,string,omitempty"`
				B uint `json:"b,string,omitempty"`
				C uint `json:"c,string,omitempty"`
			}{A: 1, B: 2, C: 3},
		},

		// HeadUintPtrMultiFields
		{
			name: "HeadUintPtrMultiFields",
			data: struct {
				A *uint `json:"a"`
				B *uint `json:"b"`
				C *uint `json:"c"`
			}{A: uptr(1), B: uptr(2), C: uptr(3)},
		},
		{
			name: "HeadUintPtrMultiFieldsOmitEmpty",
			data: struct {
				A *uint `json:"a,omitempty"`
				B *uint `json:"b,omitempty"`
				C *uint `json:"c,omitempty"`
			}{A: uptr(1), B: uptr(2), C: uptr(3)},
		},
		{
			name: "HeadUintPtrMultiFieldsString",
			data: struct {
				A *uint `json:"a,string"`
				B *uint `json:"b,string"`
				C *uint `json:"c,string"`
			}{A: uptr(1), B: uptr(2), C: uptr(3)},
		},
		{
			name: "HeadUintPtrMultiFieldsStringOmitEmpty",
			data: struct {
				A *uint `json:"a,string,omitempty"`
				B *uint `json:"b,string,omitempty"`
				C *uint `json:"c,string,omitempty"`
			}{A: uptr(1), B: uptr(2), C: uptr(3)},
		},

		// HeadUintPtrNilMultiFields
		{
			name: "HeadUintPtrNilMultiFields",
			data: struct {
				A *uint `json:"a"`
				B *uint `json:"b"`
				C *uint `json:"c"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadUintPtrNilMultiFieldsOmitEmpty",
			data: struct {
				A *uint `json:"a,omitempty"`
				B *uint `json:"b,omitempty"`
				C *uint `json:"c,omitempty"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadUintPtrNilMultiFieldsString",
			data: struct {
				A *uint `json:"a,string"`
				B *uint `json:"b,string"`
				C *uint `json:"c,string"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadUintPtrNilMultiFieldsStringOmitEmpty",
			data: struct {
				A *uint `json:"a,string,omitempty"`
				B *uint `json:"b,string,omitempty"`
				C *uint `json:"c,string,omitempty"`
			}{A: nil, B: nil, C: nil},
		},

		// PtrHeadUintZeroMultiFields
		{
			name: "PtrHeadUintZeroMultiFields",
			data: &struct {
				A uint `json:"a"`
				B uint `json:"b"`
			}{},
		},
		{
			name: "PtrHeadUintZeroMultiFieldsOmitEmpty",
			data: &struct {
				A uint `json:"a,omitempty"`
				B uint `json:"b,omitempty"`
			}{},
		},
		{
			name: "PtrHeadUintZeroMultiFieldsString",
			data: &struct {
				A uint `json:"a,string"`
				B uint `json:"b,string"`
			}{},
		},
		{
			name: "PtrHeadUintZeroMultiFieldsStringOmitEmpty",
			data: &struct {
				A uint `json:"a,string,omitempty"`
				B uint `json:"b,string,omitempty"`
			}{},
		},

		// PtrHeadUintMultiFields
		{
			name: "PtrHeadUintMultiFields",
			data: &struct {
				A uint `json:"a"`
				B uint `json:"b"`
			}{A: 1, B: 2},
		},
		{
			name: "PtrHeadUintMultiFieldsOmitEmpty",
			data: &struct {
				A uint `json:"a,omitempty"`
				B uint `json:"b,omitempty"`
			}{A: 1, B: 2},
		},
		{
			name: "PtrHeadUintMultiFieldsString",
			data: &struct {
				A uint `json:"a,string"`
				B uint `json:"b,string"`
			}{A: 1, B: 2},
		},
		{
			name: "PtrHeadUintMultiFieldsStringOmitEmpty",
			data: &struct {
				A uint `json:"a,string,omitempty"`
				B uint `json:"b,string,omitempty"`
			}{A: 1, B: 2},
		},

		// PtrHeadUintPtrMultiFields
		{
			name: "PtrHeadUintPtrMultiFields",
			data: &struct {
				A *uint `json:"a"`
				B *uint `json:"b"`
			}{A: uptr(1), B: uptr(2)},
		},
		{
			name: "PtrHeadUintPtrMultiFieldsOmitEmpty",
			data: &struct {
				A *uint `json:"a,omitempty"`
				B *uint `json:"b,omitempty"`
			}{A: uptr(1), B: uptr(2)},
		},
		{
			name: "PtrHeadUintPtrMultiFieldsString",
			data: &struct {
				A *uint `json:"a,string"`
				B *uint `json:"b,string"`
			}{A: uptr(1), B: uptr(2)},
		},
		{
			name: "PtrHeadUintPtrMultiFieldsStringOmitEmpty",
			data: &struct {
				A *uint `json:"a,string,omitempty"`
				B *uint `json:"b,string,omitempty"`
			}{A: uptr(1), B: uptr(2)},
		},

		// PtrHeadUintPtrNilMultiFields
		{
			name: "PtrHeadUintPtrNilMultiFields",
			data: &struct {
				A *uint `json:"a"`
				B *uint `json:"b"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUintPtrNilMultiFieldsOmitEmpty",
			data: &struct {
				A *uint `json:"a,omitempty"`
				B *uint `json:"b,omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUintPtrNilMultiFieldsString",
			data: &struct {
				A *uint `json:"a,string"`
				B *uint `json:"b,string"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUintPtrNilMultiFieldsStringOmitEmpty",
			data: &struct {
				A *uint `json:"a,string,omitempty"`
				B *uint `json:"b,string,omitempty"`
			}{A: nil, B: nil},
		},

		// PtrHeadUintNilMultiFields
		{
			name: "PtrHeadUintNilMultiFields",
			data: (*struct {
				A *uint `json:"a"`
				B *uint `json:"b"`
			})(nil),
		},
		{
			name: "PtrHeadUintNilMultiFieldsOmitEmpty",
			data: (*struct {
				A *uint `json:"a,omitempty"`
				B *uint `json:"b,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadUintNilMultiFieldsString",
			data: (*struct {
				A *uint `json:"a,string"`
				B *uint `json:"b,string"`
			})(nil),
		},
		{
			name: "PtrHeadUintNilMultiFieldsStringOmitEmpty",
			data: (*struct {
				A *uint `json:"a,string,omitempty"`
				B *uint `json:"b,string,omitempty"`
			})(nil),
		},

		// HeadUintZeroNotRoot
		{
			name: "HeadUintZeroNotRoot",
			data: struct {
				A struct {
					A uint `json:"a"`
				}
			}{},
		},
		{
			name: "HeadUintZeroNotRootOmitEmpty",
			data: struct {
				A struct {
					A uint `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadUintZeroNotRootString",
			data: struct {
				A struct {
					A uint `json:"a,string"`
				}
			}{},
		},
		{
			name: "HeadUintZeroNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A uint `json:"a,string,omitempty"`
				}
			}{},
		},

		// HeadUintNotRoot
		{
			name: "HeadUintNotRoot",
			data: struct {
				A struct {
					A uint `json:"a"`
				}
			}{A: struct {
				A uint `json:"a"`
			}{A: 1}},
		},
		{
			name: "HeadUintNotRootOmitEmpty",
			data: struct {
				A struct {
					A uint `json:"a,omitempty"`
				}
			}{A: struct {
				A uint `json:"a,omitempty"`
			}{A: 1}},
		},
		{
			name: "HeadUintNotRootString",
			data: struct {
				A struct {
					A uint `json:"a,string"`
				}
			}{A: struct {
				A uint `json:"a,string"`
			}{A: 1}},
		},
		{
			name: "HeadUintNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A uint `json:"a,string,omitempty"`
				}
			}{A: struct {
				A uint `json:"a,string,omitempty"`
			}{A: 1}},
		},

		// HeadUintPtrNotRoot
		{
			name: "HeadUintPtrNotRoot",
			data: struct {
				A struct {
					A *uint `json:"a"`
				}
			}{A: struct {
				A *uint `json:"a"`
			}{uptr(1)}},
		},
		{
			name: "HeadUintPtrNotRootOmitEmpty",
			data: struct {
				A struct {
					A *uint `json:"a,omitempty"`
				}
			}{A: struct {
				A *uint `json:"a,omitempty"`
			}{uptr(1)}},
		},
		{
			name: "HeadUintPtrNotRootString",
			data: struct {
				A struct {
					A *uint `json:"a,string"`
				}
			}{A: struct {
				A *uint `json:"a,string"`
			}{uptr(1)}},
		},
		{
			name: "HeadUintPtrNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A *uint `json:"a,string,omitempty"`
				}
			}{A: struct {
				A *uint `json:"a,string,omitempty"`
			}{uptr(1)}},
		},

		// HeadUintPtrNilNotRoot
		{
			name: "HeadUintPtrNilNotRoot",
			data: struct {
				A struct {
					A *uint `json:"a"`
				}
			}{},
		},
		{
			name: "HeadUintPtrNilNotRootOmitEmpty",
			data: struct {
				A struct {
					A *uint `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadUintPtrNilNotRootString",
			data: struct {
				A struct {
					A *uint `json:"a,string"`
				}
			}{},
		},
		{
			name: "HeadUintPtrNilNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A *uint `json:"a,string,omitempty"`
				}
			}{},
		},

		// PtrHeadUintZeroNotRoot
		{
			name: "PtrHeadUintZeroNotRoot",
			data: struct {
				A *struct {
					A uint `json:"a"`
				}
			}{A: new(struct {
				A uint `json:"a"`
			})},
		},
		{
			name: "PtrHeadUintZeroNotRootOmitEmpty",
			data: struct {
				A *struct {
					A uint `json:"a,omitempty"`
				}
			}{A: new(struct {
				A uint `json:"a,omitempty"`
			})},
		},
		{
			name: "PtrHeadUintZeroNotRootString",
			data: struct {
				A *struct {
					A uint `json:"a,string"`
				}
			}{A: new(struct {
				A uint `json:"a,string"`
			})},
		},
		{
			name: "PtrHeadUintZeroNotRootStringOmitEmpty",
			data: struct {
				A *struct {
					A uint `json:"a,string,omitempty"`
				}
			}{A: new(struct {
				A uint `json:"a,string,omitempty"`
			})},
		},

		// PtrHeadUintNotRoot
		{
			name: "PtrHeadUintNotRoot",
			data: struct {
				A *struct {
					A uint `json:"a"`
				}
			}{A: &(struct {
				A uint `json:"a"`
			}{A: 1})},
		},
		{
			name: "PtrHeadUintNotRootOmitEmpty",
			data: struct {
				A *struct {
					A uint `json:"a,omitempty"`
				}
			}{A: &(struct {
				A uint `json:"a,omitempty"`
			}{A: 1})},
		},
		{
			name: "PtrHeadUintNotRootString",
			data: struct {
				A *struct {
					A uint `json:"a,string"`
				}
			}{A: &(struct {
				A uint `json:"a,string"`
			}{A: 1})},
		},
		{
			name: "PtrHeadUintNotRootStringOmitEmpty",
			data: struct {
				A *struct {
					A uint `json:"a,string,omitempty"`
				}
			}{A: &(struct {
				A uint `json:"a,string,omitempty"`
			}{A: 1})},
		},

		// PtrHeadUintPtrNotRoot
		{
			name: "PtrHeadUintPtrNotRoot",
			data: struct {
				A *struct {
					A *uint `json:"a"`
				}
			}{A: &(struct {
				A *uint `json:"a"`
			}{A: uptr(1)})},
		},
		{
			name: "PtrHeadUintPtrNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *uint `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *uint `json:"a,omitempty"`
			}{A: uptr(1)})},
		},
		{
			name: "PtrHeadUintPtrNotRootString",
			data: struct {
				A *struct {
					A *uint `json:"a,string"`
				}
			}{A: &(struct {
				A *uint `json:"a,string"`
			}{A: uptr(1)})},
		},
		{
			name: "PtrHeadUintPtrNotRootStringOmitEmpty",
			data: struct {
				A *struct {
					A *uint `json:"a,string,omitempty"`
				}
			}{A: &(struct {
				A *uint `json:"a,string,omitempty"`
			}{A: uptr(1)})},
		},

		// PtrHeadUintPtrNilNotRoot
		{
			name: "PtrHeadUintPtrNilNotRoot",
			data: struct {
				A *struct {
					A *uint `json:"a"`
				}
			}{A: &(struct {
				A *uint `json:"a"`
			}{A: nil})},
		},
		{
			name: "PtrHeadUintPtrNilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *uint `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *uint `json:"a,omitempty"`
			}{A: nil})},
		},
		{
			name: "PtrHeadUintPtrNilNotRootString",
			data: struct {
				A *struct {
					A *uint `json:"a,string"`
				}
			}{A: &(struct {
				A *uint `json:"a,string"`
			}{A: nil})},
		},
		{
			name: "PtrHeadUintPtrNilNotRootStringOmitEmpty",
			data: struct {
				A *struct {
					A *uint `json:"a,string,omitempty"`
				}
			}{A: &(struct {
				A *uint `json:"a,string,omitempty"`
			}{A: nil})},
		},

		// PtrHeadUintNilNotRoot
		{
			name: "PtrHeadUintNilNotRoot",
			data: struct {
				A *struct {
					A *uint `json:"a"`
				}
			}{A: nil},
		},
		{
			name: "PtrHeadUintNilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *uint `json:"a,omitempty"`
				} `json:",omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadUintNilNotRootString",
			data: struct {
				A *struct {
					A *uint `json:"a,string"`
				} `json:",string"`
			}{A: nil},
		},
		{
			name: "PtrHeadUintNilNotRootStringOmitEmpty",
			data: struct {
				A *struct {
					A *uint `json:"a,string,omitempty"`
				} `json:",string,omitempty"`
			}{A: nil},
		},

		// HeadUintZeroMultiFieldsNotRoot
		{
			name: "HeadUintZeroMultiFieldsNotRoot",
			data: struct {
				A struct {
					A uint `json:"a"`
				}
				B struct {
					B uint `json:"b"`
				}
			}{},
		},
		{
			name: "HeadUintZeroMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A uint `json:"a,omitempty"`
				}
				B struct {
					B uint `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "HeadUintZeroMultiFieldsNotRootString",
			data: struct {
				A struct {
					A uint `json:"a,string"`
				}
				B struct {
					B uint `json:"b,string"`
				}
			}{},
		},
		{
			name: "HeadUintZeroMultiFieldsNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A uint `json:"a,string,omitempty"`
				}
				B struct {
					B uint `json:"b,string,omitempty"`
				}
			}{},
		},

		// HeadUintMultiFieldsNotRoot
		{
			name: "HeadUintMultiFieldsNotRoot",
			data: struct {
				A struct {
					A uint `json:"a"`
				}
				B struct {
					B uint `json:"b"`
				}
			}{A: struct {
				A uint `json:"a"`
			}{A: 1}, B: struct {
				B uint `json:"b"`
			}{B: 2}},
		},
		{
			name: "HeadUintMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A uint `json:"a,omitempty"`
				}
				B struct {
					B uint `json:"b,omitempty"`
				}
			}{A: struct {
				A uint `json:"a,omitempty"`
			}{A: 1}, B: struct {
				B uint `json:"b,omitempty"`
			}{B: 2}},
		},
		{
			name: "HeadUintMultiFieldsNotRootString",
			data: struct {
				A struct {
					A uint `json:"a,string"`
				}
				B struct {
					B uint `json:"b,string"`
				}
			}{A: struct {
				A uint `json:"a,string"`
			}{A: 1}, B: struct {
				B uint `json:"b,string"`
			}{B: 2}},
		},
		{
			name: "HeadUintMultiFieldsNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A uint `json:"a,string,omitempty"`
				}
				B struct {
					B uint `json:"b,string,omitempty"`
				}
			}{A: struct {
				A uint `json:"a,string,omitempty"`
			}{A: 1}, B: struct {
				B uint `json:"b,string,omitempty"`
			}{B: 2}},
		},

		// HeadUintPtrMultiFieldsNotRoot
		{
			name: "HeadUintPtrMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *uint `json:"a"`
				}
				B struct {
					B *uint `json:"b"`
				}
			}{A: struct {
				A *uint `json:"a"`
			}{A: uptr(1)}, B: struct {
				B *uint `json:"b"`
			}{B: uptr(2)}},
		},
		{
			name: "HeadUintPtrMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *uint `json:"a,omitempty"`
				}
				B struct {
					B *uint `json:"b,omitempty"`
				}
			}{A: struct {
				A *uint `json:"a,omitempty"`
			}{A: uptr(1)}, B: struct {
				B *uint `json:"b,omitempty"`
			}{B: uptr(2)}},
		},
		{
			name: "HeadUintPtrMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *uint `json:"a,string"`
				}
				B struct {
					B *uint `json:"b,string"`
				}
			}{A: struct {
				A *uint `json:"a,string"`
			}{A: uptr(1)}, B: struct {
				B *uint `json:"b,string"`
			}{B: uptr(2)}},
		},
		{
			name: "HeadUintPtrMultiFieldsNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A *uint `json:"a,string,omitempty"`
				}
				B struct {
					B *uint `json:"b,string,omitempty"`
				}
			}{A: struct {
				A *uint `json:"a,string,omitempty"`
			}{A: uptr(1)}, B: struct {
				B *uint `json:"b,string,omitempty"`
			}{B: uptr(2)}},
		},

		// HeadUintPtrNilMultiFieldsNotRoot
		{
			name: "HeadUintPtrNilMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *uint `json:"a"`
				}
				B struct {
					B *uint `json:"b"`
				}
			}{A: struct {
				A *uint `json:"a"`
			}{A: nil}, B: struct {
				B *uint `json:"b"`
			}{B: nil}},
		},
		{
			name: "HeadUintPtrNilMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *uint `json:"a,omitempty"`
				}
				B struct {
					B *uint `json:"b,omitempty"`
				}
			}{A: struct {
				A *uint `json:"a,omitempty"`
			}{A: nil}, B: struct {
				B *uint `json:"b,omitempty"`
			}{B: nil}},
		},
		{
			name: "HeadUintPtrNilMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *uint `json:"a,string"`
				}
				B struct {
					B *uint `json:"b,string"`
				}
			}{A: struct {
				A *uint `json:"a,string"`
			}{A: nil}, B: struct {
				B *uint `json:"b,string"`
			}{B: nil}},
		},
		{
			name: "HeadUintPtrNilMultiFieldsNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A *uint `json:"a,string,omitempty"`
				}
				B struct {
					B *uint `json:"b,string,omitempty"`
				}
			}{A: struct {
				A *uint `json:"a,string,omitempty"`
			}{A: nil}, B: struct {
				B *uint `json:"b,string,omitempty"`
			}{B: nil}},
		},

		// PtrHeadUintZeroMultiFieldsNotRoot
		{
			name: "PtrHeadUintZeroMultiFieldsNotRoot",
			data: &struct {
				A struct {
					A uint `json:"a"`
				}
				B struct {
					B uint `json:"b"`
				}
			}{},
		},
		{
			name: "PtrHeadUintZeroMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A uint `json:"a,omitempty"`
				}
				B struct {
					B uint `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "PtrHeadUintZeroMultiFieldsNotRootString",
			data: &struct {
				A struct {
					A uint `json:"a,string"`
				}
				B struct {
					B uint `json:"b,string"`
				}
			}{},
		},
		{
			name: "PtrHeadUintZeroMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A struct {
					A uint `json:"a,string,omitempty"`
				}
				B struct {
					B uint `json:"b,string,omitempty"`
				}
			}{},
		},

		// PtrHeadUintMultiFieldsNotRoot
		{
			name: "PtrHeadUintMultiFieldsNotRoot",
			data: &struct {
				A struct {
					A uint `json:"a"`
				}
				B struct {
					B uint `json:"b"`
				}
			}{A: struct {
				A uint `json:"a"`
			}{A: 1}, B: struct {
				B uint `json:"b"`
			}{B: 2}},
		},
		{
			name: "PtrHeadUintMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A uint `json:"a,omitempty"`
				}
				B struct {
					B uint `json:"b,omitempty"`
				}
			}{A: struct {
				A uint `json:"a,omitempty"`
			}{A: 1}, B: struct {
				B uint `json:"b,omitempty"`
			}{B: 2}},
		},
		{
			name: "PtrHeadUintMultiFieldsNotRootString",
			data: &struct {
				A struct {
					A uint `json:"a,string"`
				}
				B struct {
					B uint `json:"b,string"`
				}
			}{A: struct {
				A uint `json:"a,string"`
			}{A: 1}, B: struct {
				B uint `json:"b,string"`
			}{B: 2}},
		},
		{
			name: "PtrHeadUintMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A struct {
					A uint `json:"a,string,omitempty"`
				}
				B struct {
					B uint `json:"b,string,omitempty"`
				}
			}{A: struct {
				A uint `json:"a,string,omitempty"`
			}{A: 1}, B: struct {
				B uint `json:"b,string,omitempty"`
			}{B: 2}},
		},

		// PtrHeadUintPtrMultiFieldsNotRoot
		{
			name: "PtrHeadUintPtrMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *uint `json:"a"`
				}
				B *struct {
					B *uint `json:"b"`
				}
			}{A: &(struct {
				A *uint `json:"a"`
			}{A: uptr(1)}), B: &(struct {
				B *uint `json:"b"`
			}{B: uptr(2)})},
		},
		{
			name: "PtrHeadUintPtrMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *uint `json:"a,omitempty"`
				}
				B *struct {
					B *uint `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *uint `json:"a,omitempty"`
			}{A: uptr(1)}), B: &(struct {
				B *uint `json:"b,omitempty"`
			}{B: uptr(2)})},
		},
		{
			name: "PtrHeadUintPtrMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *uint `json:"a,string"`
				}
				B *struct {
					B *uint `json:"b,string"`
				}
			}{A: &(struct {
				A *uint `json:"a,string"`
			}{A: uptr(1)}), B: &(struct {
				B *uint `json:"b,string"`
			}{B: uptr(2)})},
		},
		{
			name: "PtrHeadUintPtrMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A *struct {
					A *uint `json:"a,string,omitempty"`
				}
				B *struct {
					B *uint `json:"b,string,omitempty"`
				}
			}{A: &(struct {
				A *uint `json:"a,string,omitempty"`
			}{A: uptr(1)}), B: &(struct {
				B *uint `json:"b,string,omitempty"`
			}{B: uptr(2)})},
		},

		// PtrHeadUintPtrNilMultiFieldsNotRoot
		{
			name: "PtrHeadUintPtrNilMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *uint `json:"a"`
				}
				B *struct {
					B *uint `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUintPtrNilMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *uint `json:"a,omitempty"`
				} `json:",omitempty"`
				B *struct {
					B *uint `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUintPtrNilMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *uint `json:"a,string"`
				} `json:",string"`
				B *struct {
					B *uint `json:"b,string"`
				} `json:",string"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUintPtrNilMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A *struct {
					A *uint `json:"a,string,omitempty"`
				} `json:",string,omitempty"`
				B *struct {
					B *uint `json:"b,string,omitempty"`
				} `json:",string,omitempty"`
			}{A: nil, B: nil},
		},

		// PtrHeadUintNilMultiFieldsNotRoot
		{
			name: "PtrHeadUintNilMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *uint `json:"a"`
				}
				B *struct {
					B *uint `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadUintNilMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *uint `json:"a,omitempty"`
				}
				B *struct {
					B *uint `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadUintNilMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *uint `json:"a,string"`
				}
				B *struct {
					B *uint `json:"b,string"`
				}
			})(nil),
		},
		{
			name: "PtrHeadUintNilMultiFieldsNotRootStringOmitEmpty",
			data: (*struct {
				A *struct {
					A *uint `json:"a,string,omitempty"`
				}
				B *struct {
					B *uint `json:"b,string,omitempty"`
				}
			})(nil),
		},

		// PtrHeadUintDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadUintDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A uint `json:"a"`
					B uint `json:"b"`
				}
				B *struct {
					A uint `json:"a"`
					B uint `json:"b"`
				}
			}{A: &(struct {
				A uint `json:"a"`
				B uint `json:"b"`
			}{A: 1, B: 2}), B: &(struct {
				A uint `json:"a"`
				B uint `json:"b"`
			}{A: 3, B: 4})},
		},
		{
			name: "PtrHeadUintDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A uint `json:"a,omitempty"`
					B uint `json:"b,omitempty"`
				}
				B *struct {
					A uint `json:"a,omitempty"`
					B uint `json:"b,omitempty"`
				}
			}{A: &(struct {
				A uint `json:"a,omitempty"`
				B uint `json:"b,omitempty"`
			}{A: 1, B: 2}), B: &(struct {
				A uint `json:"a,omitempty"`
				B uint `json:"b,omitempty"`
			}{A: 3, B: 4})},
		},
		{
			name: "PtrHeadUintDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A uint `json:"a,string"`
					B uint `json:"b,string"`
				}
				B *struct {
					A uint `json:"a,string"`
					B uint `json:"b,string"`
				}
			}{A: &(struct {
				A uint `json:"a,string"`
				B uint `json:"b,string"`
			}{A: 1, B: 2}), B: &(struct {
				A uint `json:"a,string"`
				B uint `json:"b,string"`
			}{A: 3, B: 4})},
		},
		{
			name: "PtrHeadUintDoubleMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A *struct {
					A uint `json:"a,string,omitempty"`
					B uint `json:"b,string,omitempty"`
				}
				B *struct {
					A uint `json:"a,string,omitempty"`
					B uint `json:"b,string,omitempty"`
				}
			}{A: &(struct {
				A uint `json:"a,string,omitempty"`
				B uint `json:"b,string,omitempty"`
			}{A: 1, B: 2}), B: &(struct {
				A uint `json:"a,string,omitempty"`
				B uint `json:"b,string,omitempty"`
			}{A: 3, B: 4})},
		},

		// PtrHeadUintNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadUintNilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A uint `json:"a"`
					B uint `json:"b"`
				}
				B *struct {
					A uint `json:"a"`
					B uint `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUintNilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A uint `json:"a,omitempty"`
					B uint `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A uint `json:"a,omitempty"`
					B uint `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUintNilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A uint `json:"a,string"`
					B uint `json:"b,string"`
				}
				B *struct {
					A uint `json:"a,string"`
					B uint `json:"b,string"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUintNilDoubleMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A *struct {
					A uint `json:"a,string,omitempty"`
					B uint `json:"b,string,omitempty"`
				}
				B *struct {
					A uint `json:"a,string,omitempty"`
					B uint `json:"b,string,omitempty"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadUintNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadUintNilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A uint `json:"a"`
					B uint `json:"b"`
				}
				B *struct {
					A uint `json:"a"`
					B uint `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadUintNilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A uint `json:"a,omitempty"`
					B uint `json:"b,omitempty"`
				}
				B *struct {
					A uint `json:"a,omitempty"`
					B uint `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadUintNilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A uint `json:"a,string"`
					B uint `json:"b,string"`
				}
				B *struct {
					A uint `json:"a,string"`
					B uint `json:"b,string"`
				}
			})(nil),
		},
		{
			name: "PtrHeadUintNilDoubleMultiFieldsNotRootStringOmitEmpty",
			data: (*struct {
				A *struct {
					A uint `json:"a,string,omitempty"`
					B uint `json:"b,string,omitempty"`
				}
				B *struct {
					A uint `json:"a,string,omitempty"`
					B uint `json:"b,string,omitempty"`
				}
			})(nil),
		},

		// PtrHeadUintPtrDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadUintPtrDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *uint `json:"a"`
					B *uint `json:"b"`
				}
				B *struct {
					A *uint `json:"a"`
					B *uint `json:"b"`
				}
			}{A: &(struct {
				A *uint `json:"a"`
				B *uint `json:"b"`
			}{A: uptr(1), B: uptr(2)}), B: &(struct {
				A *uint `json:"a"`
				B *uint `json:"b"`
			}{A: uptr(3), B: uptr(4)})},
		},
		{
			name: "PtrHeadUintPtrDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *uint `json:"a,omitempty"`
					B *uint `json:"b,omitempty"`
				}
				B *struct {
					A *uint `json:"a,omitempty"`
					B *uint `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *uint `json:"a,omitempty"`
				B *uint `json:"b,omitempty"`
			}{A: uptr(1), B: uptr(2)}), B: &(struct {
				A *uint `json:"a,omitempty"`
				B *uint `json:"b,omitempty"`
			}{A: uptr(3), B: uptr(4)})},
		},
		{
			name: "PtrHeadUintPtrDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *uint `json:"a,string"`
					B *uint `json:"b,string"`
				}
				B *struct {
					A *uint `json:"a,string"`
					B *uint `json:"b,string"`
				}
			}{A: &(struct {
				A *uint `json:"a,string"`
				B *uint `json:"b,string"`
			}{A: uptr(1), B: uptr(2)}), B: &(struct {
				A *uint `json:"a,string"`
				B *uint `json:"b,string"`
			}{A: uptr(3), B: uptr(4)})},
		},
		{
			name: "PtrHeadUintPtrDoubleMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A *struct {
					A *uint `json:"a,string,omitempty"`
					B *uint `json:"b,string,omitempty"`
				}
				B *struct {
					A *uint `json:"a,string,omitempty"`
					B *uint `json:"b,string,omitempty"`
				}
			}{A: &(struct {
				A *uint `json:"a,string,omitempty"`
				B *uint `json:"b,string,omitempty"`
			}{A: uptr(1), B: uptr(2)}), B: &(struct {
				A *uint `json:"a,string,omitempty"`
				B *uint `json:"b,string,omitempty"`
			}{A: uptr(3), B: uptr(4)})},
		},

		// PtrHeadUintPtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadUintPtrNilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *uint `json:"a"`
					B *uint `json:"b"`
				}
				B *struct {
					A *uint `json:"a"`
					B *uint `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUintPtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *uint `json:"a,omitempty"`
					B *uint `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A *uint `json:"a,omitempty"`
					B *uint `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUintPtrNilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *uint `json:"a,string"`
					B *uint `json:"b,string"`
				}
				B *struct {
					A *uint `json:"a,string"`
					B *uint `json:"b,string"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadUintPtrNilDoubleMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A *struct {
					A *uint `json:"a,string,omitempty"`
					B *uint `json:"b,string,omitempty"`
				}
				B *struct {
					A *uint `json:"a,string,omitempty"`
					B *uint `json:"b,string,omitempty"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadUintPtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadUintPtrNilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *uint `json:"a"`
					B *uint `json:"b"`
				}
				B *struct {
					A *uint `json:"a"`
					B *uint `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadUintPtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *uint `json:"a,omitempty"`
					B *uint `json:"b,omitempty"`
				}
				B *struct {
					A *uint `json:"a,omitempty"`
					B *uint `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadUintPtrNilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *uint `json:"a,string"`
					B *uint `json:"b,string"`
				}
				B *struct {
					A *uint `json:"a,string"`
					B *uint `json:"b,string"`
				}
			})(nil),
		},
		{
			name: "PtrHeadUintPtrNilDoubleMultiFieldsNotRootStringOmitEmpty",
			data: (*struct {
				A *struct {
					A *uint `json:"a,string,omitempty"`
					B *uint `json:"b,string,omitempty"`
				}
				B *struct {
					A *uint `json:"a,string,omitempty"`
					B *uint `json:"b,string,omitempty"`
				}
			})(nil),
		},

		// AnonymousHeadUint
		{
			name: "AnonymousHeadUint",
			data: struct {
				structUint
				B uint `json:"b"`
			}{
				structUint: structUint{A: 1},
				B:          2,
			},
		},
		{
			name: "AnonymousHeadUintOmitEmpty",
			data: struct {
				structUintOmitEmpty
				B uint `json:"b,omitempty"`
			}{
				structUintOmitEmpty: structUintOmitEmpty{A: 1},
				B:                   2,
			},
		},
		{
			name: "AnonymousHeadUintString",
			data: struct {
				structUintString
				B uint `json:"b,string"`
			}{
				structUintString: structUintString{A: 1},
				B:                2,
			},
		},
		{
			name: "AnonymousHeadUintStringOmitEmpty",
			data: struct {
				structUintStringOmitEmpty
				B uint `json:"b,string,omitempty"`
			}{
				structUintStringOmitEmpty: structUintStringOmitEmpty{A: 1},
				B:                         2,
			},
		},

		// PtrAnonymousHeadUint
		{
			name: "PtrAnonymousHeadUint",
			data: struct {
				*structUint
				B uint `json:"b"`
			}{
				structUint: &structUint{A: 1},
				B:          2,
			},
		},
		{
			name: "PtrAnonymousHeadUintOmitEmpty",
			data: struct {
				*structUintOmitEmpty
				B uint `json:"b,omitempty"`
			}{
				structUintOmitEmpty: &structUintOmitEmpty{A: 1},
				B:                   2,
			},
		},
		{
			name: "PtrAnonymousHeadUintString",
			data: struct {
				*structUintString
				B uint `json:"b,string"`
			}{
				structUintString: &structUintString{A: 1},
				B:                2,
			},
		},
		{
			name: "PtrAnonymousHeadUintStringOmitEmpty",
			data: struct {
				*structUintStringOmitEmpty
				B uint `json:"b,string,omitempty"`
			}{
				structUintStringOmitEmpty: &structUintStringOmitEmpty{A: 1},
				B:                         2,
			},
		},

		// NilPtrAnonymousHeadUint
		{
			name: "NilPtrAnonymousHeadUint",
			data: struct {
				*structUint
				B uint `json:"b"`
			}{
				structUint: nil,
				B:          2,
			},
		},
		{
			name: "NilPtrAnonymousHeadUintOmitEmpty",
			data: struct {
				*structUintOmitEmpty
				B uint `json:"b,omitempty"`
			}{
				structUintOmitEmpty: nil,
				B:                   2,
			},
		},
		{
			name: "NilPtrAnonymousHeadUintString",
			data: struct {
				*structUintString
				B uint `json:"b,string"`
			}{
				structUintString: nil,
				B:                2,
			},
		},
		{
			name: "NilPtrAnonymousHeadUintStringOmitEmpty",
			data: struct {
				*structUintStringOmitEmpty
				B uint `json:"b,string,omitempty"`
			}{
				structUintStringOmitEmpty: nil,
				B:                         2,
			},
		},

		// AnonymousHeadUintPtr
		{
			name: "AnonymousHeadUintPtr",
			data: struct {
				structUintPtr
				B *uint `json:"b"`
			}{
				structUintPtr: structUintPtr{A: uptr(1)},
				B:             uptr(2),
			},
		},
		{
			name: "AnonymousHeadUintPtrOmitEmpty",
			data: struct {
				structUintPtrOmitEmpty
				B *uint `json:"b,omitempty"`
			}{
				structUintPtrOmitEmpty: structUintPtrOmitEmpty{A: uptr(1)},
				B:                      uptr(2),
			},
		},
		{
			name: "AnonymousHeadUintPtrString",
			data: struct {
				structUintPtrString
				B *uint `json:"b,string"`
			}{
				structUintPtrString: structUintPtrString{A: uptr(1)},
				B:                   uptr(2),
			},
		},
		{
			name: "AnonymousHeadUintPtrStringOmitEmpty",
			data: struct {
				structUintPtrStringOmitEmpty
				B *uint `json:"b,string,omitempty"`
			}{
				structUintPtrStringOmitEmpty: structUintPtrStringOmitEmpty{A: uptr(1)},
				B:                            uptr(2),
			},
		},

		// AnonymousHeadUintPtrNil
		{
			name: "AnonymousHeadUintPtrNil",
			data: struct {
				structUintPtr
				B *uint `json:"b"`
			}{
				structUintPtr: structUintPtr{A: nil},
				B:             uptr(2),
			},
		},
		{
			name: "AnonymousHeadUintPtrNilOmitEmpty",
			data: struct {
				structUintPtrOmitEmpty
				B *uint `json:"b,omitempty"`
			}{
				structUintPtrOmitEmpty: structUintPtrOmitEmpty{A: nil},
				B:                      uptr(2),
			},
		},
		{
			name: "AnonymousHeadUintPtrNilString",
			data: struct {
				structUintPtrString
				B *uint `json:"b,string"`
			}{
				structUintPtrString: structUintPtrString{A: nil},
				B:                   uptr(2),
			},
		},
		{
			name: "AnonymousHeadUintPtrNilStringOmitEmpty",
			data: struct {
				structUintPtrStringOmitEmpty
				B *uint `json:"b,string,omitempty"`
			}{
				structUintPtrStringOmitEmpty: structUintPtrStringOmitEmpty{A: nil},
				B:                            uptr(2),
			},
		},

		// PtrAnonymousHeadUintPtr
		{
			name: "PtrAnonymousHeadUintPtr",
			data: struct {
				*structUintPtr
				B *uint `json:"b"`
			}{
				structUintPtr: &structUintPtr{A: uptr(1)},
				B:             uptr(2),
			},
		},
		{
			name: "PtrAnonymousHeadUintPtrOmitEmpty",
			data: struct {
				*structUintPtrOmitEmpty
				B *uint `json:"b,omitempty"`
			}{
				structUintPtrOmitEmpty: &structUintPtrOmitEmpty{A: uptr(1)},
				B:                      uptr(2),
			},
		},
		{
			name: "PtrAnonymousHeadUintPtrString",
			data: struct {
				*structUintPtrString
				B *uint `json:"b,string"`
			}{
				structUintPtrString: &structUintPtrString{A: uptr(1)},
				B:                   uptr(2),
			},
		},
		{
			name: "PtrAnonymousHeadUintPtrStringOmitEmpty",
			data: struct {
				*structUintPtrStringOmitEmpty
				B *uint `json:"b,string,omitempty"`
			}{
				structUintPtrStringOmitEmpty: &structUintPtrStringOmitEmpty{A: uptr(1)},
				B:                            uptr(2),
			},
		},

		// NilPtrAnonymousHeadUintPtr
		{
			name: "NilPtrAnonymousHeadUintPtr",
			data: struct {
				*structUintPtr
				B *uint `json:"b"`
			}{
				structUintPtr: nil,
				B:             uptr(2),
			},
		},
		{
			name: "NilPtrAnonymousHeadUintPtrOmitEmpty",
			data: struct {
				*structUintPtrOmitEmpty
				B *uint `json:"b,omitempty"`
			}{
				structUintPtrOmitEmpty: nil,
				B:                      uptr(2),
			},
		},
		{
			name: "NilPtrAnonymousHeadUintPtrString",
			data: struct {
				*structUintPtrString
				B *uint `json:"b,string"`
			}{
				structUintPtrString: nil,
				B:                   uptr(2),
			},
		},
		{
			name: "NilPtrAnonymousHeadUintPtrStringOmitEmpty",
			data: struct {
				*structUintPtrStringOmitEmpty
				B *uint `json:"b,string,omitempty"`
			}{
				structUintPtrStringOmitEmpty: nil,
				B:                            uptr(2),
			},
		},

		// AnonymousHeadUintOnly
		{
			name: "AnonymousHeadUintOnly",
			data: struct {
				structUint
			}{
				structUint: structUint{A: 1},
			},
		},
		{
			name: "AnonymousHeadUintOnlyOmitEmpty",
			data: struct {
				structUintOmitEmpty
			}{
				structUintOmitEmpty: structUintOmitEmpty{A: 1},
			},
		},
		{
			name: "AnonymousHeadUintOnlyString",
			data: struct {
				structUintString
			}{
				structUintString: structUintString{A: 1},
			},
		},
		{
			name: "AnonymousHeadUintOnlyStringOmitEmpty",
			data: struct {
				structUintStringOmitEmpty
			}{
				structUintStringOmitEmpty: structUintStringOmitEmpty{A: 1},
			},
		},

		// PtrAnonymousHeadUintOnly
		{
			name: "PtrAnonymousHeadUintOnly",
			data: struct {
				*structUint
			}{
				structUint: &structUint{A: 1},
			},
		},
		{
			name: "PtrAnonymousHeadUintOnlyOmitEmpty",
			data: struct {
				*structUintOmitEmpty
			}{
				structUintOmitEmpty: &structUintOmitEmpty{A: 1},
			},
		},
		{
			name: "PtrAnonymousHeadUintOnlyString",
			data: struct {
				*structUintString
			}{
				structUintString: &structUintString{A: 1},
			},
		},
		{
			name: "PtrAnonymousHeadUintOnlyStringOmitEmpty",
			data: struct {
				*structUintStringOmitEmpty
			}{
				structUintStringOmitEmpty: &structUintStringOmitEmpty{A: 1},
			},
		},

		// NilPtrAnonymousHeadUintOnly
		{
			name: "NilPtrAnonymousHeadUintOnly",
			data: struct {
				*structUint
			}{
				structUint: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadUintOnlyOmitEmpty",
			data: struct {
				*structUintOmitEmpty
			}{
				structUintOmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadUintOnlyString",
			data: struct {
				*structUintString
			}{
				structUintString: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadUintOnlyStringOmitEmpty",
			data: struct {
				*structUintStringOmitEmpty
			}{
				structUintStringOmitEmpty: nil,
			},
		},

		// AnonymousHeadUintPtrOnly
		{
			name: "AnonymousHeadUintPtrOnly",
			data: struct {
				structUintPtr
			}{
				structUintPtr: structUintPtr{A: uptr(1)},
			},
		},
		{
			name: "AnonymousHeadUintPtrOnlyOmitEmpty",
			data: struct {
				structUintPtrOmitEmpty
			}{
				structUintPtrOmitEmpty: structUintPtrOmitEmpty{A: uptr(1)},
			},
		},
		{
			name: "AnonymousHeadUintPtrOnlyString",
			data: struct {
				structUintPtrString
			}{
				structUintPtrString: structUintPtrString{A: uptr(1)},
			},
		},
		{
			name: "AnonymousHeadUintPtrOnlyStringOmitEmpty",
			data: struct {
				structUintPtrStringOmitEmpty
			}{
				structUintPtrStringOmitEmpty: structUintPtrStringOmitEmpty{A: uptr(1)},
			},
		},

		// AnonymousHeadUintPtrNilOnly
		{
			name: "AnonymousHeadUintPtrNilOnly",
			data: struct {
				structUintPtr
			}{
				structUintPtr: structUintPtr{A: nil},
			},
		},
		{
			name: "AnonymousHeadUintPtrNilOnlyOmitEmpty",
			data: struct {
				structUintPtrOmitEmpty
			}{
				structUintPtrOmitEmpty: structUintPtrOmitEmpty{A: nil},
			},
		},
		{
			name: "AnonymousHeadUintPtrNilOnlyString",
			data: struct {
				structUintPtrString
			}{
				structUintPtrString: structUintPtrString{A: nil},
			},
		},
		{
			name: "AnonymousHeadUintPtrNilOnlyStringOmitEmpty",
			data: struct {
				structUintPtrStringOmitEmpty
			}{
				structUintPtrStringOmitEmpty: structUintPtrStringOmitEmpty{A: nil},
			},
		},

		// PtrAnonymousHeadUintPtrOnly
		{
			name: "PtrAnonymousHeadUintPtrOnly",
			data: struct {
				*structUintPtr
			}{
				structUintPtr: &structUintPtr{A: uptr(1)},
			},
		},
		{
			name: "PtrAnonymousHeadUintPtrOnlyOmitEmpty",
			data: struct {
				*structUintPtrOmitEmpty
			}{
				structUintPtrOmitEmpty: &structUintPtrOmitEmpty{A: uptr(1)},
			},
		},
		{
			name: "PtrAnonymousHeadUintPtrOnlyString",
			data: struct {
				*structUintPtrString
			}{
				structUintPtrString: &structUintPtrString{A: uptr(1)},
			},
		},
		{
			name: "PtrAnonymousHeadUintPtrOnlyStringOmitEmpty",
			data: struct {
				*structUintPtrStringOmitEmpty
			}{
				structUintPtrStringOmitEmpty: &structUintPtrStringOmitEmpty{A: uptr(1)},
			},
		},

		// NilPtrAnonymousHeadUintPtrOnly
		{
			name: "NilPtrAnonymousHeadUintPtrOnly",
			data: struct {
				*structUintPtr
			}{
				structUintPtr: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadUintPtrOnlyOmitEmpty",
			data: struct {
				*structUintPtrOmitEmpty
			}{
				structUintPtrOmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadUintPtrOnlyString",
			data: struct {
				*structUintPtrString
			}{
				structUintPtrString: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadUintPtrOnlyStringOmitEmpty",
			data: struct {
				*structUintPtrStringOmitEmpty
			}{
				structUintPtrStringOmitEmpty: nil,
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
