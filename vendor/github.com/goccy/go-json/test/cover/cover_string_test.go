package json_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/goccy/go-json"
)

func TestCoverString(t *testing.T) {
	type structString struct {
		A string `json:"a"`
	}
	type structStringOmitEmpty struct {
		A string `json:"a,omitempty"`
	}
	type structStringString struct {
		A string `json:"a,string"`
	}
	type structStringStringOmitEmpty struct {
		A string `json:"a,string,omitempty"`
	}

	type structStringPtr struct {
		A *string `json:"a"`
	}
	type structStringPtrOmitEmpty struct {
		A *string `json:"a,omitempty"`
	}
	type structStringPtrString struct {
		A *string `json:"a,string"`
	}
	type structStringPtrStringOmitEmpty struct {
		A *string `json:"a,string,omitempty"`
	}

	tests := []struct {
		name string
		data interface{}
	}{
		{
			name: "PtrHeadStringComplicated",
			data: &struct {
				X *struct {
					A string
					B []string
				}
			}{X: &struct {
				A string
				B []string
			}{A: "hello", B: []string{"a", "b"}}},
		},
		{
			name: "String",
			data: string("a"),
		},
		{
			name: "StringPtr",
			data: stringptr("a"),
		},
		{
			name: "StringPtr3",
			data: stringptr3("a"),
		},
		{
			name: "StringPtrNil",
			data: (*string)(nil),
		},
		{
			name: "StringPtr3Nil",
			data: (***string)(nil),
		},

		// HeadStringZero
		{
			name: "HeadStringZero",
			data: struct {
				A string `json:"a"`
			}{},
		},
		{
			name: "HeadStringZeroOmitEmpty",
			data: struct {
				A string `json:"a,omitempty"`
			}{},
		},
		{
			name: "HeadStringZeroString",
			data: struct {
				A string `json:"a,string"`
			}{},
		},
		{
			name: "HeadStringZeroStringOmitEmpty",
			data: struct {
				A string `json:"a,string,omitempty"`
			}{},
		},

		// HeadString
		{
			name: "HeadString",
			data: struct {
				A string `json:"a"`
			}{A: "foo"},
		},
		{
			name: "HeadStringOmitEmpty",
			data: struct {
				A string `json:"a,omitempty"`
			}{A: "foo"},
		},
		{
			name: "HeadStringString",
			data: struct {
				A string `json:"a,string"`
			}{A: "foo"},
		},
		{
			name: "HeadStringStringOmitEmpty",
			data: struct {
				A string `json:"a,string,omitempty"`
			}{A: "foo"},
		},

		// HeadStringPtr
		{
			name: "HeadStringPtr",
			data: struct {
				A *string `json:"a"`
			}{A: stringptr("foo")},
		},
		{
			name: "HeadStringPtrOmitEmpty",
			data: struct {
				A *string `json:"a,omitempty"`
			}{A: stringptr("foo")},
		},
		{
			name: "HeadStringPtrString",
			data: struct {
				A *string `json:"a,string"`
			}{A: stringptr("foo")},
		},
		{
			name: "HeadStringPtrStringOmitEmpty",
			data: struct {
				A *string `json:"a,string,omitempty"`
			}{A: stringptr("foo")},
		},

		// HeadStringPtrNil
		{
			name: "HeadStringPtrNil",
			data: struct {
				A *string `json:"a"`
			}{A: nil},
		},
		{
			name: "HeadStringPtrNilOmitEmpty",
			data: struct {
				A *string `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "HeadStringPtrNilString",
			data: struct {
				A *string `json:"a,string"`
			}{A: nil},
		},
		{
			name: "HeadStringPtrNilStringOmitEmpty",
			data: struct {
				A *string `json:"a,string,omitempty"`
			}{A: nil},
		},

		// PtrHeadStringZero
		{
			name: "PtrHeadStringZero",
			data: &struct {
				A string `json:"a"`
			}{},
		},
		{
			name: "PtrHeadStringZeroOmitEmpty",
			data: &struct {
				A string `json:"a,omitempty"`
			}{},
		},
		{
			name: "PtrHeadStringZeroString",
			data: &struct {
				A string `json:"a,string"`
			}{},
		},
		{
			name: "PtrHeadStringZeroStringOmitEmpty",
			data: &struct {
				A string `json:"a,string,omitempty"`
			}{},
		},

		// PtrHeadString
		{
			name: "PtrHeadString",
			data: &struct {
				A string `json:"a"`
			}{A: "foo"},
		},
		{
			name: "PtrHeadStringOmitEmpty",
			data: &struct {
				A string `json:"a,omitempty"`
			}{A: "foo"},
		},
		{
			name: "PtrHeadStringString",
			data: &struct {
				A string `json:"a,string"`
			}{A: "foo"},
		},
		{
			name: "PtrHeadStringStringOmitEmpty",
			data: &struct {
				A string `json:"a,string,omitempty"`
			}{A: "foo"},
		},

		// PtrHeadStringPtr
		{
			name: "PtrHeadStringPtr",
			data: &struct {
				A *string `json:"a"`
			}{A: stringptr("foo")},
		},
		{
			name: "PtrHeadStringPtrOmitEmpty",
			data: &struct {
				A *string `json:"a,omitempty"`
			}{A: stringptr("foo")},
		},
		{
			name: "PtrHeadStringPtrString",
			data: &struct {
				A *string `json:"a,string"`
			}{A: stringptr("foo")},
		},
		{
			name: "PtrHeadStringPtrStringOmitEmpty",
			data: &struct {
				A *string `json:"a,string,omitempty"`
			}{A: stringptr("foo")},
		},

		// PtrHeadStringPtrNil
		{
			name: "PtrHeadStringPtrNil",
			data: &struct {
				A *string `json:"a"`
			}{A: nil},
		},
		{
			name: "PtrHeadStringPtrNilOmitEmpty",
			data: &struct {
				A *string `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadStringPtrNilString",
			data: &struct {
				A *string `json:"a,string"`
			}{A: nil},
		},
		{
			name: "PtrHeadStringPtrNilStringOmitEmpty",
			data: &struct {
				A *string `json:"a,string,omitempty"`
			}{A: nil},
		},

		// PtrHeadStringNil
		{
			name: "PtrHeadStringNil",
			data: (*struct {
				A *string `json:"a"`
			})(nil),
		},
		{
			name: "PtrHeadStringNilOmitEmpty",
			data: (*struct {
				A *string `json:"a,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadStringNilString",
			data: (*struct {
				A *string `json:"a,string"`
			})(nil),
		},
		{
			name: "PtrHeadStringNilStringOmitEmpty",
			data: (*struct {
				A *string `json:"a,string,omitempty"`
			})(nil),
		},

		// HeadStringZeroMultiFields
		{
			name: "HeadStringZeroMultiFields",
			data: struct {
				A string `json:"a"`
				B string `json:"b"`
				C string `json:"c"`
			}{},
		},
		{
			name: "HeadStringZeroMultiFieldsOmitEmpty",
			data: struct {
				A string `json:"a,omitempty"`
				B string `json:"b,omitempty"`
				C string `json:"c,omitempty"`
			}{},
		},
		{
			name: "HeadStringZeroMultiFieldsString",
			data: struct {
				A string `json:"a,string"`
				B string `json:"b,string"`
				C string `json:"c,string"`
			}{},
		},
		{
			name: "HeadStringZeroMultiFieldsStringOmitEmpty",
			data: struct {
				A string `json:"a,string,omitempty"`
				B string `json:"b,string,omitempty"`
				C string `json:"c,string,omitempty"`
			}{},
		},

		// HeadStringMultiFields
		{
			name: "HeadStringMultiFields",
			data: struct {
				A string `json:"a"`
				B string `json:"b"`
				C string `json:"c"`
			}{A: "foo", B: "bar", C: "baz"},
		},
		{
			name: "HeadStringMultiFieldsOmitEmpty",
			data: struct {
				A string `json:"a,omitempty"`
				B string `json:"b,omitempty"`
				C string `json:"c,omitempty"`
			}{A: "foo", B: "bar", C: "baz"},
		},
		{
			name: "HeadStringMultiFieldsString",
			data: struct {
				A string `json:"a,string"`
				B string `json:"b,string"`
				C string `json:"c,string"`
			}{A: "foo", B: "bar", C: "baz"},
		},
		{
			name: "HeadStringMultiFieldsStringOmitEmpty",
			data: struct {
				A string `json:"a,string,omitempty"`
				B string `json:"b,string,omitempty"`
				C string `json:"c,string,omitempty"`
			}{A: "foo", B: "bar", C: "baz"},
		},

		// HeadStringPtrMultiFields
		{
			name: "HeadStringPtrMultiFields",
			data: struct {
				A *string `json:"a"`
				B *string `json:"b"`
				C *string `json:"c"`
			}{A: stringptr("foo"), B: stringptr("bar"), C: stringptr("baz")},
		},
		{
			name: "HeadStringPtrMultiFieldsOmitEmpty",
			data: struct {
				A *string `json:"a,omitempty"`
				B *string `json:"b,omitempty"`
				C *string `json:"c,omitempty"`
			}{A: stringptr("foo"), B: stringptr("bar"), C: stringptr("baz")},
		},
		{
			name: "HeadStringPtrMultiFieldsString",
			data: struct {
				A *string `json:"a,string"`
				B *string `json:"b,string"`
				C *string `json:"c,string"`
			}{A: stringptr("foo"), B: stringptr("bar"), C: stringptr("baz")},
		},
		{
			name: "HeadStringPtrMultiFieldsStringOmitEmpty",
			data: struct {
				A *string `json:"a,string,omitempty"`
				B *string `json:"b,string,omitempty"`
				C *string `json:"c,string,omitempty"`
			}{A: stringptr("foo"), B: stringptr("bar"), C: stringptr("baz")},
		},

		// HeadStringPtrNilMultiFields
		{
			name: "HeadStringPtrNilMultiFields",
			data: struct {
				A *string `json:"a"`
				B *string `json:"b"`
				C *string `json:"c"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadStringPtrNilMultiFieldsOmitEmpty",
			data: struct {
				A *string `json:"a,omitempty"`
				B *string `json:"b,omitempty"`
				C *string `json:"c,omitempty"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadStringPtrNilMultiFieldsString",
			data: struct {
				A *string `json:"a,string"`
				B *string `json:"b,string"`
				C *string `json:"c,string"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadStringPtrNilMultiFieldsStringOmitEmpty",
			data: struct {
				A *string `json:"a,string,omitempty"`
				B *string `json:"b,string,omitempty"`
				C *string `json:"c,string,omitempty"`
			}{A: nil, B: nil, C: nil},
		},

		// PtrHeadStringZeroMultiFields
		{
			name: "PtrHeadStringZeroMultiFields",
			data: &struct {
				A string `json:"a"`
				B string `json:"b"`
				C string `json:"c"`
			}{},
		},
		{
			name: "PtrHeadStringZeroMultiFieldsOmitEmpty",
			data: &struct {
				A string `json:"a,omitempty"`
				B string `json:"b,omitempty"`
				C string `json:"c,omitempty"`
			}{},
		},
		{
			name: "PtrHeadStringZeroMultiFieldsString",
			data: &struct {
				A string `json:"a,string"`
				B string `json:"b,string"`
				C string `json:"c,string"`
			}{},
		},
		{
			name: "PtrHeadStringZeroMultiFieldsStringOmitEmpty",
			data: &struct {
				A string `json:"a,string,omitempty"`
				B string `json:"b,string,omitempty"`
				C string `json:"c,string,omitempty"`
			}{},
		},

		// PtrHeadStringMultiFields
		{
			name: "PtrHeadStringMultiFields",
			data: &struct {
				A string `json:"a"`
				B string `json:"b"`
				C string `json:"c"`
			}{A: "foo", B: "bar", C: "baz"},
		},
		{
			name: "PtrHeadStringMultiFieldsOmitEmpty",
			data: &struct {
				A string `json:"a,omitempty"`
				B string `json:"b,omitempty"`
				C string `json:"c,omitempty"`
			}{A: "foo", B: "bar", C: "baz"},
		},
		{
			name: "PtrHeadStringMultiFieldsString",
			data: &struct {
				A string `json:"a,string"`
				B string `json:"b,string"`
				C string `json:"c,string"`
			}{A: "foo", B: "bar", C: "baz"},
		},
		{
			name: "PtrHeadStringMultiFieldsStringOmitEmpty",
			data: &struct {
				A string `json:"a,string,omitempty"`
				B string `json:"b,string,omitempty"`
				C string `json:"c,string,omitempty"`
			}{A: "foo", B: "bar", C: "baz"},
		},

		// PtrHeadStringPtrMultiFields
		{
			name: "PtrHeadStringPtrMultiFields",
			data: &struct {
				A *string `json:"a"`
				B *string `json:"b"`
				C *string `json:"c"`
			}{A: stringptr("foo"), B: stringptr("bar"), C: stringptr("baz")},
		},
		{
			name: "PtrHeadStringPtrMultiFieldsOmitEmpty",
			data: &struct {
				A *string `json:"a,omitempty"`
				B *string `json:"b,omitempty"`
				C *string `json:"c,omitempty"`
			}{A: stringptr("foo"), B: stringptr("bar"), C: stringptr("baz")},
		},
		{
			name: "PtrHeadStringPtrMultiFieldsString",
			data: &struct {
				A *string `json:"a,string"`
				B *string `json:"b,string"`
				C *string `json:"c,string"`
			}{A: stringptr("foo"), B: stringptr("bar"), C: stringptr("baz")},
		},
		{
			name: "PtrHeadStringPtrMultiFieldsStringOmitEmpty",
			data: &struct {
				A *string `json:"a,string,omitempty"`
				B *string `json:"b,string,omitempty"`
				C *string `json:"c,string,omitempty"`
			}{A: stringptr("foo"), B: stringptr("bar"), C: stringptr("baz")},
		},

		// PtrHeadStringPtrNilMultiFields
		{
			name: "PtrHeadStringPtrNilMultiFields",
			data: &struct {
				A *string `json:"a"`
				B *string `json:"b"`
				C *string `json:"c"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "PtrHeadStringPtrNilMultiFieldsOmitEmpty",
			data: &struct {
				A *string `json:"a,omitempty"`
				B *string `json:"b,omitempty"`
				C *string `json:"c,omitempty"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "PtrHeadStringPtrNilMultiFieldsString",
			data: &struct {
				A *string `json:"a,string"`
				B *string `json:"b,string"`
				C *string `json:"c,string"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "PtrHeadStringPtrNilMultiFieldsStringOmitEmpty",
			data: &struct {
				A *string `json:"a,string,omitempty"`
				B *string `json:"b,string,omitempty"`
				C *string `json:"c,string,omitempty"`
			}{A: nil, B: nil, C: nil},
		},

		// PtrHeadStringNilMultiFields
		{
			name: "PtrHeadStringNilMultiFields",
			data: (*struct {
				A *string `json:"a"`
				B *string `json:"b"`
				C *string `json:"c"`
			})(nil),
		},
		{
			name: "PtrHeadStringNilMultiFieldsOmitEmpty",
			data: (*struct {
				A *string `json:"a,omitempty"`
				B *string `json:"b,omitempty"`
				C *string `json:"c,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadStringNilMultiFieldsString",
			data: (*struct {
				A *string `json:"a,string"`
				B *string `json:"b,string"`
				C *string `json:"c,string"`
			})(nil),
		},
		{
			name: "PtrHeadStringNilMultiFieldsStringOmitEmpty",
			data: (*struct {
				A *string `json:"a,string,omitempty"`
				B *string `json:"b,string,omitempty"`
				C *string `json:"c,string,omitempty"`
			})(nil),
		},

		// HeadStringZeroNotRoot
		{
			name: "HeadStringZeroNotRoot",
			data: struct {
				A struct {
					A string `json:"a"`
				}
			}{},
		},
		{
			name: "HeadStringZeroNotRootOmitEmpty",
			data: struct {
				A struct {
					A string `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadStringZeroNotRootString",
			data: struct {
				A struct {
					A string `json:"a,string"`
				}
			}{},
		},
		{
			name: "HeadStringZeroNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A string `json:"a,string,omitempty"`
				}
			}{},
		},

		// HeadStringNotRoot
		{
			name: "HeadStringNotRoot",
			data: struct {
				A struct {
					A string `json:"a"`
				}
			}{A: struct {
				A string `json:"a"`
			}{A: "foo"}},
		},
		{
			name: "HeadStringNotRootOmitEmpty",
			data: struct {
				A struct {
					A string `json:"a,omitempty"`
				}
			}{A: struct {
				A string `json:"a,omitempty"`
			}{A: "foo"}},
		},
		{
			name: "HeadStringNotRootString",
			data: struct {
				A struct {
					A string `json:"a,string"`
				}
			}{A: struct {
				A string `json:"a,string"`
			}{A: "foo"}},
		},
		{
			name: "HeadStringNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A string `json:"a,string,omitempty"`
				}
			}{A: struct {
				A string `json:"a,string,omitempty"`
			}{A: "foo"}},
		},

		// HeadStringPtrNotRoot
		{
			name: "HeadStringPtrNotRoot",
			data: struct {
				A struct {
					A *string `json:"a"`
				}
			}{A: struct {
				A *string `json:"a"`
			}{stringptr("foo")}},
		},
		{
			name: "HeadStringPtrNotRootOmitEmpty",
			data: struct {
				A struct {
					A *string `json:"a,omitempty"`
				}
			}{A: struct {
				A *string `json:"a,omitempty"`
			}{stringptr("foo")}},
		},
		{
			name: "HeadStringPtrNotRootString",
			data: struct {
				A struct {
					A *string `json:"a,string"`
				}
			}{A: struct {
				A *string `json:"a,string"`
			}{stringptr("foo")}},
		},
		{
			name: "HeadStringPtrNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A *string `json:"a,string,omitempty"`
				}
			}{A: struct {
				A *string `json:"a,string,omitempty"`
			}{stringptr("foo")}},
		},

		// HeadStringPtrNilNotRoot
		{
			name: "HeadStringPtrNilNotRoot",
			data: struct {
				A struct {
					A *string `json:"a"`
				}
			}{},
		},
		{
			name: "HeadStringPtrNilNotRootOmitEmpty",
			data: struct {
				A struct {
					A *string `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadStringPtrNilNotRootString",
			data: struct {
				A struct {
					A *string `json:"a,string"`
				}
			}{},
		},
		{
			name: "HeadStringPtrNilNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A *string `json:"a,string,omitempty"`
				}
			}{},
		},

		// PtrHeadStringZeroNotRoot
		{
			name: "PtrHeadStringZeroNotRoot",
			data: struct {
				A *struct {
					A string `json:"a"`
				}
			}{A: new(struct {
				A string `json:"a"`
			})},
		},
		{
			name: "PtrHeadStringZeroNotRootOmitEmpty",
			data: struct {
				A *struct {
					A string `json:"a,omitempty"`
				}
			}{A: new(struct {
				A string `json:"a,omitempty"`
			})},
		},
		{
			name: "PtrHeadStringZeroNotRootString",
			data: struct {
				A *struct {
					A string `json:"a,string"`
				}
			}{A: new(struct {
				A string `json:"a,string"`
			})},
		},
		{
			name: "PtrHeadStringZeroNotRootStringOmitEmpty",
			data: struct {
				A *struct {
					A string `json:"a,string,omitempty"`
				}
			}{A: new(struct {
				A string `json:"a,string,omitempty"`
			})},
		},

		// PtrHeadStringNotRoot
		{
			name: "PtrHeadStringNotRoot",
			data: struct {
				A *struct {
					A string `json:"a"`
				}
			}{A: &(struct {
				A string `json:"a"`
			}{A: "foo"})},
		},
		{
			name: "PtrHeadStringNotRootOmitEmpty",
			data: struct {
				A *struct {
					A string `json:"a,omitempty"`
				}
			}{A: &(struct {
				A string `json:"a,omitempty"`
			}{A: "foo"})},
		},
		{
			name: "PtrHeadStringNotRootString",
			data: struct {
				A *struct {
					A string `json:"a,string"`
				}
			}{A: &(struct {
				A string `json:"a,string"`
			}{A: "foo"})},
		},
		{
			name: "PtrHeadStringNotRootStringOmitEmpty",
			data: struct {
				A *struct {
					A string `json:"a,string,omitempty"`
				}
			}{A: &(struct {
				A string `json:"a,string,omitempty"`
			}{A: "foo"})},
		},

		// PtrHeadStringPtrNotRoot
		{
			name: "PtrHeadStringPtrNotRoot",
			data: struct {
				A *struct {
					A *string `json:"a"`
				}
			}{A: &(struct {
				A *string `json:"a"`
			}{A: stringptr("foo")})},
		},
		{
			name: "PtrHeadStringPtrNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *string `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *string `json:"a,omitempty"`
			}{A: stringptr("foo")})},
		},
		{
			name: "PtrHeadStringPtrNotRootString",
			data: struct {
				A *struct {
					A *string `json:"a,string"`
				}
			}{A: &(struct {
				A *string `json:"a,string"`
			}{A: stringptr("foo")})},
		},
		{
			name: "PtrHeadStringPtrNotRootStringOmitEmpty",
			data: struct {
				A *struct {
					A *string `json:"a,string,omitempty"`
				}
			}{A: &(struct {
				A *string `json:"a,string,omitempty"`
			}{A: stringptr("foo")})},
		},

		// PtrHeadStringPtrNilNotRoot
		{
			name: "PtrHeadStringPtrNilNotRoot",
			data: struct {
				A *struct {
					A *string `json:"a"`
				}
			}{A: &(struct {
				A *string `json:"a"`
			}{A: nil})},
		},
		{
			name: "PtrHeadStringPtrNilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *string `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *string `json:"a,omitempty"`
			}{A: nil})},
		},
		{
			name: "PtrHeadStringPtrNilNotRootString",
			data: struct {
				A *struct {
					A *string `json:"a,string"`
				}
			}{A: &(struct {
				A *string `json:"a,string"`
			}{A: nil})},
		},
		{
			name: "PtrHeadStringPtrNilNotRootStringOmitEmpty",
			data: struct {
				A *struct {
					A *string `json:"a,string,omitempty"`
				}
			}{A: &(struct {
				A *string `json:"a,string,omitempty"`
			}{A: nil})},
		},

		// PtrHeadStringNilNotRoot
		{
			name: "PtrHeadStringNilNotRoot",
			data: struct {
				A *struct {
					A *string `json:"a"`
				}
			}{A: nil},
		},
		{
			name: "PtrHeadStringNilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *string `json:"a,omitempty"`
				} `json:",omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadStringNilNotRootString",
			data: struct {
				A *struct {
					A *string `json:"a,string"`
				} `json:",string"`
			}{A: nil},
		},
		{
			name: "PtrHeadStringNilNotRootStringOmitEmpty",
			data: struct {
				A *struct {
					A *string `json:"a,string,omitempty"`
				} `json:",string,omitempty"`
			}{A: nil},
		},

		// HeadStringZeroMultiFieldsNotRoot
		{
			name: "HeadStringZeroMultiFieldsNotRoot",
			data: struct {
				A struct {
					A string `json:"a"`
				}
				B struct {
					B string `json:"b"`
				}
			}{},
		},
		{
			name: "HeadStringZeroMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A string `json:"a,omitempty"`
				}
				B struct {
					B string `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "HeadStringZeroMultiFieldsNotRootString",
			data: struct {
				A struct {
					A string `json:"a,string"`
				}
				B struct {
					B string `json:"b,string"`
				}
			}{},
		},
		{
			name: "HeadStringZeroMultiFieldsNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A string `json:"a,string,omitempty"`
				}
				B struct {
					B string `json:"b,string,omitempty"`
				}
			}{},
		},

		// HeadStringMultiFieldsNotRoot
		{
			name: "HeadStringMultiFieldsNotRoot",
			data: struct {
				A struct {
					A string `json:"a"`
				}
				B struct {
					B string `json:"b"`
				}
			}{A: struct {
				A string `json:"a"`
			}{A: "foo"}, B: struct {
				B string `json:"b"`
			}{B: "bar"}},
		},
		{
			name: "HeadStringMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A string `json:"a,omitempty"`
				}
				B struct {
					B string `json:"b,omitempty"`
				}
			}{A: struct {
				A string `json:"a,omitempty"`
			}{A: "foo"}, B: struct {
				B string `json:"b,omitempty"`
			}{B: "bar"}},
		},
		{
			name: "HeadStringMultiFieldsNotRootString",
			data: struct {
				A struct {
					A string `json:"a,string"`
				}
				B struct {
					B string `json:"b,string"`
				}
			}{A: struct {
				A string `json:"a,string"`
			}{A: "foo"}, B: struct {
				B string `json:"b,string"`
			}{B: "bar"}},
		},
		{
			name: "HeadStringMultiFieldsNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A string `json:"a,string,omitempty"`
				}
				B struct {
					B string `json:"b,string,omitempty"`
				}
			}{A: struct {
				A string `json:"a,string,omitempty"`
			}{A: "foo"}, B: struct {
				B string `json:"b,string,omitempty"`
			}{B: "bar"}},
		},

		// HeadStringPtrMultiFieldsNotRoot
		{
			name: "HeadStringPtrMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *string `json:"a"`
				}
				B struct {
					B *string `json:"b"`
				}
			}{A: struct {
				A *string `json:"a"`
			}{A: stringptr("foo")}, B: struct {
				B *string `json:"b"`
			}{B: stringptr("bar")}},
		},
		{
			name: "HeadStringPtrMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *string `json:"a,omitempty"`
				}
				B struct {
					B *string `json:"b,omitempty"`
				}
			}{A: struct {
				A *string `json:"a,omitempty"`
			}{A: stringptr("foo")}, B: struct {
				B *string `json:"b,omitempty"`
			}{B: stringptr("bar")}},
		},
		{
			name: "HeadStringPtrMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *string `json:"a,string"`
				}
				B struct {
					B *string `json:"b,string"`
				}
			}{A: struct {
				A *string `json:"a,string"`
			}{A: stringptr("foo")}, B: struct {
				B *string `json:"b,string"`
			}{B: stringptr("bar")}},
		},
		{
			name: "HeadStringPtrMultiFieldsNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A *string `json:"a,string,omitempty"`
				}
				B struct {
					B *string `json:"b,string,omitempty"`
				}
			}{A: struct {
				A *string `json:"a,string,omitempty"`
			}{A: stringptr("foo")}, B: struct {
				B *string `json:"b,string,omitempty"`
			}{B: stringptr("bar")}},
		},

		// HeadStringPtrNilMultiFieldsNotRoot
		{
			name: "HeadStringPtrNilMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *string `json:"a"`
				}
				B struct {
					B *string `json:"b"`
				}
			}{A: struct {
				A *string `json:"a"`
			}{A: nil}, B: struct {
				B *string `json:"b"`
			}{B: nil}},
		},
		{
			name: "HeadStringPtrNilMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *string `json:"a,omitempty"`
				}
				B struct {
					B *string `json:"b,omitempty"`
				}
			}{A: struct {
				A *string `json:"a,omitempty"`
			}{A: nil}, B: struct {
				B *string `json:"b,omitempty"`
			}{B: nil}},
		},
		{
			name: "HeadStringPtrNilMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *string `json:"a,string"`
				}
				B struct {
					B *string `json:"b,string"`
				}
			}{A: struct {
				A *string `json:"a,string"`
			}{A: nil}, B: struct {
				B *string `json:"b,string"`
			}{B: nil}},
		},
		{
			name: "HeadStringPtrNilMultiFieldsNotRootStringOmitEmpty",
			data: struct {
				A struct {
					A *string `json:"a,string,omitempty"`
				}
				B struct {
					B *string `json:"b,string,omitempty"`
				}
			}{A: struct {
				A *string `json:"a,string,omitempty"`
			}{A: nil}, B: struct {
				B *string `json:"b,string,omitempty"`
			}{B: nil}},
		},

		// PtrHeadStringZeroMultiFieldsNotRoot
		{
			name: "PtrHeadStringZeroMultiFieldsNotRoot",
			data: &struct {
				A struct {
					A string `json:"a"`
				}
				B struct {
					B string `json:"b"`
				}
			}{},
		},
		{
			name: "PtrHeadStringZeroMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A string `json:"a,omitempty"`
				}
				B struct {
					B string `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "PtrHeadStringZeroMultiFieldsNotRootString",
			data: &struct {
				A struct {
					A string `json:"a,string"`
				}
				B struct {
					B string `json:"b,string"`
				}
			}{},
		},
		{
			name: "PtrHeadStringZeroMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A struct {
					A string `json:"a,string,omitempty"`
				}
				B struct {
					B string `json:"b,string,omitempty"`
				}
			}{},
		},

		// PtrHeadStringMultiFieldsNotRoot
		{
			name: "PtrHeadStringMultiFieldsNotRoot",
			data: &struct {
				A struct {
					A string `json:"a"`
				}
				B struct {
					B string `json:"b"`
				}
			}{A: struct {
				A string `json:"a"`
			}{A: "foo"}, B: struct {
				B string `json:"b"`
			}{B: "bar"}},
		},
		{
			name: "PtrHeadStringMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A string `json:"a,omitempty"`
				}
				B struct {
					B string `json:"b,omitempty"`
				}
			}{A: struct {
				A string `json:"a,omitempty"`
			}{A: "foo"}, B: struct {
				B string `json:"b,omitempty"`
			}{B: "bar"}},
		},
		{
			name: "PtrHeadStringMultiFieldsNotRootString",
			data: &struct {
				A struct {
					A string `json:"a,string"`
				}
				B struct {
					B string `json:"b,string"`
				}
			}{A: struct {
				A string `json:"a,string"`
			}{A: "foo"}, B: struct {
				B string `json:"b,string"`
			}{B: "bar"}},
		},
		{
			name: "PtrHeadStringMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A struct {
					A string `json:"a,string,omitempty"`
				}
				B struct {
					B string `json:"b,string,omitempty"`
				}
			}{A: struct {
				A string `json:"a,string,omitempty"`
			}{A: "foo"}, B: struct {
				B string `json:"b,string,omitempty"`
			}{B: "bar"}},
		},

		// PtrHeadStringPtrMultiFieldsNotRoot
		{
			name: "PtrHeadStringPtrMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *string `json:"a"`
				}
				B *struct {
					B *string `json:"b"`
				}
			}{A: &(struct {
				A *string `json:"a"`
			}{A: stringptr("foo")}), B: &(struct {
				B *string `json:"b"`
			}{B: stringptr("bar")})},
		},
		{
			name: "PtrHeadStringPtrMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *string `json:"a,omitempty"`
				}
				B *struct {
					B *string `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *string `json:"a,omitempty"`
			}{A: stringptr("foo")}), B: &(struct {
				B *string `json:"b,omitempty"`
			}{B: stringptr("bar")})},
		},
		{
			name: "PtrHeadStringPtrMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *string `json:"a,string"`
				}
				B *struct {
					B *string `json:"b,string"`
				}
			}{A: &(struct {
				A *string `json:"a,string"`
			}{A: stringptr("foo")}), B: &(struct {
				B *string `json:"b,string"`
			}{B: stringptr("bar")})},
		},
		{
			name: "PtrHeadStringPtrMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A *struct {
					A *string `json:"a,string,omitempty"`
				}
				B *struct {
					B *string `json:"b,string,omitempty"`
				}
			}{A: &(struct {
				A *string `json:"a,string,omitempty"`
			}{A: stringptr("foo")}), B: &(struct {
				B *string `json:"b,string,omitempty"`
			}{B: stringptr("bar")})},
		},

		// PtrHeadStringPtrNilMultiFieldsNotRoot
		{
			name: "PtrHeadStringPtrNilMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *string `json:"a"`
				}
				B *struct {
					B *string `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadStringPtrNilMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *string `json:"a,omitempty"`
				}
				B *struct {
					B *string `json:"b,omitempty"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadStringPtrNilMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *string `json:"a,string"`
				}
				B *struct {
					B *string `json:"b,string"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadStringPtrNilMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A *struct {
					A *string `json:"a,string,omitempty"`
				}
				B *struct {
					B *string `json:"b,string,omitempty"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadStringNilMultiFieldsNotRoot
		{
			name: "PtrHeadStringNilMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *string `json:"a"`
				}
				B *struct {
					B *string `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadStringNilMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *string `json:"a,omitempty"`
				}
				B *struct {
					B *string `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadStringNilMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *string `json:"a,string"`
				}
				B *struct {
					B *string `json:"b,string"`
				}
			})(nil),
		},
		{
			name: "PtrHeadStringNilMultiFieldsNotRootStringOmitEmpty",
			data: (*struct {
				A *struct {
					A *string `json:"a,string,omitempty"`
				}
				B *struct {
					B *string `json:"b,string,omitempty"`
				}
			})(nil),
		},

		// PtrHeadStringDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadStringDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A string `json:"a"`
					B string `json:"b"`
					C string `json:"c"`
				}
				B *struct {
					A string `json:"a"`
					B string `json:"b"`
					C string `json:"c"`
				}
			}{A: &(struct {
				A string `json:"a"`
				B string `json:"b"`
				C string `json:"c"`
			}{A: "foo", B: "bar", C: "baz"}), B: &(struct {
				A string `json:"a"`
				B string `json:"b"`
				C string `json:"c"`
			}{A: "foo", B: "bar", C: "baz"})},
		},
		{
			name: "PtrHeadStringDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A string `json:"a,omitempty"`
					B string `json:"b,omitempty"`
				}
				B *struct {
					A string `json:"a,omitempty"`
					B string `json:"b,omitempty"`
				}
			}{A: &(struct {
				A string `json:"a,omitempty"`
				B string `json:"b,omitempty"`
			}{A: "foo", B: "bar"}), B: &(struct {
				A string `json:"a,omitempty"`
				B string `json:"b,omitempty"`
			}{A: "foo", B: "bar"})},
		},
		{
			name: "PtrHeadStringDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A string `json:"a,string"`
					B string `json:"b,string"`
				}
				B *struct {
					A string `json:"a,string"`
					B string `json:"b,string"`
				}
			}{A: &(struct {
				A string `json:"a,string"`
				B string `json:"b,string"`
			}{A: "foo", B: "bar"}), B: &(struct {
				A string `json:"a,string"`
				B string `json:"b,string"`
			}{A: "foo", B: "bar"})},
		},
		{
			name: "PtrHeadStringDoubleMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A *struct {
					A string `json:"a,string,omitempty"`
					B string `json:"b,string,omitempty"`
				}
				B *struct {
					A string `json:"a,string,omitempty"`
					B string `json:"b,string,omitempty"`
				}
			}{A: &(struct {
				A string `json:"a,string,omitempty"`
				B string `json:"b,string,omitempty"`
			}{A: "foo", B: "bar"}), B: &(struct {
				A string `json:"a,string,omitempty"`
				B string `json:"b,string,omitempty"`
			}{A: "foo", B: "bar"})},
		},

		// PtrHeadStringNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadStringNilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A string `json:"a"`
					B string `json:"b"`
				}
				B *struct {
					A string `json:"a"`
					B string `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadStringNilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A string `json:"a,omitempty"`
					B string `json:"b,omitempty"`
				}
				B *struct {
					A string `json:"a,omitempty"`
					B string `json:"b,omitempty"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadStringNilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A string `json:"a,string"`
					B string `json:"b,string"`
				}
				B *struct {
					A string `json:"a,string"`
					B string `json:"b,string"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadStringNilDoubleMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A *struct {
					A string `json:"a,string,omitempty"`
					B string `json:"b,string,omitempty"`
				}
				B *struct {
					A string `json:"a,string,omitempty"`
					B string `json:"b,string,omitempty"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadStringNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadStringNilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A string `json:"a"`
					B string `json:"b"`
				}
				B *struct {
					A string `json:"a"`
					B string `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadStringNilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A string `json:"a,omitempty"`
					B string `json:"b,omitempty"`
				}
				B *struct {
					A string `json:"a,omitempty"`
					B string `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadStringNilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A string `json:"a,string"`
					B string `json:"b,string"`
				}
				B *struct {
					A string `json:"a,string"`
					B string `json:"b,string"`
				}
			})(nil),
		},
		{
			name: "PtrHeadStringNilDoubleMultiFieldsNotRootStringOmitEmpty",
			data: (*struct {
				A *struct {
					A string `json:"a,string,omitempty"`
					B string `json:"b,string,omitempty"`
				}
				B *struct {
					A string `json:"a,string,omitempty"`
					B string `json:"b,string,omitempty"`
				}
			})(nil),
		},

		// PtrHeadStringPtrDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadStringPtrDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *string `json:"a"`
					B *string `json:"b"`
				}
				B *struct {
					A *string `json:"a"`
					B *string `json:"b"`
				}
			}{A: &(struct {
				A *string `json:"a"`
				B *string `json:"b"`
			}{A: stringptr("foo"), B: stringptr("bar")}), B: &(struct {
				A *string `json:"a"`
				B *string `json:"b"`
			}{A: stringptr("foo"), B: stringptr("bar")})},
		},
		{
			name: "PtrHeadStringPtrDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *string `json:"a,omitempty"`
					B *string `json:"b,omitempty"`
				}
				B *struct {
					A *string `json:"a,omitempty"`
					B *string `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *string `json:"a,omitempty"`
				B *string `json:"b,omitempty"`
			}{A: stringptr("foo"), B: stringptr("bar")}), B: &(struct {
				A *string `json:"a,omitempty"`
				B *string `json:"b,omitempty"`
			}{A: stringptr("foo"), B: stringptr("bar")})},
		},
		{
			name: "PtrHeadStringPtrDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *string `json:"a,string"`
					B *string `json:"b,string"`
				}
				B *struct {
					A *string `json:"a,string"`
					B *string `json:"b,string"`
				}
			}{A: &(struct {
				A *string `json:"a,string"`
				B *string `json:"b,string"`
			}{A: stringptr("foo"), B: stringptr("bar")}), B: &(struct {
				A *string `json:"a,string"`
				B *string `json:"b,string"`
			}{A: stringptr("foo"), B: stringptr("bar")})},
		},
		{
			name: "PtrHeadStringPtrDoubleMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A *struct {
					A *string `json:"a,string,omitempty"`
					B *string `json:"b,string,omitempty"`
				}
				B *struct {
					A *string `json:"a,string,omitempty"`
					B *string `json:"b,string,omitempty"`
				}
			}{A: &(struct {
				A *string `json:"a,string,omitempty"`
				B *string `json:"b,string,omitempty"`
			}{A: stringptr("foo"), B: stringptr("bar")}), B: &(struct {
				A *string `json:"a,string,omitempty"`
				B *string `json:"b,string,omitempty"`
			}{A: stringptr("foo"), B: stringptr("bar")})},
		},

		// PtrHeadStringPtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadStringPtrNilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *string `json:"a"`
					B *string `json:"b"`
				}
				B *struct {
					A *string `json:"a"`
					B *string `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadStringPtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *string `json:"a,omitempty"`
					B *string `json:"b,omitempty"`
				}
				B *struct {
					A *string `json:"a,omitempty"`
					B *string `json:"b,omitempty"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadStringPtrNilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *string `json:"a,string"`
					B *string `json:"b,string"`
				}
				B *struct {
					A *string `json:"a,string"`
					B *string `json:"b,string"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadStringPtrNilDoubleMultiFieldsNotRootStringOmitEmpty",
			data: &struct {
				A *struct {
					A *string `json:"a,string,omitempty"`
					B *string `json:"b,string,omitempty"`
				}
				B *struct {
					A *string `json:"a,string,omitempty"`
					B *string `json:"b,string,omitempty"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadStringPtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadStringPtrNilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *string `json:"a"`
					B *string `json:"b"`
				}
				B *struct {
					A *string `json:"a"`
					B *string `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadStringPtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *string `json:"a,omitempty"`
					B *string `json:"b,omitempty"`
				}
				B *struct {
					A *string `json:"a,omitempty"`
					B *string `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadStringPtrNilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *string `json:"a,string"`
					B *string `json:"b,string"`
				}
				B *struct {
					A *string `json:"a,string"`
					B *string `json:"b,string"`
				}
			})(nil),
		},
		{
			name: "PtrHeadStringPtrNilDoubleMultiFieldsNotRootStringOmitEmpty",
			data: (*struct {
				A *struct {
					A *string `json:"a,string,omitempty"`
					B *string `json:"b,string,omitempty"`
				}
				B *struct {
					A *string `json:"a,string,omitempty"`
					B *string `json:"b,string,omitempty"`
				}
			})(nil),
		},

		// AnonymousHeadString
		{
			name: "AnonymousHeadString",
			data: struct {
				structString
				B string `json:"b"`
			}{
				structString: structString{A: "foo"},
				B:            "bar",
			},
		},
		{
			name: "AnonymousHeadStringOmitEmpty",
			data: struct {
				structStringOmitEmpty
				B string `json:"b,omitempty"`
			}{
				structStringOmitEmpty: structStringOmitEmpty{A: "foo"},
				B:                     "bar",
			},
		},
		{
			name: "AnonymousHeadStringString",
			data: struct {
				structStringString
				B string `json:"b,string"`
			}{
				structStringString: structStringString{A: "foo"},
				B:                  "bar",
			},
		},
		{
			name: "AnonymousHeadStringStringOmitEmpty",
			data: struct {
				structStringStringOmitEmpty
				B string `json:"b,string,omitempty"`
			}{
				structStringStringOmitEmpty: structStringStringOmitEmpty{A: "foo"},
				B:                           "bar",
			},
		},

		// PtrAnonymousHeadString
		{
			name: "PtrAnonymousHeadString",
			data: struct {
				*structString
				B string `json:"b"`
			}{
				structString: &structString{A: "foo"},
				B:            "bar",
			},
		},
		{
			name: "PtrAnonymousHeadStringOmitEmpty",
			data: struct {
				*structStringOmitEmpty
				B string `json:"b,omitempty"`
			}{
				structStringOmitEmpty: &structStringOmitEmpty{A: "foo"},
				B:                     "bar",
			},
		},
		{
			name: "PtrAnonymousHeadStringString",
			data: struct {
				*structStringString
				B string `json:"b,string"`
			}{
				structStringString: &structStringString{A: "foo"},
				B:                  "bar",
			},
		},
		{
			name: "PtrAnonymousHeadStringStringOmitEmpty",
			data: struct {
				*structStringStringOmitEmpty
				B string `json:"b,string,omitempty"`
			}{
				structStringStringOmitEmpty: &structStringStringOmitEmpty{A: "foo"},
				B:                           "bar",
			},
		},

		// NilPtrAnonymousHeadString
		{
			name: "NilPtrAnonymousHeadString",
			data: struct {
				*structString
				B string `json:"b"`
			}{
				structString: nil,
				B:            "baz",
			},
		},
		{
			name: "NilPtrAnonymousHeadStringOmitEmpty",
			data: struct {
				*structStringOmitEmpty
				B string `json:"b,omitempty"`
			}{
				structStringOmitEmpty: nil,
				B:                     "baz",
			},
		},
		{
			name: "NilPtrAnonymousHeadStringString",
			data: struct {
				*structStringString
				B string `json:"b,string"`
			}{
				structStringString: nil,
				B:                  "baz",
			},
		},
		{
			name: "NilPtrAnonymousHeadStringStringOmitEmpty",
			data: struct {
				*structStringStringOmitEmpty
				B string `json:"b,string,omitempty"`
			}{
				structStringStringOmitEmpty: nil,
				B:                           "baz",
			},
		},

		// AnonymousHeadStringPtr
		{
			name: "AnonymousHeadStringPtr",
			data: struct {
				structStringPtr
				B *string `json:"b"`
			}{
				structStringPtr: structStringPtr{A: stringptr("foo")},
				B:               stringptr("bar"),
			},
		},
		{
			name: "AnonymousHeadStringPtrOmitEmpty",
			data: struct {
				structStringPtrOmitEmpty
				B *string `json:"b,omitempty"`
			}{
				structStringPtrOmitEmpty: structStringPtrOmitEmpty{A: stringptr("foo")},
				B:                        stringptr("bar"),
			},
		},
		{
			name: "AnonymousHeadStringPtrString",
			data: struct {
				structStringPtrString
				B *string `json:"b,string"`
			}{
				structStringPtrString: structStringPtrString{A: stringptr("foo")},
				B:                     stringptr("bar"),
			},
		},
		{
			name: "AnonymousHeadStringPtrStringOmitEmpty",
			data: struct {
				structStringPtrStringOmitEmpty
				B *string `json:"b,string,omitempty"`
			}{
				structStringPtrStringOmitEmpty: structStringPtrStringOmitEmpty{A: stringptr("foo")},
				B:                              stringptr("bar"),
			},
		},

		// AnonymousHeadStringPtrNil
		{
			name: "AnonymousHeadStringPtrNil",
			data: struct {
				structStringPtr
				B *string `json:"b"`
			}{
				structStringPtr: structStringPtr{A: nil},
				B:               stringptr("foo"),
			},
		},
		{
			name: "AnonymousHeadStringPtrNilOmitEmpty",
			data: struct {
				structStringPtrOmitEmpty
				B *string `json:"b,omitempty"`
			}{
				structStringPtrOmitEmpty: structStringPtrOmitEmpty{A: nil},
				B:                        stringptr("foo"),
			},
		},
		{
			name: "AnonymousHeadStringPtrNilString",
			data: struct {
				structStringPtrString
				B *string `json:"b,string"`
			}{
				structStringPtrString: structStringPtrString{A: nil},
				B:                     stringptr("foo"),
			},
		},
		{
			name: "AnonymousHeadStringPtrNilStringOmitEmpty",
			data: struct {
				structStringPtrStringOmitEmpty
				B *string `json:"b,string,omitempty"`
			}{
				structStringPtrStringOmitEmpty: structStringPtrStringOmitEmpty{A: nil},
				B:                              stringptr("foo"),
			},
		},

		// PtrAnonymousHeadStringPtr
		{
			name: "PtrAnonymousHeadStringPtr",
			data: struct {
				*structStringPtr
				B *string `json:"b"`
			}{
				structStringPtr: &structStringPtr{A: stringptr("foo")},
				B:               stringptr("bar"),
			},
		},
		{
			name: "PtrAnonymousHeadStringPtrOmitEmpty",
			data: struct {
				*structStringPtrOmitEmpty
				B *string `json:"b,omitempty"`
			}{
				structStringPtrOmitEmpty: &structStringPtrOmitEmpty{A: stringptr("foo")},
				B:                        stringptr("bar"),
			},
		},
		{
			name: "PtrAnonymousHeadStringPtrString",
			data: struct {
				*structStringPtrString
				B *string `json:"b,string"`
			}{
				structStringPtrString: &structStringPtrString{A: stringptr("foo")},
				B:                     stringptr("bar"),
			},
		},
		{
			name: "PtrAnonymousHeadStringPtrStringOmitEmpty",
			data: struct {
				*structStringPtrStringOmitEmpty
				B *string `json:"b,string,omitempty"`
			}{
				structStringPtrStringOmitEmpty: &structStringPtrStringOmitEmpty{A: stringptr("foo")},
				B:                              stringptr("bar"),
			},
		},

		// NilPtrAnonymousHeadStringPtr
		{
			name: "NilPtrAnonymousHeadStringPtr",
			data: struct {
				*structStringPtr
				B *string `json:"b"`
			}{
				structStringPtr: nil,
				B:               stringptr("foo"),
			},
		},
		{
			name: "NilPtrAnonymousHeadStringPtrOmitEmpty",
			data: struct {
				*structStringPtrOmitEmpty
				B *string `json:"b,omitempty"`
			}{
				structStringPtrOmitEmpty: nil,
				B:                        stringptr("foo"),
			},
		},
		{
			name: "NilPtrAnonymousHeadStringPtrString",
			data: struct {
				*structStringPtrString
				B *string `json:"b,string"`
			}{
				structStringPtrString: nil,
				B:                     stringptr("foo"),
			},
		},
		{
			name: "NilPtrAnonymousHeadStringPtrStringOmitEmpty",
			data: struct {
				*structStringPtrStringOmitEmpty
				B *string `json:"b,string,omitempty"`
			}{
				structStringPtrStringOmitEmpty: nil,
				B:                              stringptr("foo"),
			},
		},

		// AnonymousHeadStringOnly
		{
			name: "AnonymousHeadStringOnly",
			data: struct {
				structString
			}{
				structString: structString{A: "foo"},
			},
		},
		{
			name: "AnonymousHeadStringOnlyOmitEmpty",
			data: struct {
				structStringOmitEmpty
			}{
				structStringOmitEmpty: structStringOmitEmpty{A: "foo"},
			},
		},
		{
			name: "AnonymousHeadStringOnlyString",
			data: struct {
				structStringString
			}{
				structStringString: structStringString{A: "foo"},
			},
		},
		{
			name: "AnonymousHeadStringOnlyStringOmitEmpty",
			data: struct {
				structStringStringOmitEmpty
			}{
				structStringStringOmitEmpty: structStringStringOmitEmpty{A: "foo"},
			},
		},

		// PtrAnonymousHeadStringOnly
		{
			name: "PtrAnonymousHeadStringOnly",
			data: struct {
				*structString
			}{
				structString: &structString{A: "foo"},
			},
		},
		{
			name: "PtrAnonymousHeadStringOnlyOmitEmpty",
			data: struct {
				*structStringOmitEmpty
			}{
				structStringOmitEmpty: &structStringOmitEmpty{A: "foo"},
			},
		},
		{
			name: "PtrAnonymousHeadStringOnlyString",
			data: struct {
				*structStringString
			}{
				structStringString: &structStringString{A: "foo"},
			},
		},
		{
			name: "PtrAnonymousHeadStringOnlyStringOmitEmpty",
			data: struct {
				*structStringStringOmitEmpty
			}{
				structStringStringOmitEmpty: &structStringStringOmitEmpty{A: "foo"},
			},
		},

		// NilPtrAnonymousHeadStringOnly
		{
			name: "NilPtrAnonymousHeadStringOnly",
			data: struct {
				*structString
			}{
				structString: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadStringOnlyOmitEmpty",
			data: struct {
				*structStringOmitEmpty
			}{
				structStringOmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadStringOnlyString",
			data: struct {
				*structStringString
			}{
				structStringString: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadStringOnlyStringOmitEmpty",
			data: struct {
				*structStringStringOmitEmpty
			}{
				structStringStringOmitEmpty: nil,
			},
		},

		// AnonymousHeadStringPtrOnly
		{
			name: "AnonymousHeadStringPtrOnly",
			data: struct {
				structStringPtr
			}{
				structStringPtr: structStringPtr{A: stringptr("foo")},
			},
		},
		{
			name: "AnonymousHeadStringPtrOnlyOmitEmpty",
			data: struct {
				structStringPtrOmitEmpty
			}{
				structStringPtrOmitEmpty: structStringPtrOmitEmpty{A: stringptr("foo")},
			},
		},
		{
			name: "AnonymousHeadStringPtrOnlyString",
			data: struct {
				structStringPtrString
			}{
				structStringPtrString: structStringPtrString{A: stringptr("foo")},
			},
		},
		{
			name: "AnonymousHeadStringPtrOnlyStringOmitEmpty",
			data: struct {
				structStringPtrStringOmitEmpty
			}{
				structStringPtrStringOmitEmpty: structStringPtrStringOmitEmpty{A: stringptr("foo")},
			},
		},

		// AnonymousHeadStringPtrNilOnly
		{
			name: "AnonymousHeadStringPtrNilOnly",
			data: struct {
				structStringPtr
			}{
				structStringPtr: structStringPtr{A: nil},
			},
		},
		{
			name: "AnonymousHeadStringPtrNilOnlyOmitEmpty",
			data: struct {
				structStringPtrOmitEmpty
			}{
				structStringPtrOmitEmpty: structStringPtrOmitEmpty{A: nil},
			},
		},
		{
			name: "AnonymousHeadStringPtrNilOnlyString",
			data: struct {
				structStringPtrString
			}{
				structStringPtrString: structStringPtrString{A: nil},
			},
		},
		{
			name: "AnonymousHeadStringPtrNilOnlyStringOmitEmpty",
			data: struct {
				structStringPtrStringOmitEmpty
			}{
				structStringPtrStringOmitEmpty: structStringPtrStringOmitEmpty{A: nil},
			},
		},

		// PtrAnonymousHeadStringPtrOnly
		{
			name: "PtrAnonymousHeadStringPtrOnly",
			data: struct {
				*structStringPtr
			}{
				structStringPtr: &structStringPtr{A: stringptr("foo")},
			},
		},
		{
			name: "PtrAnonymousHeadStringPtrOnlyOmitEmpty",
			data: struct {
				*structStringPtrOmitEmpty
			}{
				structStringPtrOmitEmpty: &structStringPtrOmitEmpty{A: stringptr("foo")},
			},
		},
		{
			name: "PtrAnonymousHeadStringPtrOnlyString",
			data: struct {
				*structStringPtrString
			}{
				structStringPtrString: &structStringPtrString{A: stringptr("foo")},
			},
		},
		{
			name: "PtrAnonymousHeadStringPtrOnlyStringOmitEmpty",
			data: struct {
				*structStringPtrStringOmitEmpty
			}{
				structStringPtrStringOmitEmpty: &structStringPtrStringOmitEmpty{A: stringptr("foo")},
			},
		},

		// NilPtrAnonymousHeadStringPtrOnly
		{
			name: "NilPtrAnonymousHeadStringPtrOnly",
			data: struct {
				*structStringPtr
			}{
				structStringPtr: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadStringPtrOnlyOmitEmpty",
			data: struct {
				*structStringPtrOmitEmpty
			}{
				structStringPtrOmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadStringPtrOnlyString",
			data: struct {
				*structStringPtrString
			}{
				structStringPtrString: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadStringPtrOnlyStringOmitEmpty",
			data: struct {
				*structStringPtrStringOmitEmpty
			}{
				structStringPtrStringOmitEmpty: nil,
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
						t.Fatalf("%s(htmlEscape:%v,indent:%v): %v: %s", test.name, htmlEscape, indent, test.data, err)
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
