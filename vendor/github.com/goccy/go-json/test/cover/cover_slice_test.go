package json_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/goccy/go-json"
)

type coverSliceMarshalJSON struct {
	A int
}

func (coverSliceMarshalJSON) MarshalJSON() ([]byte, error) {
	return []byte(`"hello"`), nil
}

type coverSliceMarshalText struct {
	A int
}

func (coverSliceMarshalText) MarshalText() ([]byte, error) {
	return []byte(`"hello"`), nil
}

type recursiveSlice struct {
	A int
	B []*recursiveSlice
}

func TestCoverSlice(t *testing.T) {
	type structSlice struct {
		A []int `json:"a"`
	}
	type structSliceOmitEmpty struct {
		A []int `json:"a,omitempty"`
	}
	type structSliceString struct {
		A []int `json:"a,string"`
	}
	type structSlicePtr struct {
		A *[]int `json:"a"`
	}
	type structSlicePtrOmitEmpty struct {
		A *[]int `json:"a,omitempty"`
	}
	type structSlicePtrString struct {
		A *[]int `json:"a,string"`
	}

	type structSlicePtrContent struct {
		A []*int `json:"a"`
	}
	type structSliceOmitEmptyPtrContent struct {
		A []*int `json:"a,omitempty"`
	}
	type structSliceStringPtrContent struct {
		A []*int `json:"a,string"`
	}
	type structSlicePtrPtrContent struct {
		A *[]*int `json:"a"`
	}
	type structSlicePtrOmitEmptyPtrContent struct {
		A *[]*int `json:"a,omitempty"`
	}
	type structSlicePtrStringPtrContent struct {
		A *[]*int `json:"a,string"`
	}

	tests := []struct {
		name string
		data interface{}
	}{
		{
			name: "SliceInt",
			data: []int{1, 2, 3},
		},
		{
			name: "SliceInt8",
			data: []int8{1, 2, 3},
		},
		{
			name: "SliceInt16",
			data: []int16{1, 2, 3},
		},
		{
			name: "SliceInt32",
			data: []int32{1, 2, 3},
		},
		{
			name: "SliceInt64",
			data: []int64{1, 2, 3},
		},
		{
			name: "SliceUint",
			data: []uint{1, 2, 3},
		},
		{
			name: "SliceUint8",
			data: []uint8{1, 2, 3},
		},
		{
			name: "SliceUint16",
			data: []uint16{1, 2, 3},
		},
		{
			name: "SliceUint32",
			data: []uint32{1, 2, 3},
		},
		{
			name: "SliceUint64",
			data: []uint64{1, 2, 3},
		},
		{
			name: "SliceFloat32",
			data: []float32{1, 2, 3},
		},
		{
			name: "SliceFloat64",
			data: []float64{1, 2, 3},
		},
		{
			name: "SliceString",
			data: []string{"a", "b"},
		},
		{
			name: "SliceBool",
			data: []bool{false, true, false},
		},
		{
			name: "SliceBytes",
			data: [][]byte{[]byte("a"), []byte("b"), nil, []byte("c")},
		},
		{
			name: "SliceSlice",
			data: [][]int{[]int{1, 2, 3}, nil, []int{4, 5, 6}},
		},
		{
			name: "SliceArray",
			data: [][3]int{[3]int{1, 2, 3}, [3]int{4, 5, 6}},
		},
		{
			name: "SliceMap",
			data: []map[string]int{map[string]int{"a": 1}, nil, map[string]int{"b": 2}},
		},
		{
			name: "SliceStruct",
			data: []struct{ A int }{struct{ A int }{A: 1}, struct{ A int }{A: 2}},
		},
		{
			name: "SliceMarshalJSON",
			data: []coverSliceMarshalJSON{{A: 1}, {A: 2}},
		},
		{
			name: "SliceMarshalText",
			data: []coverSliceMarshalText{{A: 1}, {A: 2}},
		},
		{
			name: "SliceIntPtr",
			data: []*int{intptr(1), intptr(2), nil, intptr(3)},
		},
		{
			name: "SliceInt8Ptr",
			data: []*int8{int8ptr(1), int8ptr(2), nil, int8ptr(3)},
		},
		{
			name: "SliceInt16Ptr",
			data: []*int16{int16ptr(1), int16ptr(2), nil, int16ptr(3)},
		},
		{
			name: "SliceInt32Ptr",
			data: []*int32{int32ptr(1), int32ptr(2), nil, int32ptr(3)},
		},
		{
			name: "SliceInt64Ptr",
			data: []*int64{int64ptr(1), int64ptr(2), nil, int64ptr(3)},
		},
		{
			name: "SliceUintPtr",
			data: []*uint{uptr(1), uptr(2), nil, uptr(3)},
		},
		{
			name: "SliceUint8Ptr",
			data: []*uint8{uint8ptr(1), uint8ptr(2), nil, uint8ptr(3)},
		},
		{
			name: "SliceUint16Ptr",
			data: []*uint16{uint16ptr(1), uint16ptr(2), nil, uint16ptr(3)},
		},
		{
			name: "SliceUint32Ptr",
			data: []*uint32{uint32ptr(1), uint32ptr(2), nil, uint32ptr(3)},
		},
		{
			name: "SliceUint64Ptr",
			data: []*uint64{uint64ptr(1), uint64ptr(2), nil, uint64ptr(3)},
		},
		{
			name: "SliceFloat32Ptr",
			data: []*float32{float32ptr(1), float32ptr(2), nil, float32ptr(3)},
		},
		{
			name: "SliceFloat64Ptr",
			data: []*float64{float64ptr(1), float64ptr(2), nil, float64ptr(3)},
		},
		{
			name: "SliceStringPtr",
			data: []*string{stringptr("a"), nil, stringptr("b")},
		},
		{
			name: "SliceBoolPtr",
			data: []*bool{boolptr(false), boolptr(true), nil, boolptr(false)},
		},
		{
			name: "SliceBytesPtr",
			data: []*[]byte{bytesptr([]byte("a")), bytesptr([]byte("b")), nil, bytesptr([]byte("c"))},
		},
		{
			name: "SliceSlicePtr",
			data: []*[]int{sliceptr([]int{1, 2, 3}), nil, sliceptr([]int{4, 5, 6})},
		},
		{
			name: "SliceArrayPtr",
			data: []*[2]int{arrayptr([2]int{1, 2}), nil, arrayptr([2]int{4, 5})},
		},
		{
			name: "SliceMapPtr",
			data: []*map[string]int{mapptr(map[string]int{"a": 1}), nil, mapptr(map[string]int{"b": 2})},
		},
		{
			name: "SliceStructPtr",
			data: []*struct{ A int }{&struct{ A int }{A: 1}, &struct{ A int }{A: 2}},
		},
		{
			name: "RecursiveSlice",
			data: []*recursiveSlice{
				{
					A: 1, B: []*recursiveSlice{
						{
							A: 2, B: []*recursiveSlice{
								{
									A: 3,
								},
							},
						},
					},
				},
				{
					A: 4, B: []*recursiveSlice{
						{
							A: 5, B: []*recursiveSlice{
								{
									A: 6,
								},
							},
						},
					},
				},
			},
		},

		// HeadSliceZero
		{
			name: "HeadSliceZero",
			data: struct {
				A []int `json:"a"`
			}{},
		},
		{
			name: "HeadSliceZeroOmitEmpty",
			data: struct {
				A []int `json:"a,omitempty"`
			}{},
		},
		{
			name: "HeadSliceZeroString",
			data: struct {
				A []int `json:"a,string"`
			}{},
		},

		// HeadSlice
		{
			name: "HeadSlice",
			data: struct {
				A []int `json:"a"`
			}{A: []int{-1}},
		},
		{
			name: "HeadSliceOmitEmpty",
			data: struct {
				A []int `json:"a,omitempty"`
			}{A: []int{-1}},
		},
		{
			name: "HeadSliceString",
			data: struct {
				A []int `json:"a,string"`
			}{A: []int{-1}},
		},

		// HeadSlicePtr
		{
			name: "HeadSlicePtr",
			data: struct {
				A *[]int `json:"a"`
			}{A: sliceptr([]int{-1})},
		},
		{
			name: "HeadSlicePtrOmitEmpty",
			data: struct {
				A *[]int `json:"a,omitempty"`
			}{A: sliceptr([]int{-1})},
		},
		{
			name: "HeadSlicePtrString",
			data: struct {
				A *[]int `json:"a,string"`
			}{A: sliceptr([]int{-1})},
		},

		// HeadSlicePtrNil
		{
			name: "HeadSlicePtrNil",
			data: struct {
				A *[]int `json:"a"`
			}{A: nil},
		},
		{
			name: "HeadSlicePtrNilOmitEmpty",
			data: struct {
				A *[]int `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "HeadSlicePtrNilString",
			data: struct {
				A *[]int `json:"a,string"`
			}{A: nil},
		},

		// PtrHeadSliceZero
		{
			name: "PtrHeadSliceZero",
			data: &struct {
				A []int `json:"a"`
			}{},
		},
		{
			name: "PtrHeadSliceZeroOmitEmpty",
			data: &struct {
				A []int `json:"a,omitempty"`
			}{},
		},
		{
			name: "PtrHeadSliceZeroString",
			data: &struct {
				A []int `json:"a,string"`
			}{},
		},

		// PtrHeadSlice
		{
			name: "PtrHeadSlice",
			data: &struct {
				A []int `json:"a"`
			}{A: []int{-1}},
		},
		{
			name: "PtrHeadSliceOmitEmpty",
			data: &struct {
				A []int `json:"a,omitempty"`
			}{A: []int{-1}},
		},
		{
			name: "PtrHeadSliceString",
			data: &struct {
				A []int `json:"a,string"`
			}{A: []int{-1}},
		},

		// PtrHeadSlicePtr
		{
			name: "PtrHeadSlicePtr",
			data: &struct {
				A *[]int `json:"a"`
			}{A: sliceptr([]int{-1})},
		},
		{
			name: "PtrHeadSlicePtrOmitEmpty",
			data: &struct {
				A *[]int `json:"a,omitempty"`
			}{A: sliceptr([]int{-1})},
		},
		{
			name: "PtrHeadSlicePtrString",
			data: &struct {
				A *[]int `json:"a,string"`
			}{A: sliceptr([]int{-1})},
		},

		// PtrHeadSlicePtrNil
		{
			name: "PtrHeadSlicePtrNil",
			data: &struct {
				A *[]int `json:"a"`
			}{A: nil},
		},
		{
			name: "PtrHeadSlicePtrNilOmitEmpty",
			data: &struct {
				A *[]int `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadSlicePtrNilString",
			data: &struct {
				A *[]int `json:"a,string"`
			}{A: nil},
		},

		// PtrHeadSliceNil
		{
			name: "PtrHeadSliceNil",
			data: (*struct {
				A *[]int `json:"a"`
			})(nil),
		},
		{
			name: "PtrHeadSliceNilOmitEmpty",
			data: (*struct {
				A *[]int `json:"a,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadSliceNilString",
			data: (*struct {
				A *[]int `json:"a,string"`
			})(nil),
		},

		// HeadSliceZeroMultiFields
		{
			name: "HeadSliceZeroMultiFields",
			data: struct {
				A []int `json:"a"`
				B []int `json:"b"`
				C []int `json:"c"`
			}{},
		},
		{
			name: "HeadSliceZeroMultiFieldsOmitEmpty",
			data: struct {
				A []int `json:"a,omitempty"`
				B []int `json:"b,omitempty"`
				C []int `json:"c,omitempty"`
			}{},
		},
		{
			name: "HeadSliceZeroMultiFields",
			data: struct {
				A []int `json:"a,string"`
				B []int `json:"b,string"`
				C []int `json:"c,string"`
			}{},
		},

		// HeadSliceMultiFields
		{
			name: "HeadSliceMultiFields",
			data: struct {
				A []int `json:"a"`
				B []int `json:"b"`
				C []int `json:"c"`
			}{A: []int{-1}, B: []int{-2}, C: []int{-3}},
		},
		{
			name: "HeadSliceMultiFieldsOmitEmpty",
			data: struct {
				A []int `json:"a,omitempty"`
				B []int `json:"b,omitempty"`
				C []int `json:"c,omitempty"`
			}{A: []int{-1}, B: []int{-2}, C: []int{-3}},
		},
		{
			name: "HeadSliceMultiFieldsString",
			data: struct {
				A []int `json:"a,string"`
				B []int `json:"b,string"`
				C []int `json:"c,string"`
			}{A: []int{-1}, B: []int{-2}, C: []int{-3}},
		},

		// HeadSlicePtrMultiFields
		{
			name: "HeadSlicePtrMultiFields",
			data: struct {
				A *[]int `json:"a"`
				B *[]int `json:"b"`
				C *[]int `json:"c"`
			}{A: sliceptr([]int{-1}), B: sliceptr([]int{-2}), C: sliceptr([]int{-3})},
		},
		{
			name: "HeadSlicePtrMultiFieldsOmitEmpty",
			data: struct {
				A *[]int `json:"a,omitempty"`
				B *[]int `json:"b,omitempty"`
				C *[]int `json:"c,omitempty"`
			}{A: sliceptr([]int{-1}), B: sliceptr([]int{-2}), C: sliceptr([]int{-3})},
		},
		{
			name: "HeadSlicePtrMultiFieldsString",
			data: struct {
				A *[]int `json:"a,string"`
				B *[]int `json:"b,string"`
				C *[]int `json:"c,string"`
			}{A: sliceptr([]int{-1}), B: sliceptr([]int{-2}), C: sliceptr([]int{-3})},
		},

		// HeadSlicePtrNilMultiFields
		{
			name: "HeadSlicePtrNilMultiFields",
			data: struct {
				A *[]int `json:"a"`
				B *[]int `json:"b"`
				C *[]int `json:"c"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadSlicePtrNilMultiFieldsOmitEmpty",
			data: struct {
				A *[]int `json:"a,omitempty"`
				B *[]int `json:"b,omitempty"`
				C *[]int `json:"c,omitempty"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadSlicePtrNilMultiFieldsString",
			data: struct {
				A *[]int `json:"a,string"`
				B *[]int `json:"b,string"`
				C *[]int `json:"c,string"`
			}{A: nil, B: nil, C: nil},
		},

		// PtrHeadSliceZeroMultiFields
		{
			name: "PtrHeadSliceZeroMultiFields",
			data: &struct {
				A []int `json:"a"`
				B []int `json:"b"`
			}{},
		},
		{
			name: "PtrHeadSliceZeroMultiFieldsOmitEmpty",
			data: &struct {
				A []int `json:"a,omitempty"`
				B []int `json:"b,omitempty"`
			}{},
		},
		{
			name: "PtrHeadSliceZeroMultiFieldsString",
			data: &struct {
				A []int `json:"a,string"`
				B []int `json:"b,string"`
			}{},
		},

		// PtrHeadSliceMultiFields
		{
			name: "PtrHeadSliceMultiFields",
			data: &struct {
				A []int `json:"a"`
				B []int `json:"b"`
			}{A: []int{-1}, B: nil},
		},
		{
			name: "PtrHeadSliceMultiFieldsOmitEmpty",
			data: &struct {
				A []int `json:"a,omitempty"`
				B []int `json:"b,omitempty"`
			}{A: []int{-1}, B: nil},
		},
		{
			name: "PtrHeadSliceMultiFieldsString",
			data: &struct {
				A []int `json:"a,string"`
				B []int `json:"b,string"`
			}{A: []int{-1}, B: nil},
		},

		// PtrHeadSlicePtrMultiFields
		{
			name: "PtrHeadSlicePtrMultiFields",
			data: &struct {
				A *[]int `json:"a"`
				B *[]int `json:"b"`
			}{A: sliceptr([]int{-1}), B: sliceptr([]int{-2})},
		},
		{
			name: "PtrHeadSlicePtrMultiFieldsOmitEmpty",
			data: &struct {
				A *[]int `json:"a,omitempty"`
				B *[]int `json:"b,omitempty"`
			}{A: sliceptr([]int{-1}), B: sliceptr([]int{-2})},
		},
		{
			name: "PtrHeadSlicePtrMultiFieldsString",
			data: &struct {
				A *[]int `json:"a,string"`
				B *[]int `json:"b,string"`
			}{A: sliceptr([]int{-1}), B: sliceptr([]int{-2})},
		},

		// PtrHeadSlicePtrNilMultiFields
		{
			name: "PtrHeadSlicePtrNilMultiFields",
			data: &struct {
				A *[]int `json:"a"`
				B *[]int `json:"b"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadSlicePtrNilMultiFieldsOmitEmpty",
			data: &struct {
				A *[]int `json:"a,omitempty"`
				B *[]int `json:"b,omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadSlicePtrNilMultiFieldsString",
			data: &struct {
				A *[]int `json:"a,string"`
				B *[]int `json:"b,string"`
			}{A: nil, B: nil},
		},

		// PtrHeadSliceNilMultiFields
		{
			name: "PtrHeadSliceNilMultiFields",
			data: (*struct {
				A []int `json:"a"`
				B []int `json:"b"`
			})(nil),
		},
		{
			name: "PtrHeadSliceNilMultiFieldsOmitEmpty",
			data: (*struct {
				A []int `json:"a,omitempty"`
				B []int `json:"b,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadSliceNilMultiFieldsString",
			data: (*struct {
				A []int `json:"a,string"`
				B []int `json:"b,string"`
			})(nil),
		},

		// PtrHeadSliceNilMultiFields
		{
			name: "PtrHeadSliceNilMultiFields",
			data: (*struct {
				A *[]int `json:"a"`
				B *[]int `json:"b"`
			})(nil),
		},
		{
			name: "PtrHeadSliceNilMultiFieldsOmitEmpty",
			data: (*struct {
				A *[]int `json:"a,omitempty"`
				B *[]int `json:"b,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadSliceNilMultiFieldsString",
			data: (*struct {
				A *[]int `json:"a,string"`
				B *[]int `json:"b,string"`
			})(nil),
		},

		// HeadSliceZeroNotRoot
		{
			name: "HeadSliceZeroNotRoot",
			data: struct {
				A struct {
					A []int `json:"a"`
				}
			}{},
		},
		{
			name: "HeadSliceZeroNotRootOmitEmpty",
			data: struct {
				A struct {
					A []int `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadSliceZeroNotRootString",
			data: struct {
				A struct {
					A []int `json:"a,string"`
				}
			}{},
		},

		// HeadSliceNotRoot
		{
			name: "HeadSliceNotRoot",
			data: struct {
				A struct {
					A []int `json:"a"`
				}
			}{A: struct {
				A []int `json:"a"`
			}{A: []int{-1}}},
		},
		{
			name: "HeadSliceNotRootOmitEmpty",
			data: struct {
				A struct {
					A []int `json:"a,omitempty"`
				}
			}{A: struct {
				A []int `json:"a,omitempty"`
			}{A: []int{-1}}},
		},
		{
			name: "HeadSliceNotRootString",
			data: struct {
				A struct {
					A []int `json:"a,string"`
				}
			}{A: struct {
				A []int `json:"a,string"`
			}{A: []int{-1}}},
		},

		// HeadSlicePtrNotRoot
		{
			name: "HeadSlicePtrNotRoot",
			data: struct {
				A struct {
					A *[]int `json:"a"`
				}
			}{A: struct {
				A *[]int `json:"a"`
			}{sliceptr([]int{-1})}},
		},
		{
			name: "HeadSlicePtrNotRootOmitEmpty",
			data: struct {
				A struct {
					A *[]int `json:"a,omitempty"`
				}
			}{A: struct {
				A *[]int `json:"a,omitempty"`
			}{sliceptr([]int{-1})}},
		},
		{
			name: "HeadSlicePtrNotRootString",
			data: struct {
				A struct {
					A *[]int `json:"a,string"`
				}
			}{A: struct {
				A *[]int `json:"a,string"`
			}{sliceptr([]int{-1})}},
		},

		// HeadSlicePtrNilNotRoot
		{
			name: "HeadSlicePtrNilNotRoot",
			data: struct {
				A struct {
					A *[]int `json:"a"`
				}
			}{},
		},
		{
			name: "HeadSlicePtrNilNotRootOmitEmpty",
			data: struct {
				A struct {
					A *[]int `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadSlicePtrNilNotRootString",
			data: struct {
				A struct {
					A *[]int `json:"a,string"`
				}
			}{},
		},

		// PtrHeadSliceZeroNotRoot
		{
			name: "PtrHeadSliceZeroNotRoot",
			data: struct {
				A *struct {
					A []int `json:"a"`
				}
			}{A: new(struct {
				A []int `json:"a"`
			})},
		},
		{
			name: "PtrHeadSliceZeroNotRootOmitEmpty",
			data: struct {
				A *struct {
					A []int `json:"a,omitempty"`
				}
			}{A: new(struct {
				A []int `json:"a,omitempty"`
			})},
		},
		{
			name: "PtrHeadSliceZeroNotRootString",
			data: struct {
				A *struct {
					A []int `json:"a,string"`
				}
			}{A: new(struct {
				A []int `json:"a,string"`
			})},
		},

		// PtrHeadSliceNotRoot
		{
			name: "PtrHeadSliceNotRoot",
			data: struct {
				A *struct {
					A []int `json:"a"`
				}
			}{A: &(struct {
				A []int `json:"a"`
			}{A: []int{-1}})},
		},
		{
			name: "PtrHeadSliceNotRootOmitEmpty",
			data: struct {
				A *struct {
					A []int `json:"a,omitempty"`
				}
			}{A: &(struct {
				A []int `json:"a,omitempty"`
			}{A: []int{-1}})},
		},
		{
			name: "PtrHeadSliceNotRootString",
			data: struct {
				A *struct {
					A []int `json:"a,string"`
				}
			}{A: &(struct {
				A []int `json:"a,string"`
			}{A: []int{-1}})},
		},

		// PtrHeadSlicePtrNotRoot
		{
			name: "PtrHeadSlicePtrNotRoot",
			data: struct {
				A *struct {
					A *[]int `json:"a"`
				}
			}{A: &(struct {
				A *[]int `json:"a"`
			}{A: sliceptr([]int{-1})})},
		},
		{
			name: "PtrHeadSlicePtrNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *[]int `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *[]int `json:"a,omitempty"`
			}{A: sliceptr([]int{-1})})},
		},
		{
			name: "PtrHeadSlicePtrNotRootString",
			data: struct {
				A *struct {
					A *[]int `json:"a,string"`
				}
			}{A: &(struct {
				A *[]int `json:"a,string"`
			}{A: sliceptr([]int{-1})})},
		},

		// PtrHeadSlicePtrNilNotRoot
		{
			name: "PtrHeadSlicePtrNilNotRoot",
			data: struct {
				A *struct {
					A *[]int `json:"a"`
				}
			}{A: &(struct {
				A *[]int `json:"a"`
			}{A: nil})},
		},
		{
			name: "PtrHeadSlicePtrNilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *[]int `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *[]int `json:"a,omitempty"`
			}{A: nil})},
		},
		{
			name: "PtrHeadSlicePtrNilNotRootString",
			data: struct {
				A *struct {
					A *[]int `json:"a,string"`
				}
			}{A: &(struct {
				A *[]int `json:"a,string"`
			}{A: nil})},
		},

		// PtrHeadSliceNilNotRoot
		{
			name: "PtrHeadSliceNilNotRoot",
			data: struct {
				A *struct {
					A *[]int `json:"a"`
				}
			}{A: nil},
		},
		{
			name: "PtrHeadSliceNilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *[]int `json:"a,omitempty"`
				} `json:",omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadSliceNilNotRootString",
			data: struct {
				A *struct {
					A *[]int `json:"a,string"`
				} `json:",string"`
			}{A: nil},
		},

		// HeadSliceZeroMultiFieldsNotRoot
		{
			name: "HeadSliceZeroMultiFieldsNotRoot",
			data: struct {
				A struct {
					A []int `json:"a"`
				}
				B struct {
					B []int `json:"b"`
				}
			}{},
		},
		{
			name: "HeadSliceZeroMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A []int `json:"a,omitempty"`
				}
				B struct {
					B []int `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "HeadSliceZeroMultiFieldsNotRootString",
			data: struct {
				A struct {
					A []int `json:"a,string"`
				}
				B struct {
					B []int `json:"b,string"`
				}
			}{},
		},

		// HeadSliceMultiFieldsNotRoot
		{
			name: "HeadSliceMultiFieldsNotRoot",
			data: struct {
				A struct {
					A []int `json:"a"`
				}
				B struct {
					B []int `json:"b"`
				}
			}{A: struct {
				A []int `json:"a"`
			}{A: []int{-1}}, B: struct {
				B []int `json:"b"`
			}{B: []int{0}}},
		},
		{
			name: "HeadSliceMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A []int `json:"a,omitempty"`
				}
				B struct {
					B []int `json:"b,omitempty"`
				}
			}{A: struct {
				A []int `json:"a,omitempty"`
			}{A: []int{-1}}, B: struct {
				B []int `json:"b,omitempty"`
			}{B: []int{1}}},
		},
		{
			name: "HeadSliceMultiFieldsNotRootString",
			data: struct {
				A struct {
					A []int `json:"a,string"`
				}
				B struct {
					B []int `json:"b,string"`
				}
			}{A: struct {
				A []int `json:"a,string"`
			}{A: []int{-1}}, B: struct {
				B []int `json:"b,string"`
			}{B: []int{1}}},
		},

		// HeadSlicePtrMultiFieldsNotRoot
		{
			name: "HeadSlicePtrMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *[]int `json:"a"`
				}
				B struct {
					B *[]int `json:"b"`
				}
			}{A: struct {
				A *[]int `json:"a"`
			}{A: sliceptr([]int{-1})}, B: struct {
				B *[]int `json:"b"`
			}{B: sliceptr([]int{1})}},
		},
		{
			name: "HeadSlicePtrMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *[]int `json:"a,omitempty"`
				}
				B struct {
					B *[]int `json:"b,omitempty"`
				}
			}{A: struct {
				A *[]int `json:"a,omitempty"`
			}{A: sliceptr([]int{-1})}, B: struct {
				B *[]int `json:"b,omitempty"`
			}{B: sliceptr([]int{1})}},
		},
		{
			name: "HeadSlicePtrMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *[]int `json:"a,string"`
				}
				B struct {
					B *[]int `json:"b,string"`
				}
			}{A: struct {
				A *[]int `json:"a,string"`
			}{A: sliceptr([]int{-1})}, B: struct {
				B *[]int `json:"b,string"`
			}{B: sliceptr([]int{1})}},
		},

		// HeadSlicePtrNilMultiFieldsNotRoot
		{
			name: "HeadSlicePtrNilMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *[]int `json:"a"`
				}
				B struct {
					B *[]int `json:"b"`
				}
			}{A: struct {
				A *[]int `json:"a"`
			}{A: nil}, B: struct {
				B *[]int `json:"b"`
			}{B: nil}},
		},
		{
			name: "HeadSlicePtrNilMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *[]int `json:"a,omitempty"`
				}
				B struct {
					B *[]int `json:"b,omitempty"`
				}
			}{A: struct {
				A *[]int `json:"a,omitempty"`
			}{A: nil}, B: struct {
				B *[]int `json:"b,omitempty"`
			}{B: nil}},
		},
		{
			name: "HeadSlicePtrNilMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *[]int `json:"a,string"`
				}
				B struct {
					B *[]int `json:"b,string"`
				}
			}{A: struct {
				A *[]int `json:"a,string"`
			}{A: nil}, B: struct {
				B *[]int `json:"b,string"`
			}{B: nil}},
		},

		// PtrHeadSliceZeroMultiFieldsNotRoot
		{
			name: "PtrHeadSliceZeroMultiFieldsNotRoot",
			data: &struct {
				A struct {
					A []int `json:"a"`
				}
				B struct {
					B []int `json:"b"`
				}
			}{},
		},
		{
			name: "PtrHeadSliceZeroMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A []int `json:"a,omitempty"`
				}
				B struct {
					B []int `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "PtrHeadSliceZeroMultiFieldsNotRootString",
			data: &struct {
				A struct {
					A []int `json:"a,string"`
				}
				B struct {
					B []int `json:"b,string"`
				}
			}{},
		},

		// PtrHeadSliceMultiFieldsNotRoot
		{
			name: "PtrHeadSliceMultiFieldsNotRoot",
			data: &struct {
				A struct {
					A []int `json:"a"`
				}
				B struct {
					B []int `json:"b"`
				}
			}{A: struct {
				A []int `json:"a"`
			}{A: []int{-1}}, B: struct {
				B []int `json:"b"`
			}{B: []int{1}}},
		},
		{
			name: "PtrHeadSliceMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A []int `json:"a,omitempty"`
				}
				B struct {
					B []int `json:"b,omitempty"`
				}
			}{A: struct {
				A []int `json:"a,omitempty"`
			}{A: []int{-1}}, B: struct {
				B []int `json:"b,omitempty"`
			}{B: []int{1}}},
		},
		{
			name: "PtrHeadSliceMultiFieldsNotRootString",
			data: &struct {
				A struct {
					A []int `json:"a,string"`
				}
				B struct {
					B []int `json:"b,string"`
				}
			}{A: struct {
				A []int `json:"a,string"`
			}{A: []int{-1}}, B: struct {
				B []int `json:"b,string"`
			}{B: []int{1}}},
		},

		// PtrHeadSlicePtrMultiFieldsNotRoot
		{
			name: "PtrHeadSlicePtrMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *[]int `json:"a"`
				}
				B *struct {
					B *[]int `json:"b"`
				}
			}{A: &(struct {
				A *[]int `json:"a"`
			}{A: sliceptr([]int{-1})}), B: &(struct {
				B *[]int `json:"b"`
			}{B: sliceptr([]int{1})})},
		},
		{
			name: "PtrHeadSlicePtrMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *[]int `json:"a,omitempty"`
				}
				B *struct {
					B *[]int `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *[]int `json:"a,omitempty"`
			}{A: sliceptr([]int{-1})}), B: &(struct {
				B *[]int `json:"b,omitempty"`
			}{B: sliceptr([]int{1})})},
		},
		{
			name: "PtrHeadSlicePtrMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *[]int `json:"a,string"`
				}
				B *struct {
					B *[]int `json:"b,string"`
				}
			}{A: &(struct {
				A *[]int `json:"a,string"`
			}{A: sliceptr([]int{-1})}), B: &(struct {
				B *[]int `json:"b,string"`
			}{B: sliceptr([]int{1})})},
		},

		// PtrHeadSlicePtrNilMultiFieldsNotRoot
		{
			name: "PtrHeadSlicePtrNilMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *[]int `json:"a"`
				}
				B *struct {
					B *[]int `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadSlicePtrNilMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *[]int `json:"a,omitempty"`
				} `json:",omitempty"`
				B *struct {
					B *[]int `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadSlicePtrNilMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *[]int `json:"a,string"`
				} `json:",string"`
				B *struct {
					B *[]int `json:"b,string"`
				} `json:",string"`
			}{A: nil, B: nil},
		},

		// PtrHeadSliceNilMultiFieldsNotRoot
		{
			name: "PtrHeadSliceNilMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *[]int `json:"a"`
				}
				B *struct {
					B *[]int `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadSliceNilMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *[]int `json:"a,omitempty"`
				}
				B *struct {
					B *[]int `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadSliceNilMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *[]int `json:"a,string"`
				}
				B *struct {
					B *[]int `json:"b,string"`
				}
			})(nil),
		},

		// PtrHeadSliceDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadSliceDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A []int `json:"a"`
					B []int `json:"b"`
				}
				B *struct {
					A []int `json:"a"`
					B []int `json:"b"`
				}
			}{A: &(struct {
				A []int `json:"a"`
				B []int `json:"b"`
			}{A: []int{-1}, B: []int{1}}), B: &(struct {
				A []int `json:"a"`
				B []int `json:"b"`
			}{A: []int{-1}, B: nil})},
		},
		{
			name: "PtrHeadSliceDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A []int `json:"a,omitempty"`
					B []int `json:"b,omitempty"`
				}
				B *struct {
					A []int `json:"a,omitempty"`
					B []int `json:"b,omitempty"`
				}
			}{A: &(struct {
				A []int `json:"a,omitempty"`
				B []int `json:"b,omitempty"`
			}{A: []int{-1}, B: []int{1}}), B: &(struct {
				A []int `json:"a,omitempty"`
				B []int `json:"b,omitempty"`
			}{A: []int{-1}, B: nil})},
		},
		{
			name: "PtrHeadSliceDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A []int `json:"a,string"`
					B []int `json:"b,string"`
				}
				B *struct {
					A []int `json:"a,string"`
					B []int `json:"b,string"`
				}
			}{A: &(struct {
				A []int `json:"a,string"`
				B []int `json:"b,string"`
			}{A: []int{-1}, B: []int{1}}), B: &(struct {
				A []int `json:"a,string"`
				B []int `json:"b,string"`
			}{A: []int{-1}, B: nil})},
		},

		// PtrHeadSliceNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadSliceNilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A []int `json:"a"`
					B []int `json:"b"`
				}
				B *struct {
					A []int `json:"a"`
					B []int `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadSliceNilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A []int `json:"a,omitempty"`
					B []int `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A []int `json:"a,omitempty"`
					B []int `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadSliceNilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A []int `json:"a,string"`
					B []int `json:"b,string"`
				}
				B *struct {
					A []int `json:"a,string"`
					B []int `json:"b,string"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadSliceNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadSliceNilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A []int `json:"a"`
					B []int `json:"b"`
				}
				B *struct {
					A []int `json:"a"`
					B []int `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadSliceNilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A []int `json:"a,omitempty"`
					B []int `json:"b,omitempty"`
				}
				B *struct {
					A []int `json:"a,omitempty"`
					B []int `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadSliceNilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A []int `json:"a,string"`
					B []int `json:"b,string"`
				}
				B *struct {
					A []int `json:"a,string"`
					B []int `json:"b,string"`
				}
			})(nil),
		},

		// PtrHeadSlicePtrDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadSlicePtrDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *[]int `json:"a"`
					B *[]int `json:"b"`
				}
				B *struct {
					A *[]int `json:"a"`
					B *[]int `json:"b"`
				}
			}{A: &(struct {
				A *[]int `json:"a"`
				B *[]int `json:"b"`
			}{A: sliceptr([]int{-1}), B: sliceptr([]int{1})}), B: &(struct {
				A *[]int `json:"a"`
				B *[]int `json:"b"`
			}{A: nil, B: nil})},
		},
		{
			name: "PtrHeadSlicePtrDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *[]int `json:"a,omitempty"`
					B *[]int `json:"b,omitempty"`
				}
				B *struct {
					A *[]int `json:"a,omitempty"`
					B *[]int `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *[]int `json:"a,omitempty"`
				B *[]int `json:"b,omitempty"`
			}{A: sliceptr([]int{-1}), B: sliceptr([]int{1})}), B: &(struct {
				A *[]int `json:"a,omitempty"`
				B *[]int `json:"b,omitempty"`
			}{A: nil, B: nil})},
		},
		{
			name: "PtrHeadSlicePtrDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *[]int `json:"a,string"`
					B *[]int `json:"b,string"`
				}
				B *struct {
					A *[]int `json:"a,string"`
					B *[]int `json:"b,string"`
				}
			}{A: &(struct {
				A *[]int `json:"a,string"`
				B *[]int `json:"b,string"`
			}{A: sliceptr([]int{-1}), B: sliceptr([]int{1})}), B: &(struct {
				A *[]int `json:"a,string"`
				B *[]int `json:"b,string"`
			}{A: nil, B: nil})},
		},

		// PtrHeadSlicePtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadSlicePtrNilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *[]int `json:"a"`
					B *[]int `json:"b"`
				}
				B *struct {
					A *[]int `json:"a"`
					B *[]int `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadSlicePtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *[]int `json:"a,omitempty"`
					B *[]int `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A *[]int `json:"a,omitempty"`
					B *[]int `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadSlicePtrNilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *[]int `json:"a,string"`
					B *[]int `json:"b,string"`
				}
				B *struct {
					A *[]int `json:"a,string"`
					B *[]int `json:"b,string"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadSlicePtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadSlicePtrNilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *[]int `json:"a"`
					B *[]int `json:"b"`
				}
				B *struct {
					A *[]int `json:"a"`
					B *[]int `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadSlicePtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *[]int `json:"a,omitempty"`
					B *[]int `json:"b,omitempty"`
				}
				B *struct {
					A *[]int `json:"a,omitempty"`
					B *[]int `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadSlicePtrNilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *[]int `json:"a,string"`
					B *[]int `json:"b,string"`
				}
				B *struct {
					A *[]int `json:"a,string"`
					B *[]int `json:"b,string"`
				}
			})(nil),
		},

		// AnonymousHeadSlice
		{
			name: "AnonymousHeadSlice",
			data: struct {
				structSlice
				B []int `json:"b"`
			}{
				structSlice: structSlice{A: []int{-1}},
				B:           nil,
			},
		},
		{
			name: "AnonymousHeadSliceOmitEmpty",
			data: struct {
				structSliceOmitEmpty
				B []int `json:"b,omitempty"`
			}{
				structSliceOmitEmpty: structSliceOmitEmpty{A: []int{-1}},
				B:                    nil,
			},
		},
		{
			name: "AnonymousHeadSliceString",
			data: struct {
				structSliceString
				B []int `json:"b,string"`
			}{
				structSliceString: structSliceString{A: []int{-1}},
				B:                 nil,
			},
		},

		// PtrAnonymousHeadSlice
		{
			name: "PtrAnonymousHeadSlice",
			data: struct {
				*structSlice
				B []int `json:"b"`
			}{
				structSlice: &structSlice{A: []int{-1}},
				B:           nil,
			},
		},
		{
			name: "PtrAnonymousHeadSliceOmitEmpty",
			data: struct {
				*structSliceOmitEmpty
				B []int `json:"b,omitempty"`
			}{
				structSliceOmitEmpty: &structSliceOmitEmpty{A: []int{-1}},
				B:                    nil,
			},
		},
		{
			name: "PtrAnonymousHeadSliceString",
			data: struct {
				*structSliceString
				B []int `json:"b,string"`
			}{
				structSliceString: &structSliceString{A: []int{-1}},
				B:                 nil,
			},
		},

		// PtrAnonymousHeadSliceNil
		{
			name: "PtrAnonymousHeadSliceNil",
			data: struct {
				*structSlice
				B []int `json:"b"`
			}{
				structSlice: &structSlice{A: nil},
				B:           nil,
			},
		},
		{
			name: "PtrAnonymousHeadSliceNilOmitEmpty",
			data: struct {
				*structSliceOmitEmpty
				B []int `json:"b,omitempty"`
			}{
				structSliceOmitEmpty: &structSliceOmitEmpty{A: nil},
				B:                    nil,
			},
		},
		{
			name: "PtrAnonymousHeadSliceNilString",
			data: struct {
				*structSliceString
				B []int `json:"b,string"`
			}{
				structSliceString: &structSliceString{A: nil},
				B:                 nil,
			},
		},

		// NilPtrAnonymousHeadSlice
		{
			name: "NilPtrAnonymousHeadSlice",
			data: struct {
				*structSlice
				B []int `json:"b"`
			}{
				structSlice: nil,
				B:           []int{-1},
			},
		},
		{
			name: "NilPtrAnonymousHeadSliceOmitEmpty",
			data: struct {
				*structSliceOmitEmpty
				B []int `json:"b,omitempty"`
			}{
				structSliceOmitEmpty: nil,
				B:                    []int{-1},
			},
		},
		{
			name: "NilPtrAnonymousHeadSliceString",
			data: struct {
				*structSliceString
				B []int `json:"b,string"`
			}{
				structSliceString: nil,
				B:                 []int{-1},
			},
		},

		// AnonymousHeadSlicePtr
		{
			name: "AnonymousHeadSlicePtr",
			data: struct {
				structSlicePtr
				B *[]int `json:"b"`
			}{
				structSlicePtr: structSlicePtr{A: sliceptr([]int{-1})},
				B:              nil,
			},
		},
		{
			name: "AnonymousHeadSlicePtrOmitEmpty",
			data: struct {
				structSlicePtrOmitEmpty
				B *[]int `json:"b,omitempty"`
			}{
				structSlicePtrOmitEmpty: structSlicePtrOmitEmpty{A: sliceptr([]int{-1})},
				B:                       nil,
			},
		},
		{
			name: "AnonymousHeadSlicePtrString",
			data: struct {
				structSlicePtrString
				B *[]int `json:"b,string"`
			}{
				structSlicePtrString: structSlicePtrString{A: sliceptr([]int{-1})},
				B:                    nil,
			},
		},

		// AnonymousHeadSlicePtrNil
		{
			name: "AnonymousHeadSlicePtrNil",
			data: struct {
				structSlicePtr
				B *[]int `json:"b"`
			}{
				structSlicePtr: structSlicePtr{A: nil},
				B:              sliceptr([]int{-1}),
			},
		},
		{
			name: "AnonymousHeadSlicePtrNilOmitEmpty",
			data: struct {
				structSlicePtrOmitEmpty
				B *[]int `json:"b,omitempty"`
			}{
				structSlicePtrOmitEmpty: structSlicePtrOmitEmpty{A: nil},
				B:                       sliceptr([]int{-1}),
			},
		},
		{
			name: "AnonymousHeadSlicePtrNilString",
			data: struct {
				structSlicePtrString
				B *[]int `json:"b,string"`
			}{
				structSlicePtrString: structSlicePtrString{A: nil},
				B:                    sliceptr([]int{-1}),
			},
		},

		// PtrAnonymousHeadSlicePtr
		{
			name: "PtrAnonymousHeadSlicePtr",
			data: struct {
				*structSlicePtr
				B *[]int `json:"b"`
			}{
				structSlicePtr: &structSlicePtr{A: sliceptr([]int{-1})},
				B:              nil,
			},
		},
		{
			name: "PtrAnonymousHeadSlicePtrOmitEmpty",
			data: struct {
				*structSlicePtrOmitEmpty
				B *[]int `json:"b,omitempty"`
			}{
				structSlicePtrOmitEmpty: &structSlicePtrOmitEmpty{A: sliceptr([]int{-1})},
				B:                       nil,
			},
		},
		{
			name: "PtrAnonymousHeadSlicePtrString",
			data: struct {
				*structSlicePtrString
				B *[]int `json:"b,string"`
			}{
				structSlicePtrString: &structSlicePtrString{A: sliceptr([]int{-1})},
				B:                    nil,
			},
		},

		// NilPtrAnonymousHeadSlicePtr
		{
			name: "NilPtrAnonymousHeadSlicePtr",
			data: struct {
				*structSlicePtr
				B *[]int `json:"b"`
			}{
				structSlicePtr: nil,
				B:              sliceptr([]int{-1}),
			},
		},
		{
			name: "NilPtrAnonymousHeadSlicePtrOmitEmpty",
			data: struct {
				*structSlicePtrOmitEmpty
				B *[]int `json:"b,omitempty"`
			}{
				structSlicePtrOmitEmpty: nil,
				B:                       sliceptr([]int{-1}),
			},
		},
		{
			name: "NilPtrAnonymousHeadSlicePtrString",
			data: struct {
				*structSlicePtrString
				B *[]int `json:"b,string"`
			}{
				structSlicePtrString: nil,
				B:                    sliceptr([]int{-1}),
			},
		},

		// AnonymousHeadSliceOnly
		{
			name: "AnonymousHeadSliceOnly",
			data: struct {
				structSlice
			}{
				structSlice: structSlice{A: []int{-1}},
			},
		},
		{
			name: "AnonymousHeadSliceOnlyOmitEmpty",
			data: struct {
				structSliceOmitEmpty
			}{
				structSliceOmitEmpty: structSliceOmitEmpty{A: []int{-1}},
			},
		},
		{
			name: "AnonymousHeadSliceOnlyString",
			data: struct {
				structSliceString
			}{
				structSliceString: structSliceString{A: []int{-1}},
			},
		},

		// PtrAnonymousHeadSliceOnly
		{
			name: "PtrAnonymousHeadSliceOnly",
			data: struct {
				*structSlice
			}{
				structSlice: &structSlice{A: []int{-1}},
			},
		},
		{
			name: "PtrAnonymousHeadSliceOnlyOmitEmpty",
			data: struct {
				*structSliceOmitEmpty
			}{
				structSliceOmitEmpty: &structSliceOmitEmpty{A: []int{-1}},
			},
		},
		{
			name: "PtrAnonymousHeadSliceOnlyString",
			data: struct {
				*structSliceString
			}{
				structSliceString: &structSliceString{A: []int{-1}},
			},
		},

		// NilPtrAnonymousHeadSliceOnly
		{
			name: "NilPtrAnonymousHeadSliceOnly",
			data: struct {
				*structSlice
			}{
				structSlice: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadSliceOnlyOmitEmpty",
			data: struct {
				*structSliceOmitEmpty
			}{
				structSliceOmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadSliceOnlyString",
			data: struct {
				*structSliceString
			}{
				structSliceString: nil,
			},
		},

		// AnonymousHeadSlicePtrOnly
		{
			name: "AnonymousHeadSlicePtrOnly",
			data: struct {
				structSlicePtr
			}{
				structSlicePtr: structSlicePtr{A: sliceptr([]int{-1})},
			},
		},
		{
			name: "AnonymousHeadSlicePtrOnlyOmitEmpty",
			data: struct {
				structSlicePtrOmitEmpty
			}{
				structSlicePtrOmitEmpty: structSlicePtrOmitEmpty{A: sliceptr([]int{-1})},
			},
		},
		{
			name: "AnonymousHeadSlicePtrOnlyString",
			data: struct {
				structSlicePtrString
			}{
				structSlicePtrString: structSlicePtrString{A: sliceptr([]int{-1})},
			},
		},

		// AnonymousHeadSlicePtrNilOnly
		{
			name: "AnonymousHeadSlicePtrNilOnly",
			data: struct {
				structSlicePtr
			}{
				structSlicePtr: structSlicePtr{A: nil},
			},
		},
		{
			name: "AnonymousHeadSlicePtrNilOnlyOmitEmpty",
			data: struct {
				structSlicePtrOmitEmpty
			}{
				structSlicePtrOmitEmpty: structSlicePtrOmitEmpty{A: nil},
			},
		},
		{
			name: "AnonymousHeadSlicePtrNilOnlyString",
			data: struct {
				structSlicePtrString
			}{
				structSlicePtrString: structSlicePtrString{A: nil},
			},
		},

		// PtrAnonymousHeadSlicePtrOnly
		{
			name: "PtrAnonymousHeadSlicePtrOnly",
			data: struct {
				*structSlicePtr
			}{
				structSlicePtr: &structSlicePtr{A: sliceptr([]int{-1})},
			},
		},
		{
			name: "PtrAnonymousHeadSlicePtrOnlyOmitEmpty",
			data: struct {
				*structSlicePtrOmitEmpty
			}{
				structSlicePtrOmitEmpty: &structSlicePtrOmitEmpty{A: sliceptr([]int{-1})},
			},
		},
		{
			name: "PtrAnonymousHeadSlicePtrOnlyString",
			data: struct {
				*structSlicePtrString
			}{
				structSlicePtrString: &structSlicePtrString{A: sliceptr([]int{-1})},
			},
		},

		// NilPtrAnonymousHeadSlicePtrOnly
		{
			name: "NilPtrAnonymousHeadSlicePtrOnly",
			data: struct {
				*structSlicePtr
			}{
				structSlicePtr: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadSlicePtrOnlyOmitEmpty",
			data: struct {
				*structSlicePtrOmitEmpty
			}{
				structSlicePtrOmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadSlicePtrOnlyString",
			data: struct {
				*structSlicePtrString
			}{
				structSlicePtrString: nil,
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
