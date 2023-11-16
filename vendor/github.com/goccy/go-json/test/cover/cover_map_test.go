package json_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/goccy/go-json"
)

type recursiveMap struct {
	A int
	B map[string]*recursiveMap
}

func TestCoverMap(t *testing.T) {
	type structMap struct {
		A map[string]int `json:"a"`
	}
	type structMapOmitEmpty struct {
		A map[string]int `json:"a,omitempty"`
	}
	type structMapString struct {
		A map[string]int `json:"a,string"`
	}
	type structMapPtr struct {
		A *map[string]int `json:"a"`
	}
	type structMapPtrOmitEmpty struct {
		A *map[string]int `json:"a,omitempty"`
	}
	type structMapPtrString struct {
		A *map[string]int `json:"a,string"`
	}

	type structMapPtrContent struct {
		A map[string]*int `json:"a"`
	}
	type structMapOmitEmptyPtrContent struct {
		A map[string]*int `json:"a,omitempty"`
	}
	type structMapStringPtrContent struct {
		A map[string]*int `json:"a,string"`
	}
	type structMapPtrPtrContent struct {
		A *map[string]*int `json:"a"`
	}
	type structMapPtrOmitEmptyPtrContent struct {
		A *map[string]*int `json:"a,omitempty"`
	}
	type structMapPtrStringPtrContent struct {
		A *map[string]*int `json:"a,string"`
	}

	tests := []struct {
		name string
		data interface{}
	}{
		{
			name: "NestedMap",
			data: map[string]map[string]int{"a": {"b": 1}},
		},
		{
			name: "RecursiveMap",
			data: map[string]*recursiveMap{
				"keyA": {
					A: 1,
					B: map[string]*recursiveMap{
						"keyB": {
							A: 2,
							B: map[string]*recursiveMap{
								"keyC": {
									A: 3,
								},
							},
						},
					},
				},
				"keyD": {
					A: 4,
					B: map[string]*recursiveMap{
						"keyE": {
							A: 5,
							B: map[string]*recursiveMap{
								"keyF": {
									A: 6,
								},
							},
						},
					},
				},
			},
		},

		// HeadMapZero
		{
			name: "HeadMapZero",
			data: struct {
				A map[string]int `json:"a"`
			}{},
		},
		{
			name: "HeadMapZeroOmitEmpty",
			data: struct {
				A map[string]int `json:"a,omitempty"`
			}{},
		},
		{
			name: "HeadMapZeroString",
			data: struct {
				A map[string]int `json:"a,string"`
			}{},
		},

		// HeadMap
		{
			name: "HeadMap",
			data: struct {
				A map[string]int `json:"a"`
			}{A: map[string]int{"m": -1}},
		},
		{
			name: "HeadMapOmitEmpty",
			data: struct {
				A map[string]int `json:"a,omitempty"`
			}{A: map[string]int{"m": -1}},
		},
		{
			name: "HeadMapString",
			data: struct {
				A map[string]int `json:"a,string"`
			}{A: map[string]int{"m": -1}},
		},

		// HeadMapPtr
		{
			name: "HeadMapPtr",
			data: struct {
				A *map[string]int `json:"a"`
			}{A: mapptr(map[string]int{"m": -1})},
		},
		{
			name: "HeadMapPtrOmitEmpty",
			data: struct {
				A *map[string]int `json:"a,omitempty"`
			}{A: mapptr(map[string]int{"m": -1})},
		},
		{
			name: "HeadMapPtrString",
			data: struct {
				A *map[string]int `json:"a,string"`
			}{A: mapptr(map[string]int{"m": -1})},
		},

		// HeadMapPtrNil
		{
			name: "HeadMapPtrNil",
			data: struct {
				A *map[string]int `json:"a"`
			}{A: nil},
		},
		{
			name: "HeadMapPtrNilOmitEmpty",
			data: struct {
				A *map[string]int `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "HeadMapPtrNilString",
			data: struct {
				A *map[string]int `json:"a,string"`
			}{A: nil},
		},

		// PtrHeadMapZero
		{
			name: "PtrHeadMapZero",
			data: &struct {
				A map[string]int `json:"a"`
			}{},
		},
		{
			name: "PtrHeadMapZeroOmitEmpty",
			data: &struct {
				A map[string]int `json:"a,omitempty"`
			}{},
		},
		{
			name: "PtrHeadMapZeroString",
			data: &struct {
				A map[string]int `json:"a,string"`
			}{},
		},

		// PtrHeadMap
		{
			name: "PtrHeadMap",
			data: &struct {
				A map[string]int `json:"a"`
			}{A: map[string]int{"m": -1}},
		},
		{
			name: "PtrHeadMapOmitEmpty",
			data: &struct {
				A map[string]int `json:"a,omitempty"`
			}{A: map[string]int{"m": -1}},
		},
		{
			name: "PtrHeadMapString",
			data: &struct {
				A map[string]int `json:"a,string"`
			}{A: map[string]int{"m": -1}},
		},

		// PtrHeadMapPtr
		{
			name: "PtrHeadMapPtr",
			data: &struct {
				A *map[string]int `json:"a"`
			}{A: mapptr(map[string]int{"m": -1})},
		},
		{
			name: "PtrHeadMapPtrOmitEmpty",
			data: &struct {
				A *map[string]int `json:"a,omitempty"`
			}{A: mapptr(map[string]int{"m": -1})},
		},
		{
			name: "PtrHeadMapPtrString",
			data: &struct {
				A *map[string]int `json:"a,string"`
			}{A: mapptr(map[string]int{"m": -1})},
		},

		// PtrHeadMapPtrNil
		{
			name: "PtrHeadMapPtrNil",
			data: &struct {
				A *map[string]int `json:"a"`
			}{A: nil},
		},
		{
			name: "PtrHeadMapPtrNilOmitEmpty",
			data: &struct {
				A *map[string]int `json:"a,omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadMapPtrNilString",
			data: &struct {
				A *map[string]int `json:"a,string"`
			}{A: nil},
		},

		// PtrHeadMapNil
		{
			name: "PtrHeadMapNil",
			data: (*struct {
				A *map[string]int `json:"a"`
			})(nil),
		},
		{
			name: "PtrHeadMapNilOmitEmpty",
			data: (*struct {
				A *map[string]int `json:"a,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadMapNilString",
			data: (*struct {
				A *map[string]int `json:"a,string"`
			})(nil),
		},

		// HeadMapZeroMultiFields
		{
			name: "HeadMapZeroMultiFields",
			data: struct {
				A map[string]int `json:"a"`
				B map[string]int `json:"b"`
				C map[string]int `json:"c"`
			}{},
		},
		{
			name: "HeadMapZeroMultiFieldsOmitEmpty",
			data: struct {
				A map[string]int `json:"a,omitempty"`
				B map[string]int `json:"b,omitempty"`
				C map[string]int `json:"c,omitempty"`
			}{},
		},
		{
			name: "HeadMapZeroMultiFields",
			data: struct {
				A map[string]int `json:"a,string"`
				B map[string]int `json:"b,string"`
				C map[string]int `json:"c,string"`
			}{},
		},

		// HeadMapMultiFields
		{
			name: "HeadMapMultiFields",
			data: struct {
				A map[string]int `json:"a"`
				B map[string]int `json:"b"`
				C map[string]int `json:"c"`
			}{A: map[string]int{"m": -1}, B: map[string]int{"m": 2}, C: map[string]int{"m": -3}},
		},
		{
			name: "HeadMapMultiFieldsOmitEmpty",
			data: struct {
				A map[string]int `json:"a,omitempty"`
				B map[string]int `json:"b,omitempty"`
				C map[string]int `json:"c,omitempty"`
			}{A: map[string]int{"m": -1}, B: map[string]int{"m": 2}, C: map[string]int{"m": -3}},
		},
		{
			name: "HeadMapMultiFieldsString",
			data: struct {
				A map[string]int `json:"a,string"`
				B map[string]int `json:"b,string"`
				C map[string]int `json:"c,string"`
			}{A: map[string]int{"m": -1}, B: map[string]int{"m": 2}, C: map[string]int{"m": -3}},
		},

		// HeadMapPtrMultiFields
		{
			name: "HeadMapPtrMultiFields",
			data: struct {
				A *map[string]int `json:"a"`
				B *map[string]int `json:"b"`
				C *map[string]int `json:"c"`
			}{A: mapptr(map[string]int{"m": -1}), B: mapptr(map[string]int{"m": 2}), C: mapptr(map[string]int{"m": -3})},
		},
		{
			name: "HeadMapPtrMultiFieldsOmitEmpty",
			data: struct {
				A *map[string]int `json:"a,omitempty"`
				B *map[string]int `json:"b,omitempty"`
				C *map[string]int `json:"c,omitempty"`
			}{A: mapptr(map[string]int{"m": -1}), B: mapptr(map[string]int{"m": 2}), C: mapptr(map[string]int{"m": -3})},
		},
		{
			name: "HeadMapPtrMultiFieldsString",
			data: struct {
				A *map[string]int `json:"a,string"`
				B *map[string]int `json:"b,string"`
				C *map[string]int `json:"c,string"`
			}{A: mapptr(map[string]int{"m": -1}), B: mapptr(map[string]int{"m": 2}), C: mapptr(map[string]int{"m": -3})},
		},

		// HeadMapPtrNilMultiFields
		{
			name: "HeadMapPtrNilMultiFields",
			data: struct {
				A *map[string]int `json:"a"`
				B *map[string]int `json:"b"`
				C *map[string]int `json:"c"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadMapPtrNilMultiFieldsOmitEmpty",
			data: struct {
				A *map[string]int `json:"a,omitempty"`
				B *map[string]int `json:"b,omitempty"`
				C *map[string]int `json:"c,omitempty"`
			}{A: nil, B: nil, C: nil},
		},
		{
			name: "HeadMapPtrNilMultiFieldsString",
			data: struct {
				A *map[string]int `json:"a,string"`
				B *map[string]int `json:"b,string"`
				C *map[string]int `json:"c,string"`
			}{A: nil, B: nil, C: nil},
		},

		// PtrHeadMapZeroMultiFields
		{
			name: "PtrHeadMapZeroMultiFields",
			data: &struct {
				A map[string]int `json:"a"`
				B map[string]int `json:"b"`
			}{},
		},
		{
			name: "PtrHeadMapZeroMultiFieldsOmitEmpty",
			data: &struct {
				A map[string]int `json:"a,omitempty"`
				B map[string]int `json:"b,omitempty"`
			}{},
		},
		{
			name: "PtrHeadMapZeroMultiFieldsString",
			data: &struct {
				A map[string]int `json:"a,string"`
				B map[string]int `json:"b,string"`
			}{},
		},

		// PtrHeadMapMultiFields
		{
			name: "PtrHeadMapMultiFields",
			data: &struct {
				A map[string]int `json:"a"`
				B map[string]int `json:"b"`
			}{A: map[string]int{"m": -1}, B: nil},
		},
		{
			name: "PtrHeadMapMultiFieldsOmitEmpty",
			data: &struct {
				A map[string]int `json:"a,omitempty"`
				B map[string]int `json:"b,omitempty"`
			}{A: map[string]int{"m": -1}, B: nil},
		},
		{
			name: "PtrHeadMapMultiFieldsString",
			data: &struct {
				A map[string]int `json:"a,string"`
				B map[string]int `json:"b,string"`
			}{A: map[string]int{"m": -1}, B: nil},
		},

		// PtrHeadMapPtrMultiFields
		{
			name: "PtrHeadMapPtrMultiFields",
			data: &struct {
				A *map[string]int `json:"a"`
				B *map[string]int `json:"b"`
			}{A: mapptr(map[string]int{"m": -1}), B: mapptr(map[string]int{"m": 2})},
		},
		{
			name: "PtrHeadMapPtrMultiFieldsOmitEmpty",
			data: &struct {
				A *map[string]int `json:"a,omitempty"`
				B *map[string]int `json:"b,omitempty"`
			}{A: mapptr(map[string]int{"m": -1}), B: mapptr(map[string]int{"m": 2})},
		},
		{
			name: "PtrHeadMapPtrMultiFieldsString",
			data: &struct {
				A *map[string]int `json:"a,string"`
				B *map[string]int `json:"b,string"`
			}{A: mapptr(map[string]int{"m": -1}), B: mapptr(map[string]int{"m": 2})},
		},

		// PtrHeadMapPtrNilMultiFields
		{
			name: "PtrHeadMapPtrNilMultiFields",
			data: &struct {
				A *map[string]int `json:"a"`
				B *map[string]int `json:"b"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadMapPtrNilMultiFieldsOmitEmpty",
			data: &struct {
				A *map[string]int `json:"a,omitempty"`
				B *map[string]int `json:"b,omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadMapPtrNilMultiFieldsString",
			data: &struct {
				A *map[string]int `json:"a,string"`
				B *map[string]int `json:"b,string"`
			}{A: nil, B: nil},
		},

		// PtrHeadMapNilMultiFields
		{
			name: "PtrHeadMapNilMultiFields",
			data: (*struct {
				A map[string]int `json:"a"`
				B map[string]int `json:"b"`
			})(nil),
		},
		{
			name: "PtrHeadMapNilMultiFieldsOmitEmpty",
			data: (*struct {
				A map[string]int `json:"a,omitempty"`
				B map[string]int `json:"b,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadMapNilMultiFieldsString",
			data: (*struct {
				A map[string]int `json:"a,string"`
				B map[string]int `json:"b,string"`
			})(nil),
		},

		// PtrHeadMapNilMultiFields
		{
			name: "PtrHeadMapNilMultiFields",
			data: (*struct {
				A *map[string]int `json:"a"`
				B *map[string]int `json:"b"`
			})(nil),
		},
		{
			name: "PtrHeadMapNilMultiFieldsOmitEmpty",
			data: (*struct {
				A *map[string]int `json:"a,omitempty"`
				B *map[string]int `json:"b,omitempty"`
			})(nil),
		},
		{
			name: "PtrHeadMapNilMultiFieldsString",
			data: (*struct {
				A *map[string]int `json:"a,string"`
				B *map[string]int `json:"b,string"`
			})(nil),
		},

		// HeadMapZeroNotRoot
		{
			name: "HeadMapZeroNotRoot",
			data: struct {
				A struct {
					A map[string]int `json:"a"`
				}
			}{},
		},
		{
			name: "HeadMapZeroNotRootOmitEmpty",
			data: struct {
				A struct {
					A map[string]int `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadMapZeroNotRootString",
			data: struct {
				A struct {
					A map[string]int `json:"a,string"`
				}
			}{},
		},

		// HeadMapNotRoot
		{
			name: "HeadMapNotRoot",
			data: struct {
				A struct {
					A map[string]int `json:"a"`
				}
			}{A: struct {
				A map[string]int `json:"a"`
			}{A: map[string]int{"m": -1}}},
		},
		{
			name: "HeadMapNotRootOmitEmpty",
			data: struct {
				A struct {
					A map[string]int `json:"a,omitempty"`
				}
			}{A: struct {
				A map[string]int `json:"a,omitempty"`
			}{A: map[string]int{"m": -1}}},
		},
		{
			name: "HeadMapNotRootString",
			data: struct {
				A struct {
					A map[string]int `json:"a,string"`
				}
			}{A: struct {
				A map[string]int `json:"a,string"`
			}{A: map[string]int{"m": -1}}},
		},

		// HeadMapPtrNotRoot
		{
			name: "HeadMapPtrNotRoot",
			data: struct {
				A struct {
					A *map[string]int `json:"a"`
				}
			}{A: struct {
				A *map[string]int `json:"a"`
			}{mapptr(map[string]int{"m": -1})}},
		},
		{
			name: "HeadMapPtrNotRootOmitEmpty",
			data: struct {
				A struct {
					A *map[string]int `json:"a,omitempty"`
				}
			}{A: struct {
				A *map[string]int `json:"a,omitempty"`
			}{mapptr(map[string]int{"m": -1})}},
		},
		{
			name: "HeadMapPtrNotRootString",
			data: struct {
				A struct {
					A *map[string]int `json:"a,string"`
				}
			}{A: struct {
				A *map[string]int `json:"a,string"`
			}{mapptr(map[string]int{"m": -1})}},
		},

		// HeadMapPtrNilNotRoot
		{
			name: "HeadMapPtrNilNotRoot",
			data: struct {
				A struct {
					A *map[string]int `json:"a"`
				}
			}{},
		},
		{
			name: "HeadMapPtrNilNotRootOmitEmpty",
			data: struct {
				A struct {
					A *map[string]int `json:"a,omitempty"`
				}
			}{},
		},
		{
			name: "HeadMapPtrNilNotRootString",
			data: struct {
				A struct {
					A *map[string]int `json:"a,string"`
				}
			}{},
		},

		// PtrHeadMapZeroNotRoot
		{
			name: "PtrHeadMapZeroNotRoot",
			data: struct {
				A *struct {
					A map[string]int `json:"a"`
				}
			}{A: new(struct {
				A map[string]int `json:"a"`
			})},
		},
		{
			name: "PtrHeadMapZeroNotRootOmitEmpty",
			data: struct {
				A *struct {
					A map[string]int `json:"a,omitempty"`
				}
			}{A: new(struct {
				A map[string]int `json:"a,omitempty"`
			})},
		},
		{
			name: "PtrHeadMapZeroNotRootString",
			data: struct {
				A *struct {
					A map[string]int `json:"a,string"`
				}
			}{A: new(struct {
				A map[string]int `json:"a,string"`
			})},
		},

		// PtrHeadMapNotRoot
		{
			name: "PtrHeadMapNotRoot",
			data: struct {
				A *struct {
					A map[string]int `json:"a"`
				}
			}{A: &(struct {
				A map[string]int `json:"a"`
			}{A: map[string]int{"m": -1}})},
		},
		{
			name: "PtrHeadMapNotRootOmitEmpty",
			data: struct {
				A *struct {
					A map[string]int `json:"a,omitempty"`
				}
			}{A: &(struct {
				A map[string]int `json:"a,omitempty"`
			}{A: map[string]int{"m": -1}})},
		},
		{
			name: "PtrHeadMapNotRootString",
			data: struct {
				A *struct {
					A map[string]int `json:"a,string"`
				}
			}{A: &(struct {
				A map[string]int `json:"a,string"`
			}{A: map[string]int{"m": -1}})},
		},

		// PtrHeadMapPtrNotRoot
		{
			name: "PtrHeadMapPtrNotRoot",
			data: struct {
				A *struct {
					A *map[string]int `json:"a"`
				}
			}{A: &(struct {
				A *map[string]int `json:"a"`
			}{A: mapptr(map[string]int{"m": -1})})},
		},
		{
			name: "PtrHeadMapPtrNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *map[string]int `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *map[string]int `json:"a,omitempty"`
			}{A: mapptr(map[string]int{"m": -1})})},
		},
		{
			name: "PtrHeadMapPtrNotRootString",
			data: struct {
				A *struct {
					A *map[string]int `json:"a,string"`
				}
			}{A: &(struct {
				A *map[string]int `json:"a,string"`
			}{A: mapptr(map[string]int{"m": -1})})},
		},

		// PtrHeadMapPtrNilNotRoot
		{
			name: "PtrHeadMapPtrNilNotRoot",
			data: struct {
				A *struct {
					A *map[string]int `json:"a"`
				}
			}{A: &(struct {
				A *map[string]int `json:"a"`
			}{A: nil})},
		},
		{
			name: "PtrHeadMapPtrNilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *map[string]int `json:"a,omitempty"`
				}
			}{A: &(struct {
				A *map[string]int `json:"a,omitempty"`
			}{A: nil})},
		},
		{
			name: "PtrHeadMapPtrNilNotRootString",
			data: struct {
				A *struct {
					A *map[string]int `json:"a,string"`
				}
			}{A: &(struct {
				A *map[string]int `json:"a,string"`
			}{A: nil})},
		},

		// PtrHeadMapNilNotRoot
		{
			name: "PtrHeadMapNilNotRoot",
			data: struct {
				A *struct {
					A *map[string]int `json:"a"`
				}
			}{A: nil},
		},
		{
			name: "PtrHeadMapNilNotRootOmitEmpty",
			data: struct {
				A *struct {
					A *map[string]int `json:"a,omitempty"`
				} `json:",omitempty"`
			}{A: nil},
		},
		{
			name: "PtrHeadMapNilNotRootString",
			data: struct {
				A *struct {
					A *map[string]int `json:"a,string"`
				} `json:",string"`
			}{A: nil},
		},

		// HeadMapZeroMultiFieldsNotRoot
		{
			name: "HeadMapZeroMultiFieldsNotRoot",
			data: struct {
				A struct {
					A map[string]int `json:"a"`
				}
				B struct {
					B map[string]int `json:"b"`
				}
			}{},
		},
		{
			name: "HeadMapZeroMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A map[string]int `json:"a,omitempty"`
				}
				B struct {
					B map[string]int `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "HeadMapZeroMultiFieldsNotRootString",
			data: struct {
				A struct {
					A map[string]int `json:"a,string"`
				}
				B struct {
					B map[string]int `json:"b,string"`
				}
			}{},
		},

		// HeadMapMultiFieldsNotRoot
		{
			name: "HeadMapMultiFieldsNotRoot",
			data: struct {
				A struct {
					A map[string]int `json:"a"`
				}
				B struct {
					B map[string]int `json:"b"`
				}
			}{A: struct {
				A map[string]int `json:"a"`
			}{A: map[string]int{"m": -1}}, B: struct {
				B map[string]int `json:"b"`
			}{B: map[string]int{"m": 0}}},
		},
		{
			name: "HeadMapMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A map[string]int `json:"a,omitempty"`
				}
				B struct {
					B map[string]int `json:"b,omitempty"`
				}
			}{A: struct {
				A map[string]int `json:"a,omitempty"`
			}{A: map[string]int{"m": -1}}, B: struct {
				B map[string]int `json:"b,omitempty"`
			}{B: map[string]int{"m": 1}}},
		},
		{
			name: "HeadMapMultiFieldsNotRootString",
			data: struct {
				A struct {
					A map[string]int `json:"a,string"`
				}
				B struct {
					B map[string]int `json:"b,string"`
				}
			}{A: struct {
				A map[string]int `json:"a,string"`
			}{A: map[string]int{"m": -1}}, B: struct {
				B map[string]int `json:"b,string"`
			}{B: map[string]int{"m": 1}}},
		},

		// HeadMapPtrMultiFieldsNotRoot
		{
			name: "HeadMapPtrMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *map[string]int `json:"a"`
				}
				B struct {
					B *map[string]int `json:"b"`
				}
			}{A: struct {
				A *map[string]int `json:"a"`
			}{A: mapptr(map[string]int{"m": -1})}, B: struct {
				B *map[string]int `json:"b"`
			}{B: mapptr(map[string]int{"m": 1})}},
		},
		{
			name: "HeadMapPtrMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *map[string]int `json:"a,omitempty"`
				}
				B struct {
					B *map[string]int `json:"b,omitempty"`
				}
			}{A: struct {
				A *map[string]int `json:"a,omitempty"`
			}{A: mapptr(map[string]int{"m": -1})}, B: struct {
				B *map[string]int `json:"b,omitempty"`
			}{B: mapptr(map[string]int{"m": 1})}},
		},
		{
			name: "HeadMapPtrMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *map[string]int `json:"a,string"`
				}
				B struct {
					B *map[string]int `json:"b,string"`
				}
			}{A: struct {
				A *map[string]int `json:"a,string"`
			}{A: mapptr(map[string]int{"m": -1})}, B: struct {
				B *map[string]int `json:"b,string"`
			}{B: mapptr(map[string]int{"m": 1})}},
		},

		// HeadMapPtrNilMultiFieldsNotRoot
		{
			name: "HeadMapPtrNilMultiFieldsNotRoot",
			data: struct {
				A struct {
					A *map[string]int `json:"a"`
				}
				B struct {
					B *map[string]int `json:"b"`
				}
			}{A: struct {
				A *map[string]int `json:"a"`
			}{A: nil}, B: struct {
				B *map[string]int `json:"b"`
			}{B: nil}},
		},
		{
			name: "HeadMapPtrNilMultiFieldsNotRootOmitEmpty",
			data: struct {
				A struct {
					A *map[string]int `json:"a,omitempty"`
				}
				B struct {
					B *map[string]int `json:"b,omitempty"`
				}
			}{A: struct {
				A *map[string]int `json:"a,omitempty"`
			}{A: nil}, B: struct {
				B *map[string]int `json:"b,omitempty"`
			}{B: nil}},
		},
		{
			name: "HeadMapPtrNilMultiFieldsNotRootString",
			data: struct {
				A struct {
					A *map[string]int `json:"a,string"`
				}
				B struct {
					B *map[string]int `json:"b,string"`
				}
			}{A: struct {
				A *map[string]int `json:"a,string"`
			}{A: nil}, B: struct {
				B *map[string]int `json:"b,string"`
			}{B: nil}},
		},

		// PtrHeadMapZeroMultiFieldsNotRoot
		{
			name: "PtrHeadMapZeroMultiFieldsNotRoot",
			data: &struct {
				A struct {
					A map[string]int `json:"a"`
				}
				B struct {
					B map[string]int `json:"b"`
				}
			}{},
		},
		{
			name: "PtrHeadMapZeroMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A map[string]int `json:"a,omitempty"`
				}
				B struct {
					B map[string]int `json:"b,omitempty"`
				}
			}{},
		},
		{
			name: "PtrHeadMapZeroMultiFieldsNotRootString",
			data: &struct {
				A struct {
					A map[string]int `json:"a,string"`
				}
				B struct {
					B map[string]int `json:"b,string"`
				}
			}{},
		},

		// PtrHeadMapMultiFieldsNotRoot
		{
			name: "PtrHeadMapMultiFieldsNotRoot",
			data: &struct {
				A struct {
					A map[string]int `json:"a"`
				}
				B struct {
					B map[string]int `json:"b"`
				}
			}{A: struct {
				A map[string]int `json:"a"`
			}{A: map[string]int{"m": -1}}, B: struct {
				B map[string]int `json:"b"`
			}{B: map[string]int{"m": 1}}},
		},
		{
			name: "PtrHeadMapMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A struct {
					A map[string]int `json:"a,omitempty"`
				}
				B struct {
					B map[string]int `json:"b,omitempty"`
				}
			}{A: struct {
				A map[string]int `json:"a,omitempty"`
			}{A: map[string]int{"m": -1}}, B: struct {
				B map[string]int `json:"b,omitempty"`
			}{B: map[string]int{"m": 1}}},
		},
		{
			name: "PtrHeadMapMultiFieldsNotRootString",
			data: &struct {
				A struct {
					A map[string]int `json:"a,string"`
				}
				B struct {
					B map[string]int `json:"b,string"`
				}
			}{A: struct {
				A map[string]int `json:"a,string"`
			}{A: map[string]int{"m": -1}}, B: struct {
				B map[string]int `json:"b,string"`
			}{B: map[string]int{"m": 1}}},
		},

		// PtrHeadMapPtrMultiFieldsNotRoot
		{
			name: "PtrHeadMapPtrMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *map[string]int `json:"a"`
				}
				B *struct {
					B *map[string]int `json:"b"`
				}
			}{A: &(struct {
				A *map[string]int `json:"a"`
			}{A: mapptr(map[string]int{"m": -1})}), B: &(struct {
				B *map[string]int `json:"b"`
			}{B: mapptr(map[string]int{"m": 1})})},
		},
		{
			name: "PtrHeadMapPtrMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *map[string]int `json:"a,omitempty"`
				}
				B *struct {
					B *map[string]int `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *map[string]int `json:"a,omitempty"`
			}{A: mapptr(map[string]int{"m": -1})}), B: &(struct {
				B *map[string]int `json:"b,omitempty"`
			}{B: mapptr(map[string]int{"m": 1})})},
		},
		{
			name: "PtrHeadMapPtrMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *map[string]int `json:"a,string"`
				}
				B *struct {
					B *map[string]int `json:"b,string"`
				}
			}{A: &(struct {
				A *map[string]int `json:"a,string"`
			}{A: mapptr(map[string]int{"m": -1})}), B: &(struct {
				B *map[string]int `json:"b,string"`
			}{B: mapptr(map[string]int{"m": 1})})},
		},

		// PtrHeadMapPtrNilMultiFieldsNotRoot
		{
			name: "PtrHeadMapPtrNilMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *map[string]int `json:"a"`
				}
				B *struct {
					B *map[string]int `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadMapPtrNilMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *map[string]int `json:"a,omitempty"`
				} `json:",omitempty"`
				B *struct {
					B *map[string]int `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadMapPtrNilMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *map[string]int `json:"a,string"`
				} `json:",string"`
				B *struct {
					B *map[string]int `json:"b,string"`
				} `json:",string"`
			}{A: nil, B: nil},
		},

		// PtrHeadMapNilMultiFieldsNotRoot
		{
			name: "PtrHeadMapNilMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *map[string]int `json:"a"`
				}
				B *struct {
					B *map[string]int `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadMapNilMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *map[string]int `json:"a,omitempty"`
				}
				B *struct {
					B *map[string]int `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadMapNilMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *map[string]int `json:"a,string"`
				}
				B *struct {
					B *map[string]int `json:"b,string"`
				}
			})(nil),
		},

		// PtrHeadMapDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadMapDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A map[string]int `json:"a"`
					B map[string]int `json:"b"`
				}
				B *struct {
					A map[string]int `json:"a"`
					B map[string]int `json:"b"`
				}
			}{A: &(struct {
				A map[string]int `json:"a"`
				B map[string]int `json:"b"`
			}{A: map[string]int{"m": -1}, B: map[string]int{"m": 1}}), B: &(struct {
				A map[string]int `json:"a"`
				B map[string]int `json:"b"`
			}{A: map[string]int{"m": -1}, B: nil})},
		},
		{
			name: "PtrHeadMapDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A map[string]int `json:"a,omitempty"`
					B map[string]int `json:"b,omitempty"`
				}
				B *struct {
					A map[string]int `json:"a,omitempty"`
					B map[string]int `json:"b,omitempty"`
				}
			}{A: &(struct {
				A map[string]int `json:"a,omitempty"`
				B map[string]int `json:"b,omitempty"`
			}{A: map[string]int{"m": -1}, B: map[string]int{"m": 1}}), B: &(struct {
				A map[string]int `json:"a,omitempty"`
				B map[string]int `json:"b,omitempty"`
			}{A: map[string]int{"m": -1}, B: nil})},
		},
		{
			name: "PtrHeadMapDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A map[string]int `json:"a,string"`
					B map[string]int `json:"b,string"`
				}
				B *struct {
					A map[string]int `json:"a,string"`
					B map[string]int `json:"b,string"`
				}
			}{A: &(struct {
				A map[string]int `json:"a,string"`
				B map[string]int `json:"b,string"`
			}{A: map[string]int{"m": -1}, B: map[string]int{"m": 1}}), B: &(struct {
				A map[string]int `json:"a,string"`
				B map[string]int `json:"b,string"`
			}{A: map[string]int{"m": -1}, B: nil})},
		},

		// PtrHeadMapNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadMapNilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A map[string]int `json:"a"`
					B map[string]int `json:"b"`
				}
				B *struct {
					A map[string]int `json:"a"`
					B map[string]int `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadMapNilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A map[string]int `json:"a,omitempty"`
					B map[string]int `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A map[string]int `json:"a,omitempty"`
					B map[string]int `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadMapNilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A map[string]int `json:"a,string"`
					B map[string]int `json:"b,string"`
				}
				B *struct {
					A map[string]int `json:"a,string"`
					B map[string]int `json:"b,string"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadMapNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadMapNilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A map[string]int `json:"a"`
					B map[string]int `json:"b"`
				}
				B *struct {
					A map[string]int `json:"a"`
					B map[string]int `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadMapNilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A map[string]int `json:"a,omitempty"`
					B map[string]int `json:"b,omitempty"`
				}
				B *struct {
					A map[string]int `json:"a,omitempty"`
					B map[string]int `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadMapNilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A map[string]int `json:"a,string"`
					B map[string]int `json:"b,string"`
				}
				B *struct {
					A map[string]int `json:"a,string"`
					B map[string]int `json:"b,string"`
				}
			})(nil),
		},

		// PtrHeadMapPtrDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadMapPtrDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *map[string]int `json:"a"`
					B *map[string]int `json:"b"`
				}
				B *struct {
					A *map[string]int `json:"a"`
					B *map[string]int `json:"b"`
				}
			}{A: &(struct {
				A *map[string]int `json:"a"`
				B *map[string]int `json:"b"`
			}{A: mapptr(map[string]int{"m": -1}), B: mapptr(map[string]int{"m": 1})}), B: &(struct {
				A *map[string]int `json:"a"`
				B *map[string]int `json:"b"`
			}{A: nil, B: nil})},
		},
		{
			name: "PtrHeadMapPtrDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *map[string]int `json:"a,omitempty"`
					B *map[string]int `json:"b,omitempty"`
				}
				B *struct {
					A *map[string]int `json:"a,omitempty"`
					B *map[string]int `json:"b,omitempty"`
				}
			}{A: &(struct {
				A *map[string]int `json:"a,omitempty"`
				B *map[string]int `json:"b,omitempty"`
			}{A: mapptr(map[string]int{"m": -1}), B: mapptr(map[string]int{"m": 1})}), B: &(struct {
				A *map[string]int `json:"a,omitempty"`
				B *map[string]int `json:"b,omitempty"`
			}{A: nil, B: nil})},
		},
		{
			name: "PtrHeadMapPtrDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *map[string]int `json:"a,string"`
					B *map[string]int `json:"b,string"`
				}
				B *struct {
					A *map[string]int `json:"a,string"`
					B *map[string]int `json:"b,string"`
				}
			}{A: &(struct {
				A *map[string]int `json:"a,string"`
				B *map[string]int `json:"b,string"`
			}{A: mapptr(map[string]int{"m": -1}), B: mapptr(map[string]int{"m": 1})}), B: &(struct {
				A *map[string]int `json:"a,string"`
				B *map[string]int `json:"b,string"`
			}{A: nil, B: nil})},
		},

		// PtrHeadMapPtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadMapPtrNilDoubleMultiFieldsNotRoot",
			data: &struct {
				A *struct {
					A *map[string]int `json:"a"`
					B *map[string]int `json:"b"`
				}
				B *struct {
					A *map[string]int `json:"a"`
					B *map[string]int `json:"b"`
				}
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadMapPtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: &struct {
				A *struct {
					A *map[string]int `json:"a,omitempty"`
					B *map[string]int `json:"b,omitempty"`
				} `json:",omitempty"`
				B *struct {
					A *map[string]int `json:"a,omitempty"`
					B *map[string]int `json:"b,omitempty"`
				} `json:",omitempty"`
			}{A: nil, B: nil},
		},
		{
			name: "PtrHeadMapPtrNilDoubleMultiFieldsNotRootString",
			data: &struct {
				A *struct {
					A *map[string]int `json:"a,string"`
					B *map[string]int `json:"b,string"`
				}
				B *struct {
					A *map[string]int `json:"a,string"`
					B *map[string]int `json:"b,string"`
				}
			}{A: nil, B: nil},
		},

		// PtrHeadMapPtrNilDoubleMultiFieldsNotRoot
		{
			name: "PtrHeadMapPtrNilDoubleMultiFieldsNotRoot",
			data: (*struct {
				A *struct {
					A *map[string]int `json:"a"`
					B *map[string]int `json:"b"`
				}
				B *struct {
					A *map[string]int `json:"a"`
					B *map[string]int `json:"b"`
				}
			})(nil),
		},
		{
			name: "PtrHeadMapPtrNilDoubleMultiFieldsNotRootOmitEmpty",
			data: (*struct {
				A *struct {
					A *map[string]int `json:"a,omitempty"`
					B *map[string]int `json:"b,omitempty"`
				}
				B *struct {
					A *map[string]int `json:"a,omitempty"`
					B *map[string]int `json:"b,omitempty"`
				}
			})(nil),
		},
		{
			name: "PtrHeadMapPtrNilDoubleMultiFieldsNotRootString",
			data: (*struct {
				A *struct {
					A *map[string]int `json:"a,string"`
					B *map[string]int `json:"b,string"`
				}
				B *struct {
					A *map[string]int `json:"a,string"`
					B *map[string]int `json:"b,string"`
				}
			})(nil),
		},

		// AnonymousHeadMap
		{
			name: "AnonymousHeadMap",
			data: struct {
				structMap
				B map[string]int `json:"b"`
			}{
				structMap: structMap{A: map[string]int{"m": -1}},
				B:         nil,
			},
		},
		{
			name: "AnonymousHeadMapOmitEmpty",
			data: struct {
				structMapOmitEmpty
				B map[string]int `json:"b,omitempty"`
			}{
				structMapOmitEmpty: structMapOmitEmpty{A: map[string]int{"m": -1}},
				B:                  nil,
			},
		},
		{
			name: "AnonymousHeadMapString",
			data: struct {
				structMapString
				B map[string]int `json:"b,string"`
			}{
				structMapString: structMapString{A: map[string]int{"m": -1}},
				B:               nil,
			},
		},

		// PtrAnonymousHeadMap
		{
			name: "PtrAnonymousHeadMap",
			data: struct {
				*structMap
				B map[string]int `json:"b"`
			}{
				structMap: &structMap{A: map[string]int{"m": -1}},
				B:         nil,
			},
		},
		{
			name: "PtrAnonymousHeadMapOmitEmpty",
			data: struct {
				*structMapOmitEmpty
				B map[string]int `json:"b,omitempty"`
			}{
				structMapOmitEmpty: &structMapOmitEmpty{A: map[string]int{"m": -1}},
				B:                  nil,
			},
		},
		{
			name: "PtrAnonymousHeadMapString",
			data: struct {
				*structMapString
				B map[string]int `json:"b,string"`
			}{
				structMapString: &structMapString{A: map[string]int{"m": -1}},
				B:               nil,
			},
		},

		// PtrAnonymousHeadMapNil
		{
			name: "PtrAnonymousHeadMapNil",
			data: struct {
				*structMap
				B map[string]int `json:"b"`
			}{
				structMap: &structMap{A: nil},
				B:         nil,
			},
		},
		{
			name: "PtrAnonymousHeadMapNilOmitEmpty",
			data: struct {
				*structMapOmitEmpty
				B map[string]int `json:"b,omitempty"`
			}{
				structMapOmitEmpty: &structMapOmitEmpty{A: nil},
				B:                  nil,
			},
		},
		{
			name: "PtrAnonymousHeadMapNilString",
			data: struct {
				*structMapString
				B map[string]int `json:"b,string"`
			}{
				structMapString: &structMapString{A: nil},
				B:               nil,
			},
		},

		// NilPtrAnonymousHeadMap
		{
			name: "NilPtrAnonymousHeadMap",
			data: struct {
				*structMap
				B map[string]int `json:"b"`
			}{
				structMap: nil,
				B:         map[string]int{"m": -1},
			},
		},
		{
			name: "NilPtrAnonymousHeadMapOmitEmpty",
			data: struct {
				*structMapOmitEmpty
				B map[string]int `json:"b,omitempty"`
			}{
				structMapOmitEmpty: nil,
				B:                  map[string]int{"m": -1},
			},
		},
		{
			name: "NilPtrAnonymousHeadMapString",
			data: struct {
				*structMapString
				B map[string]int `json:"b,string"`
			}{
				structMapString: nil,
				B:               map[string]int{"m": -1},
			},
		},

		// AnonymousHeadMapPtr
		{
			name: "AnonymousHeadMapPtr",
			data: struct {
				structMapPtr
				B *map[string]int `json:"b"`
			}{
				structMapPtr: structMapPtr{A: mapptr(map[string]int{"m": -1})},
				B:            nil,
			},
		},
		{
			name: "AnonymousHeadMapPtrOmitEmpty",
			data: struct {
				structMapPtrOmitEmpty
				B *map[string]int `json:"b,omitempty"`
			}{
				structMapPtrOmitEmpty: structMapPtrOmitEmpty{A: mapptr(map[string]int{"m": -1})},
				B:                     nil,
			},
		},
		{
			name: "AnonymousHeadMapPtrString",
			data: struct {
				structMapPtrString
				B *map[string]int `json:"b,string"`
			}{
				structMapPtrString: structMapPtrString{A: mapptr(map[string]int{"m": -1})},
				B:                  nil,
			},
		},

		// AnonymousHeadMapPtrNil
		{
			name: "AnonymousHeadMapPtrNil",
			data: struct {
				structMapPtr
				B *map[string]int `json:"b"`
			}{
				structMapPtr: structMapPtr{A: nil},
				B:            mapptr(map[string]int{"m": -1}),
			},
		},
		{
			name: "AnonymousHeadMapPtrNilOmitEmpty",
			data: struct {
				structMapPtrOmitEmpty
				B *map[string]int `json:"b,omitempty"`
			}{
				structMapPtrOmitEmpty: structMapPtrOmitEmpty{A: nil},
				B:                     mapptr(map[string]int{"m": -1}),
			},
		},
		{
			name: "AnonymousHeadMapPtrNilString",
			data: struct {
				structMapPtrString
				B *map[string]int `json:"b,string"`
			}{
				structMapPtrString: structMapPtrString{A: nil},
				B:                  mapptr(map[string]int{"m": -1}),
			},
		},

		// PtrAnonymousHeadMapPtr
		{
			name: "PtrAnonymousHeadMapPtr",
			data: struct {
				*structMapPtr
				B *map[string]int `json:"b"`
			}{
				structMapPtr: &structMapPtr{A: mapptr(map[string]int{"m": -1})},
				B:            nil,
			},
		},
		{
			name: "PtrAnonymousHeadMapPtrOmitEmpty",
			data: struct {
				*structMapPtrOmitEmpty
				B *map[string]int `json:"b,omitempty"`
			}{
				structMapPtrOmitEmpty: &structMapPtrOmitEmpty{A: mapptr(map[string]int{"m": -1})},
				B:                     nil,
			},
		},
		{
			name: "PtrAnonymousHeadMapPtrString",
			data: struct {
				*structMapPtrString
				B *map[string]int `json:"b,string"`
			}{
				structMapPtrString: &structMapPtrString{A: mapptr(map[string]int{"m": -1})},
				B:                  nil,
			},
		},

		// NilPtrAnonymousHeadMapPtr
		{
			name: "NilPtrAnonymousHeadMapPtr",
			data: struct {
				*structMapPtr
				B *map[string]int `json:"b"`
			}{
				structMapPtr: nil,
				B:            mapptr(map[string]int{"m": -1}),
			},
		},
		{
			name: "NilPtrAnonymousHeadMapPtrOmitEmpty",
			data: struct {
				*structMapPtrOmitEmpty
				B *map[string]int `json:"b,omitempty"`
			}{
				structMapPtrOmitEmpty: nil,
				B:                     mapptr(map[string]int{"m": -1}),
			},
		},
		{
			name: "NilPtrAnonymousHeadMapPtrString",
			data: struct {
				*structMapPtrString
				B *map[string]int `json:"b,string"`
			}{
				structMapPtrString: nil,
				B:                  mapptr(map[string]int{"m": -1}),
			},
		},

		// AnonymousHeadMapOnly
		{
			name: "AnonymousHeadMapOnly",
			data: struct {
				structMap
			}{
				structMap: structMap{A: map[string]int{"m": -1}},
			},
		},
		{
			name: "AnonymousHeadMapOnlyOmitEmpty",
			data: struct {
				structMapOmitEmpty
			}{
				structMapOmitEmpty: structMapOmitEmpty{A: map[string]int{"m": -1}},
			},
		},
		{
			name: "AnonymousHeadMapOnlyString",
			data: struct {
				structMapString
			}{
				structMapString: structMapString{A: map[string]int{"m": -1}},
			},
		},

		// PtrAnonymousHeadMapOnly
		{
			name: "PtrAnonymousHeadMapOnly",
			data: struct {
				*structMap
			}{
				structMap: &structMap{A: map[string]int{"m": -1}},
			},
		},
		{
			name: "PtrAnonymousHeadMapOnlyOmitEmpty",
			data: struct {
				*structMapOmitEmpty
			}{
				structMapOmitEmpty: &structMapOmitEmpty{A: map[string]int{"m": -1}},
			},
		},
		{
			name: "PtrAnonymousHeadMapOnlyString",
			data: struct {
				*structMapString
			}{
				structMapString: &structMapString{A: map[string]int{"m": -1}},
			},
		},

		// NilPtrAnonymousHeadMapOnly
		{
			name: "NilPtrAnonymousHeadMapOnly",
			data: struct {
				*structMap
			}{
				structMap: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadMapOnlyOmitEmpty",
			data: struct {
				*structMapOmitEmpty
			}{
				structMapOmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadMapOnlyString",
			data: struct {
				*structMapString
			}{
				structMapString: nil,
			},
		},

		// AnonymousHeadMapPtrOnly
		{
			name: "AnonymousHeadMapPtrOnly",
			data: struct {
				structMapPtr
			}{
				structMapPtr: structMapPtr{A: mapptr(map[string]int{"m": -1})},
			},
		},
		{
			name: "AnonymousHeadMapPtrOnlyOmitEmpty",
			data: struct {
				structMapPtrOmitEmpty
			}{
				structMapPtrOmitEmpty: structMapPtrOmitEmpty{A: mapptr(map[string]int{"m": -1})},
			},
		},
		{
			name: "AnonymousHeadMapPtrOnlyString",
			data: struct {
				structMapPtrString
			}{
				structMapPtrString: structMapPtrString{A: mapptr(map[string]int{"m": -1})},
			},
		},

		// AnonymousHeadMapPtrNilOnly
		{
			name: "AnonymousHeadMapPtrNilOnly",
			data: struct {
				structMapPtr
			}{
				structMapPtr: structMapPtr{A: nil},
			},
		},
		{
			name: "AnonymousHeadMapPtrNilOnlyOmitEmpty",
			data: struct {
				structMapPtrOmitEmpty
			}{
				structMapPtrOmitEmpty: structMapPtrOmitEmpty{A: nil},
			},
		},
		{
			name: "AnonymousHeadMapPtrNilOnlyString",
			data: struct {
				structMapPtrString
			}{
				structMapPtrString: structMapPtrString{A: nil},
			},
		},

		// PtrAnonymousHeadMapPtrOnly
		{
			name: "PtrAnonymousHeadMapPtrOnly",
			data: struct {
				*structMapPtr
			}{
				structMapPtr: &structMapPtr{A: mapptr(map[string]int{"m": -1})},
			},
		},
		{
			name: "PtrAnonymousHeadMapPtrOnlyOmitEmpty",
			data: struct {
				*structMapPtrOmitEmpty
			}{
				structMapPtrOmitEmpty: &structMapPtrOmitEmpty{A: mapptr(map[string]int{"m": -1})},
			},
		},
		{
			name: "PtrAnonymousHeadMapPtrOnlyString",
			data: struct {
				*structMapPtrString
			}{
				structMapPtrString: &structMapPtrString{A: mapptr(map[string]int{"m": -1})},
			},
		},

		// NilPtrAnonymousHeadMapPtrOnly
		{
			name: "NilPtrAnonymousHeadMapPtrOnly",
			data: struct {
				*structMapPtr
			}{
				structMapPtr: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadMapPtrOnlyOmitEmpty",
			data: struct {
				*structMapPtrOmitEmpty
			}{
				structMapPtrOmitEmpty: nil,
			},
		},
		{
			name: "NilPtrAnonymousHeadMapPtrOnlyString",
			data: struct {
				*structMapPtrString
			}{
				structMapPtrString: nil,
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
