package geojson

import "encoding/json"

// CustomJSONMarshaler can be set to have the code use a different
// json marshaler than the default in the standard library.
// One use case in enabling `github.com/json-iterator/go`
// with something like this:
//
//	import (
//	  jsoniter "github.com/json-iterator/go"
//	  "github.com/paulmach/orb"
//	)
//
//	var c = jsoniter.Config{
//	  EscapeHTML:              true,
//	  SortMapKeys:             false,
//	  MarshalFloatWith6Digits: true,
//	}.Froze()
//
//	orb.CustomJSONMarshaler = c
//	orb.CustomJSONUnmarshaler = c
//
// Note that any errors encountered during marshaling will be different.
var CustomJSONMarshaler interface {
	Marshal(v interface{}) ([]byte, error)
} = nil

// CustomJSONUnmarshaler can be set to have the code use a different
// json unmarshaler than the default in the standard library.
// One use case in enabling `github.com/json-iterator/go`
// with something like this:
//
//	import (
//	  jsoniter "github.com/json-iterator/go"
//	  "github.com/paulmach/orb"
//	)
//
//	var c = jsoniter.Config{
//	  EscapeHTML:              true,
//	  SortMapKeys:             false,
//	  MarshalFloatWith6Digits: true,
//	}.Froze()
//
//	orb.CustomJSONMarshaler = c
//	orb.CustomJSONUnmarshaler = c
//
// Note that any errors encountered during unmarshaling will be different.
var CustomJSONUnmarshaler interface {
	Unmarshal(data []byte, v interface{}) error
} = nil

func marshalJSON(v interface{}) ([]byte, error) {
	if CustomJSONMarshaler == nil {
		return json.Marshal(v)
	}

	return CustomJSONMarshaler.Marshal(v)
}

func unmarshalJSON(data []byte, v interface{}) error {
	if CustomJSONUnmarshaler == nil {
		return json.Unmarshal(data, v)
	}

	return CustomJSONUnmarshaler.Unmarshal(data, v)
}

type nocopyRawMessage []byte

func (m *nocopyRawMessage) UnmarshalJSON(data []byte) error {
	*m = data
	return nil
}
