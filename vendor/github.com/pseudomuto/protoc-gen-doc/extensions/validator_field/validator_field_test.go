package extensions_test

import (
	"testing"

	"github.com/golang/protobuf/proto"
	validator "github.com/mwitkow/go-proto-validators"
	"github.com/pseudomuto/protoc-gen-doc/extensions"
	. "github.com/pseudomuto/protoc-gen-doc/extensions/validator_field"
	"github.com/stretchr/testify/require"
)

func TestTransform(t *testing.T) {
	fieldValidator := &validator.FieldValidator{
		StringNotEmpty: proto.Bool(true),
	}

	transformed := extensions.Transform(map[string]interface{}{"validator.field": fieldValidator})
	require.NotEmpty(t, transformed)

	rules := transformed["validator.field"].(ValidatorExtension).Rules()
	require.Equal(t, rules, []ValidatorRule{
		{Name: "string_not_empty", Value: true},
	})
}
