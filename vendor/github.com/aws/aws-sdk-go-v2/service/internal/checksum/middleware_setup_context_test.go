//go:build go1.16
// +build go1.16

package checksum

import (
	"context"
	"testing"

	"github.com/aws/smithy-go/middleware"
)

func TestSetupInput(t *testing.T) {
	type Params struct {
		Value string
	}

	cases := map[string]struct {
		inputParams  interface{}
		getAlgorithm func(interface{}) (string, bool)
		expectValue  string
	}{
		"nil accessor": {
			expectValue: "",
		},
		"found empty": {
			inputParams: Params{Value: ""},
			getAlgorithm: func(v interface{}) (string, bool) {
				vv := v.(Params)
				return vv.Value, true
			},
			expectValue: "",
		},
		"found not set": {
			inputParams: Params{Value: ""},
			getAlgorithm: func(v interface{}) (string, bool) {
				return "", false
			},
			expectValue: "",
		},
		"found": {
			inputParams: Params{Value: "abc123"},
			getAlgorithm: func(v interface{}) (string, bool) {
				vv := v.(Params)
				return vv.Value, true
			},
			expectValue: "abc123",
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			m := setupInputContext{
				GetAlgorithm: c.getAlgorithm,
			}

			_, _, err := m.HandleInitialize(context.Background(),
				middleware.InitializeInput{Parameters: c.inputParams},
				middleware.InitializeHandlerFunc(
					func(ctx context.Context, input middleware.InitializeInput) (
						out middleware.InitializeOutput, metadata middleware.Metadata, err error,
					) {
						v := getContextInputAlgorithm(ctx)
						if e, a := c.expectValue, v; e != a {
							t.Errorf("expect value %v, got %v", e, a)
						}

						return out, metadata, nil
					},
				))
			if err != nil {
				t.Fatalf("expect no error, got %v", err)
			}

		})
	}
}

func TestSetupOutput(t *testing.T) {
	type Params struct {
		Value string
	}

	cases := map[string]struct {
		inputParams       interface{}
		getValidationMode func(interface{}) (string, bool)
		expectValue       string
	}{
		"nil accessor": {
			expectValue: "",
		},
		"found empty": {
			inputParams: Params{Value: ""},
			getValidationMode: func(v interface{}) (string, bool) {
				vv := v.(Params)
				return vv.Value, true
			},
			expectValue: "",
		},
		"found not set": {
			inputParams: Params{Value: ""},
			getValidationMode: func(v interface{}) (string, bool) {
				return "", false
			},
			expectValue: "",
		},
		"found": {
			inputParams: Params{Value: "abc123"},
			getValidationMode: func(v interface{}) (string, bool) {
				vv := v.(Params)
				return vv.Value, true
			},
			expectValue: "abc123",
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			m := setupOutputContext{
				GetValidationMode: c.getValidationMode,
			}

			_, _, err := m.HandleInitialize(context.Background(),
				middleware.InitializeInput{Parameters: c.inputParams},
				middleware.InitializeHandlerFunc(
					func(ctx context.Context, input middleware.InitializeInput) (
						out middleware.InitializeOutput, metadata middleware.Metadata, err error,
					) {
						v := getContextOutputValidationMode(ctx)
						if e, a := c.expectValue, v; e != a {
							t.Errorf("expect value %v, got %v", e, a)
						}

						return out, metadata, nil
					},
				))
			if err != nil {
				t.Fatalf("expect no error, got %v", err)
			}

		})
	}
}
