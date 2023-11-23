package json_test

import (
	"testing"

	"github.com/goccy/go-json"
)

func TestColorize(t *testing.T) {
	v := struct {
		A int
		B uint
		C float32
		D string
		E bool
		F []byte
		G []int
		H *struct{}
		I map[string]interface{}
	}{
		A: 123,
		B: 456,
		C: 3.14,
		D: "hello",
		E: true,
		F: []byte("binary"),
		G: []int{1, 2, 3, 4},
		H: nil,
		I: map[string]interface{}{
			"mapA": -10,
			"mapB": 10,
			"mapC": nil,
		},
	}
	t.Run("marshal with color", func(t *testing.T) {
		b, err := json.MarshalWithOption(v, json.Colorize(json.DefaultColorScheme))
		if err != nil {
			t.Fatal(err)
		}
		t.Log(string(b))
	})
	t.Run("marshal indent with color", func(t *testing.T) {
		b, err := json.MarshalIndentWithOption(v, "", "\t", json.Colorize(json.DefaultColorScheme))
		if err != nil {
			t.Fatal(err)
		}
		t.Log("\n" + string(b))
	})
}
