package json_test

import (
	"bytes"
	"reflect"
	"sort"
	"testing"

	"github.com/goccy/go-json"
)

func TestExtractPath(t *testing.T) {
	src := []byte(`{"a":{"b":10,"c":true},"b":"text"}`)
	t.Run("$.a.b", func(t *testing.T) {
		path, err := json.CreatePath("$.a.b")
		if err != nil {
			t.Fatal(err)
		}
		contents, err := path.Extract(src)
		if err != nil {
			t.Fatal(err)
		}
		if len(contents) != 1 {
			t.Fatal("failed to extract")
		}
		if !bytes.Equal(contents[0], []byte("10")) {
			t.Fatal("failed to extract")
		}
	})
	t.Run("$.b", func(t *testing.T) {
		path, err := json.CreatePath("$.b")
		if err != nil {
			t.Fatal(err)
		}
		contents, err := path.Extract(src)
		if err != nil {
			t.Fatal(err)
		}
		if len(contents) != 1 {
			t.Fatal("failed to extract")
		}
		if !bytes.Equal(contents[0], []byte(`"text"`)) {
			t.Fatal("failed to extract")
		}
	})
	t.Run("$.a", func(t *testing.T) {
		path, err := json.CreatePath("$.a")
		if err != nil {
			t.Fatal(err)
		}
		contents, err := path.Extract(src)
		if err != nil {
			t.Fatal(err)
		}
		if len(contents) != 1 {
			t.Fatal("failed to extract")
		}
		if !bytes.Equal(contents[0], []byte(`{"b":10,"c":true}`)) {
			t.Fatal("failed to extract")
		}
	})
}

func TestUnmarshalPath(t *testing.T) {
	t.Run("int", func(t *testing.T) {
		src := []byte(`{"a":{"b":10,"c":true},"b":"text"}`)
		t.Run("success", func(t *testing.T) {
			path, err := json.CreatePath("$.a.b")
			if err != nil {
				t.Fatal(err)
			}
			var v int
			if err := path.Unmarshal(src, &v); err != nil {
				t.Fatal(err)
			}
			if v != 10 {
				t.Fatal("failed to unmarshal path")
			}
		})
		t.Run("failure", func(t *testing.T) {
			path, err := json.CreatePath("$.a.c")
			if err != nil {
				t.Fatal(err)
			}
			var v map[string]interface{}
			if err := path.Unmarshal(src, &v); err == nil {
				t.Fatal("expected error")
			}
		})
	})
	t.Run("bool", func(t *testing.T) {
		src := []byte(`{"a":{"b":10,"c":true},"b":"text"}`)
		t.Run("success", func(t *testing.T) {
			path, err := json.CreatePath("$.a.c")
			if err != nil {
				t.Fatal(err)
			}
			var v bool
			if err := path.Unmarshal(src, &v); err != nil {
				t.Fatal(err)
			}
			if !v {
				t.Fatal("failed to unmarshal path")
			}
		})
		t.Run("failure", func(t *testing.T) {
			path, err := json.CreatePath("$.a.b")
			if err != nil {
				t.Fatal(err)
			}
			var v bool
			if err := path.Unmarshal(src, &v); err == nil {
				t.Fatal("expected error")
			}
		})
	})
	t.Run("map", func(t *testing.T) {
		src := []byte(`{"a":{"b":10,"c":true},"b":"text"}`)
		t.Run("success", func(t *testing.T) {
			path, err := json.CreatePath("$.a")
			if err != nil {
				t.Fatal(err)
			}
			var v map[string]interface{}
			if err := path.Unmarshal(src, &v); err != nil {
				t.Fatal(err)
			}
			if len(v) != 2 {
				t.Fatal("failed to decode map")
			}
		})
	})

	t.Run("path with single quote selector", func(t *testing.T) {
		path, err := json.CreatePath("$['a.b'].c")
		if err != nil {
			t.Fatal(err)
		}

		var v string
		if err := path.Unmarshal([]byte(`{"a.b": {"c": "world"}}`), &v); err != nil {
			t.Fatal(err)
		}
		if v != "world" {
			t.Fatal("failed to unmarshal path")
		}
	})
	t.Run("path with double quote selector", func(t *testing.T) {
		path, err := json.CreatePath(`$."a.b".c`)
		if err != nil {
			t.Fatal(err)
		}

		var v string
		if err := path.Unmarshal([]byte(`{"a.b": {"c": "world"}}`), &v); err != nil {
			t.Fatal(err)
		}
		if v != "world" {
			t.Fatal("failed to unmarshal path")
		}
	})
}

func TestGetPath(t *testing.T) {
	t.Run("selector", func(t *testing.T) {
		var v interface{}
		if err := json.Unmarshal([]byte(`{"a":{"b":10,"c":true},"b":"text"}`), &v); err != nil {
			t.Fatal(err)
		}
		path, err := json.CreatePath("$.a.b")
		if err != nil {
			t.Fatal(err)
		}
		var b int
		if err := path.Get(v, &b); err != nil {
			t.Fatal(err)
		}
		if b != 10 {
			t.Fatalf("failed to decode by json.Get")
		}
	})
	t.Run("index", func(t *testing.T) {
		var v interface{}
		if err := json.Unmarshal([]byte(`{"a":[{"b":10,"c":true},{"b":"text"}]}`), &v); err != nil {
			t.Fatal(err)
		}
		path, err := json.CreatePath("$.a[0].b")
		if err != nil {
			t.Fatal(err)
		}
		var b int
		if err := path.Get(v, &b); err != nil {
			t.Fatal(err)
		}
		if b != 10 {
			t.Fatalf("failed to decode by json.Get")
		}
	})
	t.Run("indexAll", func(t *testing.T) {
		var v interface{}
		if err := json.Unmarshal([]byte(`{"a":[{"b":1,"c":true},{"b":2},{"b":3}]}`), &v); err != nil {
			t.Fatal(err)
		}
		path, err := json.CreatePath("$.a[*].b")
		if err != nil {
			t.Fatal(err)
		}
		var b []int
		if err := path.Get(v, &b); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(b, []int{1, 2, 3}) {
			t.Fatalf("failed to decode by json.Get")
		}
	})
	t.Run("recursive", func(t *testing.T) {
		var v interface{}
		if err := json.Unmarshal([]byte(`{"a":[{"b":1,"c":true},{"b":2},{"b":3}],"a2":{"b":4}}`), &v); err != nil {
			t.Fatal(err)
		}
		path, err := json.CreatePath("$..b")
		if err != nil {
			t.Fatal(err)
		}
		var b []int
		if err := path.Get(v, &b); err != nil {
			t.Fatal(err)
		}
		sort.Ints(b)
		if !reflect.DeepEqual(b, []int{1, 2, 3, 4}) {
			t.Fatalf("failed to decode by json.Get")
		}
	})
}
