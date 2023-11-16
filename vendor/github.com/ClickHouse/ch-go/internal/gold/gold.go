// Package gold implements golden files.
package gold

import (
	"bytes"
	"encoding/hex"
	"flag"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

const defaultDir = "_golden"

// _update reports whether golden files update is requested.
//
// Call Init() in TestMain to propagate.
var _update bool

// _clean reports whether all golden files should be removed before
// running tests.
//
// Call Init() in TestMain to propagate.
var _clean bool

// Init should be called in TestMain.
func Init() {
	flag.BoolVar(&_update, "update", false, "update golden files")
	flag.BoolVar(&_clean, "clean", true, "clean golden files")
	flag.Parse()

	if _clean && _update {
		dir, err := os.ReadDir(defaultDir)
		if err != nil {
			// Ignore any error.
			return
		}
		for _, f := range dir {
			p := filepath.Join(defaultDir, f.Name())
			if err := os.RemoveAll(p); err != nil {
				panic(err)
			}
		}
	}
}

// filePath returns path to golden file.
func filePath(elems ...string) string {
	return filepath.Join(
		append([]string{defaultDir}, elems...)...,
	)
}

func exists(t testing.TB, elems ...string) bool {
	t.Helper()

	p := filePath(elems...)
	data, err := os.Stat(p)
	if err == nil {
		if data.IsDir() {
			t.Fatalf("golden file %s is directory", p)
		}
		return true
	}
	if os.IsNotExist(err) {
		return false
	}

	// Unexpected error
	t.Fatal(err)
	return false
}

// readFile reads golden file.
func readFile(t testing.TB, elems ...string) []byte {
	t.Helper()

	p := filePath(elems...)
	data, err := os.ReadFile(p) // nolint:gosec // testing
	if err != nil {
		t.Fatalf("golden file %s: %+v", path.Join(elems...), err)
	}

	return data
}

func writeFile(t testing.TB, data []byte, elems ...string) {
	t.Helper()

	p := filePath(elems...)
	require.NoError(t, os.MkdirAll(path.Dir(p), 0o700), "make dir for golden files")
	require.NoError(t, os.WriteFile(p, data, 0o600), "write golden file")
}

// normalizeNewlines normalizes \r\n (windows) and \r (mac)
// into \n (unix).
func normalizeNewlines(d []byte) []byte {
	// replace CR LF \r\n (windows) with LF \n (unix)
	d = bytes.ReplaceAll(d, []byte{13, 10}, []byte{10})
	// replace CF \r (mac) with LF \n (unix)
	d = bytes.ReplaceAll(d, []byte{13}, []byte{10})
	return d
}

// Str checks text golden file.
func Str(t testing.TB, s string, name ...string) {
	t.Helper()

	if len(name) == 0 {
		name = []string{"file.txt"}
	}

	update := _update
	if !exists(t, name...) {
		t.Log("Populating initial golden file")
		update = true
	}
	if update {
		writeFile(t, []byte(s), name...)
	}

	data := readFile(t, name...)
	data = normalizeNewlines(data)

	require.Equal(t, string(data), s, "golden file text mismatch")
}

// Bytes check binary golden file.
func Bytes(t testing.TB, data []byte, name ...string) {
	t.Helper()

	if len(name) == 0 {
		name = []string{"file"}
	}

	// Adding ".raw" prefix to visually distinguish hex and raw.
	last := len(name) - 1
	rawName := append([]string{}, name...)
	rawName[last] += ".raw"

	update := _update
	if !exists(t, rawName...) {
		t.Log("Populating initial golden file")
		update = true
	}
	if update {
		// Writing hex dump next to raw binary to make
		// git diff more understandable on golden file
		// updates.
		dump := hex.Dump(data)
		dumpName := append([]string{}, name...)
		dumpName[last] += ".hex"
		writeFile(t, []byte(dump), dumpName...)

		// Writing raw file.
		writeFile(t, data, rawName...)
	}

	expected := readFile(t, rawName...)
	require.Equal(t, expected, data, "golden file binary mismatch")
}
