package json

import (
	"testing"
	"unsafe"

	"github.com/goccy/go-json/internal/encoder"
)

func TestOpcodeSize(t *testing.T) {
	const uintptrSize = 4 << (^uintptr(0) >> 63)
	if uintptrSize == 8 {
		size := unsafe.Sizeof(encoder.Opcode{})
		if size != 120 {
			t.Fatalf("unexpected opcode size: expected 112bytes but got %dbytes", size)
		}
	}
}
