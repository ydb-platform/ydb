package blockcodecs

import (
	"encoding/binary"
	"fmt"
	"sync"

	"go.uber.org/atomic"
)

type CodecID uint16

type Codec interface {
	ID() CodecID
	Name() string

	DecodedLen(in []byte) (int, error)
	Encode(dst, src []byte) ([]byte, error)
	Decode(dst, src []byte) ([]byte, error)
}

var (
	codecsByID   sync.Map
	codecsByName sync.Map
)

// Register new codec.
//
// NOTE: update FindCodecByName description, after adding new codecs.
func Register(c Codec) {
	if _, duplicate := codecsByID.LoadOrStore(c.ID(), c); duplicate {
		panic(fmt.Sprintf("codec with id %d is already registered", c.ID()))
	}

	RegisterAlias(c.Name(), c)
}

func RegisterAlias(name string, c Codec) {
	if _, duplicate := codecsByName.LoadOrStore(name, c); duplicate {
		panic(fmt.Sprintf("codec with name %s is already registered", c.Name()))
	}
}

func ListCodecs() []Codec {
	var c []Codec
	codecsByID.Range(func(key, value interface{}) bool {
		c = append(c, value.(Codec))
		return true
	})
	return c
}

func FindCodec(id CodecID) Codec {
	c, ok := codecsByID.Load(id)
	if ok {
		return c.(Codec)
	} else {
		return nil
	}
}

// FindCodecByName returns codec by name.
//
// Possible names:
//
//	null
//	snappy
//	zstd08_{level} - level is integer 1, 3 or 7.
//	zstd_{level} - level is integer 1, 3 or 7.
func FindCodecByName(name string) Codec {
	c, ok := codecsByName.Load(name)
	if ok {
		return c.(Codec)
	} else {
		return nil
	}
}

var (
	maxDecompressedBlockSize = atomic.NewInt32(16 << 20) // 16 MB
)

func DecodedLen(in []byte) (int, error) {
	if len(in) < 8 {
		return 0, fmt.Errorf("short block: %d < 8", len(in))
	}

	return int(binary.LittleEndian.Uint64(in[:8])), nil
}
