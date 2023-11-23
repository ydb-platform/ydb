package solomon

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func compress(t *testing.T, c uint8, s string) []byte {
	buf := bytes.Buffer{}
	w := newCompressedWriter(&buf, CompressionType(c))
	_, err := w.Write([]byte(s))
	assert.Equal(t, nil, err)
	assert.Equal(t, nil, w.Close())
	return buf.Bytes()
}

func TestCompression_None(t *testing.T) {
	assert.Equal(t, []byte(nil), compress(t, uint8(CompressionNone), ""))
	assert.Equal(t, []byte{'a'}, compress(t, uint8(CompressionNone), "a"))
}

func TestCompression_Lz4(t *testing.T) {
	assert.Equal(t, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, compress(t, uint8(CompressionLz4), ""))
}
