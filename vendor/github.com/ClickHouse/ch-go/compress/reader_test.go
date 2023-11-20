package compress

import (
	"testing"

	"github.com/go-faster/city"
	"github.com/stretchr/testify/require"
)

func TestFormatU128(t *testing.T) {
	v := city.CH128([]byte("Moscow"))
	require.Equal(t, "6ddf3eeebf17df2e559d40c605f3ae22", FormatU128(v))
}
