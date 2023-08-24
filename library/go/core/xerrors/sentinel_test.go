package xerrors

import (
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSentinelWrapNil(t *testing.T) {
	sentinel := NewSentinel("sentinel")
	assert.Panics(t, func() { assert.NoError(t, sentinel.Wrap(nil)) })
}

func TestSentinelWrap(t *testing.T) {
	sentinel := NewSentinel("sentinel")
	assert.EqualError(t, sentinel.Wrap(New("err")), "sentinel: err")
}

func TestSentinelMultiWrap(t *testing.T) {
	top := NewSentinel("top")
	middle := NewSentinel("middle")
	err := top.Wrap(middle.Wrap(New("bottom")))
	assert.EqualError(t, err, "top: middle: bottom")
}

func TestSentinelIs(t *testing.T) {
	sentinel := NewSentinel("sentinel")
	assert.True(t, Is(sentinel, sentinel))
	assert.True(t, Is(sentinel.Wrap(New("err")), sentinel))
	assert.True(t, Is(NewSentinel("err").Wrap(sentinel), sentinel))
	assert.True(t, Is(Errorf("wrapper: %w", sentinel), sentinel))
	assert.True(t, Is(sentinel.WithStackTrace(), sentinel))
	assert.True(t, Is(Errorf("wrapper: %w", sentinel.WithStackTrace()), sentinel))
}

func TestSentinelMultiWrapIs(t *testing.T) {
	top := NewSentinel("top")
	middle := NewSentinel("middle")
	err := top.Wrap(middle.Wrap(io.EOF))
	assert.True(t, Is(err, top))
	assert.True(t, Is(err, middle))
	assert.True(t, Is(err, io.EOF))
	assert.False(t, Is(err, New("random")))
}

func TestSentinelAs(t *testing.T) {
	sentinel := NewSentinel("sentinel")
	var target *Sentinel

	assert.True(t, As(sentinel, &target))
	assert.NotNil(t, target)
	target = nil

	assert.True(t, As(sentinel.Wrap(New("err")), &target))
	assert.NotNil(t, target)
	target = nil

	assert.True(t, As(NewSentinel("err").Wrap(sentinel), &target))
	assert.NotNil(t, target)
	target = nil

	assert.True(t, As(Errorf("wrapper: %w", sentinel), &target))
	assert.NotNil(t, target)
	target = nil

	assert.True(t, As(sentinel.WithStackTrace(), &target))
	assert.NotNil(t, target)
	target = nil

	assert.True(t, As(Errorf("wrapper: %w", sentinel.WithStackTrace()), &target))
	assert.NotNil(t, target)
	target = nil
}

func TestSentinelMultiWrapAs(t *testing.T) {
	top := NewSentinel("top")
	middle := NewSentinel("middle")
	err := top.Wrap(middle.Wrap(io.EOF))

	var target *Sentinel
	assert.True(t, As(err, &target))
	assert.NotNil(t, target)
}

func TestSentinelFormatting(t *testing.T) {
	sentinel := NewSentinel("sentinel")
	assert.Equal(t, "sentinel", fmt.Sprintf("%s", sentinel))
	assert.Equal(t, "sentinel", fmt.Sprintf("%v", sentinel))
	assert.Equal(t, "sentinel", fmt.Sprintf("%+v", sentinel))
}
