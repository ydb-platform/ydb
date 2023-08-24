package xreflect_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb/library/go/x/xreflect"
)

type Foo struct {
	S string
}

var _ InterfaceFoo = &Foo{}

func (f *Foo) FuncFoo() {}

type InterfaceFoo interface {
	FuncFoo()
}

type Bar struct {
	I int
}

var _ InterfaceBar = &Bar{}

func (f *Bar) FuncBar() {}

type InterfaceBar interface {
	FuncBar()
}

func TestAssign(t *testing.T) {
	t.Run("ValueToValue", func(t *testing.T) {
		src := Foo{S: "S"}
		var dst Foo
		require.True(t, xreflect.Assign(src, &dst))
		require.Equal(t, src, dst)
	})

	t.Run("ValueToValueInvalid", func(t *testing.T) {
		src := Foo{S: "S"}
		var dst Bar
		require.False(t, xreflect.Assign(src, &dst))
		require.Equal(t, Bar{}, dst)
	})

	t.Run("ValueToInterface", func(t *testing.T) {
		src := Foo{S: "S"}
		var dst InterfaceFoo
		require.True(t, xreflect.Assign(&src, &dst))
		require.NotNil(t, dst)
		v, ok := dst.(*Foo)
		require.True(t, ok)
		require.Equal(t, &src, v)
	})

	t.Run("ValueToInterfaceInvalid", func(t *testing.T) {
		src := Bar{I: 42}
		var dst InterfaceFoo
		require.False(t, xreflect.Assign(&src, &dst))
		require.Nil(t, dst)
	})

	t.Run("InterfaceToInterface", func(t *testing.T) {
		src := InterfaceFoo(&Foo{S: "S"})
		var dst InterfaceFoo
		require.True(t, xreflect.Assign(src, &dst))
		require.NotNil(t, dst)
		require.Equal(t, src, dst)
		v, ok := dst.(*Foo)
		require.True(t, ok)
		require.Equal(t, src, v)
	})

	t.Run("InterfaceToInterfaceInvalid", func(t *testing.T) {
		src := InterfaceFoo(&Foo{S: "S"})
		var dst InterfaceBar
		require.False(t, xreflect.Assign(src, &dst))
		require.Nil(t, dst)
	})
}
