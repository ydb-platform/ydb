package xruntime

import (
	"fmt"
	"testing"

	"github.com/ydb-platform/ydb/library/go/test/testhelpers"
)

func BenchmarkNew(b *testing.B) {
	inputs := []struct {
		Name string
		Func func(skip int) *StackTrace
	}{
		{
			Name: "Frame",
			Func: NewFrame,
		},
		{
			Name: "StackTrace16",
			Func: NewStackTrace16,
		},
		{
			Name: "StackTrace32",
			Func: NewStackTrace32,
		},
		{
			Name: "StackTrace64",
			Func: NewStackTrace64,
		},
		{
			Name: "StackTrace128",
			Func: NewStackTrace128,
		},
	}

	for _, depth := range []int{1, 16, 32, 64, 128, 256} {
		for _, input := range inputs {
			b.Run(fmt.Sprintf("Depth%d_%s", depth, input.Name), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					testhelpers.Recurse(depth, func() { input.Func(0) })
				}
			})
		}
	}
}
