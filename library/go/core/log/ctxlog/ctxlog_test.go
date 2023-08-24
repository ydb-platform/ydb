package ctxlog

import (
	"context"
	"reflect"
	"testing"

	"github.com/ydb-platform/ydb/library/go/core/log"
)

func TestContextFields(t *testing.T) {
	for _, test := range []struct {
		ctx context.Context
		exp []log.Field
	}{
		{
			ctx: context.Background(),
			exp: nil,
		},
		{
			ctx: contextWithFields(
				log.String("foo", "bar"),
				log.String("bar", "baz"),
			),
			exp: []log.Field{
				log.String("foo", "bar"),
				log.String("bar", "baz"),
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			act := ContextFields(test.ctx)
			if exp := test.exp; !reflect.DeepEqual(act, exp) {
				t.Fatalf(
					"ContextFields() = %v; want %v",
					act, exp,
				)
			}
		})
	}
}

// TestWithFields tests the case when race condition may occur on adding fields
// to a bound field slice capable enough to store additional ones.
func TestWithFields(t *testing.T) {
	fs := make([]log.Field, 2, 4)
	fs[0] = log.String("a", "a")
	fs[1] = log.String("b", "b")

	// Bind to ctx1 field slice with cap(fs) = 2.
	ctx1 := WithFields(context.Background(), fs...)

	// Bind additional two fields to ctx2 that are able to fit the parent's
	// ctx1 bound fields.
	_ = WithFields(ctx1, log.String("c", "c"), log.String("d", "d"))

	var act, exp [2]log.Field // Expect to zero-values of Field.
	copy(act[:], fs[2:4])     // Check the tail of initial slice.
	if act != exp {
		t.Fatalf("fields tail is non-empty: %v", act)
	}
}

func contextWithFields(fs ...log.Field) context.Context {
	return context.WithValue(context.Background(), ctxKey{}, fs)
}
