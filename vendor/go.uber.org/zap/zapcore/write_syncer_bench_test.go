// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package zapcore

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/internal/ztest"
)

func BenchmarkMultiWriteSyncer(b *testing.B) {
	b.Run("2 discarder", func(b *testing.B) {
		w := NewMultiWriteSyncer(
			&ztest.Discarder{},
			&ztest.Discarder{},
		)
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := w.Write([]byte("foobarbazbabble")); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
	b.Run("4 discarder", func(b *testing.B) {
		w := NewMultiWriteSyncer(
			&ztest.Discarder{},
			&ztest.Discarder{},
			&ztest.Discarder{},
			&ztest.Discarder{},
		)
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := w.Write([]byte("foobarbazbabble")); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
	b.Run("4 discarder with buffer", func(b *testing.B) {
		w := &BufferedWriteSyncer{
			WS: NewMultiWriteSyncer(
				&ztest.Discarder{},
				&ztest.Discarder{},
				&ztest.Discarder{},
				&ztest.Discarder{},
			),
		}
		defer func() {
			assert.NoError(b, w.Stop(), "Unexpected error stopping buffered write syncer.")
		}()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := w.Write([]byte("foobarbazbabble")); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}

func BenchmarkWriteSyncer(b *testing.B) {
	b.Run("write file with no buffer", func(b *testing.B) {
		file, err := os.CreateTemp(b.TempDir(), "test.log")
		require.NoError(b, err)

		w := AddSync(file)
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := w.Write([]byte("foobarbazbabble")); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}
