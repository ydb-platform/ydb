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
package zapcore_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	. "go.uber.org/zap/zapcore"
)

var (
	testEntry = Entry{
		LoggerName: "main",
		Level:      InfoLevel,
		Message:    `hello`,
		Time:       _epoch,
		Stack:      "fake-stack",
		Caller:     EntryCaller{Defined: true, File: "foo.go", Line: 42, Function: "foo.Foo"},
	}
)

func TestConsoleSeparator(t *testing.T) {
	tests := []struct {
		desc        string
		separator   string
		wantConsole string
	}{
		{
			desc:        "space console separator",
			separator:   " ",
			wantConsole: "0 info main foo.go:42 foo.Foo hello\nfake-stack\n",
		},
		{
			desc:        "default console separator",
			separator:   "",
			wantConsole: "0\tinfo\tmain\tfoo.go:42\tfoo.Foo\thello\nfake-stack\n",
		},
		{
			desc:        "tag console separator",
			separator:   "\t",
			wantConsole: "0\tinfo\tmain\tfoo.go:42\tfoo.Foo\thello\nfake-stack\n",
		},
		{
			desc:        "dash console separator",
			separator:   "--",
			wantConsole: "0--info--main--foo.go:42--foo.Foo--hello\nfake-stack\n",
		},
	}

	for _, tt := range tests {
		console := NewConsoleEncoder(encoderTestEncoderConfig(tt.separator))
		t.Run(tt.desc, func(t *testing.T) {
			entry := testEntry
			consoleOut, err := console.EncodeEntry(entry, nil)
			if !assert.NoError(t, err) {
				return
			}
			assert.Equal(
				t,
				tt.wantConsole,
				consoleOut.String(),
				"Unexpected console output",
			)
		})

	}
}

func encoderTestEncoderConfig(separator string) EncoderConfig {
	testEncoder := testEncoderConfig()
	testEncoder.ConsoleSeparator = separator
	return testEncoder
}
