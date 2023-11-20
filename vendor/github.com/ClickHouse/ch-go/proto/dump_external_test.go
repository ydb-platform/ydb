package proto_test

import (
	"bytes"
	"fmt"

	"github.com/ClickHouse/ch-go/proto"
)

func ExampleBlock_EncodeBlock() {
	// See ./internal/cmd/ch-native-dump for more sophisticated example.
	var (
		colK proto.ColInt64
		colV proto.ColInt64
	)
	// Generate some data.
	for i := 0; i < 100; i++ {
		colK.Append(int64(i))
		colV.Append(int64(i) + 1000)
	}
	// Write data to buffer.
	var buf proto.Buffer
	input := proto.Input{
		{"k", colK},
		{"v", colV},
	}
	b := proto.Block{
		Rows:    colK.Rows(),
		Columns: len(input),
	}
	// Note that we are using version 54451, proto.Version will fail.
	if err := b.EncodeRawBlock(&buf, 54451, input); err != nil {
		panic(err)
	}

	// You can write buf.Buf to io.Writer, e.g. os.Stdout or file.
	var out bytes.Buffer
	_, _ = out.Write(buf.Buf)

	// You can encode multiple buffers in sequence.
	//
	// To do this, reset buf and all columns, append new values
	// to columns and call EncodeRawBlock again.
	buf.Reset()
	colV.Reset()
	colV.Reset()

	fmt.Println(out.Len())
	// Output: 1618
}
