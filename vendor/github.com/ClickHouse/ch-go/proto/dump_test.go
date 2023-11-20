package proto_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go/proto"
)

func TestDump(t *testing.T) {
	// Testing decoding of Native format dump.
	//
	// CREATE TABLE test_dump (id Int8, v String)
	//   ENGINE = MergeTree()
	// ORDER BY id;
	//
	// SELECT * FROM test_dump
	//   ORDER BY id
	// INTO OUTFILE 'test_dump_native.raw' FORMAT Native;
	data, err := os.ReadFile(filepath.Join("_testdata", "test_dump_native.raw"))
	require.NoError(t, err)
	var (
		dec    proto.Block
		ids    proto.ColInt8
		values proto.ColStr
	)
	require.NoError(t, dec.DecodeRawBlock(proto.NewReader(bytes.NewReader(data)), 54451, proto.Results{
		{Name: "id", Data: &ids},
		{Name: "v", Data: &values},
	}),
	)
}

func TestDumpLowCardinality(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("_testdata", "select_lc.raw"))
	require.NoError(t, err)
	col := new(proto.ColStr).LowCardinality().Array()
	var dec proto.Block
	require.NoError(t, dec.DecodeRawBlock(proto.NewReader(bytes.NewReader(data)), 54451, proto.Results{
		{Name: "v", Data: col},
	}),
	)
}
