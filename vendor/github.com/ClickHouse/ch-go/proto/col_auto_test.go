package proto

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestColAuto_Infer(t *testing.T) {
	for _, columnType := range []ColumnType{
		ColumnTypeString,
		ColumnTypeArray.Sub(ColumnTypeString),
		ColumnTypeArray.Sub(ColumnTypeLowCardinality.Sub(ColumnTypeString)),
		ColumnTypeDate,
		ColumnTypeDate32,
		ColumnTypeInt8,
		ColumnTypeInt16,
		ColumnTypeArray.Sub(ColumnTypeInt16),
		ColumnTypeNullable.Sub(ColumnTypeInt16),
		ColumnTypeInt32,
		ColumnTypeInt64,
		ColumnTypeInt128,
		ColumnTypeInt256,
		ColumnTypeUInt8,
		ColumnTypeUInt16,
		ColumnTypeUInt32,
		ColumnTypeUInt64,
		ColumnTypeUInt128,
		ColumnTypeUInt256,
		ColumnTypeFloat32,
		ColumnTypeFloat64,
		ColumnTypeIPv4,
		ColumnTypeIPv6,
		ColumnTypeLowCardinality.Sub(ColumnTypeString),
		ColumnTypeDateTime.Sub("Europe/Berlin"),
		ColumnTypeDateTime64.Sub("9"),
		"Map(String,String)",
		"Enum8('hello'=1,'world'=2)",
		"Enum16('hello'=-1,'world'=10)",
		"IntervalSecond",
		"IntervalMinute",
		ColumnType(IntervalHour.String()),
		ColumnTypeNothing,
		"Nullable(Nothing)",
		"Array(Nothing)",
		ColumnTypeUUID,
		ColumnTypeArray.Sub(ColumnTypeUUID),
		ColumnTypeNullable.Sub(ColumnTypeUUID),
	} {
		r := AutoResult("foo")
		require.NoError(t, r.Data.(Inferable).Infer(columnType))
		require.Equal(t, columnType, r.Data.Type())
		r.Data.Reset()
		require.Equal(t, 0, r.Data.Rows())
	}
}
