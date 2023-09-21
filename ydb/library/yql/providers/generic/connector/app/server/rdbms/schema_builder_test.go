package rdbms

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/clickhouse"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/postgresql"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	"google.golang.org/protobuf/proto"
)

func TestSchemaBuilder(t *testing.T) {
	t.Run("ClickHouse", func(t *testing.T) {
		sb := &schemaBuilder{
			typeMapper: clickhouse.NewTypeMapper(),
		}

		require.NoError(t, sb.addColumn("col1", "Int32"))  // supported
		require.NoError(t, sb.addColumn("col2", "String")) // supported
		require.NoError(t, sb.addColumn("col3", "UUID"))   // yet unsupported

		logger := utils.NewTestLogger(t)
		schema, err := sb.build(logger)
		require.NoError(t, err)
		require.NotNil(t, schema)

		require.Len(t, schema.Columns, 2)

		require.Equal(t, schema.Columns[0].Name, "col1")
		require.True(
			t,
			proto.Equal(schema.Columns[0].Type, &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT32}}),
			schema.Columns[0].Type)

		require.Equal(t, schema.Columns[1].Name, "col2")
		require.True(
			t,
			proto.Equal(schema.Columns[1].Type, &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_STRING}}),
			schema.Columns[1].Type)
	})

	t.Run("PostgreSQL", func(t *testing.T) {
		sb := &schemaBuilder{
			typeMapper: postgresql.NewTypeMapper(),
		}

		require.NoError(t, sb.addColumn("col1", "bigint")) // supported
		require.NoError(t, sb.addColumn("col2", "text"))   // supported
		require.NoError(t, sb.addColumn("col3", "time"))   // yet unsupported

		logger := utils.NewTestLogger(t)
		schema, err := sb.build(logger)
		require.NoError(t, err)
		require.NotNil(t, schema)

		require.Len(t, schema.Columns, 2)

		require.Equal(t, schema.Columns[0].Name, "col1")
		require.True(
			t,
			proto.Equal(
				schema.Columns[0].Type,
				&Ydb.Type{Type: &Ydb.Type_OptionalType{OptionalType: &Ydb.OptionalType{Item: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT64}}}}},
			),
			schema.Columns[0].Type)

		require.Equal(t, schema.Columns[1].Name, "col2")
		require.True(
			t,
			proto.Equal(
				schema.Columns[1].Type,
				&Ydb.Type{Type: &Ydb.Type_OptionalType{OptionalType: &Ydb.OptionalType{Item: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UTF8}}}}},
			),
			schema.Columns[1].Type)
	})

	t.Run("NonExistingTable", func(t *testing.T) {
		sb := &schemaBuilder{}
		schema, err := sb.build(utils.NewTestLogger(t))
		require.ErrorIs(t, err, utils.ErrTableDoesNotExist)
		require.Nil(t, schema)
	})

	t.Run("EmptyTable", func(t *testing.T) {
		sb := &schemaBuilder{
			typeMapper: clickhouse.NewTypeMapper(),
		}

		require.NoError(t, sb.addColumn("col1", "UUID")) // yet unsupported

		schema, err := sb.build(utils.NewTestLogger(t))
		require.NoError(t, err)
		require.NotNil(t, schema)
		require.Len(t, schema.Columns, 0)
	})
}
