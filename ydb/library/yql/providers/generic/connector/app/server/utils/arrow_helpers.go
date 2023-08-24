package utils

import (
	"fmt"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

type ArrowBuilder[VT ValueType] interface {
	AppendNull()
	Append(value VT)
}

func SelectWhatToArrow(
	selectWhat *api_service_protos.TSelect_TWhat,
	arrowAllocator memory.Allocator,
) (*arrow.Schema, []array.Builder, error) {
	fields := make([]arrow.Field, 0, len(selectWhat.Items))
	builders := make([]array.Builder, 0, len(selectWhat.Items))

	for i, item := range selectWhat.Items {
		column := item.GetColumn()
		if column == nil {
			return nil, nil, fmt.Errorf("item #%d (%v) is not a column", i, item)
		}

		ydbType := column.GetType()

		var (
			field   arrow.Field
			builder array.Builder
			err     error
		)

		// Reference table: https://wiki.yandex-team.ru/rtmapreduce/yql-streams-corner/connectors/lld-02-tipy-dannyx
		switch t := ydbType.Type.(type) {
		// Primitive types
		case *Ydb.Type_TypeId:
			field, builder, err = primitiveTypeToArrow(t.TypeId, column, arrowAllocator)
			if err != nil {
				return nil, nil, fmt.Errorf("convert primitive type: %w", err)
			}
		case *Ydb.Type_OptionalType:
			field, builder, err = primitiveTypeToArrow(t.OptionalType.Item.GetTypeId(), column, arrowAllocator)
			if err != nil {
				return nil, nil, fmt.Errorf("convert optional type: %w", err)
			}
		default:
			return nil, nil, fmt.Errorf("only primitive types are supported, got '%T' instead: %w", t, ErrDataTypeNotSupported)
		}

		fields = append(fields, field)
		builders = append(builders, builder)
	}

	schema := arrow.NewSchema(fields, nil)

	return schema, builders, nil
}

func primitiveTypeToArrow(typeID Ydb.Type_PrimitiveTypeId, column *Ydb.Column, arrowAllocator memory.Allocator) (arrow.Field, array.Builder, error) {
	var (
		field   arrow.Field
		builder array.Builder
	)

	switch typeID {
	case Ydb.Type_BOOL:
		// NOTE: for some reason YDB bool type is mapped to Arrow uint8
		// https://st.yandex-team.ru/YQL-15332
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Uint8}
		builder = array.NewUint8Builder(arrowAllocator)
	case Ydb.Type_INT8:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Int8}
		builder = array.NewInt8Builder(arrowAllocator)
	case Ydb.Type_UINT8:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Uint8}
		builder = array.NewUint8Builder(arrowAllocator)
	case Ydb.Type_INT16:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Int16}
		builder = array.NewInt16Builder(arrowAllocator)
	case Ydb.Type_UINT16:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Uint16}
		builder = array.NewUint16Builder(arrowAllocator)
	case Ydb.Type_INT32:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Int32}
		builder = array.NewInt32Builder(arrowAllocator)
	case Ydb.Type_UINT32:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Uint32}
		builder = array.NewUint32Builder(arrowAllocator)
	case Ydb.Type_INT64:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Int64}
		builder = array.NewInt64Builder(arrowAllocator)
	case Ydb.Type_UINT64:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Uint64}
		builder = array.NewUint64Builder(arrowAllocator)
	case Ydb.Type_FLOAT:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Float32}
		builder = array.NewFloat32Builder(arrowAllocator)
	case Ydb.Type_DOUBLE:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Float64}
		builder = array.NewFloat64Builder(arrowAllocator)
	case Ydb.Type_STRING:
		// TODO: what about LargeBinary?
		// https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv4N5arrow4Type4type12LARGE_BINARYE
		field = arrow.Field{Name: column.Name, Type: arrow.BinaryTypes.Binary}
		builder = array.NewBinaryBuilder(arrowAllocator, arrow.BinaryTypes.Binary)
	case Ydb.Type_UTF8:
		// TODO: what about LargeString?
		// https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv4N5arrow4Type4type12LARGE_STRINGE
		field = arrow.Field{Name: column.Name, Type: arrow.BinaryTypes.String}
		builder = array.NewStringBuilder(arrowAllocator)
	case Ydb.Type_DATE:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Uint16}
		builder = array.NewUint16Builder(arrowAllocator)
	case Ydb.Type_DATETIME:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Uint32}
		builder = array.NewUint32Builder(arrowAllocator)
	case Ydb.Type_TIMESTAMP:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Uint64}
		builder = array.NewUint64Builder(arrowAllocator)
	default:
		return arrow.Field{}, nil, fmt.Errorf("register type '%v': %w", typeID, ErrDataTypeNotSupported)
	}
	return field, builder, nil
}
