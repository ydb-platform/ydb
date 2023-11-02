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

func SelectWhatToArrowSchema(selectWhat *api_service_protos.TSelect_TWhat) (*arrow.Schema, error) {
	fields := make([]arrow.Field, 0, len(selectWhat.Items))

	for i, item := range selectWhat.Items {
		column := item.GetColumn()
		if column == nil {
			return nil, fmt.Errorf("item #%d (%v) is not a column", i, item)
		}

		ydbType := column.GetType()

		var (
			field arrow.Field
			err   error
		)

		// Reference table: https://wiki.yandex-team.ru/rtmapreduce/yql-streams-corner/connectors/lld-02-tipy-dannyx
		switch t := ydbType.Type.(type) {
		// Primitive types
		case *Ydb.Type_TypeId:
			field, err = ydbTypeToArrowField(t.TypeId, column)
			if err != nil {
				return nil, fmt.Errorf("primitive YDB type to arrow field: %w", err)
			}
		case *Ydb.Type_OptionalType:
			field, err = ydbTypeToArrowField(t.OptionalType.Item.GetTypeId(), column)
			if err != nil {
				return nil, fmt.Errorf("optional YDB type to arrow field: %w", err)
			}
		default:
			return nil, fmt.Errorf("only primitive and optional types are supported, got '%T' instead: %w", t, ErrDataTypeNotSupported)
		}

		fields = append(fields, field)
	}

	schema := arrow.NewSchema(fields, nil)

	return schema, nil
}

func YdbTypesToArrowBuilders(ydbTypes []*Ydb.Type, arrowAllocator memory.Allocator) ([]array.Builder, error) {
	var (
		builders []array.Builder
		builder  array.Builder
		err      error
	)

	for _, ydbType := range ydbTypes {
		switch t := ydbType.Type.(type) {
		// Primitive types
		case *Ydb.Type_TypeId:
			builder, err = ydbTypeToArrowBuilder(t.TypeId, arrowAllocator)
			if err != nil {
				return nil, fmt.Errorf("primitive YDB type to Arrow builder: %w", err)
			}
		case *Ydb.Type_OptionalType:
			builder, err = ydbTypeToArrowBuilder(t.OptionalType.Item.GetTypeId(), arrowAllocator)
			if err != nil {
				return nil, fmt.Errorf("optional YDB type to Arrow builder: %w", err)
			}
		default:
			return nil, fmt.Errorf("only primitive and optional types are supported, got '%T' instead: %w", t, ErrDataTypeNotSupported)
		}

		builders = append(builders, builder)
	}

	return builders, nil
}

func ydbTypeToArrowBuilder(typeID Ydb.Type_PrimitiveTypeId, arrowAllocator memory.Allocator) (array.Builder, error) {
	var builder array.Builder

	switch typeID {
	case Ydb.Type_BOOL:
		// NOTE: for some reason YDB bool type is mapped to Arrow uint8
		// https://st.yandex-team.ru/YQL-15332
		builder = array.NewUint8Builder(arrowAllocator)
	case Ydb.Type_INT8:
		builder = array.NewInt8Builder(arrowAllocator)
	case Ydb.Type_UINT8:
		builder = array.NewUint8Builder(arrowAllocator)
	case Ydb.Type_INT16:
		builder = array.NewInt16Builder(arrowAllocator)
	case Ydb.Type_UINT16:
		builder = array.NewUint16Builder(arrowAllocator)
	case Ydb.Type_INT32:
		builder = array.NewInt32Builder(arrowAllocator)
	case Ydb.Type_UINT32:
		builder = array.NewUint32Builder(arrowAllocator)
	case Ydb.Type_INT64:
		builder = array.NewInt64Builder(arrowAllocator)
	case Ydb.Type_UINT64:
		builder = array.NewUint64Builder(arrowAllocator)
	case Ydb.Type_FLOAT:
		builder = array.NewFloat32Builder(arrowAllocator)
	case Ydb.Type_DOUBLE:
		builder = array.NewFloat64Builder(arrowAllocator)
	case Ydb.Type_STRING:
		builder = array.NewStringBuilder(arrowAllocator)
	case Ydb.Type_UTF8:
		// TODO: what about LargeString?
		// https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv4N5arrow4Type4type12LARGE_STRINGE
		builder = array.NewStringBuilder(arrowAllocator)
	case Ydb.Type_DATE:
		builder = array.NewUint16Builder(arrowAllocator)
	case Ydb.Type_DATETIME:
		builder = array.NewUint32Builder(arrowAllocator)
	case Ydb.Type_TIMESTAMP:
		builder = array.NewUint64Builder(arrowAllocator)
	default:
		return nil, fmt.Errorf("register type '%v': %w", typeID, ErrDataTypeNotSupported)
	}

	return builder, nil
}

func ydbTypeToArrowField(typeID Ydb.Type_PrimitiveTypeId, column *Ydb.Column) (arrow.Field, error) {
	var field arrow.Field

	switch typeID {
	case Ydb.Type_BOOL:
		// NOTE: for some reason YDB bool type is mapped to Arrow uint8
		// https://st.yandex-team.ru/YQL-15332
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Uint8}
	case Ydb.Type_INT8:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Int8}
	case Ydb.Type_UINT8:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Uint8}
	case Ydb.Type_INT16:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Int16}
	case Ydb.Type_UINT16:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Uint16}
	case Ydb.Type_INT32:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Int32}
	case Ydb.Type_UINT32:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Uint32}
	case Ydb.Type_INT64:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Int64}
	case Ydb.Type_UINT64:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Uint64}
	case Ydb.Type_FLOAT:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Float32}
	case Ydb.Type_DOUBLE:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Float64}
	case Ydb.Type_STRING:
		field = arrow.Field{Name: column.Name, Type: arrow.BinaryTypes.String}
	case Ydb.Type_UTF8:
		// TODO: what about LargeString?
		// https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv4N5arrow4Type4type12LARGE_STRINGE
		field = arrow.Field{Name: column.Name, Type: arrow.BinaryTypes.String}
	case Ydb.Type_DATE:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Uint16}
	case Ydb.Type_DATETIME:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Uint32}
	case Ydb.Type_TIMESTAMP:
		field = arrow.Field{Name: column.Name, Type: arrow.PrimitiveTypes.Uint64}
	default:
		return arrow.Field{}, fmt.Errorf("register type '%v': %w", typeID, ErrDataTypeNotSupported)
	}

	return field, nil
}
