package postgresql

import (
	"fmt"
	"time"

	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
)

var _ utils.TypeMapper = typeMapper{}

type typeMapper struct{}

func (tm typeMapper) SQLTypeToYDBColumn(columnName, typeName string) (*Ydb.Column, error) {
	var ydbType *Ydb.Type

	// Reference table: https://wiki.yandex-team.ru/rtmapreduce/yql-streams-corner/connectors/lld-02-tipy-dannyx/
	switch typeName {
	case "boolean", "bool":
		ydbType = &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_BOOL}}
	case "smallint", "int2", "smallserial", "serial2":
		ydbType = &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT16}}
	case "integer", "int", "int4", "serial", "serial4":
		ydbType = &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT32}}
	case "bigint", "int8", "bigserial", "serial8":
		ydbType = &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT64}}
	case "real", "float4":
		ydbType = &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_FLOAT}}
	case "double precision", "float8":
		ydbType = &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_DOUBLE}}
	case "bytea":
		ydbType = &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_STRING}}
	case "character", "character varying", "text":
		ydbType = &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UTF8}}
	case "date":
		ydbType = &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_DATE}}
	// TODO: PostgreSQL `time` data type has no direct counterparts in the YDB's type system;
	// but it can be supported when the PG-compatible types is added to YDB:
	// https://st.yandex-team.ru/YQ-2285
	// case "time":
	case "timestamp without time zone":
		ydbType = &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_TIMESTAMP}}
	default:
		return nil, fmt.Errorf("convert type '%s': %w", typeName, utils.ErrDataTypeNotSupported)
	}

	// In PostgreSQL all columns are actually nullable, hence we wrap every T in Optional<T>.
	// See this issue for details: https://st.yandex-team.ru/YQ-2256
	ydbType = &Ydb.Type{Type: &Ydb.Type_OptionalType{OptionalType: &Ydb.OptionalType{Item: ydbType}}}

	return &Ydb.Column{
		Name: columnName,
		Type: ydbType,
	}, nil
}

func (tm typeMapper) YDBTypeToAcceptor(ydbType *Ydb.Type) (any, error) {
	var (
		acceptor any
		err      error
	)

	switch t := ydbType.Type.(type) {
	// Primitive types
	case *Ydb.Type_TypeId:
		acceptor, err = acceptorFromPrimitiveYDBType(t.TypeId)
		if err != nil {
			return nil, fmt.Errorf("make acceptor from primitive YDB type: %w", err)
		}
	case *Ydb.Type_OptionalType:
		acceptor, err = acceptorFromPrimitiveYDBType(t.OptionalType.Item.GetTypeId())
		if err != nil {
			return nil, fmt.Errorf("make acceptor from optional YDB type: %w", err)
		}
	default:
		return nil, fmt.Errorf("only primitive types are supported, got '%v' instead", ydbType)
	}

	return acceptor, nil
}

func acceptorFromPrimitiveYDBType(typeID Ydb.Type_PrimitiveTypeId) (any, error) {
	switch typeID {
	case Ydb.Type_BOOL:
		return new(pgtype.Bool), nil
	case Ydb.Type_INT16:
		return new(pgtype.Int2), nil
	case Ydb.Type_INT32:
		return new(pgtype.Int4), nil
	case Ydb.Type_INT64:
		return new(pgtype.Int8), nil
	case Ydb.Type_FLOAT:
		return new(pgtype.Float4), nil
	case Ydb.Type_DOUBLE:
		return new(pgtype.Float8), nil
	case Ydb.Type_STRING:
		return new(*[]byte), nil
	case Ydb.Type_UTF8:
		return new(pgtype.Text), nil
	case Ydb.Type_DATE:
		return new(pgtype.Date), nil
	case Ydb.Type_TIMESTAMP:
		return new(pgtype.Timestamp), nil
	default:
		return nil, fmt.Errorf("make acceptor for type '%v': %w", typeID, utils.ErrDataTypeNotSupported)
	}
}

func appendValueToArrowBuilder[IN utils.ValueType, OUT utils.ValueType, AB utils.ArrowBuilder[OUT], CONV utils.ValueConverter[IN, OUT]](
	value IN,
	builder array.Builder,
	valid bool,
) error {
	if valid {
		var converter CONV
		out, err := converter.Convert(value)
		if err != nil {
			return fmt.Errorf("convert value: %w", err)
		}
		builder.(AB).Append(out)
	} else {
		builder.AppendNull()
	}
	return nil
}

// AddRow saves a row obtained from the datasource into the buffer
func (tm typeMapper) AddRowToArrowIPCStreaming(
	_ []*Ydb.Type, // TODO: use detailed YDB type information when acceptor type is not enough
	acceptors []any,
	builders []array.Builder,
) error {
	if len(builders) != len(acceptors) {
		return fmt.Errorf("expected row %v values, got %v", len(builders), len(acceptors))
	}

	for i, acceptor := range acceptors {
		var err error
		switch t := acceptor.(type) {
		case *pgtype.Bool:
			err = appendValueToArrowBuilder[bool, uint8, *array.Uint8Builder, utils.BoolConverter](t.Bool, builders[i], t.Valid)
		case *pgtype.Int2:
			err = appendValueToArrowBuilder[int16, int16, *array.Int16Builder, utils.Int16Converter](t.Int16, builders[i], t.Valid)
		case *pgtype.Int4:
			err = appendValueToArrowBuilder[int32, int32, *array.Int32Builder, utils.Int32Converter](t.Int32, builders[i], t.Valid)
		case *pgtype.Int8:
			err = appendValueToArrowBuilder[int64, int64, *array.Int64Builder, utils.Int64Converter](t.Int64, builders[i], t.Valid)
		case *pgtype.Float4:
			err = appendValueToArrowBuilder[float32, float32, *array.Float32Builder, utils.Float32Converter](t.Float32, builders[i], t.Valid)
		case *pgtype.Float8:
			err = appendValueToArrowBuilder[float64, float64, *array.Float64Builder, utils.Float64Converter](t.Float64, builders[i], t.Valid)
		case *pgtype.Text:
			err = appendValueToArrowBuilder[string, string, *array.StringBuilder, utils.StringConverter](t.String, builders[i], t.Valid)
		case **[]byte:
			// TODO: Bytea exists in the upstream library, but missing in jackx/pgx:
			// https://github.com/jackc/pgtype/blob/v1.14.0/bytea.go
			// https://github.com/jackc/pgx/blob/v5.3.1/pgtype/bytea.go
			// https://github.com/jackc/pgx/issues/1714
			if *t != nil {
				builders[i].(*array.BinaryBuilder).Append(**t)
			} else {
				builders[i].(*array.BinaryBuilder).AppendNull()
			}
		case *pgtype.Date:
			err = appendValueToArrowBuilder[time.Time, uint16, *array.Uint16Builder, utils.DateConverter](t.Time, builders[i], t.Valid)
		case *pgtype.Timestamp:
			err = appendValueToArrowBuilder[time.Time, uint64, *array.Uint64Builder, utils.TimestampConverter](t.Time, builders[i], t.Valid)
		default:
			return fmt.Errorf("item #%d of a type '%T': %w", i, t, utils.ErrDataTypeNotSupported)
		}

		if err != nil {
			return fmt.Errorf("append value to arrow builder: %w", err)
		}
	}

	return nil
}

func NewTypeMapper() utils.TypeMapper { return typeMapper{} }
