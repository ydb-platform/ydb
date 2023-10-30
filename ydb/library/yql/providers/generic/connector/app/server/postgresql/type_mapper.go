package postgresql

import (
	"errors"
	"fmt"

	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

var _ utils.TypeMapper = typeMapper{}

type typeMapper struct{}

func (tm typeMapper) SQLTypeToYDBColumn(columnName, typeName string, rules *api_service_protos.TTypeMappingSettings) (*Ydb.Column, error) {
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
		switch rules.GetDateTimeFormat() {
		case api_service_protos.EDateTimeFormat_STRING_FORMAT:
			ydbType = &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_STRING}}
		case api_service_protos.EDateTimeFormat_YQL_FORMAT:
			ydbType = &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_DATE}}
		default:
			return nil, fmt.Errorf("unexpected date format '%s': %w", rules.GetDateTimeFormat(), utils.ErrDataTypeNotSupported)
		}
	// TODO: PostgreSQL `time` data type has no direct counterparts in the YDB's type system;
	// but it can be supported when the PG-compatible types is added to YDB:
	// https://st.yandex-team.ru/YQ-2285
	// case "time":
	case "timestamp without time zone":
		switch rules.GetDateTimeFormat() {
		case api_service_protos.EDateTimeFormat_STRING_FORMAT:
			ydbType = &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_STRING}}
		case api_service_protos.EDateTimeFormat_YQL_FORMAT:
			ydbType = &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_TIMESTAMP}}
		default:
			return nil, fmt.Errorf("unexpected timestamp format '%s': %w", rules.GetDateTimeFormat(), utils.ErrDataTypeNotSupported)
		}
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

func appendValueToArrowBuilder[IN utils.ValueType, OUT utils.ValueType, AB utils.ArrowBuilder[OUT], CONV utils.ValueConverter[IN, OUT]](
	value IN,
	builder array.Builder,
	valid bool,
) error {
	if !valid {
		builder.AppendNull()

		return nil
	}

	var converter CONV

	out, err := converter.Convert(value)
	if err != nil {
		if errors.Is(err, utils.ErrValueOutOfTypeBounds) {
			// TODO: logger ?
			builder.AppendNull()

			return nil
		}

		return fmt.Errorf("convert value: %w", err)
	}

	builder.(AB).Append(out)

	return nil
}

// AddRow saves a row obtained from the datasource into the buffer
func (tm typeMapper) AddRowToArrowIPCStreaming(
	ydbTypes []*Ydb.Type,
	acceptors []any,
	builders []array.Builder,
) error {
	if len(builders) != len(acceptors) {
		return fmt.Errorf("expected row %v values, got %v", len(builders), len(acceptors))
	}

	for i, acceptor := range acceptors {
		var ydbTypeID Ydb.Type_PrimitiveTypeId
		switch t := ydbTypes[i].Type.(type) {
		case *Ydb.Type_TypeId:
			ydbTypeID = t.TypeId
		case *Ydb.Type_OptionalType:
			switch t.OptionalType.Item.Type.(type) {
			case *Ydb.Type_TypeId:
				ydbTypeID = t.OptionalType.Item.GetTypeId()
			default:
				return fmt.Errorf("unexpected type %v: %w", t.OptionalType.Item, utils.ErrDataTypeNotSupported)
			}
		default:
			return fmt.Errorf("unexpected type %v: %w", t, utils.ErrDataTypeNotSupported)
		}

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
			switch ydbTypeID {
			case Ydb.Type_STRING:
				err = appendValueToArrowBuilder[utils.Date, string, *array.StringBuilder, utils.DateToStringConverter](utils.Date(t.Time), builders[i], t.Valid)
			case Ydb.Type_DATE:
				err = appendValueToArrowBuilder[utils.Date, uint16, *array.Uint16Builder, utils.DateConverter](utils.Date(t.Time), builders[i], t.Valid)
			default:
				return fmt.Errorf("unexpected ydb type id %d with acceptor type %T: %w", ydbTypeID, t, utils.ErrDataTypeNotSupported)
			}
		case *pgtype.Timestamp:
			switch ydbTypeID {
			case Ydb.Type_STRING:
				err = appendValueToArrowBuilder[utils.Timestamp, string, *array.StringBuilder, utils.TimestampToStringConverter](utils.Timestamp(t.Time), builders[i], t.Valid)
			case Ydb.Type_TIMESTAMP:
				err = appendValueToArrowBuilder[utils.Timestamp, uint64, *array.Uint64Builder, utils.TimestampConverter](utils.Timestamp(t.Time), builders[i], t.Valid)
			default:
				return fmt.Errorf("unexpected ydb type id %d with acceptor type %T: %w", ydbTypeID, t, utils.ErrDataTypeNotSupported)
			}
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

func acceptorFromOID(oid uint32) (any, error) {
	switch oid {
	case pgtype.BoolOID:
		return new(pgtype.Bool), nil
	case pgtype.Int2OID:
		return new(pgtype.Int2), nil
	case pgtype.Int4OID:
		return new(pgtype.Int4), nil
	case pgtype.Int8OID:
		return new(pgtype.Int8), nil
	case pgtype.Float4OID:
		return new(pgtype.Float4), nil
	case pgtype.Float8OID:
		return new(pgtype.Float8), nil
	case pgtype.TextOID, pgtype.BPCharOID, pgtype.VarcharOID, pgtype.ByteaOID:
		return new(pgtype.Text), nil
	case pgtype.DateOID:
		return new(pgtype.Date), nil
	case pgtype.TimestampOID:
		return new(pgtype.Timestamp), nil
	default:
		return nil, fmt.Errorf("convert type OID %d: %w", oid, utils.ErrDataTypeNotSupported)
	}
}
