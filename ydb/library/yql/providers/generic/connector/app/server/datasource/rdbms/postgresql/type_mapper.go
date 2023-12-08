package postgresql

import (
	"errors"
	"fmt"
	"time"

	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

var _ utils.TypeMapper = typeMapper{}
var _ utils.Transformer = Transformer{}

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
			ydbType = &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UTF8}}
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
			ydbType = &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UTF8}}
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

type Transformer struct {
	acceptors []any
	appenders []func(acceptor any, builder array.Builder) error
}

func (t Transformer) GetAcceptors() []any {
	return t.acceptors
}

func (t Transformer) AppendToArrowBuilders(builders []array.Builder) error {
	for i, acceptor := range t.acceptors {
		if err := t.appenders[i](acceptor, builders[i]); err != nil {
			return fmt.Errorf("append acceptor %#v of %d column to arrow builder %#v: %w", acceptor, i, builders[i], err)
		}
	}

	return nil
}

func transformerFromOIDs(oids []uint32, ydbTypes []*Ydb.Type) (utils.Transformer, error) {
	acceptors := make([]any, 0, len(oids))
	appenders := make([]func(acceptor any, builder array.Builder) error, 0, len(oids))

	for i, oid := range oids {
		switch oid {
		case pgtype.BoolOID:
			acceptors = append(acceptors, new(pgtype.Bool))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				cast := acceptor.(*pgtype.Bool)

				return appendValueToArrowBuilder[bool, uint8, *array.Uint8Builder, utils.BoolConverter](cast.Bool, builder, cast.Valid)
			})
		case pgtype.Int2OID:
			acceptors = append(acceptors, new(pgtype.Int2))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				cast := acceptor.(*pgtype.Int2)

				return appendValueToArrowBuilder[int16, int16, *array.Int16Builder, utils.Int16Converter](cast.Int16, builder, cast.Valid)
			})
		case pgtype.Int4OID:
			acceptors = append(acceptors, new(pgtype.Int4))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				cast := acceptor.(*pgtype.Int4)

				return appendValueToArrowBuilder[int32, int32, *array.Int32Builder, utils.Int32Converter](cast.Int32, builder, cast.Valid)
			})
		case pgtype.Int8OID:
			acceptors = append(acceptors, new(pgtype.Int8))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				cast := acceptor.(*pgtype.Int8)

				return appendValueToArrowBuilder[int64, int64, *array.Int64Builder, utils.Int64Converter](cast.Int64, builder, cast.Valid)
			})
		case pgtype.Float4OID:
			acceptors = append(acceptors, new(pgtype.Float4))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				cast := acceptor.(*pgtype.Float4)

				return appendValueToArrowBuilder[float32, float32, *array.Float32Builder, utils.Float32Converter](cast.Float32, builder, cast.Valid)
			})
		case pgtype.Float8OID:
			acceptors = append(acceptors, new(pgtype.Float8))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				cast := acceptor.(*pgtype.Float8)

				return appendValueToArrowBuilder[float64, float64, *array.Float64Builder, utils.Float64Converter](cast.Float64, builder, cast.Valid)
			})
		case pgtype.TextOID, pgtype.BPCharOID, pgtype.VarcharOID:
			acceptors = append(acceptors, new(pgtype.Text))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				cast := acceptor.(*pgtype.Text)

				return appendValueToArrowBuilder[string, string, *array.StringBuilder, utils.StringConverter](cast.String, builder, cast.Valid)
			})
		case pgtype.ByteaOID:
			acceptors = append(acceptors, new(*[]byte))
			appenders = append(appenders, func(acceptor any, builder array.Builder) error {
				// TODO: Bytea exists in the upstream library, but missing in jackx/pgx:
				// https://github.com/jackc/pgtype/blob/v1.14.0/bytea.go
				// https://github.com/jackc/pgx/blob/v5.3.1/pgtype/bytea.go
				// https://github.com/jackc/pgx/issues/1714
				cast := acceptor.(**[]byte)
				if *cast != nil {
					builder.(*array.BinaryBuilder).Append(**cast)
				} else {
					builder.(*array.BinaryBuilder).AppendNull()
				}

				return nil
			})
		case pgtype.DateOID:
			acceptors = append(acceptors, new(pgtype.Date))

			ydbTypeID, err := utils.YdbTypeToYdbPrimitiveTypeID(ydbTypes[i])
			if err != nil {
				return nil, fmt.Errorf("ydb type to ydb primitive type id: %w", err)
			}

			switch ydbTypeID {
			case Ydb.Type_UTF8:
				appenders = append(appenders, func(acceptor any, builder array.Builder) error {
					cast := acceptor.(*pgtype.Date)

					return appendValueToArrowBuilder[time.Time, string, *array.StringBuilder, utils.DateToStringConverter](cast.Time, builder, cast.Valid)
				})
			case Ydb.Type_DATE:
				appenders = append(appenders, func(acceptor any, builder array.Builder) error {
					cast := acceptor.(*pgtype.Date)

					return appendValueToArrowBuilder[time.Time, uint16, *array.Uint16Builder, utils.DateConverter](cast.Time, builder, cast.Valid)
				})
			default:
				return nil, fmt.Errorf("unexpected ydb type %v with type oid %d: %w", ydbTypes[i], oid, utils.ErrDataTypeNotSupported)
			}
		case pgtype.TimestampOID:
			acceptors = append(acceptors, new(pgtype.Timestamp))

			ydbTypeID, err := utils.YdbTypeToYdbPrimitiveTypeID(ydbTypes[i])
			if err != nil {
				return nil, fmt.Errorf("ydb type to ydb primitive type id: %w", err)
			}

			switch ydbTypeID {
			case Ydb.Type_UTF8:
				appenders = append(appenders, func(acceptor any, builder array.Builder) error {
					cast := acceptor.(*pgtype.Timestamp)

					return appendValueToArrowBuilder[time.Time, string, *array.StringBuilder, utils.TimestampToStringConverter](cast.Time, builder, cast.Valid)
				})
			case Ydb.Type_TIMESTAMP:
				appenders = append(appenders, func(acceptor any, builder array.Builder) error {
					cast := acceptor.(*pgtype.Timestamp)

					return appendValueToArrowBuilder[time.Time, uint64, *array.Uint64Builder, utils.TimestampConverter](cast.Time, builder, cast.Valid)
				})
			default:
				return nil, fmt.Errorf("unexpected ydb type %v with type oid %d: %w", ydbTypes[i], oid, utils.ErrDataTypeNotSupported)
			}
		default:
			return nil, fmt.Errorf("convert type OID %d: %w", oid, utils.ErrDataTypeNotSupported)
		}
	}

	return Transformer{acceptors: acceptors, appenders: appenders}, nil
}

func appendValueToArrowBuilder[IN utils.ValueType, OUT utils.ValueType, AB utils.ArrowBuilder[OUT], CONV utils.ValueConverter[IN, OUT]](
	value any,
	builder array.Builder,
	valid bool,
) error {
	if !valid {
		builder.AppendNull()

		return nil
	}

	cast := value.(IN)

	var converter CONV

	out, err := converter.Convert(cast)
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

func NewTypeMapper() utils.TypeMapper { return typeMapper{} }
