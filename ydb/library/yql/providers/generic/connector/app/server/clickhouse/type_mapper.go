package clickhouse

import (
	"fmt"
	"regexp"
	"time"

	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
)

var _ utils.TypeMapper = typeMapper{}

type typeMapper struct {
	isFixedString *regexp.Regexp
	isDateTime64  *regexp.Regexp
	isNullable    *regexp.Regexp
}

func (tm typeMapper) SQLTypeToYDBColumn(columnName, typeName string) (*Ydb.Column, error) {
	var (
		ydbType  *Ydb.Type
		nullable bool
	)

	if matches := tm.isNullable.FindStringSubmatch(typeName); len(matches) > 0 {
		nullable = true
		typeName = matches[1]
	}

	// Reference table: https://wiki.yandex-team.ru/rtmapreduce/yql-streams-corner/connectors/lld-02-tipy-dannyx/
	switch {
	case typeName == "Bool":
		ydbType = &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_BOOL}}
	case typeName == "Int8":
		ydbType = &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT8}}
	case typeName == "UInt8":
		ydbType = &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT8}}
	case typeName == "Int16":
		ydbType = &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT16}}
	case typeName == "UInt16":
		ydbType = &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT16}}
	case typeName == "Int32":
		ydbType = &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT32}}
	case typeName == "UInt32":
		ydbType = &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT32}}
	case typeName == "Int64":
		ydbType = &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT64}}
	case typeName == "UInt64":
		ydbType = &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT64}}
	case typeName == "Float32":
		ydbType = &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_FLOAT}}
	case typeName == "Float64":
		ydbType = &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_DOUBLE}}
	// String/FixedString are binary in ClickHouse, so we map it to YDB's String instead of UTF8:
	// https://ydb.tech/en/docs/yql/reference/types/primitive#string
	// https://clickhouse.com/docs/en/sql-reference/data-types/string#encodings
	case typeName == "String":
		ydbType = &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_STRING}}
	case tm.isFixedString.MatchString(typeName):
		ydbType = &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_STRING}}
	case typeName == "Date":
		ydbType = &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_DATE}}
	// FIXME: https://st.yandex-team.ru/YQ-2295
	// Date32 is not displayed correctly.
	// case typeName == "Date32":
	// 	ydbType = &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_DATE}}
	case typeName == "DateTime":
		ydbType = &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_DATETIME}}
	case tm.isDateTime64.MatchString(typeName):
		// NOTE: ClickHouse's DateTime64 value range is much more wide than YDB's Timestamp value range
		ydbType = &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_TIMESTAMP}}
	default:
		return nil, fmt.Errorf("convert type '%s': %w", typeName, utils.ErrDataTypeNotSupported)
	}

	// If the column is nullable, wrap it into YQL's optional
	if nullable {
		ydbType = &Ydb.Type{Type: &Ydb.Type_OptionalType{OptionalType: &Ydb.OptionalType{Item: ydbType}}}
	}

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
		acceptor, err = acceptorFromPrimitiveYDBType(t.TypeId, false)
		if err != nil {
			return nil, fmt.Errorf("make acceptor from primitive YDB type: %w", err)
		}
	case *Ydb.Type_OptionalType:
		acceptor, err = acceptorFromPrimitiveYDBType(t.OptionalType.Item.GetTypeId(), true)
		if err != nil {
			return nil, fmt.Errorf("make acceptor from optional YDB type: %w", err)
		}
	default:
		return nil, fmt.Errorf("only primitive types are supported, got '%v' instead", ydbType)
	}

	return acceptor, nil
}

func allocatePrimitiveAcceptor[VT utils.ValueType](optional bool) any {
	if !optional {
		return new(VT)
	} else {
		return new(*VT)
	}
}

func acceptorFromPrimitiveYDBType(typeID Ydb.Type_PrimitiveTypeId, optional bool) (any, error) {
	switch typeID {
	case Ydb.Type_BOOL:
		return allocatePrimitiveAcceptor[bool](optional), nil
	case Ydb.Type_INT8:
		return allocatePrimitiveAcceptor[int8](optional), nil
	case Ydb.Type_INT16:
		return allocatePrimitiveAcceptor[int16](optional), nil
	case Ydb.Type_INT32:
		return allocatePrimitiveAcceptor[int32](optional), nil
	case Ydb.Type_INT64:
		return allocatePrimitiveAcceptor[int64](optional), nil
	case Ydb.Type_UINT8:
		return allocatePrimitiveAcceptor[uint8](optional), nil
	case Ydb.Type_UINT16:
		return allocatePrimitiveAcceptor[uint16](optional), nil
	case Ydb.Type_UINT32:
		return allocatePrimitiveAcceptor[uint32](optional), nil
	case Ydb.Type_UINT64:
		return allocatePrimitiveAcceptor[uint64](optional), nil
	case Ydb.Type_FLOAT:
		return allocatePrimitiveAcceptor[float32](optional), nil
	case Ydb.Type_DOUBLE:
		return allocatePrimitiveAcceptor[float64](optional), nil
	case Ydb.Type_STRING:
		// Looks like []byte would be a better choice here, but clickhouse driver prefers string
		return allocatePrimitiveAcceptor[string](optional), nil
	case Ydb.Type_DATE, Ydb.Type_DATETIME, Ydb.Type_TIMESTAMP:
		return allocatePrimitiveAcceptor[time.Time](optional), nil
	default:
		return nil, fmt.Errorf("unknown type '%v'", typeID)
	}
}

// AddRow saves a row obtained from the datasource into the buffer
func (tm typeMapper) AddRowToArrowIPCStreaming(ydbTypes []*Ydb.Type, acceptors []any, builders []array.Builder) error {
	if len(builders) != len(acceptors) {
		return fmt.Errorf("builders vs acceptors mismatch: %v %v", len(builders), len(acceptors))
	}

	if len(ydbTypes) != len(acceptors) {
		return fmt.Errorf("ydbtypes vs acceptors mismatch: %v %v", len(ydbTypes), len(acceptors))
	}

	for i, ydbType := range ydbTypes {

		switch t := ydbType.Type.(type) {
		case *Ydb.Type_TypeId:
			if err := tm.appendValueToBuilder(t.TypeId, acceptors[i], builders[i], false); err != nil {
				return fmt.Errorf("add primitive value: %w", err)
			}
		case *Ydb.Type_OptionalType:
			switch t.OptionalType.Item.Type.(type) {
			case *Ydb.Type_TypeId:
				if err := tm.appendValueToBuilder(t.OptionalType.Item.GetTypeId(), acceptors[i], builders[i], true); err != nil {
					return fmt.Errorf("add optional primitive value: %w", err)
				}
			default:
				return fmt.Errorf("unexpected type %v: %w", t.OptionalType.Item, utils.ErrDataTypeNotSupported)
			}
		default:
			return fmt.Errorf("unexpected type %v: %w", t, utils.ErrDataTypeNotSupported)
		}
	}

	return nil
}

func appendValueToArrowBuilder[IN utils.ValueType, OUT utils.ValueType, AB utils.ArrowBuilder[OUT], CONV utils.ValueConverter[IN, OUT]](
	acceptor any,
	builder array.Builder,
	optional bool,
) error {
	var value IN
	if optional {
		cast := acceptor.(**IN)
		if *cast == nil {
			builder.AppendNull()
			return nil
		}
		value = **cast
	} else {
		value = *acceptor.(*IN)
	}

	var converter CONV
	out, err := converter.Convert(value)
	if err != nil {
		return fmt.Errorf("convert value %v: %w", value, err)
	}

	builder.(AB).Append(out)
	return nil
}

func (typeMapper) appendValueToBuilder(
	typeID Ydb.Type_PrimitiveTypeId,
	acceptor any,
	builder array.Builder,
	optional bool,
) error {
	var err error
	switch typeID {
	case Ydb.Type_BOOL:
		err = appendValueToArrowBuilder[bool, uint8, *array.Uint8Builder, utils.BoolConverter](acceptor, builder, optional)
	case Ydb.Type_INT8:
		err = appendValueToArrowBuilder[int8, int8, *array.Int8Builder, utils.Int8Converter](acceptor, builder, optional)
	case Ydb.Type_INT16:
		err = appendValueToArrowBuilder[int16, int16, *array.Int16Builder, utils.Int16Converter](acceptor, builder, optional)
	case Ydb.Type_INT32:
		err = appendValueToArrowBuilder[int32, int32, *array.Int32Builder, utils.Int32Converter](acceptor, builder, optional)
	case Ydb.Type_INT64:
		err = appendValueToArrowBuilder[int64, int64, *array.Int64Builder, utils.Int64Converter](acceptor, builder, optional)
	case Ydb.Type_UINT8:
		err = appendValueToArrowBuilder[uint8, uint8, *array.Uint8Builder, utils.Uint8Converter](acceptor, builder, optional)
	case Ydb.Type_UINT16:
		err = appendValueToArrowBuilder[uint16, uint16, *array.Uint16Builder, utils.Uint16Converter](acceptor, builder, optional)
	case Ydb.Type_UINT32:
		err = appendValueToArrowBuilder[uint32, uint32, *array.Uint32Builder, utils.Uint32Converter](acceptor, builder, optional)
	case Ydb.Type_UINT64:
		err = appendValueToArrowBuilder[uint64, uint64, *array.Uint64Builder, utils.Uint64Converter](acceptor, builder, optional)
	case Ydb.Type_FLOAT:
		err = appendValueToArrowBuilder[float32, float32, *array.Float32Builder, utils.Float32Converter](acceptor, builder, optional)
	case Ydb.Type_DOUBLE:
		err = appendValueToArrowBuilder[float64, float64, *array.Float64Builder, utils.Float64Converter](acceptor, builder, optional)
	case Ydb.Type_STRING:
		err = appendValueToArrowBuilder[string, []byte, *array.BinaryBuilder, utils.StringToBytesConverter](acceptor, builder, optional)
	case Ydb.Type_DATE:
		err = appendValueToArrowBuilder[time.Time, uint16, *array.Uint16Builder, utils.DateConverter](acceptor, builder, optional)
	case Ydb.Type_DATETIME:
		err = appendValueToArrowBuilder[time.Time, uint32, *array.Uint32Builder, utils.DatetimeConverter](acceptor, builder, optional)
	case Ydb.Type_TIMESTAMP:
		err = appendValueToArrowBuilder[time.Time, uint64, *array.Uint64Builder, utils.TimestampConverter](acceptor, builder, optional)
	default:
		return fmt.Errorf("unexpected type %v: %w", typeID, utils.ErrDataTypeNotSupported)
	}

	return err
}

func NewTypeMapper() utils.TypeMapper {
	return typeMapper{
		isFixedString: regexp.MustCompile(`FixedString\([0-9]+\)`),
		isDateTime64:  regexp.MustCompile(`DateTime64\(\d\)`),
		isNullable:    regexp.MustCompile(`Nullable\((?P<Internal>\w+)\)`),
	}
}
