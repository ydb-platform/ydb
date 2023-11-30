package clickhouse

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

var _ utils.TypeMapper = typeMapper{}

type typeMapper struct {
	isFixedString *regexp.Regexp
	isDateTime    *regexp.Regexp
	isDateTime64  *regexp.Regexp
	isNullable    *regexp.Regexp
}

func (tm typeMapper) SQLTypeToYDBColumn(columnName, typeName string, rules *api_service_protos.TTypeMappingSettings) (*Ydb.Column, error) {
	var (
		ydbType *Ydb.Type
		err     error
	)

	// By default all columns in CH are non-nullable, so
	// we wrap YDB types into Optional type only in such cases:
	//
	// 1. The column is explicitly defined as nullable;
	// 2. The column type is a date/time. CH value ranges for date/time are much wider than YQL value ranges,
	// so every time we encounter a value that is out of YQL ranges, we have to return NULL.
	nullable := false

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
		var overflow bool
		ydbType, overflow, err = makeYdbDateTimeType(Ydb.Type_DATE, rules.GetDateTimeFormat())
		nullable = overflow || nullable
	case typeName == "Date32":
		// NOTE: ClickHouse's Date32 value range is much more wide than YDB's Date value range
		var overflow bool
		ydbType, overflow, err = makeYdbDateTimeType(Ydb.Type_DATE, rules.GetDateTimeFormat())
		nullable = overflow || nullable
	case tm.isDateTime64.MatchString(typeName):
		// NOTE: ClickHouse's DateTime64 value range is much more wide than YDB's Timestamp value range
		var overflow bool
		ydbType, overflow, err = makeYdbDateTimeType(Ydb.Type_TIMESTAMP, rules.GetDateTimeFormat())
		nullable = overflow || nullable
	case tm.isDateTime.MatchString(typeName):
		var overflow bool
		ydbType, overflow, err = makeYdbDateTimeType(Ydb.Type_DATETIME, rules.GetDateTimeFormat())
		nullable = overflow || nullable
	default:
		err = fmt.Errorf("convert type '%s': %w", typeName, utils.ErrDataTypeNotSupported)
	}

	if err != nil {
		return nil, err
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

func makeYdbDateTimeType(ydbTypeID Ydb.Type_PrimitiveTypeId, format api_service_protos.EDateTimeFormat) (*Ydb.Type, bool, error) {
	switch format {
	case api_service_protos.EDateTimeFormat_YQL_FORMAT:
		// type marked as nullable because ClickHouse's type value range is much more wide than YDB's type value range
		return &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: ydbTypeID}}, true, nil
	case api_service_protos.EDateTimeFormat_STRING_FORMAT:
		return &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UTF8}}, false, nil
	default:
		return nil, false, fmt.Errorf("unexpected datetime format '%s': %w", format, utils.ErrDataTypeNotSupported)
	}
}

func acceptorsFromSQLTypes(typeNames []string) ([]any, error) {
	acceptors := make([]any, 0, len(typeNames))
	isNullable := regexp.MustCompile(`Nullable\((?P<Internal>[\w\(\)]+)\)`)
	isFixedString := regexp.MustCompile(`FixedString\([0-9]+\)`)
	isDateTime := regexp.MustCompile(`DateTime(\('[\w,/]+'\))?`)
	isDateTime64 := regexp.MustCompile(`DateTime64\(\d{1}(, '[\w,/]+')?\)`)

	for _, typeName := range typeNames {
		if matches := isNullable.FindStringSubmatch(typeName); len(matches) > 0 {
			typeName = matches[1]
		}

		switch {
		case typeName == "Bool":
			acceptors = append(acceptors, new(*bool))
		case typeName == "Int8":
			acceptors = append(acceptors, new(*int8))
		case typeName == "Int16":
			acceptors = append(acceptors, new(*int16))
		case typeName == "Int32":
			acceptors = append(acceptors, new(*int32))
		case typeName == "Int64":
			acceptors = append(acceptors, new(*int64))
		case typeName == "UInt8":
			acceptors = append(acceptors, new(*uint8))
		case typeName == "UInt16":
			acceptors = append(acceptors, new(*uint16))
		case typeName == "UInt32":
			acceptors = append(acceptors, new(*uint32))
		case typeName == "UInt64":
			acceptors = append(acceptors, new(*uint64))
		case typeName == "Float32":
			acceptors = append(acceptors, new(*float32))
		case typeName == "Float64":
			acceptors = append(acceptors, new(*float64))
		case typeName == "String", isFixedString.MatchString(typeName):
			// Looks like []byte would be a better choice here, but clickhouse driver prefers string
			acceptors = append(acceptors, new(*string))
		case typeName == "Date":
			acceptors = append(acceptors, new(*utils.Date))
		case typeName == "Date32":
			acceptors = append(acceptors, new(*utils.Date))
		case isDateTime64.MatchString(typeName):
			acceptors = append(acceptors, new(*utils.Timestamp))
		case isDateTime.MatchString(typeName):
			acceptors = append(acceptors, new(*utils.Datetime))
		default:
			return nil, fmt.Errorf("unknown type '%v'", typeName)
		}
	}

	return acceptors, nil
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
) error {
	cast := acceptor.(**IN)

	if *cast == nil {
		builder.AppendNull()

		return nil
	}

	value := **cast

	var converter CONV

	out, err := converter.Convert(value)
	if err != nil {
		if errors.Is(err, utils.ErrValueOutOfTypeBounds) {
			// TODO: write warning to logger
			builder.AppendNull()

			return nil
		}

		return fmt.Errorf("convert value %v: %w", value, err)
	}

	//nolint:forcetypeassert
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
		err = appendValueToArrowBuilder[bool, uint8, *array.Uint8Builder, utils.BoolConverter](acceptor, builder)
	case Ydb.Type_INT8:
		err = appendValueToArrowBuilder[int8, int8, *array.Int8Builder, utils.Int8Converter](acceptor, builder)
	case Ydb.Type_INT16:
		err = appendValueToArrowBuilder[int16, int16, *array.Int16Builder, utils.Int16Converter](acceptor, builder)
	case Ydb.Type_INT32:
		err = appendValueToArrowBuilder[int32, int32, *array.Int32Builder, utils.Int32Converter](acceptor, builder)
	case Ydb.Type_INT64:
		err = appendValueToArrowBuilder[int64, int64, *array.Int64Builder, utils.Int64Converter](acceptor, builder)
	case Ydb.Type_UINT8:
		err = appendValueToArrowBuilder[uint8, uint8, *array.Uint8Builder, utils.Uint8Converter](acceptor, builder)
	case Ydb.Type_UINT16:
		err = appendValueToArrowBuilder[uint16, uint16, *array.Uint16Builder, utils.Uint16Converter](acceptor, builder)
	case Ydb.Type_UINT32:
		err = appendValueToArrowBuilder[uint32, uint32, *array.Uint32Builder, utils.Uint32Converter](acceptor, builder)
	case Ydb.Type_UINT64:
		err = appendValueToArrowBuilder[uint64, uint64, *array.Uint64Builder, utils.Uint64Converter](acceptor, builder)
	case Ydb.Type_FLOAT:
		err = appendValueToArrowBuilder[float32, float32, *array.Float32Builder, utils.Float32Converter](acceptor, builder)
	case Ydb.Type_DOUBLE:
		err = appendValueToArrowBuilder[float64, float64, *array.Float64Builder, utils.Float64Converter](acceptor, builder)
	case Ydb.Type_STRING:
		err = appendValueToArrowBuilder[string, []byte, *array.BinaryBuilder, utils.StringToBytesConverter](acceptor, builder)
	case Ydb.Type_UTF8:
		// date/time in string representation format
		switch acceptor.(type) {
		case **utils.Date:
			err = appendValueToArrowBuilder[utils.Date, string, *array.StringBuilder, utils.DateToStringConverter](acceptor, builder)
		case **utils.Datetime:
			err = appendValueToArrowBuilder[utils.Datetime, string, *array.StringBuilder, utils.DatetimeToStringConverter](acceptor, builder)
		case **utils.Timestamp:
			err = appendValueToArrowBuilder[utils.Timestamp, string, *array.StringBuilder, utils.TimestampToStringConverter](acceptor, builder)
		default:
			return fmt.Errorf("unexpected type %v with acceptor type %T: %w", typeID, acceptor, utils.ErrDataTypeNotSupported)
		}
	case Ydb.Type_DATE:
		err = appendValueToArrowBuilder[utils.Date, uint16, *array.Uint16Builder, utils.DateConverter](acceptor, builder)
	case Ydb.Type_DATETIME:
		err = appendValueToArrowBuilder[utils.Datetime, uint32, *array.Uint32Builder, utils.DatetimeConverter](acceptor, builder)
	case Ydb.Type_TIMESTAMP:
		err = appendValueToArrowBuilder[utils.Timestamp, uint64, *array.Uint64Builder, utils.TimestampConverter](acceptor, builder)
	default:
		return fmt.Errorf("unexpected type %v: %w", typeID, utils.ErrDataTypeNotSupported)
	}

	return err
}

func NewTypeMapper() utils.TypeMapper {
	return typeMapper{
		isFixedString: regexp.MustCompile(`FixedString\([0-9]+\)`),
		isDateTime:    regexp.MustCompile(`DateTime(\('[\w,/]+'\))?`),
		isDateTime64:  regexp.MustCompile(`DateTime64\(\d{1}(, '[\w,/]+')?\)`),
		isNullable:    regexp.MustCompile(`Nullable\((?P<Internal>[\w\(\)]+)\)`),
	}
}
