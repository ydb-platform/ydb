package clickhouse

import (
	"errors"
	"fmt"
	"regexp"
	"time"

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

func transformerFromSQLTypes(typeNames []string, ydbTypes []*Ydb.Type) (utils.RowTransformer[any], error) {
	acceptors := make([]any, 0, len(typeNames))
	appenders := make([]func(acceptor any, builder array.Builder) error, 0, len(typeNames))
	isNullable := regexp.MustCompile(`Nullable\((?P<Internal>[\w\(\)]+)\)`)
	isFixedString := regexp.MustCompile(`FixedString\([0-9]+\)`)
	isDateTime := regexp.MustCompile(`DateTime(\('[\w,/]+'\))?`)
	isDateTime64 := regexp.MustCompile(`DateTime64\(\d{1}(, '[\w,/]+')?\)`)

	for i, typeName := range typeNames {
		if matches := isNullable.FindStringSubmatch(typeName); len(matches) > 0 {
			typeName = matches[1]
		}

		switch {
		case typeName == "Bool":
			acceptors = append(acceptors, new(*bool))
			appenders = append(appenders, appendValueToArrowBuilder[bool, uint8, *array.Uint8Builder, utils.BoolConverter])
		case typeName == "Int8":
			acceptors = append(acceptors, new(*int8))
			appenders = append(appenders, appendValueToArrowBuilder[int8, int8, *array.Int8Builder, utils.Int8Converter])
		case typeName == "Int16":
			acceptors = append(acceptors, new(*int16))
			appenders = append(appenders, appendValueToArrowBuilder[int16, int16, *array.Int16Builder, utils.Int16Converter])
		case typeName == "Int32":
			acceptors = append(acceptors, new(*int32))
			appenders = append(appenders, appendValueToArrowBuilder[int32, int32, *array.Int32Builder, utils.Int32Converter])
		case typeName == "Int64":
			acceptors = append(acceptors, new(*int64))
			appenders = append(appenders, appendValueToArrowBuilder[int64, int64, *array.Int64Builder, utils.Int64Converter])
		case typeName == "UInt8":
			acceptors = append(acceptors, new(*uint8))
			appenders = append(appenders, appendValueToArrowBuilder[uint8, uint8, *array.Uint8Builder, utils.Uint8Converter])
		case typeName == "UInt16":
			acceptors = append(acceptors, new(*uint16))
			appenders = append(appenders, appendValueToArrowBuilder[uint16, uint16, *array.Uint16Builder, utils.Uint16Converter])
		case typeName == "UInt32":
			acceptors = append(acceptors, new(*uint32))
			appenders = append(appenders, appendValueToArrowBuilder[uint32, uint32, *array.Uint32Builder, utils.Uint32Converter])
		case typeName == "UInt64":
			acceptors = append(acceptors, new(*uint64))
			appenders = append(appenders, appendValueToArrowBuilder[uint64, uint64, *array.Uint64Builder, utils.Uint64Converter])
		case typeName == "Float32":
			acceptors = append(acceptors, new(*float32))
			appenders = append(appenders, appendValueToArrowBuilder[float32, float32, *array.Float32Builder, utils.Float32Converter])
		case typeName == "Float64":
			acceptors = append(acceptors, new(*float64))
			appenders = append(appenders, appendValueToArrowBuilder[float64, float64, *array.Float64Builder, utils.Float64Converter])
		case typeName == "String", isFixedString.MatchString(typeName):
			// Looks like []byte would be a better choice here, but clickhouse driver prefers string
			acceptors = append(acceptors, new(*string))
			appenders = append(appenders, appendValueToArrowBuilder[string, []byte, *array.BinaryBuilder, utils.StringToBytesConverter])
		case typeName == "Date":
			acceptors = append(acceptors, new(*time.Time))

			ydbTypeID, err := utils.YdbTypeToYdbPrimitiveTypeID(ydbTypes[i])
			if err != nil {
				return nil, fmt.Errorf("ydb type to ydb primitive type id: %w", err)
			}

			switch ydbTypeID {
			case Ydb.Type_UTF8:
				appenders = append(appenders, appendValueToArrowBuilder[time.Time, string, *array.StringBuilder, dateToStringConverter])
			case Ydb.Type_DATE:
				appenders = append(appenders, appendValueToArrowBuilder[time.Time, uint16, *array.Uint16Builder, utils.DateConverter])
			default:
				return nil, fmt.Errorf("unexpected ydb type %v with sql type %s: %w", ydbTypes[i], typeName, utils.ErrDataTypeNotSupported)
			}
		case typeName == "Date32":
			acceptors = append(acceptors, new(*time.Time))

			ydbTypeID, err := utils.YdbTypeToYdbPrimitiveTypeID(ydbTypes[i])
			if err != nil {
				return nil, fmt.Errorf("ydb type to ydb primitive type id: %w", err)
			}

			switch ydbTypeID {
			case Ydb.Type_UTF8:
				appenders = append(appenders, appendValueToArrowBuilder[time.Time, string, *array.StringBuilder, date32ToStringConverter])
			case Ydb.Type_DATE:
				appenders = append(appenders, appendValueToArrowBuilder[time.Time, uint16, *array.Uint16Builder, utils.DateConverter])
			default:
				return nil, fmt.Errorf("unexpected ydb type %v with sql type %s: %w", ydbTypes[i], typeName, utils.ErrDataTypeNotSupported)
			}
		case isDateTime64.MatchString(typeName):
			acceptors = append(acceptors, new(*time.Time))

			ydbTypeID, err := utils.YdbTypeToYdbPrimitiveTypeID(ydbTypes[i])
			if err != nil {
				return nil, fmt.Errorf("ydb type to ydb primitive type id: %w", err)
			}

			switch ydbTypeID {
			case Ydb.Type_UTF8:
				appenders = append(appenders, appendValueToArrowBuilder[time.Time, string, *array.StringBuilder, dateTime64ToStringConverter])
			case Ydb.Type_TIMESTAMP:
				appenders = append(appenders, appendValueToArrowBuilder[time.Time, uint64, *array.Uint64Builder, utils.TimestampConverter])
			default:
				return nil, fmt.Errorf("unexpected ydb type %v with sql type %s: %w", ydbTypes[i], typeName, utils.ErrDataTypeNotSupported)
			}
		case isDateTime.MatchString(typeName):
			acceptors = append(acceptors, new(*time.Time))

			ydbTypeID, err := utils.YdbTypeToYdbPrimitiveTypeID(ydbTypes[i])
			if err != nil {
				return nil, fmt.Errorf("ydb type to ydb primitive type id: %w", err)
			}

			switch ydbTypeID {
			case Ydb.Type_UTF8:
				appenders = append(appenders, appendValueToArrowBuilder[time.Time, string, *array.StringBuilder, dateTimeToStringConverter])
			case Ydb.Type_DATETIME:
				appenders = append(appenders, appendValueToArrowBuilder[time.Time, uint32, *array.Uint32Builder, utils.DatetimeConverter])
			default:
				return nil, fmt.Errorf("unexpected ydb type %v with sql type %s: %w", ydbTypes[i], typeName, utils.ErrDataTypeNotSupported)
			}
		default:
			return nil, fmt.Errorf("unknown type '%v'", typeName)
		}
	}

	return utils.NewRowTransformer[any](acceptors, appenders, nil), nil
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

// If time value is under of type bounds ClickHouse behaviour is undefined
// See note: https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions#tostartofmonth

var (
	minClickHouseDate       = time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)
	maxClickHouseDate       = time.Date(2149, time.June, 6, 0, 0, 0, 0, time.UTC)
	minClickHouseDate32     = time.Date(1900, time.January, 1, 0, 0, 0, 0, time.UTC)
	maxClickHouseDate32     = time.Date(2299, time.December, 31, 0, 0, 0, 0, time.UTC)
	minClickHouseDatetime   = time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)
	maxClickHouseDatetime   = time.Date(2106, time.February, 7, 6, 28, 15, 0, time.UTC)
	minClickHouseDatetime64 = time.Date(1900, time.January, 1, 0, 0, 0, 0, time.UTC)
	maxClickHouseDatetime64 = time.Date(2299, time.December, 31, 23, 59, 59, 99999999, time.UTC)
)

func saturateDateTime(in, min, max time.Time) time.Time {
	if in.Before(min) {
		in = min
	}

	if in.After(max) {
		in = max
	}

	return in
}

type dateToStringConverter struct{}

func (dateToStringConverter) Convert(in time.Time) (string, error) {
	return utils.DateToStringConverter{}.Convert(saturateDateTime(in, minClickHouseDate, maxClickHouseDate))
}

type date32ToStringConverter struct{}

func (date32ToStringConverter) Convert(in time.Time) (string, error) {
	return utils.DateToStringConverter{}.Convert(saturateDateTime(in, minClickHouseDate32, maxClickHouseDate32))
}

type dateTimeToStringConverter struct{}

func (dateTimeToStringConverter) Convert(in time.Time) (string, error) {
	return utils.DatetimeToStringConverter{}.Convert(saturateDateTime(in, minClickHouseDatetime, maxClickHouseDatetime))
}

type dateTime64ToStringConverter struct{}

func (dateTime64ToStringConverter) Convert(in time.Time) (string, error) {
	return utils.TimestampToStringConverter{}.Convert(saturateDateTime(in, minClickHouseDatetime64, maxClickHouseDatetime64))
}

func NewTypeMapper() utils.TypeMapper {
	return typeMapper{
		isFixedString: regexp.MustCompile(`FixedString\([0-9]+\)`),
		isDateTime:    regexp.MustCompile(`DateTime(\('[\w,/]+'\))?`),
		isDateTime64:  regexp.MustCompile(`DateTime64\(\d{1}(, '[\w,/]+')?\)`),
		isNullable:    regexp.MustCompile(`Nullable\((?P<Internal>[\w\(\)]+)\)`),
	}
}
