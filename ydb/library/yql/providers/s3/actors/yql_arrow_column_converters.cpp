#include "yql_arrow_column_converters.h"

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/cast.h>
#include <contrib/libs/apache/arrow/cpp/src/parquet/exception.h>

#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>
#include <ydb/library/yql/public/udf/arrow/block_builder.h>
#include <ydb/library/yql/public/udf/arrow/block_item.h>
#include <ydb/library/yql/public/udf/arrow/block_reader.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <arrow/api.h>

#ifdef THROW
#undef THROW
#endif

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"

#include <ydb/library/yql/udfs/common/clickhouse/client/src/Functions/FunctionsConversion.h>

#pragma clang diagnostic pop

namespace {

#define THROW_ARROW_NOT_OK(status)                                     \
    do                                                                 \
    {                                                                  \
        if (::arrow::Status _s = (status); !_s.ok())                   \
            throw yexception() << _s.ToString(); \
    } while (false)

using namespace NYql;
using namespace NKikimr::NMiniKQL;

ui64 GetMultiplierForTimestamp(NDB::FormatSettings::TimestampFormat format) {
    switch (format) {
        case NDB::FormatSettings::TimestampFormat::UnixTimeMilliseconds:
            return 1000;
        case NDB::FormatSettings::TimestampFormat::UnixTimeSeconds:
            return 1000000;
        case NDB::FormatSettings::TimestampFormat::UnixTimeMicroSeconds:
        case NDB::FormatSettings::TimestampFormat::ISO:
        case NDB::FormatSettings::TimestampFormat::POSIX:
        case NDB::FormatSettings::TimestampFormat::Unspecified:
            return 1;
    }
}

ui64 GetMultiplierForTimestamp(arrow::DateUnit unit) {
    switch (unit) {
        case arrow::DateUnit::MILLI:
            return 1000;
        case arrow::DateUnit::DAY:
            return 1000000 * 24 * 3600UL;
    }
}

ui32 GetMultiplierForDatetime(arrow::DateUnit unit) {
    switch (unit) {
        case arrow::DateUnit::MILLI:
            throw parquet::ParquetException(TStringBuilder() << "millisecond accuracy does not fit into the datetime");
        case arrow::DateUnit::DAY:
            return 24 * 3600UL;
    }
}

ui64 GetMultiplierForTimestamp(arrow::TimeUnit::type unit) {
    switch (unit) {
        case arrow::TimeUnit::SECOND:
            return 1000000;
        case arrow::TimeUnit::MILLI:
            return 1000;
        case arrow::TimeUnit::MICRO:
            return 1;
        case arrow::TimeUnit::NANO:
            throw parquet::ParquetException(TStringBuilder() << "nanosecond accuracy does not fit into the timestamp");
    }
}

ui32 GetMultiplierForDatetime(arrow::TimeUnit::type unit) {
    switch (unit) {
        case arrow::TimeUnit::SECOND:
            return 1;
        case arrow::TimeUnit::MILLI:
            throw parquet::ParquetException(TStringBuilder() << "millisecond accuracy does not fit into the datetime");
        case arrow::TimeUnit::MICRO:
            throw parquet::ParquetException(TStringBuilder() << "microsecond accuracy does not fit into the datetime");
        case arrow::TimeUnit::NANO:
            throw parquet::ParquetException(TStringBuilder() << "nanosecond accuracy does not fit into the datetime");
    }
}

// DateTime Converters
template <bool isOptional, typename TArrowType>
std::shared_ptr<arrow::Array> ArrowTypeAsYqlDatetime(const std::shared_ptr<arrow::DataType>& targetType, const std::shared_ptr<arrow::Array>& value, ui32 multiplier) {
    ::NYql::NUdf::TFixedSizeArrayBuilder<ui32, isOptional> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), targetType, *arrow::system_memory_pool(), value->length());
    ::NYql::NUdf::TFixedSizeBlockReader<TArrowType, isOptional> reader;
    for (i64 i = 0; i < value->length(); ++i) {
        const NUdf::TBlockItem item = reader.GetItem(*value->data(), i);
        if constexpr (isOptional) {
            if (!item) {
                builder.Add(item);
                continue;
            }
        } else if (!item) {
            throw parquet::ParquetException(TStringBuilder() << "null value for datetime could not be represented in non-optional type");
        }

        const TArrowType baseValue = item.As<TArrowType>();
        if (baseValue < 0 && baseValue > static_cast<int64_t>(::NYql::NUdf::MAX_DATETIME)) {
            throw parquet::ParquetException(TStringBuilder() << "datetime in parquet is out of range [0, " << ::NYql::NUdf::MAX_DATETIME << "]: " << baseValue);
        }

        const ui64 v = baseValue * static_cast<ui64>(multiplier);
        if (v > ::NYql::NUdf::MAX_DATETIME) {
            throw parquet::ParquetException(TStringBuilder() << "datetime in parquet is out of range [0, " << ::NYql::NUdf::MAX_DATETIME << "] after transformation: " << v);
        }
        builder.Add(NUdf::TBlockItem(static_cast<ui32>(v)));
    }
    return builder.Build(true).make_array();
}

template <bool isOptional>
std::shared_ptr<arrow::Array> ArrowStringAsYqlDateTime(const std::shared_ptr<arrow::DataType>& targetType, const std::shared_ptr<arrow::Array>& value, const NDB::FormatSettings& formatSettings) {
    ::NYql::NUdf::TFixedSizeArrayBuilder<ui32, isOptional> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), targetType, *arrow::system_memory_pool(), value->length());
    ::NYql::NUdf::TStringBlockReader<arrow::BinaryType, isOptional, NKikimr::NUdf::EDataSlot::String> reader;
    for (i64 i = 0; i < value->length(); ++i) {
        NUdf::TBlockItem item = reader.GetItem(*value->data(), i);

        if constexpr (isOptional) {
            if (!item) {
                builder.Add(item);
                continue;
            }
        } else if (!item) {
            throw parquet::ParquetException(TStringBuilder() << "null value for date could not be represented in non-optional type");
        }

        auto ref = item.AsStringRef();
        NDB::ReadBufferFromMemory rb{ref.Data(), ref.Size()};
        uint32_t result = 0;
        parseImpl<NDB::DataTypeDateTime>(result, rb, nullptr, formatSettings);
        builder.Add(NUdf::TBlockItem(static_cast<ui32>(result)));
    }
    return builder.Build(true).make_array();
}

template <bool isOptional, typename TArrowType>
std::shared_ptr<arrow::Array> ArrowTypeAsYqlTimestamp(const std::shared_ptr<arrow::DataType>& targetType, const std::shared_ptr<arrow::Array>& value, ui64 multiplier) {
    ::NYql::NUdf::TFixedSizeArrayBuilder<ui64, isOptional> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), targetType, *arrow::system_memory_pool(), value->length());
    ::NYql::NUdf::TFixedSizeBlockReader<TArrowType, isOptional> reader;
    for (i64 i = 0; i < value->length(); ++i) {
        const NUdf::TBlockItem item = reader.GetItem(*value->data(), i);
        if constexpr (isOptional) {
            if (!item) {
                builder.Add(item);
                continue;
            }
        } else if (!item) {
            throw parquet::ParquetException(TStringBuilder() << "null value for timestamp could not be represented in non-optional type");
        }

        const TArrowType baseValue = item.As<TArrowType>();
        if (baseValue < 0 && baseValue > static_cast<i64>(::NYql::NUdf::MAX_TIMESTAMP)) {
            throw parquet::ParquetException(TStringBuilder() << "timestamp in parquet is out of range [0, " << ::NYql::NUdf::MAX_TIMESTAMP << "]: " << baseValue);
        }

        if (static_cast<ui64>(baseValue) > ::NYql::NUdf::MAX_TIMESTAMP / multiplier) {
            throw parquet::ParquetException(TStringBuilder() << "timestamp in parquet is out of range [0, " << ::NYql::NUdf::MAX_TIMESTAMP << "] after transformation: " << baseValue);
        }

        const ui64 v = baseValue * multiplier;
        builder.Add(NUdf::TBlockItem(static_cast<ui64>(v)));
    }
    return builder.Build(true).make_array();
}

template <bool isOptional, typename TArrowType>
std::shared_ptr<arrow::Array> ArrowTypeAsYqlString(const std::shared_ptr<arrow::DataType>& targetType, const std::shared_ptr<arrow::Array>& value, ui64 multiplier, const TString& format = {}) {
    ::NYql::NUdf::TStringArrayBuilder<arrow::BinaryType, isOptional> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), targetType, *arrow::system_memory_pool(), value->length());
    ::NYql::NUdf::TFixedSizeBlockReader<TArrowType, isOptional> reader;
    for (i64 i = 0; i < value->length(); ++i) {
        const NUdf::TBlockItem item = reader.GetItem(*value->data(), i);
        if constexpr (isOptional) {
            if (!item) {
                builder.Add(item);
                continue;
            }
        } else if (!item) {
            throw parquet::ParquetException(TStringBuilder() << "null value for timestamp could not be represented in non-optional type");
        }

        const TArrowType baseValue = item.As<TArrowType>();
        if (baseValue < 0 && baseValue > static_cast<i64>(::NYql::NUdf::MAX_TIMESTAMP)) {
            throw parquet::ParquetException(TStringBuilder() << "timestamp in parquet is out of range [0, " << ::NYql::NUdf::MAX_TIMESTAMP << "]: " << baseValue);
        }

        if (static_cast<ui64>(baseValue) > ::NYql::NUdf::MAX_TIMESTAMP / multiplier) {
            throw parquet::ParquetException(TStringBuilder() << "timestamp in parquet is out of range [0, " << ::NYql::NUdf::MAX_TIMESTAMP << "] after transformation: " << baseValue);
        }

        const ui64 v = baseValue * multiplier;
        TString result = format ? TInstant::FromValue(v).FormatGmTime(format.c_str()) : TInstant::FromValue(v).ToString();
        builder.Add(NUdf::TBlockItem(NUdf::TStringRef(result.c_str(), result.Size())));
    }
    return builder.Build(true).make_array();
}

template <bool isOptional>
std::shared_ptr<arrow::Array> ArrowStringAsYqlTimestamp(const std::shared_ptr<arrow::DataType>& targetType, const std::shared_ptr<arrow::Array>& value, const NDB::FormatSettings& formatSettings) {
    ::NYql::NUdf::TFixedSizeArrayBuilder<ui64, isOptional> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), targetType, *arrow::system_memory_pool(), value->length());
    ::NYql::NUdf::TStringBlockReader<arrow::BinaryType, isOptional, NKikimr::NUdf::EDataSlot::String> reader;
    for (i64 i = 0; i < value->length(); ++i) {
        NUdf::TBlockItem item = reader.GetItem(*value->data(), i);

        if constexpr (isOptional) {
            if (!item) {
                builder.Add(item);
                continue;
            }
        } else if (!item) {
            throw parquet::ParquetException(TStringBuilder() << "null value for date could not be represented in non-optional type");
        }

        auto ref = item.AsStringRef();
        NDB::ReadBufferFromMemory rb{ref.Data(), ref.Size()};
        NDB::DateTime64 result = 0;
        readTextTimestamp64(result, 0, rb, DateLUT::instance(), formatSettings);
        builder.Add(NUdf::TBlockItem(static_cast<ui64>(result)));
    }
    return builder.Build(true).make_array();
}

template <bool isOptional>
std::shared_ptr<arrow::Array> ArrowDate32AsYqlDate(const std::shared_ptr<arrow::DataType>& targetType, const std::shared_ptr<arrow::Array>& value) {
    ::NYql::NUdf::TFixedSizeArrayBuilder<ui16, isOptional> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), targetType, *arrow::system_memory_pool(), value->length());
    ::NYql::NUdf::TFixedSizeBlockReader<i32, isOptional> reader;
    for (i64 i = 0; i < value->length(); ++i) {
        const NUdf::TBlockItem item = reader.GetItem(*value->data(), i);
        if constexpr (isOptional) {
            if (!item) {
                builder.Add(item);
                continue;
            }
        } else if (!item) {
            throw parquet::ParquetException(TStringBuilder() << "null value for date could not be represented in non-optional type");
        }

        const i32 v = item.As<i32>();
        if (v < 0 || v > ::NYql::NUdf::MAX_DATE) {
            throw parquet::ParquetException(TStringBuilder() << "date in parquet is out of range [0, " << ::NYql::NUdf::MAX_DATE << "]: " << v);
        }
        builder.Add(NUdf::TBlockItem(static_cast<ui16>(v)));
    }
    return builder.Build(true).make_array();
}

TColumnConverter ArrowUInt32AsYqlDatetime(const std::shared_ptr<arrow::DataType>& targetType, bool isOptional) {
    return [targetType, isOptional](const std::shared_ptr<arrow::Array>& value) {
        return isOptional
                ? ArrowTypeAsYqlDatetime<true, ui32>(targetType, value, 1)
                : ArrowTypeAsYqlDatetime<false, ui32>(targetType, value, 1);
    };
}

TColumnConverter ArrowInt64AsYqlDatetime(const std::shared_ptr<arrow::DataType>& targetType, bool isOptional) {
    return [targetType, isOptional](const std::shared_ptr<arrow::Array>& value) {
        return isOptional
                ? ArrowTypeAsYqlDatetime<true, i64>(targetType, value, 1)
                : ArrowTypeAsYqlDatetime<false, i64>(targetType, value, 1);
    };
}

TColumnConverter ArrowUInt16AsYqlDatetime(const std::shared_ptr<arrow::DataType>& targetType, bool isOptional) {
    return [targetType, isOptional](const std::shared_ptr<arrow::Array>& value) {
        return isOptional
                ? ArrowTypeAsYqlDatetime<true, ui16>(targetType, value, 24*3600)
                : ArrowTypeAsYqlDatetime<false, ui16>(targetType, value, 24*3600);
    };
}

TColumnConverter ArrowUInt64AsYqlDatetime(const std::shared_ptr<arrow::DataType>& targetType, bool isOptional) {
    return [targetType, isOptional](const std::shared_ptr<arrow::Array>& value) {
        return isOptional
                ? ArrowTypeAsYqlDatetime<true, ui64>(targetType, value, 1)
                : ArrowTypeAsYqlDatetime<false, ui64>(targetType, value, 1);
    };
}

TColumnConverter ArrowDate64AsYqlDatetime(const std::shared_ptr<arrow::DataType>& targetType, bool isOptional, arrow::DateUnit dateUnit) {
    return [targetType, isOptional, multiplier=GetMultiplierForDatetime(dateUnit)](const std::shared_ptr<arrow::Array>& value) {
        return isOptional
                ? ArrowTypeAsYqlDatetime<true, i64>(targetType, value, multiplier)
                : ArrowTypeAsYqlDatetime<false, i64>(targetType, value, multiplier);
    };
}

TColumnConverter ArrowTimestampAsYqlDatetime(const std::shared_ptr<arrow::DataType>& targetType, bool isOptional, arrow::TimeUnit::type timeUnit) {
    return [targetType, isOptional, multiplier = GetMultiplierForDatetime(timeUnit)](const std::shared_ptr<arrow::Array>& value) {
        return isOptional
                ? ArrowTypeAsYqlDatetime<true, i64>(targetType, value, multiplier)
                : ArrowTypeAsYqlDatetime<false, i64>(targetType, value, multiplier);
    };
}

TColumnConverter ArrowStringAsYqlDateTime(const std::shared_ptr<arrow::DataType>& targetType, bool isOptional, const NDB::FormatSettings& formatSettings) {
    return [targetType, isOptional, formatSettings](const std::shared_ptr<arrow::Array>& value) {
        return isOptional
            ? ArrowStringAsYqlDateTime<true>(targetType, value, formatSettings)
            : ArrowStringAsYqlDateTime<false>(targetType, value, formatSettings);
    };
}

TColumnConverter ArrowInt32AsYqlDatetime(const std::shared_ptr<arrow::DataType>& targetType, bool isOptional) {
    return [targetType, isOptional](const std::shared_ptr<arrow::Array>& value) {
        return isOptional
                ? ArrowTypeAsYqlDatetime<true, i32>(targetType, value, 1)
                : ArrowTypeAsYqlDatetime<false, i32>(targetType, value, 1);
    };
}

TColumnConverter ArrowDate32AsYqlDatetime(const std::shared_ptr<arrow::DataType>& targetType, bool isOptional, arrow::DateUnit dateUnit) {
    return [targetType, isOptional, multiplier=GetMultiplierForDatetime(dateUnit)](const std::shared_ptr<arrow::Array>& value) {
        return isOptional
                ? ArrowTypeAsYqlDatetime<true, i32>(targetType, value, multiplier)
                : ArrowTypeAsYqlDatetime<false, i32>(targetType, value, multiplier);
    };
}

TColumnConverter ArrowStringAsYqlTimestamp(const std::shared_ptr<arrow::DataType>& targetType, bool isOptional, const NDB::FormatSettings& formatSettings) {
    return [targetType, isOptional, formatSettings](const std::shared_ptr<arrow::Array>& value) {
        return isOptional
                ? ArrowStringAsYqlTimestamp<true>(targetType, value, formatSettings)
                : ArrowStringAsYqlTimestamp<false>(targetType, value, formatSettings);
    };
}

TColumnConverter ArrowDate64AsYqlTimestamp(const std::shared_ptr<arrow::DataType>& targetType, bool isOptional, arrow::DateUnit dateUnit) {
    return [targetType, isOptional, multiplier=GetMultiplierForDatetime(dateUnit)](const std::shared_ptr<arrow::Array>& value) {
        return isOptional
                ? ArrowTypeAsYqlTimestamp<true, i64>(targetType, value, multiplier)
                : ArrowTypeAsYqlTimestamp<false, i64>(targetType, value, multiplier);
    };
}

TColumnConverter ArrowDate32AsYqlTimestamp(const std::shared_ptr<arrow::DataType>& targetType, bool isOptional, arrow::DateUnit dateUnit) {
    return [targetType, isOptional, multiplier=GetMultiplierForTimestamp(dateUnit)](const std::shared_ptr<arrow::Array>& value) {
        return isOptional
                ? ArrowTypeAsYqlTimestamp<true, i32>(targetType, value, multiplier)
                : ArrowTypeAsYqlTimestamp<false, i32>(targetType, value, multiplier);
    };
}

TColumnConverter ArrowInt32AsYqlTimestamp(const std::shared_ptr<arrow::DataType>& targetType, bool isOptional, NDB::FormatSettings::TimestampFormat timestampFormat) {
    return [targetType, isOptional, multiplier = GetMultiplierForTimestamp(timestampFormat)](const std::shared_ptr<arrow::Array>& value) {
        return isOptional
                ? ArrowTypeAsYqlTimestamp<true, i32>(targetType, value, multiplier)
                : ArrowTypeAsYqlTimestamp<false, i32>(targetType, value, multiplier);
    };
}

TColumnConverter ArrowInt64AsYqlTimestamp(const std::shared_ptr<arrow::DataType>& targetType, bool isOptional, NDB::FormatSettings::TimestampFormat timestampFormat) {
    return [targetType, isOptional, multiplier=GetMultiplierForTimestamp(timestampFormat)](const std::shared_ptr<arrow::Array>& value) {
        return isOptional
                ? ArrowTypeAsYqlTimestamp<true, i64>(targetType, value, multiplier)
                : ArrowTypeAsYqlTimestamp<false, i64>(targetType, value, multiplier);
    };
}

TColumnConverter ArrowUInt64AsYqlTimestamp(const std::shared_ptr<arrow::DataType>& targetType, bool isOptional, NDB::FormatSettings::TimestampFormat timestampFormat) {
    return [targetType, isOptional, multiplier=GetMultiplierForTimestamp(timestampFormat)](const std::shared_ptr<arrow::Array>& value) {
        return isOptional 
                ? ArrowTypeAsYqlTimestamp<true, ui64>(targetType, value, multiplier)
                : ArrowTypeAsYqlTimestamp<false, ui64>(targetType, value, multiplier);
    };
}

TColumnConverter ArrowUInt32AsYqlTimestamp(const std::shared_ptr<arrow::DataType>& targetType, bool isOptional, NDB::FormatSettings::TimestampFormat timestampFormat) {
    return [targetType, isOptional, multiplier=GetMultiplierForTimestamp(timestampFormat)](const std::shared_ptr<arrow::Array>& value) {
        return isOptional
                ? ArrowTypeAsYqlTimestamp<true, ui32>(targetType, value, multiplier)
                : ArrowTypeAsYqlTimestamp<false, ui32>(targetType, value, multiplier);
    };
}

TColumnConverter ArrowUInt16AsYqlTimestamp(const std::shared_ptr<arrow::DataType>& targetType, bool isOptional) {
    return [targetType, isOptional, multiplier=24*3600*1000000ULL](const std::shared_ptr<arrow::Array>& value) {
        return isOptional
                ? ArrowTypeAsYqlTimestamp<true, ui16>(targetType, value, multiplier)
                : ArrowTypeAsYqlTimestamp<false, ui16>(targetType, value, multiplier);
    };
}

TColumnConverter ArrowTimestampAsYqlTimestamp(const std::shared_ptr<arrow::DataType>& targetType, bool isOptional, arrow::TimeUnit::type timeUnit) {
    return [targetType, isOptional, multiplier=GetMultiplierForTimestamp(timeUnit)](const std::shared_ptr<arrow::Array>& value) {
        return isOptional 
                ? ArrowTypeAsYqlTimestamp<true, i64>(targetType, value, multiplier)
                : ArrowTypeAsYqlTimestamp<false, i64>(targetType, value, multiplier);
    };
}

TColumnConverter ArrowTimestampAsYqlString(const std::shared_ptr<arrow::DataType>& targetType, bool isOptional, arrow::TimeUnit::type timeUnit) {
    return [targetType, isOptional, multiplier=GetMultiplierForTimestamp(timeUnit)](const std::shared_ptr<arrow::Array>& value) {
        return isOptional
                ? ArrowTypeAsYqlString<true, i64>(targetType, value, multiplier)
                : ArrowTypeAsYqlString<false, i64>(targetType, value, multiplier);
    };
}

TColumnConverter ArrowDate64AsYqlString(const std::shared_ptr<arrow::DataType>& targetType, bool isOptional, arrow::DateUnit dateUnit) {
    return [targetType, isOptional, multiplier=GetMultiplierForDatetime(dateUnit)](const std::shared_ptr<arrow::Array>& value) {
        return isOptional
                ? ArrowTypeAsYqlString<true, i64>(targetType, value, multiplier, "%Y-%m-%d")
                : ArrowTypeAsYqlString<false, i64>(targetType, value, multiplier, "%Y-%m-%d");
    };
}

TColumnConverter ArrowDate32AsYqlString(const std::shared_ptr<arrow::DataType>& targetType, bool isOptional, arrow::DateUnit dateUnit) {
    return [targetType, isOptional, multiplier=GetMultiplierForTimestamp(dateUnit)](const std::shared_ptr<arrow::Array>& value) {
        return isOptional
                ? ArrowTypeAsYqlString<true, i32>(targetType, value, multiplier, "%Y-%m-%d")
                : ArrowTypeAsYqlString<false, i32>(targetType, value, multiplier, "%Y-%m-%d");
    };
}

TColumnConverter ArrowDate32AsYqlDate(const std::shared_ptr<arrow::DataType>& targetType, bool isOptional, arrow::DateUnit unit) {
    if (unit == arrow::DateUnit::MILLI) {
        throw parquet::ParquetException(TStringBuilder() << "millisecond accuracy does not fit into the date");
    }
    return [targetType, isOptional](const std::shared_ptr<arrow::Array>& value) {
        return isOptional 
                ? ArrowDate32AsYqlDate<true>(targetType, value)
                : ArrowDate32AsYqlDate<false>(targetType, value);
    };
}

TColumnConverter BuildCustomConverter(const std::shared_ptr<arrow::DataType>& originalType, const std::shared_ptr<arrow::DataType>& targetType, TType* yqlType, const NDB::FormatSettings& formatSettings) {
    // TODO: support more than 1 optional level
    bool isOptional = false;
    auto unpackedYqlType = UnpackOptional(yqlType, isOptional);
    if (!unpackedYqlType->IsData()) {
        return {};
    }

    auto slot = AS_TYPE(TDataType, unpackedYqlType)->GetDataSlot();
    if (!slot) {
        return {};
    }
    auto slotItem = *slot;
    switch (originalType->id()) {
        case arrow::Type::UINT16: {
            switch (slotItem) {
                case NUdf::EDataSlot::Datetime:
                    return ArrowUInt16AsYqlDatetime(targetType, isOptional);
                case NUdf::EDataSlot::Timestamp:
                    return ArrowUInt16AsYqlTimestamp(targetType, isOptional);
                default:
                    return {};
            }
        }
        case arrow::Type::INT32: {
            switch (slotItem) {
                case NUdf::EDataSlot::Datetime:
                    return ArrowInt32AsYqlDatetime(targetType, isOptional);
                case NUdf::EDataSlot::Timestamp:
                    return ArrowInt32AsYqlTimestamp(targetType, isOptional, formatSettings.timestamp_format_name);
                default:
                    return {};
            }
        }
        case arrow::Type::UINT32: {
            switch (slotItem) {
                case NUdf::EDataSlot::Datetime:
                    return ArrowUInt32AsYqlDatetime(targetType, isOptional);
                case NUdf::EDataSlot::Timestamp:
                    return ArrowUInt32AsYqlTimestamp(targetType, isOptional, formatSettings.timestamp_format_name);
                default:
                    return {};
            }
        }
        case arrow::Type::INT64: {
            switch (slotItem) {
                case NUdf::EDataSlot::Datetime:
                    return ArrowInt64AsYqlDatetime(targetType, isOptional);
                case NUdf::EDataSlot::Timestamp:
                    return ArrowInt64AsYqlTimestamp(targetType, isOptional, formatSettings.timestamp_format_name);
                default:
                    return {};
            }
        }
        case arrow::Type::UINT64: {
            switch (slotItem) {
                case NUdf::EDataSlot::Datetime:
                    return ArrowUInt64AsYqlDatetime(targetType, isOptional);
                case NUdf::EDataSlot::Timestamp:
                    return ArrowUInt64AsYqlTimestamp(targetType, isOptional, formatSettings.timestamp_format_name);
                default:
                    return {};
            }
        }
        case arrow::Type::DATE32: {
            auto& dateType = static_cast<arrow::Date32Type&>(*originalType);
            switch (slotItem) {
                case NUdf::EDataSlot::Date:
                    return ArrowDate32AsYqlDate(targetType, isOptional, dateType.unit());
                case NUdf::EDataSlot::Datetime:
                    return ArrowDate32AsYqlDatetime(targetType, isOptional, dateType.unit());
                case NUdf::EDataSlot::Timestamp:
                    return ArrowDate32AsYqlTimestamp(targetType, isOptional, dateType.unit());
                case NUdf::EDataSlot::String:
                case NUdf::EDataSlot::Utf8:
                    return ArrowDate32AsYqlString(targetType, isOptional, dateType.unit());
                default:
                    return {};
            }
            return {};
        }
        case arrow::Type::DATE64: {
            auto& dateType = static_cast<arrow::Date64Type&>(*originalType);
            switch (slotItem) {
                case NUdf::EDataSlot::Datetime:
                    return ArrowDate64AsYqlDatetime(targetType, isOptional, dateType.unit());
                case NUdf::EDataSlot::Timestamp:
                    return ArrowDate64AsYqlTimestamp(targetType, isOptional, dateType.unit());
                case NUdf::EDataSlot::String:
                case NUdf::EDataSlot::Utf8:
                    return ArrowDate64AsYqlString(targetType, isOptional, dateType.unit());
                default:
                    return {};
            }
        }
        case arrow::Type::TIMESTAMP: {
            auto& timestampType = static_cast<arrow::TimestampType&>(*originalType);
            switch (slotItem) {
                case NUdf::EDataSlot::Datetime:
                    return ArrowTimestampAsYqlDatetime(targetType, isOptional, timestampType.unit());
                case NUdf::EDataSlot::Timestamp:
                    return ArrowTimestampAsYqlTimestamp(targetType, isOptional, timestampType.unit());
                case NUdf::EDataSlot::String:
                case NUdf::EDataSlot::Utf8:
                    return ArrowTimestampAsYqlString(targetType, isOptional, timestampType.unit());
                default:
                    return {};
            }
        }
        case arrow::Type::STRING:
        case arrow::Type::BINARY: {
            switch (slotItem) {
                case NUdf::EDataSlot::Datetime:
                    return ArrowStringAsYqlDateTime(targetType, isOptional, formatSettings);
                case NUdf::EDataSlot::Timestamp:
                    return ArrowStringAsYqlTimestamp(targetType, isOptional, formatSettings);
                default:
                    return {};
            }
        }
        case arrow::Type::DECIMAL128: {
            switch (slotItem) {
                case NUdf::EDataSlot::Decimal: {
                    if (targetType->id() == arrow::Type::FIXED_SIZE_BINARY && 
                        (static_cast<arrow::FixedSizeBinaryType&>(*targetType)).byte_width() == 16
                    ) {
                        return [](const std::shared_ptr<arrow::Array>& value) {
                            auto decimals = std::static_pointer_cast<arrow::Decimal128Array>(value);
                            auto output = std::make_shared<arrow::FixedSizeBinaryArray>(arrow::fixed_size_binary(16), decimals->length(), decimals->values());
                            return output;
                        };
                    }
                    return {};
                }
                default:
                    return {};
            }
        }
        default:
            return {};
    }
}

}

namespace NYql::NDq {

TColumnConverter BuildColumnConverter(const std::string& columnName, const std::shared_ptr<arrow::DataType>& originalType, const std::shared_ptr<arrow::DataType>& targetType, TType* yqlType, const NDB::FormatSettings& formatSettings) {
    if (yqlType->IsPg()) {
        auto pgType = AS_TYPE(TPgType, yqlType);
        auto conv = BuildPgColumnConverter(originalType, pgType);
        if (!conv) {
            ythrow yexception() << "Arrow type: " << originalType->ToString() <<
                " of field: " << columnName << " isn't compatible to PG type: " << NPg::LookupType(pgType->GetTypeId()).Name;
        }

        return conv;
    }

    if (auto customConverter = BuildCustomConverter(originalType, targetType, yqlType, formatSettings); customConverter) {
        return customConverter;
    }

    if (targetType->Equals(originalType)) {
        return {};
    }

    YQL_ENSURE(arrow::compute::CanCast(*originalType, *targetType), "Mismatch type for field: " << columnName << ", expected: "
        << targetType->ToString() << ", got: " << originalType->ToString());


    return [targetType](const std::shared_ptr<arrow::Array>& value) {
        auto res = arrow::compute::Cast(*value, targetType);
        THROW_ARROW_NOT_OK(res.status());
        return std::move(res).ValueOrDie();
    };
}

void BuildColumnConverters(std::shared_ptr<arrow::Schema> outputSchema, std::shared_ptr<arrow::Schema> dataSchema,
    std::vector<int>& columnIndices, std::vector<TColumnConverter>& columnConverters,
    std::unordered_map<TStringBuf, NKikimr::NMiniKQL::TType*, THash<TStringBuf>> rowTypes, const NDB::FormatSettings& settings) {

    for (int i = 0; i < dataSchema->num_fields(); ++i) {
        switch (dataSchema->field(i)->type()->id()) {
        case arrow::Type::LIST:
            throw parquet::ParquetException(TStringBuilder() << "File contains LIST field "
                << dataSchema->field(i)->name() << " and can't be parsed");
        case arrow::Type::STRUCT:
            throw parquet::ParquetException(TStringBuilder() << "File contains STRUCT field "
                << dataSchema->field(i)->name() << " and can't be parsed");
        default:
            ;
        }
    }

    columnConverters.reserve(outputSchema->num_fields());
    for (int i = 0; i < outputSchema->num_fields(); ++i) {
        const auto& targetField = outputSchema->field(i);
        auto srcFieldIndex = dataSchema->GetFieldIndex(targetField->name());
        if (srcFieldIndex == -1) {
            throw parquet::ParquetException(TStringBuilder() << "Missing field: " << targetField->name());
        };
        auto targetType = targetField->type();
        auto originalType = dataSchema->field(srcFieldIndex)->type();
        if (originalType->layout().has_dictionary) {
            throw parquet::ParquetException(TStringBuilder() << "Unsupported dictionary encoding is used for field: "
                << targetField->name() << ", type: " << originalType->ToString());
        }
        columnIndices.push_back(srcFieldIndex);
        auto rowSpecColumnIt = rowTypes.find(targetField->name());
        YQL_ENSURE(rowSpecColumnIt != rowTypes.end(), "Column " << targetField->name() << " not found in row spec");
        columnConverters.emplace_back(BuildColumnConverter(targetField->name(), originalType, targetType, rowSpecColumnIt->second, settings));
    }
}

std::shared_ptr<arrow::RecordBatch> ConvertArrowColumns(std::shared_ptr<arrow::RecordBatch> batch, std::vector<TColumnConverter>& columnConverters) {
    auto columns = batch->columns();
    for (size_t i = 0; i < columnConverters.size(); ++i) {
        auto converter = columnConverters[i];
        if (converter) {
            columns[i] = converter(columns[i]);
        }
    }
    return arrow::RecordBatch::Make(batch->schema(), batch->num_rows(), columns);
}

} // namespace NYql::NDq
