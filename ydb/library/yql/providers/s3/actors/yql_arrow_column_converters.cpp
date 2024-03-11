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

TColumnConverter ArrowDate32AsYqlDate(const std::shared_ptr<arrow::DataType>& targetType, bool isOptional) {
    return [targetType, isOptional](const std::shared_ptr<arrow::Array>& value) {
        return isOptional 
                ? ArrowDate32AsYqlDate<true>(targetType, value)
                : ArrowDate32AsYqlDate<false>(targetType, value);
    };
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

TColumnConverter ArrowStringAsYqlDateTime(const std::shared_ptr<arrow::DataType>& targetType, bool isOptional, const NDB::FormatSettings& formatSettings) {
    return [targetType, isOptional, formatSettings](const std::shared_ptr<arrow::Array>& value) {
        return isOptional
            ? ArrowStringAsYqlDateTime<true>(targetType, value, formatSettings)
            : ArrowStringAsYqlDateTime<false>(targetType, value, formatSettings);
    };
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

TColumnConverter ArrowStringAsYqlTimestamp(const std::shared_ptr<arrow::DataType>& targetType, bool isOptional, const NDB::FormatSettings& formatSettings) {
    return [targetType, isOptional, formatSettings](const std::shared_ptr<arrow::Array>& value) {
        return isOptional
                ? ArrowStringAsYqlTimestamp<true>(targetType, value, formatSettings)
                : ArrowStringAsYqlTimestamp<false>(targetType, value, formatSettings);
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
        case arrow::Type::DATE32:
            switch (slotItem) {
                case NUdf::EDataSlot::Date:
                    return ArrowDate32AsYqlDate(targetType, isOptional);
                default:
                    return {};
            }
            return {};
        case arrow::Type::BINARY:
            switch (slotItem) {
                case NUdf::EDataSlot::Datetime:
                    return ArrowStringAsYqlDateTime(targetType, isOptional, formatSettings);
                case NUdf::EDataSlot::Timestamp:
                    return ArrowStringAsYqlTimestamp(targetType, isOptional, formatSettings);
                default:
                    return {};
            }
            return {};
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

} // namespace NYql::NDq
