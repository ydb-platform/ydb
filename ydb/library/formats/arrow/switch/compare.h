#pragma once
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/key_value_metadata.h>
#include <yql/essentials/public/decimal/yql_decimal.h>
#include <util/system/yassert.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NArrow {

class TComparator {
public:
    static constexpr const char* DecimalComparisonMetadata = "ydb.decimal_comparison";

    static bool IsDecimal(const arrow::Field& field) {
        return field.type()->id() == arrow::Type::FIXED_SIZE_BINARY && field.metadata() &&
            field.metadata()->Contains(DecimalComparisonMetadata);
    }

    static std::partial_ordering CompareDecimal(const std::string_view left, const std::string_view right) {
        NYql::NDecimal::TInt128 lhs, rhs;
        AFL_VERIFY(left.size() == sizeof(lhs) && right.size() == sizeof(rhs));
        // The stored mantissa can be unaligned (in particular in serialized PK borders).
        memcpy(&lhs, left.data(), sizeof(lhs));
        memcpy(&rhs, right.data(), sizeof(rhs));
        return lhs < rhs ? std::partial_ordering::less : rhs < lhs ? std::partial_ordering::greater : std::partial_ordering::equivalent;
    }

    template <bool notNull>
    static std::partial_ordering ConcreteTypedCompare(const arrow::Type::type typeId, const arrow::Array& lhs, const int lpos, const arrow::Array& rhs, const int rpos) {
        switch (typeId) {
            case arrow::Type::NA:
            case arrow::Type::BOOL:
                break;
            case arrow::Type::UINT8:
                return CompareView<arrow::UInt8Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::INT8:
                return CompareView<arrow::Int8Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::UINT16:
                return CompareView<arrow::UInt16Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::INT16:
                return CompareView<arrow::Int16Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::UINT32:
                return CompareView<arrow::UInt32Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::INT32:
                return CompareView<arrow::Int32Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::UINT64:
                return CompareView<arrow::UInt64Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::INT64:
                return CompareView<arrow::Int64Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::HALF_FLOAT:
                break;
            case arrow::Type::FLOAT:
                return CompareView<arrow::FloatArray, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::DOUBLE:
                return CompareView<arrow::DoubleArray, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::STRING:
                return CompareView<arrow::StringArray, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::BINARY:
                return CompareView<arrow::BinaryArray, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::FIXED_SIZE_BINARY:
                return CompareView<arrow::FixedSizeBinaryArray, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::DATE32:
                return CompareView<arrow::Date32Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::DATE64:
                return CompareView<arrow::Date64Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::TIMESTAMP:
                return CompareView<arrow::TimestampArray, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::TIME32:
                return CompareView<arrow::Time32Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::TIME64:
                return CompareView<arrow::Time64Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::DURATION:
                return CompareView<arrow::DurationArray, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::DECIMAL256:
                return CompareView<arrow::Decimal256Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::DECIMAL:
                return CompareView<arrow::Decimal128Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow::Type::DENSE_UNION:
            case arrow::Type::DICTIONARY:
            case arrow::Type::EXTENSION:
            case arrow::Type::FIXED_SIZE_LIST:
            case arrow::Type::INTERVAL_DAY_TIME:
            case arrow::Type::INTERVAL_MONTHS:
            case arrow::Type::LARGE_BINARY:
            case arrow::Type::LARGE_LIST:
            case arrow::Type::LARGE_STRING:
            case arrow::Type::LIST:
            case arrow::Type::MAP:
            case arrow::Type::MAX_ID:
            case arrow::Type::SPARSE_UNION:
            case arrow::Type::STRUCT:
                Y_ABORT("not implemented");
                break;
        }
        return std::partial_ordering::equivalent;
    }

    template <bool notNull>
    static std::partial_ordering TypedCompare(const arrow::Array& lhs, const int lpos, const arrow::Array& rhs, const int rpos,
        const bool decimal = false) {
        AFL_VERIFY(lhs.type_id() == rhs.type_id())("lhs", lhs.type()->ToString())("rhs", rhs.type()->ToString());
        if (decimal) {
            AFL_VERIFY(lhs.type_id() == arrow::Type::FIXED_SIZE_BINARY);
            if (lhs.IsNull(lpos) || rhs.IsNull(rpos)) {
                AFL_VERIFY(!notNull);
                return CompareValueNotNull(!lhs.IsNull(lpos), !rhs.IsNull(rpos));
            }
            const auto left = static_cast<const arrow::FixedSizeBinaryArray&>(lhs).GetView(lpos);
            const auto right = static_cast<const arrow::FixedSizeBinaryArray&>(rhs).GetView(rpos);
            return CompareDecimal({left.data(), left.size()}, {right.data(), right.size()});
        }
        return ConcreteTypedCompare<notNull>(lhs.type_id(), lhs, lpos, rhs, rpos);
    }

    template <bool notNull>
    static std::partial_ordering CompareRows(const std::vector<std::shared_ptr<arrow::Array>>& columns,
        const int lhs, const int rhs, const arrow::Schema* schema = nullptr) {
        for (ui32 i = 0; i < columns.size(); ++i) {
            const auto cmp = TypedCompare<notNull>(*columns[i], lhs, *columns[i], rhs, schema && IsDecimal(*schema->field(i)));
            if (cmp != std::partial_ordering::equivalent) {
                return cmp;
            }
        }
        return std::partial_ordering::equivalent;
    }

    template <typename T, bool notNull>
    static std::partial_ordering CompareView(const arrow::Array& lhs, int lpos, const arrow::Array& rhs, int rpos) {
        auto& left = static_cast<const T&>(lhs);
        auto& right = static_cast<const T&>(rhs);
        if constexpr (notNull) {
            AFL_VERIFY(!left.IsNull(lpos));
            AFL_VERIFY(!right.IsNull(rpos));
            return CompareValueNotNull(left.GetView(lpos), right.GetView(rpos));
        } else {
            return CompareValue(left.GetView(lpos), right.GetView(rpos), left.IsNull(lpos), right.IsNull(rpos));
        }
    }

    template <typename T>
    static std::partial_ordering CompareValue(const T& x, const T& y, bool xIsNull, bool yIsNull) {
        if (xIsNull && yIsNull) {
            return std::partial_ordering::equivalent;
        }
        if (xIsNull) {
            return std::partial_ordering::less;
        }
        if (yIsNull) {
            return std::partial_ordering::greater;
        }
        return CompareValueNotNull(x, y);
    }

    template <typename T>
    static std::partial_ordering CompareValueNotNull(const T& x, const T& y) {
        if constexpr (std::is_same_v<T, arrow::util::string_view>) {
            size_t minSize = (x.size() < y.size()) ? x.size() : y.size();
            int cmp = memcmp(x.data(), y.data(), minSize);
            if (cmp < 0) {
                return std::partial_ordering::less;
            } else if (cmp > 0) {
                return std::partial_ordering::greater;
            }
            return CompareValueNotNull(x.size(), y.size());
        } else {
            return x <=> y;
        }
    }
};
}
