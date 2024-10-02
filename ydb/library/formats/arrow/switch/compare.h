#pragma once
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <util/system/yassert.h>

namespace NKikimr::NArrow {

class TComparator {
public:
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
            case arrow::Type::DATE32:
            case arrow::Type::DATE64:
                break;
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
    static std::partial_ordering TypedCompare(const arrow::Array& lhs, const int lpos, const arrow::Array& rhs, const int rpos) {
        return ConcreteTypedCompare<notNull>(lhs.type_id(), lhs, lpos, rhs, rpos);
    }

    template <typename T, bool notNull>
    static std::partial_ordering CompareView(const arrow::Array& lhs, int lpos, const arrow::Array& rhs, int rpos) {
        auto& left = static_cast<const T&>(lhs);
        auto& right = static_cast<const T&>(rhs);
        if constexpr (notNull) {
            return CompareValueNotNull(left.GetView(lpos), right.GetView(rpos));
        } else {
            return CompareValue(left.GetView(lpos), right.GetView(rpos), left.IsNull(lpos), right.IsNull(rpos));
        }
    }

    template <typename T>
    static std::partial_ordering CompareValue(const T& x, const T& y, bool xIsNull, bool yIsNull) {
        // TODO: std::partial_ordering::unordered for both nulls?
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