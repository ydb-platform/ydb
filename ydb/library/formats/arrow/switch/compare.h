#pragma once
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <util/system/yassert.h>

namespace NKikimr::NArrow {

class TComparator {
public:
    template <bool notNull>
    static std::partial_ordering ConcreteTypedCompare(const arrow20::Type::type typeId, const arrow20::Array& lhs, const int lpos, const arrow20::Array& rhs, const int rpos) {
        switch (typeId) {
            case arrow20::Type::NA:
            case arrow20::Type::BOOL:
                break;
            case arrow20::Type::UINT8:
                return CompareView<arrow20::UInt8Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow20::Type::INT8:
                return CompareView<arrow20::Int8Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow20::Type::UINT16:
                return CompareView<arrow20::UInt16Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow20::Type::INT16:
                return CompareView<arrow20::Int16Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow20::Type::UINT32:
                return CompareView<arrow20::UInt32Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow20::Type::INT32:
                return CompareView<arrow20::Int32Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow20::Type::UINT64:
                return CompareView<arrow20::UInt64Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow20::Type::INT64:
                return CompareView<arrow20::Int64Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow20::Type::HALF_FLOAT:
                break;
            case arrow20::Type::FLOAT:
                return CompareView<arrow20::FloatArray, notNull>(lhs, lpos, rhs, rpos);
            case arrow20::Type::DOUBLE:
                return CompareView<arrow20::DoubleArray, notNull>(lhs, lpos, rhs, rpos);
            case arrow20::Type::STRING:
                return CompareView<arrow20::StringArray, notNull>(lhs, lpos, rhs, rpos);
            case arrow20::Type::BINARY:
                return CompareView<arrow20::BinaryArray, notNull>(lhs, lpos, rhs, rpos);
            case arrow20::Type::FIXED_SIZE_BINARY:
                return CompareView<arrow20::FixedSizeBinaryArray, notNull>(lhs, lpos, rhs, rpos);
            case arrow20::Type::DATE32:
                return CompareView<arrow20::Date32Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow20::Type::DATE64:
                return CompareView<arrow20::Date64Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow20::Type::TIMESTAMP:
                return CompareView<arrow20::TimestampArray, notNull>(lhs, lpos, rhs, rpos);
            case arrow20::Type::TIME32:
                return CompareView<arrow20::Time32Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow20::Type::TIME64:
                return CompareView<arrow20::Time64Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow20::Type::DURATION:
                return CompareView<arrow20::DurationArray, notNull>(lhs, lpos, rhs, rpos);
            case arrow20::Type::DECIMAL256:
                return CompareView<arrow20::Decimal256Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow20::Type::DECIMAL:
                return CompareView<arrow20::Decimal128Array, notNull>(lhs, lpos, rhs, rpos);
            case arrow20::Type::DENSE_UNION:
            case arrow20::Type::DICTIONARY:
            case arrow20::Type::EXTENSION:
            case arrow20::Type::FIXED_SIZE_LIST:
            case arrow20::Type::INTERVAL_DAY_TIME:
            case arrow20::Type::INTERVAL_MONTHS:
            case arrow20::Type::LARGE_BINARY:
            case arrow20::Type::LARGE_LIST:
            case arrow20::Type::LARGE_STRING:
            case arrow20::Type::LIST:
            case arrow20::Type::MAP:
            case arrow20::Type::MAX_ID:
            case arrow20::Type::SPARSE_UNION:
            case arrow20::Type::STRUCT:
                Y_ABORT("not implemented");
                break;
        }
        return std::partial_ordering::equivalent;
    }

    template <bool notNull>
    static std::partial_ordering TypedCompare(const arrow20::Array& lhs, const int lpos, const arrow20::Array& rhs, const int rpos) {
        return ConcreteTypedCompare<notNull>(lhs.type_id(), lhs, lpos, rhs, rpos);
    }

    template <typename T, bool notNull>
    static std::partial_ordering CompareView(const arrow20::Array& lhs, int lpos, const arrow20::Array& rhs, int rpos) {
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
        if constexpr (std::is_same_v<T, arrow20::util::string_view>) {
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