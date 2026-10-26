#pragma once
#include <ydb/core/scheme_types/scheme_type_info.h>

#include <ydb/library/actors/core/log.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <util/system/unaligned_mem.h>
#include <util/system/yassert.h>

namespace NKikimr::NArrow {

class TComparator {
public:
    static bool IsNumericDecimal(const NScheme::TTypeInfo& logicalType);

    template <bool notNull>
    static std::partial_ordering ConcreteTypedCompare(const arrow::Type::type typeId, const arrow::Array& lhs, const int lpos,
        const arrow::Array& rhs, const int rpos, const NScheme::TTypeInfo& logicalType) {
        switch (typeId) {
            case arrow::Type::NA:
            case arrow::Type::BOOL:
                break;
            case arrow::Type::UINT8:
                return CompareView<arrow::UInt8Array, notNull>(lhs, lpos, rhs, rpos, logicalType);
            case arrow::Type::INT8:
                return CompareView<arrow::Int8Array, notNull>(lhs, lpos, rhs, rpos, logicalType);
            case arrow::Type::UINT16:
                return CompareView<arrow::UInt16Array, notNull>(lhs, lpos, rhs, rpos, logicalType);
            case arrow::Type::INT16:
                return CompareView<arrow::Int16Array, notNull>(lhs, lpos, rhs, rpos, logicalType);
            case arrow::Type::UINT32:
                return CompareView<arrow::UInt32Array, notNull>(lhs, lpos, rhs, rpos, logicalType);
            case arrow::Type::INT32:
                return CompareView<arrow::Int32Array, notNull>(lhs, lpos, rhs, rpos, logicalType);
            case arrow::Type::UINT64:
                return CompareView<arrow::UInt64Array, notNull>(lhs, lpos, rhs, rpos, logicalType);
            case arrow::Type::INT64:
                return CompareView<arrow::Int64Array, notNull>(lhs, lpos, rhs, rpos, logicalType);
            case arrow::Type::HALF_FLOAT:
                break;
            case arrow::Type::FLOAT:
                return CompareView<arrow::FloatArray, notNull>(lhs, lpos, rhs, rpos, logicalType);
            case arrow::Type::DOUBLE:
                return CompareView<arrow::DoubleArray, notNull>(lhs, lpos, rhs, rpos, logicalType);
            case arrow::Type::STRING:
                return CompareView<arrow::StringArray, notNull>(lhs, lpos, rhs, rpos, logicalType);
            case arrow::Type::BINARY:
                return CompareView<arrow::BinaryArray, notNull>(lhs, lpos, rhs, rpos, logicalType);
            case arrow::Type::FIXED_SIZE_BINARY:
                return CompareView<arrow::FixedSizeBinaryArray, notNull>(lhs, lpos, rhs, rpos, logicalType);
            case arrow::Type::DATE32:
                return CompareView<arrow::Date32Array, notNull>(lhs, lpos, rhs, rpos, logicalType);
            case arrow::Type::DATE64:
                return CompareView<arrow::Date64Array, notNull>(lhs, lpos, rhs, rpos, logicalType);
            case arrow::Type::TIMESTAMP:
                return CompareView<arrow::TimestampArray, notNull>(lhs, lpos, rhs, rpos, logicalType);
            case arrow::Type::TIME32:
                return CompareView<arrow::Time32Array, notNull>(lhs, lpos, rhs, rpos, logicalType);
            case arrow::Type::TIME64:
                return CompareView<arrow::Time64Array, notNull>(lhs, lpos, rhs, rpos, logicalType);
            case arrow::Type::DURATION:
                return CompareView<arrow::DurationArray, notNull>(lhs, lpos, rhs, rpos, logicalType);
            case arrow::Type::DECIMAL256:
                return CompareView<arrow::Decimal256Array, notNull>(lhs, lpos, rhs, rpos, logicalType);
            case arrow::Type::DECIMAL:
                return CompareView<arrow::Decimal128Array, notNull>(lhs, lpos, rhs, rpos, logicalType);
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
    static std::partial_ordering TypedCompare(
        const arrow::Array& lhs, const int lpos, const arrow::Array& rhs, const int rpos, const NScheme::TTypeInfo& logicalType) {
        AFL_VERIFY(lhs.type_id() == rhs.type_id())("lhs", lhs.type()->ToString())("rhs", rhs.type()->ToString());
        return ConcreteTypedCompare<notNull>(lhs.type_id(), lhs, lpos, rhs, rpos, logicalType);
    }

    template <typename T, bool notNull>
    static std::partial_ordering CompareView(
        const arrow::Array& lhs, int lpos, const arrow::Array& rhs, int rpos, const NScheme::TTypeInfo& logicalType) {
        auto& left = static_cast<const T&>(lhs);
        auto& right = static_cast<const T&>(rhs);
        if constexpr (notNull) {
            AFL_VERIFY(!left.IsNull(lpos));
            AFL_VERIFY(!right.IsNull(rpos));
            return CompareValueNotNull(left.GetView(lpos), right.GetView(rpos), logicalType);
        } else {
            return CompareValue(left.GetView(lpos), right.GetView(rpos), left.IsNull(lpos), right.IsNull(rpos), logicalType);
        }
    }

    template <typename T>
    static std::partial_ordering CompareValue(const T& x, const T& y, bool xIsNull, bool yIsNull, const NScheme::TTypeInfo& logicalType) {
        if (xIsNull && yIsNull) {
            return std::partial_ordering::equivalent;
        }
        if (xIsNull) {
            return std::partial_ordering::less;
        }
        if (yIsNull) {
            return std::partial_ordering::greater;
        }
        return CompareValueNotNull(x, y, logicalType);
    }

    template <typename T>
    static std::partial_ordering CompareValueNotNull(const T& x, const T& y, [[maybe_unused]] const NScheme::TTypeInfo& logicalType) {
        if constexpr (std::is_same_v<T, arrow::util::string_view>) {
            if (IsNumericDecimal(logicalType)) {
                AFL_VERIFY(x.size() == 16 && y.size() == 16);
                const auto high = ReadUnaligned<i64>(x.data() + sizeof(ui64)) <=> ReadUnaligned<i64>(y.data() + sizeof(ui64));
                if (high != 0) {
                    return high;
                }

                return ReadUnaligned<ui64>(x.data()) <=> ReadUnaligned<ui64>(y.data());
            }

            size_t minSize = (x.size() < y.size()) ? x.size() : y.size();
            int cmp = minSize ? memcmp(x.data(), y.data(), minSize) : 0;
            if (cmp < 0) {
                return std::partial_ordering::less;
            } else if (cmp > 0) {
                return std::partial_ordering::greater;
            }
            return x.size() <=> y.size();
        } else {
            return x <=> y;
        }
    }
};
}
