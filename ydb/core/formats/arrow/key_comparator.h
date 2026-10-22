#pragma once

#include <ydb/library/formats/arrow/switch/compare.h>
#include <yql/essentials/public/decimal/yql_decimal.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/key_value_metadata.h>

#include <algorithm>

namespace NKikimr::NArrow {

class TKeyColumnComparator {
    using TBinaryCompare = std::partial_ordering (*)(std::string_view, std::string_view);
    using TArrayCompare = std::partial_ordering (*)(const arrow::Array&, i32, const arrow::Array&, i32);

    static std::partial_ordering CompareBytes(std::string_view lhs, std::string_view rhs) {
        return lhs <=> rhs;
    }

    static std::partial_ordering ComparePhysical(const arrow::Array& lhs, i32 lpos, const arrow::Array& rhs, i32 rpos) {
        return TComparator::TypedCompare<false>(lhs, lpos, rhs, rpos);
    }

    static std::partial_ordering CompareDecimalArrays(const arrow::Array& lhs, i32 lpos, const arrow::Array& rhs, i32 rpos) {
        AFL_VERIFY(lhs.type_id() == arrow::Type::FIXED_SIZE_BINARY && rhs.type_id() == lhs.type_id());
        if (lhs.IsNull(lpos) || rhs.IsNull(rpos)) {
            return TComparator::CompareValueNotNull(!lhs.IsNull(lpos), !rhs.IsNull(rpos));
        }
        const auto l = static_cast<const arrow::FixedSizeBinaryArray&>(lhs).GetView(lpos);
        const auto r = static_cast<const arrow::FixedSizeBinaryArray&>(rhs).GetView(rpos);
        return CompareDecimal({l.data(), l.size()}, {r.data(), r.size()});
    }

    TBinaryCompare BinaryCompare = CompareBytes;
    TArrayCompare ArrayCompare = ComparePhysical;

public:
    static constexpr const char* DecimalComparisonMetadata = "ydb.decimal_comparison";

    static bool IsDecimal(const arrow::Field& field) {
        return field.type()->id() == arrow::Type::FIXED_SIZE_BINARY && field.metadata() &&
            field.metadata()->Contains(DecimalComparisonMetadata);
    }

    static std::partial_ordering CompareDecimal(std::string_view lhs, std::string_view rhs) {
        NYql::NDecimal::TInt128 l, r;
        AFL_VERIFY(lhs.size() == sizeof(l) && rhs.size() == sizeof(r));
        memcpy(&l, lhs.data(), sizeof(l));
        memcpy(&r, rhs.data(), sizeof(r));
        return l < r ? std::partial_ordering::less : r < l ? std::partial_ordering::greater : std::partial_ordering::equivalent;
    }

    TKeyColumnComparator() = default;

    explicit TKeyColumnComparator(const arrow::Field& field) {
        if (IsDecimal(field)) {
            BinaryCompare = CompareDecimal;
            ArrayCompare = CompareDecimalArrays;
        }
    }

    std::partial_ordering Compare(const arrow::Array& lhs, i32 lpos, const arrow::Array& rhs, i32 rpos) const {
        return ArrayCompare(lhs, lpos, rhs, rpos);
    }

    std::partial_ordering Compare(std::string_view lhs, std::string_view rhs) const {
        return BinaryCompare(lhs, rhs);
    }

    template <class TAddress>
    std::partial_ordering Compare(const TAddress& lhs, ui64 lpos, const TAddress& rhs, ui64 rpos) const {
        AFL_VERIFY(lhs.GetAddress().Contains(lpos) && rhs.GetAddress().Contains(rpos));
        return Compare(*lhs.GetArray(), lhs.GetAddress().GetLocalIndex(lpos), *rhs.GetArray(), rhs.GetAddress().GetLocalIndex(rpos));
    }
};

class TKeyComparator {
    std::vector<TKeyColumnComparator> Columns;

public:
    static bool HasDecimalComparison(const arrow::Schema& schema) {
        return std::any_of(schema.fields().begin(), schema.fields().end(), [](const auto& field) {
            return TKeyColumnComparator::IsDecimal(*field);
        });
    }

    explicit TKeyComparator(const arrow::FieldVector& fields) {
        for (const auto& field : fields) {
            Columns.emplace_back(*field);
        }
    }

    explicit TKeyComparator(const arrow::Schema* schema, size_t size)
        : TKeyComparator(schema ? schema->fields() : arrow::FieldVector()) {
        if (!schema) {
            Columns.resize(size);
        }
        AFL_VERIFY(Columns.size() == size);
    }

    const TKeyColumnComparator& operator[](size_t i) const {
        return Columns[i];
    }

    std::partial_ordering Compare(const std::vector<std::shared_ptr<arrow::Array>>& columns, i32 lhs, i32 rhs) const {
        AFL_VERIFY(columns.size() == Columns.size());
        for (size_t i = 0; i < columns.size(); ++i) {
            const auto cmp = Columns[i].Compare(*columns[i], lhs, *columns[i], rhs);
            if (cmp != std::partial_ordering::equivalent) {
                return cmp;
            }
        }
        return std::partial_ordering::equivalent;
    }
};

} // namespace NKikimr::NArrow
