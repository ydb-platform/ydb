#pragma once

#include "layout_converter_common.h"

#include <limits>
#include <unordered_set>

namespace NKikimr::NMiniKQL {

// Like GraceJoin's ANY table, remember keys before joining or spilling rows.
// Store offsets rather than pointers so growing the key buffers is safe.
class TAnyJoinKeys {
    struct THash {
        const TAnyJoinKeys* Self;

        size_t operator()(size_t index) const {
            return NPackedTuple::Hash(Self->Key(index).PackedData);
        }
    };

    struct TEqual {
        const TAnyJoinKeys* Self;

        bool operator()(size_t lhs, size_t rhs) const {
            const auto left = Self->Key(lhs);
            const auto right = Self->Key(rhs);
            return Self->Layout_->KeysEqual(left.PackedData, left.OverflowBegin,
                                           right.PackedData, right.OverflowBegin);
        }
    };

public:
    explicit TAnyJoinKeys(const NPackedTuple::TTupleLayout* layout)
        : Layout_(layout)
        , Keys_(0, THash{this}, TEqual{this})
    {}

    TAnyJoinKeys(const TAnyJoinKeys&) = delete;
    TAnyJoinKeys& operator=(const TAnyJoinKeys&) = delete;

    void Filter(TPackResult& pack) {
        TPackResult unique;
        for (const auto row : pack) {
            if (Keep(row)) {
                unique.AppendTuple(row, Layout_);
            }
        }
        pack = std::move(unique);
    }

private:
    TSingleTuple Key(size_t index) const {
        return {Rows_.data() + index * Layout_->BitmaskEnd, Overflow_.data()};
    }

    bool Keep(TSingleTuple row) {
        // GraceJoin does not deduplicate non-matching NULL keys. EqualNulls
        // keys, when present, use the layout's normal equality and hashing.
        if (!Layout_->KeysEqual(row.PackedData, row.OverflowBegin, row.PackedData, row.OverflowBegin)) {
            return true;
        }
        const size_t offset = Rows_.size();
        const size_t overflowSize = Overflow_.size();
        Rows_.insert(Rows_.end(), row.PackedData, row.PackedData + Layout_->BitmaskEnd);
        for (const auto& column : Layout_->KeyColumns) {
            if (column.SizeType != NPackedTuple::EColumnSizeType::Variable || row.PackedData[column.Offset] != 255) {
                continue;
            }
            const ui32 sourceOffset = ReadUnaligned<ui32>(row.PackedData + column.Offset + 1);
            const ui32 size = ReadUnaligned<ui32>(row.PackedData + column.Offset + 1 + sizeof(ui32));
            MKQL_ENSURE(Overflow_.size() <= std::numeric_limits<ui32>::max(), "ANY key overflow exceeds ui32 range");
            WriteUnaligned<ui32>(Rows_.data() + offset + column.Offset + 1, static_cast<ui32>(Overflow_.size()));
            Overflow_.insert(Overflow_.end(), row.OverflowBegin + sourceOffset, row.OverflowBegin + sourceOffset + size);
        }
        if (Keys_.insert(offset / Layout_->BitmaskEnd).second) {
            return true;
        }
        Rows_.resize(offset);
        Overflow_.resize(overflowSize);
        return false;
    }

    const NPackedTuple::TTupleLayout* Layout_;
    TTupleData Rows_;
    TTupleData Overflow_;
    std::unordered_set<size_t, THash, TEqual, TMKQLAllocator<size_t>> Keys_;
};

} // namespace NKikimr::NMiniKQL
