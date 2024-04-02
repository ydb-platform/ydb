#include "accessor.h"
#include <ydb/core/formats/arrow/switch/compare.h>
#include <ydb/core/formats/arrow/switch/switch_type.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/core/formats/arrow/permutations.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>

namespace NKikimr::NArrow::NAccessor {

void IChunkedArray::AppendPositionTo(arrow::ArrayBuilder& builder, const ui64 position, ui64* recordSize) {
    auto address = GetAddress(position);
    AFL_VERIFY(NArrow::Append(builder, *address.GetArray(), address.GetPosition(), recordSize));
}

std::shared_ptr<arrow::Array> IChunkedArray::CopyRecord(const ui64 recordIndex) const {
    auto address = GetAddress(recordIndex);
    return NArrow::CopyRecords(address.GetArray(), {address.GetPosition()});
}

std::shared_ptr<arrow::ChunkedArray> IChunkedArray::Slice(const ui32 offset, const ui32 count) const {
    AFL_VERIFY(offset + count <= (ui64)RecordsCount)("offset", offset)("count", count)("length", RecordsCount);
    ui32 currentOffset = offset;
    ui32 countLeast = count;
    std::vector<std::shared_ptr<arrow::Array>> chunks;
    while (countLeast) {
        auto address = GetAddress(currentOffset);
        if (address.GetPosition() + countLeast <= (ui64)address.GetArray()->length()) {
            chunks.emplace_back(address.GetArray()->Slice(address.GetPosition(), countLeast));
            break;
        } else {
            const ui32 deltaCount = address.GetArray()->length() - address.GetPosition();
            chunks.emplace_back(address.GetArray()->Slice(address.GetPosition(), deltaCount));
            AFL_VERIFY(countLeast >= deltaCount);
            countLeast -= deltaCount;
            currentOffset += deltaCount;
        }
    }
    return std::make_shared<arrow::ChunkedArray>(chunks, DataType);
}

TString IChunkedArray::DebugString(const ui32 position) const {
    auto address = GetAddress(position);
    return NArrow::DebugString(address.GetArray(), address.GetPosition());
}

std::partial_ordering IChunkedArray::CompareColumns(const std::vector<std::shared_ptr<IChunkedArray>>& l, const ui64 lPosition, const std::vector<std::shared_ptr<IChunkedArray>>& r, const ui64 rPosition) {
    AFL_VERIFY(l.size() == r.size());
    for (ui32 i = 0; i < l.size(); ++i) {
        const TAddress lAddress = l[i]->GetAddress(lPosition);
        const TAddress rAddress = r[i]->GetAddress(rPosition);
        auto cmp = lAddress.Compare(rAddress);
        if (std::is_neq(cmp)) {
            return cmp;
        }
    }
    return std::partial_ordering::equivalent;
}

IChunkedArray::TAddress IChunkedArray::GetAddress(const ui64 position) const {
    AFL_VERIFY(position < RecordsCount);
    if (!CurrentChunkAddress || position < CurrentChunkAddress->GetStartPosition() || CurrentChunkAddress->GetStartPosition() + CurrentChunkAddress->GetArray()->length() <= position) {
        CurrentChunkAddress = DoGetChunk(position);
    }
    return IChunkedArray::TAddress(CurrentChunkAddress->GetArray(), position - CurrentChunkAddress->GetStartPosition());
}

const std::partial_ordering IChunkedArray::TAddress::Compare(const TAddress& item) const {
    return TComparator::TypedCompare<true>(*Array, Position, *item.Array, item.Position);
}

IChunkedArray::TCurrentChunkAddress TTrivialChunkedArray::DoGetChunk(const ui64 position) const {
    ui64 idx = 0;
    for (auto&& i : Array->chunks()) {
        if (idx <= position && idx + (ui64)i->length() > position) {
            return TCurrentChunkAddress(i, idx);
        }
        idx += i->length();
    }
    AFL_VERIFY(false);
    return TCurrentChunkAddress(nullptr, 0);
}

}
