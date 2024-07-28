#include "accessor.h"
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/formats/arrow/switch/compare.h>
#include <ydb/core/formats/arrow/switch/switch_type.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/core/formats/arrow/permutations.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/splitter/simple.h>
#include <ydb/core/formats/arrow/save_load/saver.h>

namespace NKikimr::NArrow::NAccessor {

void IChunkedArray::TReader::AppendPositionTo(arrow::ArrayBuilder& builder, const ui64 position, ui64* recordSize) const {
    auto address = GetReadChunk(position);
    AFL_VERIFY(NArrow::Append(builder, *address.GetArray(), address.GetPosition(), recordSize));
}

std::shared_ptr<arrow::Array> IChunkedArray::TReader::CopyRecord(const ui64 recordIndex) const {
    auto address = GetReadChunk(recordIndex);
    return NArrow::CopyRecords(address.GetArray(), {address.GetPosition()});
}

std::shared_ptr<arrow::ChunkedArray> IChunkedArray::Slice(const ui32 offset, const ui32 count) const {
    AFL_VERIFY(offset + count <= (ui64)GetRecordsCount())("offset", offset)("count", count)("length", GetRecordsCount());
    ui32 currentOffset = offset;
    ui32 countLeast = count;
    std::vector<std::shared_ptr<arrow::Array>> chunks;
    auto address = GetChunk({}, offset);
    while (countLeast) {
        address = GetChunk(address, currentOffset);
        const ui64 internalPos = currentOffset - address.GetStartPosition();
        if (internalPos + countLeast <= (ui64)address.GetArray()->length()) {
            chunks.emplace_back(address.GetArray()->Slice(internalPos, countLeast));
            break;
        } else {
            const ui32 deltaCount = address.GetArray()->length() - internalPos;
            chunks.emplace_back(address.GetArray()->Slice(internalPos, deltaCount));
            AFL_VERIFY(countLeast >= deltaCount);
            countLeast -= deltaCount;
            currentOffset += deltaCount;
        }
    }
    return std::make_shared<arrow::ChunkedArray>(chunks, DataType);
}

TString IChunkedArray::TReader::DebugString(const ui32 position) const {
    auto address = GetReadChunk(position);
    return NArrow::DebugString(address.GetArray(), address.GetPosition());
}

std::partial_ordering IChunkedArray::TReader::CompareColumns(const std::vector<TReader>& l, const ui64 lPosition, const std::vector<TReader>& r, const ui64 rPosition) {
    AFL_VERIFY(l.size() == r.size());
    for (ui32 i = 0; i < l.size(); ++i) {
        const TAddress lAddress = l[i].GetReadChunk(lPosition);
        const TAddress rAddress = r[i].GetReadChunk(rPosition);
        auto cmp = lAddress.Compare(rAddress);
        if (std::is_neq(cmp)) {
            return cmp;
        }
    }
    return std::partial_ordering::equivalent;
}

IChunkedArray::TAddress IChunkedArray::TReader::GetReadChunk(const ui64 position) const {
    AFL_VERIFY(position < ChunkedArray->GetRecordsCount());
    if (CurrentChunkAddress && position < CurrentChunkAddress->GetStartPosition() + CurrentChunkAddress->GetArray()->length() && CurrentChunkAddress->GetStartPosition() <= position) {
    } else {
        CurrentChunkAddress = ChunkedArray->DoGetChunk(CurrentChunkAddress, position);
    }
    return IChunkedArray::TAddress(CurrentChunkAddress->GetArray(), position - CurrentChunkAddress->GetStartPosition(), CurrentChunkAddress->GetChunkIndex());
}

const std::partial_ordering IChunkedArray::TAddress::Compare(const TAddress& item) const {
    return TComparator::TypedCompare<true>(*Array, Position, *item.Array, item.Position);
}

 TChunkedArraySerialized::TChunkedArraySerialized(const std::shared_ptr<IChunkedArray>& array, const TString& serializedData)
    : Array(array)
    , SerializedData(serializedData) {
    AFL_VERIFY(serializedData);
    AFL_VERIFY(Array);
    AFL_VERIFY(Array->GetRecordsCount());
}

std::partial_ordering IChunkedArray::TCurrentChunkAddress::Compare(const ui64 position, const TCurrentChunkAddress& item, const ui64 itemPosition) const {
    AFL_VERIFY(GetStartPosition() <= position)("pos", position)("start", GetStartPosition());
    AFL_VERIFY(position < GetFinishPosition())("pos", position)("finish", GetFinishPosition());
    AFL_VERIFY(item.GetStartPosition() <= itemPosition)("start", item.GetStartPosition())("item", itemPosition);
    AFL_VERIFY(itemPosition < item.GetFinishPosition())("item", itemPosition)("finish", item.GetFinishPosition());
    return TComparator::TypedCompare<true>(*Array, position - GetStartPosition(), *item.Array, itemPosition - item.GetStartPosition());
}

std::shared_ptr<arrow::Array> IChunkedArray::TCurrentChunkAddress::CopyRecord(const ui64 recordIndex) const {
    AFL_VERIFY(GetStartPosition() <= recordIndex);
    AFL_VERIFY(recordIndex < GetFinishPosition());
    return NArrow::CopyRecords(Array, { recordIndex - GetStartPosition() });
}

TString IChunkedArray::TCurrentChunkAddress::DebugString(const ui64 position) const {
    AFL_VERIFY(position < GetFinishPosition());
    AFL_VERIFY(GetStartPosition() <= position);
    return NArrow::DebugString(Array, position - GetStartPosition());
}

}
