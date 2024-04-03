#include "accessor.h"
#include <ydb/core/formats/arrow/switch/compare.h>
#include <ydb/core/formats/arrow/switch/switch_type.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/core/formats/arrow/permutations.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>

namespace NKikimr::NArrow::NAccessor {

void IChunkedArray::TReader::AppendPositionTo(arrow::ArrayBuilder& builder, const ui64 position, ui64* recordSize) const {
    auto address = GetReadChunk(position);
    AFL_VERIFY(NArrow::Append(builder, *address.GetArray(), address.GetPosition(), recordSize));
}

std::shared_ptr<arrow::Array> IChunkedArray::TReader::CopyRecord(const ui64 recordIndex) const {
    auto address = GetReadChunk(recordIndex);
    return NArrow::CopyRecords(address.GetArray(), {address.GetPosition()});
}

std::shared_ptr<arrow::ChunkedArray> IChunkedArray::TReader::Slice(const ui32 offset, const ui32 count) const {
    AFL_VERIFY(offset + count <= (ui64)GetRecordsCount())("offset", offset)("count", count)("length", GetRecordsCount());
    ui32 currentOffset = offset;
    ui32 countLeast = count;
    std::vector<std::shared_ptr<arrow::Array>> chunks;
    while (countLeast) {
        auto address = GetReadChunk(currentOffset);
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
    return std::make_shared<arrow::ChunkedArray>(chunks, ChunkedArray->DataType);
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

const std::partial_ordering IChunkedArray::TAddress::Compare(const TAddress& item) const {
    return TComparator::TypedCompare<true>(*Array, Position, *item.Array, item.Position);
}

namespace {
class TChunkAccessor {
private:
    std::shared_ptr<arrow::ChunkedArray> ChunkedArray;
public:
    TChunkAccessor(const std::shared_ptr<arrow::ChunkedArray>& chunkedArray)
        : ChunkedArray(chunkedArray)
    {

    }
    ui64 GetChunksCount() const {
        return (ui64)ChunkedArray->num_chunks();
    }
    ui64 GetChunkLength(const ui32 idx) const {
        return (ui64)ChunkedArray->chunk(idx)->length();
    }
    std::shared_ptr<arrow::Array> GetArray(const ui32 idx) const {
        return ChunkedArray->chunk(idx);
    }
};
}

IChunkedArray::TCurrentChunkAddress TTrivialChunkedArray::DoGetChunk(const std::optional<TCurrentChunkAddress>& chunkCurrent, const ui64 position) const {
    TChunkAccessor accessor(Array);
    return SelectChunk(chunkCurrent, position, accessor);
}

}
