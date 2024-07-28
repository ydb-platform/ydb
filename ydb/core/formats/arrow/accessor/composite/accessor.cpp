#include "accessor.h"
namespace NKikimr::NArrow::NAccessor {

namespace {
class TCompositeChunkAccessor {
private:
    const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& Chunks;
    std::optional<IChunkedArray::TCurrentChunkAddress>* ResultChunkAddress = nullptr;
    std::optional<IChunkedArray::TCurrentArrayAddress>* ResultArrayAddress = nullptr;

public:
    TCompositeChunkAccessor(
        const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& chunks, std::optional<IChunkedArray::TCurrentChunkAddress>& result)
        : Chunks(chunks)
        , ResultChunkAddress(&result) {
    }
    TCompositeChunkAccessor(
        const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& chunks, std::optional<IChunkedArray::TCurrentArrayAddress>& result)
        : Chunks(chunks)
        , ResultArrayAddress(&result) {
    }
    ui64 GetChunksCount() const {
        return Chunks.size();
    }
    ui64 GetChunkLength(const ui32 idx) const {
        return Chunks[idx]->GetRecordsCount();
    }
    void OnArray(const ui32 chunkIdx, const ui32 startPosition, const ui32 internalPosition) const {
        if (ResultChunkAddress) {
            auto internalAddress = Chunks[chunkIdx]->GetChunk({}, internalPosition);
            *ResultChunkAddress = NArrow::NAccessor::IChunkedArray::TCurrentChunkAddress(
                internalAddress.GetArray(), startPosition + internalAddress.GetStartPosition(), chunkIdx);
        }
        if (ResultArrayAddress) {
            *ResultArrayAddress = NArrow::NAccessor::IChunkedArray::TCurrentArrayAddress(Chunks[chunkIdx], startPosition, chunkIdx);
        }
    }
};
}   // namespace

NArrow::NAccessor::IChunkedArray::TCurrentChunkAddress TCompositeChunkedArray::DoGetChunk(
    const std::optional<TCurrentChunkAddress>& chunkCurrent, const ui64 position) const {
    std::optional<IChunkedArray::TCurrentChunkAddress> result;
    TCompositeChunkAccessor accessor(Chunks, result);
    SelectChunk(chunkCurrent, position, accessor);
    AFL_VERIFY(result);
    return *result;
}

NArrow::NAccessor::IChunkedArray::TCurrentArrayAddress TCompositeChunkedArray::DoGetArray(
    const std::optional<TCurrentArrayAddress>& chunkCurrent, const ui64 position, const std::shared_ptr<IChunkedArray>& /*selfPtr*/) const {
    std::optional<IChunkedArray::TCurrentArrayAddress> result;
    TCompositeChunkAccessor accessor(Chunks, result);
    SelectChunk(chunkCurrent, position, accessor);
    AFL_VERIFY(result);
    return *result;
}

std::shared_ptr<arrow::ChunkedArray> TCompositeChunkedArray::DoGetChunkedArray() const {
    std::vector<std::shared_ptr<arrow::Array>> chunks;
    for (auto&& i : Chunks) {
        auto arr = i->GetChunkedArray();
        AFL_VERIFY(arr->num_chunks());
        for (auto&& chunk : arr->chunks()) {
            chunks.emplace_back(chunk);
        }
    }
    return std::make_shared<arrow::ChunkedArray>(chunks);
}

}   // namespace NKikimr::NArrow::NAccessor
