#include "accessor.h"
namespace NKikimr::NArrow::NAccessor {

namespace {
class TCompositeChunkAccessor {
private:
    const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& Chunks;
    std::optional<IChunkedArray::TLocalChunkedArrayAddress>* ResultArrayAddress = nullptr;

public:
    TCompositeChunkAccessor(const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& chunks,
        std::optional<IChunkedArray::TLocalChunkedArrayAddress>& result)
        : Chunks(chunks)
        , ResultArrayAddress(&result) {
    }
    ui64 GetChunksCount() const {
        return Chunks.size();
    }
    ui64 GetChunkLength(const ui32 idx) const {
        return Chunks[idx]->GetRecordsCount();
    }
    void OnArray(const ui32 chunkIdx, const ui32 startPosition) const {
        if (ResultArrayAddress) {
            *ResultArrayAddress = NArrow::NAccessor::IChunkedArray::TLocalChunkedArrayAddress(Chunks[chunkIdx], startPosition, chunkIdx);
        }
    }
};
}   // namespace

IChunkedArray::TLocalDataAddress TCompositeChunkedArray::DoGetLocalData(
    const std::optional<TCommonChunkAddress>& /*chunkCurrent*/, const ui64 /*position*/) const {
    AFL_VERIFY(false);
    return IChunkedArray::TLocalDataAddress(nullptr, 0, 0);
}

IChunkedArray::TLocalChunkedArrayAddress TCompositeChunkedArray::DoGetLocalChunkedArray(
    const std::optional<TCommonChunkAddress>& chunkCurrent, const ui64 position) const {
    std::optional<IChunkedArray::TLocalChunkedArrayAddress> result;
    TCompositeChunkAccessor accessor(Chunks, result);
    SelectChunk(chunkCurrent, position, accessor);
    AFL_VERIFY(result);
    return *result;
}

}   // namespace NKikimr::NArrow::NAccessor
