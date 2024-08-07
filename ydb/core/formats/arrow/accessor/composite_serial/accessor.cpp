#include "accessor.h"

namespace NKikimr::NArrow::NAccessor {

namespace {
class TSerializedChunkAccessor {
private:
    const std::vector<TDeserializeChunkedArray::TChunk>& Chunks;
    const std::shared_ptr<TColumnLoader>& Loader;
    TDeserializeChunkedArray::TChunkCacheInfo* CachedDataOwner;

public:
    TSerializedChunkAccessor(const std::vector<TDeserializeChunkedArray::TChunk>& chunks, const std::shared_ptr<TColumnLoader>& loader,
        TDeserializeChunkedArray::TChunkCacheInfo* cachedDataOwner)
        : Chunks(chunks)
        , Loader(loader)
        , CachedDataOwner(cachedDataOwner) {
    }
    ui64 GetChunksCount() const {
        return Chunks.size();
    }
    ui64 GetChunkLength(const ui32 idx) const {
        return Chunks[idx].GetRecordsCount();
    }
    void OnArray(const ui32 chunkIdx, const ui32 startPosition) const {
        if (!CachedDataOwner->GetChunk() || CachedDataOwner->GetIndex() != chunkIdx) {
            CachedDataOwner->SetChunk(Chunks[chunkIdx].GetArrayVerified(Loader));
            CachedDataOwner->SetIndex(chunkIdx);
            CachedDataOwner->SetStartPosition(startPosition);
        }
    }
};
}   // namespace

IChunkedArray::TLocalDataAddress TDeserializeChunkedArray::DoGetLocalData(
    const std::optional<TCommonChunkAddress>& /*chunkCurrent*/, const ui64 /*position*/) const {
    AFL_VERIFY(false);
    return IChunkedArray::TLocalDataAddress(nullptr, 0, 0);
}

IChunkedArray::TLocalChunkedArrayAddress TDeserializeChunkedArray::DoGetLocalChunkedArray(
    const std::optional<TCommonChunkAddress>& chunkCurrent, const ui64 position) const {
    TSerializedChunkAccessor accessor(Chunks, Loader, CurrentChunkCache.get());
    SelectChunk(chunkCurrent, position, accessor);
    return IChunkedArray::TLocalChunkedArrayAddress(
        CurrentChunkCache->GetChunk(), CurrentChunkCache->GetStartPosition(), CurrentChunkCache->GetIndex());
}

}   // namespace NKikimr::NArrow::NAccessor
