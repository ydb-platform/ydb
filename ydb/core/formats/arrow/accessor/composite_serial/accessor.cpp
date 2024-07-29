#include "accessor.h"

namespace NKikimr::NArrow::NAccessor {

namespace {
class TSerializedChunkAccessor {
private:
    const std::vector<TDeserializeChunkedArray::TChunk>& Chunks;
    const std::shared_ptr<TColumnLoader>& Loader;
    TDeserializeChunkedArray::TChunkCacheInfo* CachedDataOwner;
    std::optional<NArrow::NAccessor::IChunkedArray::TCurrentChunkAddress>* ResultAddress;

public:
    TSerializedChunkAccessor(const std::vector<TDeserializeChunkedArray::TChunk>& chunks, const std::shared_ptr<TColumnLoader>& loader,
        TDeserializeChunkedArray::TChunkCacheInfo* cachedDataOwner,
        std::optional<NArrow::NAccessor::IChunkedArray::TCurrentChunkAddress>* resultAddress)
        : Chunks(chunks)
        , Loader(loader)
        , CachedDataOwner(cachedDataOwner)
        , ResultAddress(resultAddress) {
    }
    ui64 GetChunksCount() const {
        return Chunks.size();
    }
    ui64 GetChunkLength(const ui32 idx) const {
        return Chunks[idx].GetRecordsCount();
    }
    void OnArray(const ui32 chunkIdx, const ui32 startPosition, const ui32 internalPosition) const {
        if (!CachedDataOwner->GetChunk() || CachedDataOwner->GetIndex() != chunkIdx) {
            CachedDataOwner->SetChunk(Chunks[chunkIdx].GetArrayVerified(Loader));
            CachedDataOwner->SetIndex(chunkIdx);
            CachedDataOwner->SetStartPosition(startPosition);
        }
        if (ResultAddress) {
            auto addressInternal = CachedDataOwner->GetChunk()->GetChunk({}, internalPosition);
            *ResultAddress = NArrow::NAccessor::IChunkedArray::TCurrentChunkAddress(
                addressInternal.GetArray(), startPosition + addressInternal.GetStartPosition(), chunkIdx);
        }
    }
};
}   // namespace

NArrow::NAccessor::IChunkedArray::TCurrentChunkAddress TDeserializeChunkedArray::DoGetChunk(
    const std::optional<TCurrentChunkAddress>& chunkCurrent, const ui64 position) const {
    std::optional<IChunkedArray::TCurrentChunkAddress> result;
    TSerializedChunkAccessor accessor(Chunks, Loader, CurrentChunkCache.get(), &result);
    SelectChunk(chunkCurrent, position, accessor);
    AFL_VERIFY(result);
    return *result;
}

NArrow::NAccessor::IChunkedArray::TCurrentArrayAddress TDeserializeChunkedArray::DoGetArray(
    const std::optional<TCurrentArrayAddress>& chunkCurrent, const ui64 position, const std::shared_ptr<IChunkedArray>& /*selfPtr*/) const {
    TSerializedChunkAccessor accessor(Chunks, Loader, CurrentChunkCache.get(), nullptr);
    SelectChunk(chunkCurrent, position, accessor);
    return IChunkedArray::TCurrentArrayAddress(
        CurrentChunkCache->GetChunk(), CurrentChunkCache->GetStartPosition(), CurrentChunkCache->GetIndex());
}

}   // namespace NKikimr::NArrow::NAccessor
