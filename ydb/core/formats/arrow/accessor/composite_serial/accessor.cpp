#include "accessor.h"

#include <ydb/core/formats/arrow/accessor/sparsed/constructor.h>

#include <ydb/library/actors/prof/tag.h>

namespace NKikimr::NArrow::NAccessor {

namespace {
class TSerializedChunkAccessor {
private:
    const std::vector<TDeserializeChunkedArray::TChunk>& Chunks;
    const std::shared_ptr<TColumnLoader>& Loader;
    std::optional<IChunkedArray::TLocalChunkedArrayAddress>& Result;

public:
    TSerializedChunkAccessor(const std::vector<TDeserializeChunkedArray::TChunk>& chunks, const std::shared_ptr<TColumnLoader>& loader,
        std::optional<IChunkedArray::TLocalChunkedArrayAddress>& result)
        : Chunks(chunks)
        , Loader(loader)
        , Result(result) {
    }
    ui64 GetChunksCount() const {
        return Chunks.size();
    }
    ui64 GetChunkLength(const ui32 idx) const {
        return Chunks[idx].GetRecordsCount();
    }
    void OnArray(const ui32 chunkIdx, const ui32 startPosition) const {
        Result = IChunkedArray::TLocalChunkedArrayAddress(Chunks[chunkIdx].GetArrayVerified(Loader), startPosition, chunkIdx);
    }
};

}   // namespace

std::shared_ptr<IChunkedArray> TDeserializeChunkedArray::TChunk::GetArrayVerified(const std::shared_ptr<TColumnLoader>& loader) const {
    if (PredefinedArray) {
        return PredefinedArray;
    }
    if (!!Data) {
        return loader->ApplyVerified(Data, RecordsCount);
    } else {
        AFL_VERIFY(!!DataBuffer);
        return loader->ApplyVerified(TString(DataBuffer.data(), DataBuffer.size()), RecordsCount);
    }
}

IChunkedArray::TLocalDataAddress TDeserializeChunkedArray::DoGetLocalData(
    const std::optional<TCommonChunkAddress>& /*chunkCurrent*/, const ui64 /*position*/) const {
    AFL_VERIFY(false);
    return IChunkedArray::TLocalDataAddress(nullptr, 0, 0);
}

IChunkedArray::TLocalChunkedArrayAddress TDeserializeChunkedArray::DoGetLocalChunkedArray(
    const std::optional<TCommonChunkAddress>& chunkCurrent, const ui64 position) const {
    std::optional<IChunkedArray::TLocalChunkedArrayAddress> result;
    TSerializedChunkAccessor accessor(Chunks, Loader, result);
    SelectChunk(chunkCurrent, position, accessor);
    AFL_VERIFY(result);
    return *result;
}

}   // namespace NKikimr::NArrow::NAccessor
