#pragma once
#include "constructor.h"

#include <ydb/core/formats/arrow/accessor/common/chunk_data.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_primitive.h>

namespace NKikimr::NOlap::NReader::NCommon {

// Restores one column chunk when we only read the dictionary part of the blob (first DictionaryBlobSize bytes).
class TDictionaryChunkRestoreInfo {
private:
    const NArrow::NAccessor::TChunkConstructionData ChunkExternalInfo;
    std::optional<TBlobRange> DictionaryBlobRange;
    std::shared_ptr<arrow::Array> DictionaryArray;
    YDB_READONLY_DEF(TBlobRange, FullChunkRange);

public:
    ui32 GetRecordsCount() const;

    const std::optional<TBlobRange>& GetDictionaryBlobRangeOptional() const;

    void SetDictionaryBlob(TString&& blob);

    const std::shared_ptr<arrow::Array>& GetDictionaryArray() const;

    TDictionaryChunkRestoreInfo(const TBlobRange& fullChunkRange,
        const NArrow::NAccessor::TChunkConstructionData& chunkExternalInfo);

    static TDictionaryChunkRestoreInfo BuildEmpty(const NArrow::NAccessor::TChunkConstructionData& chunkExternalInfo);
};

// Fetches only the dictionary part of each blob (first DictionaryBlobSize bytes per chunk)
class TDictionaryFetchLogic : public IKernelFetchLogic {
private:
    using TBase = IKernelFetchLogic;

    const NArrow::NAccessor::TChunkConstructionData ChunkExternalInfo;
    std::vector<TDictionaryChunkRestoreInfo> ColumnChunks;
    std::optional<TString> StorageId;

    void DoOnDataCollected(TFetchingResultContext& context) override;
    void DoOnDataReceived(TReadActionsCollection& nextRead, NBlobOperations::NRead::TCompositeReadBlobs& blobs) override;
    void DoStart(TReadActionsCollection& nextRead, TFetchingResultContext& context) override;

public:
    TDictionaryFetchLogic(const ui32 columnId, const std::shared_ptr<IDataSource>& source);
    TDictionaryFetchLogic(const ui32 columnId, const std::shared_ptr<ISnapshotSchema>& sourceSchema,
        const std::shared_ptr<IStoragesManager>& storages, const ui32 recordsCount);
};

}   // namespace NKikimr::NOlap::NReader::NCommon
