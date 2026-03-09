#pragma once
#include "constructor.h"

#include <ydb/core/formats/arrow/accessor/composite/accessor.h>
#include <ydb/core/formats/arrow/accessor/dictionary/additional_data.h>
#include <ydb/core/formats/arrow/accessor/dictionary/constructor.h>
#include <ydb/core/formats/arrow/accessor/common/chunk_data.h>
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_primitive.h>

namespace NKikimr::NOlap::NReader::NCommon {

// Restores one column chunk when we only read the dictionary part of the blob (first DictionaryBlobSize
// bytes). The full blob on disk is unchanged (dictionary + positions); we just request and deserialize
// only the dictionary prefix.
class TDictionaryChunkRestoreInfo {
private:
    const NArrow::NAccessor::TChunkConstructionData ChunkExternalInfo;
    std::optional<TBlobRange> DictionaryBlobRange;
    std::shared_ptr<arrow::Array> DictionaryArray;
    YDB_READONLY_DEF(TBlobRange, FullChunkRange);

public:
    ui32 GetRecordsCount() const {
        return ChunkExternalInfo.GetRecordsCount();
    }

    const std::optional<TBlobRange>& GetDictionaryBlobRangeOptional() const {
        return DictionaryBlobRange;
    }

    void SetDictionaryBlob(const TString& blob) {
        AFL_VERIFY(!!DictionaryBlobRange);
        DictionaryBlobRange = std::nullopt;
        auto conclusion = NArrow::NAccessor::NDictionary::TConstructor::BuildDictionaryOnlyReader(blob, ChunkExternalInfo);
        AFL_VERIFY(conclusion.IsSuccess())("error", conclusion.GetErrorMessage());
        DictionaryArray = conclusion.DetachResult();
    }

    const std::shared_ptr<arrow::Array>& GetDictionaryArray() const {
        return DictionaryArray;
    }

    TDictionaryChunkRestoreInfo(const TBlobRange& fullChunkRange,
        const NArrow::NAccessor::TChunkConstructionData& chunkExternalInfo)
        : ChunkExternalInfo(chunkExternalInfo)
        , FullChunkRange(fullChunkRange) {
        const auto* dictData = dynamic_cast<const NArrow::NAccessor::TDictionaryAccessorData*>(ChunkExternalInfo.GetAdditionalAccessorData().get());
        if (dictData && dictData->DictionaryBlobSize > 0 && fullChunkRange.GetSize() > 0) {
            DictionaryBlobRange = fullChunkRange.BuildSubset(0, dictData->DictionaryBlobSize);
        } else {
            DictionaryArray = NArrow::TStatusValidator::GetValid(arrow::MakeArrayOfNull(chunkExternalInfo.GetColumnType(), 0));
        }
    }

    static TDictionaryChunkRestoreInfo BuildEmpty(const NArrow::NAccessor::TChunkConstructionData& chunkExternalInfo) {
        return TDictionaryChunkRestoreInfo(TBlobRange(), chunkExternalInfo);
    }
};

// Fetches only the dictionary part of each blob (first DictionaryBlobSize bytes per chunk). Result is
// a composite of trivial arrays (one per chunk), each holding that chunk's unique values. No new
// blob layout; we just read a prefix of the existing blob.
class TDictionaryFetchLogic : public IKernelFetchLogic {
private:
    using TBase = IKernelFetchLogic;

    const NArrow::NAccessor::TChunkConstructionData ChunkExternalInfo;
    std::vector<TDictionaryChunkRestoreInfo> ColumnChunks;
    std::optional<TString> StorageId;

    void DoOnDataCollected(TFetchingResultContext& context) override {
        NArrow::NAccessor::TCompositeChunkedArray::TBuilder compositeBuilder(ChunkExternalInfo.GetColumnType());
        for (auto&& i : ColumnChunks) {
            AFL_VERIFY(i.GetDictionaryArray());
            compositeBuilder.AddChunk(std::make_shared<NArrow::NAccessor::TTrivialArray>(i.GetDictionaryArray()));
        }
        context.GetAccessors().AddVerified(GetEntityId(), compositeBuilder.Finish(), true);
    }

    void DoOnDataReceived(TReadActionsCollection& /*nextRead*/, NBlobOperations::NRead::TCompositeReadBlobs& blobs) override {
        AFL_VERIFY(!!StorageId);
        for (auto&& i : ColumnChunks) {
            if (!!i.GetDictionaryBlobRangeOptional()) {
                i.SetDictionaryBlob(blobs.ExtractVerified(*StorageId, *i.GetDictionaryBlobRangeOptional()));
            }
        }
    }

    void DoStart(TReadActionsCollection& nextRead, TFetchingResultContext& context) override {
        auto source = context.GetSource();
        auto columnChunks = source->GetPortionAccessor().GetColumnChunksPointers(GetEntityId());
        AFL_VERIFY(columnChunks.size());
        StorageId = source->GetColumnStorageId(GetEntityId());
        TBlobsAction blobsAction(StoragesManager, NBlobOperations::EConsumer::SCAN);
        auto reading = blobsAction.GetReading(*StorageId);
        reading->SetIsBackgroundProcess(false);
        auto filterPtr = context.GetAppliedFilter();
        const NArrow::TColumnFilter& cFilter = filterPtr ? *filterPtr : NArrow::TColumnFilter::BuildAllowFilter();
        auto itFilter = cFilter.GetBegin(false, context.GetRecordsCount());
        bool itFinished = false;
        for (ui32 chunkIdx = 0; chunkIdx < columnChunks.size(); ++chunkIdx) {
            auto& meta = columnChunks[chunkIdx]->GetMeta();
            AFL_VERIFY(!itFinished);
            if (!itFilter.IsBatchForSkip(meta.GetRecordsCount())) {
                const TBlobRange range = source->RestoreBlobRange(columnChunks[chunkIdx]->BlobRange);
                auto chunkInfo = ChunkExternalInfo.GetSubset(meta.GetRecordsCount())
                    .WithAdditionalData(meta.GetAdditionalAccessorData());
                ColumnChunks.emplace_back(range, chunkInfo);
                reading->AddRange(*ColumnChunks.back().GetDictionaryBlobRangeOptional());
            } else {
                ColumnChunks.emplace_back(TDictionaryChunkRestoreInfo::BuildEmpty(ChunkExternalInfo.GetSubset(meta.GetRecordsCount())));
            }
            itFinished = !itFilter.Next(meta.GetRecordsCount());
        }
        AFL_VERIFY(itFinished)("filter", itFilter.DebugString())("count", context.GetRecordsCount());
        for (auto&& i : blobsAction.GetReadingActions()) {
            nextRead.Add(i);
        }
    }

public:
    TDictionaryFetchLogic(const ui32 columnId, const std::shared_ptr<IDataSource>& source)
        : TBase(columnId, source->GetContext()->GetCommonContext()->GetStoragesManager())
        , ChunkExternalInfo(source->GetSourceSchema()->GetColumnLoaderVerified(GetEntityId())->BuildAccessorContext(source->GetRecordsCount())) {
        const auto loader = source->GetSourceSchema()->GetColumnLoaderVerified(GetEntityId());
        AFL_VERIFY(loader->GetAccessorConstructor()->GetType() == NArrow::NAccessor::IChunkedArray::EType::Dictionary)(
            "type", loader->GetAccessorConstructor()->GetType());
    }

    TDictionaryFetchLogic(const ui32 columnId, const std::shared_ptr<ISnapshotSchema>& sourceSchema,
        const std::shared_ptr<IStoragesManager>& storages, const ui32 recordsCount)
        : TBase(columnId, storages)
        , ChunkExternalInfo(sourceSchema->GetColumnLoaderVerified(GetEntityId())->BuildAccessorContext(recordsCount)) {
        const auto loader = sourceSchema->GetColumnLoaderVerified(GetEntityId());
        AFL_VERIFY(loader->GetAccessorConstructor()->GetType() == NArrow::NAccessor::IChunkedArray::EType::Dictionary)(
            "type", loader->GetAccessorConstructor()->GetType());
    }
};

}   // namespace NKikimr::NOlap::NReader::NCommon
