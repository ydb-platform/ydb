#include "dictionary_fetching.h"

#include <ydb/core/formats/arrow/accessor/composite/accessor.h>
#include <ydb/core/formats/arrow/accessor/dictionary/additional_data.h>
#include <ydb/core/formats/arrow/accessor/dictionary/constructor.h>
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_primitive.h>

namespace NKikimr::NOlap::NReader::NCommon {

ui32 TDictionaryChunkRestoreInfo::GetRecordsCount() const {
    return ChunkExternalInfo.GetRecordsCount();
}

const std::optional<TBlobRange>& TDictionaryChunkRestoreInfo::GetDictionaryBlobRangeOptional() const {
    return DictionaryBlobRange;
}

void TDictionaryChunkRestoreInfo::SetDictionaryBlob(TString&& blob) {
    AFL_VERIFY(!!DictionaryBlobRange);
    DictionaryBlobRange = std::nullopt;
    auto conclusion = NArrow::NAccessor::NDictionary::TConstructor::BuildDictionaryOnlyReader(blob, ChunkExternalInfo);
    AFL_VERIFY(conclusion.IsSuccess())("error", conclusion.GetErrorMessage());
    DictionaryArray = conclusion.DetachResult();
}

const std::shared_ptr<arrow::Array>& TDictionaryChunkRestoreInfo::GetDictionaryArray() const {
    return DictionaryArray;
}

TDictionaryChunkRestoreInfo::TDictionaryChunkRestoreInfo(const TBlobRange& fullChunkRange,
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

TDictionaryChunkRestoreInfo TDictionaryChunkRestoreInfo::BuildEmpty(const NArrow::NAccessor::TChunkConstructionData& chunkExternalInfo) {
    return TDictionaryChunkRestoreInfo(TBlobRange(), chunkExternalInfo);
}

void TDictionaryFetchLogic::DoOnDataCollected(TFetchingResultContext& context) {
    NArrow::NAccessor::TCompositeChunkedArray::TBuilder compositeBuilder(ChunkExternalInfo.GetColumnType());
    for (auto&& i : ColumnChunks) {
        const auto& dictArray = i.GetDictionaryArray();
        AFL_VERIFY(dictArray);
        compositeBuilder.AddChunk(std::make_shared<NArrow::NAccessor::TTrivialArray>(dictArray));
    }
    context.GetAccessors().AddVerified(GetEntityId(), compositeBuilder.Finish(), true);
}

void TDictionaryFetchLogic::DoOnDataReceived(TReadActionsCollection& /*nextRead*/, NBlobOperations::NRead::TCompositeReadBlobs& blobs) {
    AFL_VERIFY(!!StorageId);
    for (auto&& i : ColumnChunks) {
        if (!!i.GetDictionaryBlobRangeOptional()) {
            i.SetDictionaryBlob(blobs.ExtractVerified(*StorageId, *i.GetDictionaryBlobRangeOptional()));
        }
    }
}

void TDictionaryFetchLogic::DoStart(TReadActionsCollection& nextRead, TFetchingResultContext& context) {
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
                .WithAdditionalAccessorData(meta.GetAdditionalAccessorData());
            ColumnChunks.emplace_back(range, chunkInfo);
            const auto dictBlobRange = ColumnChunks.back().GetDictionaryBlobRangeOptional();
            AFL_VERIFY(dictBlobRange.has_value());
            reading->AddRange(dictBlobRange.value());
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

TDictionaryFetchLogic::TDictionaryFetchLogic(const ui32 columnId, const std::shared_ptr<IDataSource>& source)
    : TBase(columnId, source->GetContext()->GetCommonContext()->GetStoragesManager())
    , ChunkExternalInfo(source->GetSourceSchema()->GetColumnLoaderVerified(GetEntityId())->BuildAccessorContext(source->GetRecordsCount())) {
    const auto loader = source->GetSourceSchema()->GetColumnLoaderVerified(GetEntityId());
    AFL_VERIFY(loader->GetAccessorConstructor()->GetType() == NArrow::NAccessor::IChunkedArray::EType::Dictionary)(
        "type", loader->GetAccessorConstructor()->GetType());
}

TDictionaryFetchLogic::TDictionaryFetchLogic(const ui32 columnId, const std::shared_ptr<ISnapshotSchema>& sourceSchema,
    const std::shared_ptr<IStoragesManager>& storages, const ui32 recordsCount)
    : TBase(columnId, storages)
    , ChunkExternalInfo(sourceSchema->GetColumnLoaderVerified(GetEntityId())->BuildAccessorContext(recordsCount)) {
    const auto loader = sourceSchema->GetColumnLoaderVerified(GetEntityId());
    AFL_VERIFY(loader->GetAccessorConstructor()->GetType() == NArrow::NAccessor::IChunkedArray::EType::Dictionary)(
        "type", loader->GetAccessorConstructor()->GetType());
}

}   // namespace NKikimr::NOlap::NReader::NCommon
