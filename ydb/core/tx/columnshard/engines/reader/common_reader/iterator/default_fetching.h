#pragma once
#include "constructor.h"

namespace NKikimr::NOlap::NReader::NCommon {

class TDefaultFetchLogic: public IKernelFetchLogic {
private:
    using TBase = IKernelFetchLogic;

    class TChunkRestoreInfo {
    private:
        std::optional<TBlobRange> BlobRange;
        std::optional<TPortionDataAccessor::TAssembleBlobInfo> Data;
        const ui32 RecordsCount;

    public:
        TChunkRestoreInfo(const ui32 recordsCount, const TBlobRange& range)
            : BlobRange(range)
            , RecordsCount(recordsCount) {
        }

        const std::optional<TBlobRange>& GetBlobRangeOptional() const {
            return BlobRange;
        }

        TChunkRestoreInfo(const ui32 recordsCount, const TPortionDataAccessor::TAssembleBlobInfo& defaultData)
            : Data(defaultData)
            , RecordsCount(recordsCount) {
        }

        TPortionDataAccessor::TAssembleBlobInfo ExtractDataVerified() {
            AFL_VERIFY(!!Data);
            Data->SetExpectedRecordsCount(RecordsCount);
            return std::move(*Data);
        }

        void SetBlobData(const TString& data) {
            AFL_VERIFY(!Data);
            Data.emplace(data);
        }
    };

    std::vector<TChunkRestoreInfo> ColumnChunks;
    std::optional<TString> StorageId;
    virtual void DoOnDataCollected(TFetchingResultContext& context) override {
        AFL_VERIFY(!IIndexInfo::IsSpecialColumn(GetEntityId()));
        std::vector<TPortionDataAccessor::TAssembleBlobInfo> chunks;
        for (auto&& i : ColumnChunks) {
            chunks.emplace_back(i.ExtractDataVerified());
        }

        TPortionDataAccessor::TPreparedColumn column(
            std::move(chunks), context.GetSource()->GetSourceSchema()->GetColumnLoaderVerified(GetEntityId()));
        context.GetAccessors().AddVerified(GetEntityId(), column.AssembleAccessor().DetachResult(), true);
    }

    virtual void DoOnDataReceived(TReadActionsCollection& /*nextRead*/, NBlobOperations::NRead::TCompositeReadBlobs& blobs) override {
        if (ColumnChunks.empty()) {
            return;
        }
        for (auto&& i : ColumnChunks) {
            if (!i.GetBlobRangeOptional()) {
                continue;
            }
            AFL_VERIFY(!!StorageId);
            i.SetBlobData(blobs.Extract(*StorageId, *i.GetBlobRangeOptional()));
        }
    }

    virtual void DoStart(TReadActionsCollection& nextRead, TFetchingResultContext& context) override {
        auto source = context.GetSource();
        auto columnChunks = source->GetStageData().GetPortionAccessor().GetColumnChunksPointers(GetEntityId());
        if (columnChunks.empty()) {
            ColumnChunks.emplace_back(source->GetRecordsCount(), TPortionDataAccessor::TAssembleBlobInfo(source->GetRecordsCount(),
                                                                     source->GetSourceSchema()->GetExternalDefaultValueVerified(GetEntityId())));
            return;
        }
        StorageId = source->GetColumnStorageId(GetEntityId());
        TBlobsAction blobsAction(source->GetContext()->GetCommonContext()->GetStoragesManager(), NBlobOperations::EConsumer::SCAN);
        auto reading = blobsAction.GetReading(*StorageId);
        auto filterPtr = source->GetStageData().GetAppliedFilter();
        const NArrow::TColumnFilter& cFilter = filterPtr ? *filterPtr : NArrow::TColumnFilter::BuildAllowFilter();
        auto itFilter = cFilter.GetIterator(false, source->GetRecordsCount());
        bool itFinished = false;
        for (auto&& c : columnChunks) {
            AFL_VERIFY(!itFinished);
            if (!itFilter.IsBatchForSkip(c->GetMeta().GetRecordsCount())) {
                reading->SetIsBackgroundProcess(false);
                reading->AddRange(source->RestoreBlobRange(c->BlobRange));
                ColumnChunks.emplace_back(c->GetMeta().GetRecordsCount(), source->RestoreBlobRange(c->BlobRange));
            } else {
                ColumnChunks.emplace_back(
                    c->GetMeta().GetRecordsCount(), TPortionDataAccessor::TAssembleBlobInfo(c->GetMeta().GetRecordsCount(),
                                                        source->GetSourceSchema()->GetExternalDefaultValueVerified(c->GetEntityId())));
            }
            itFinished = !itFilter.Next(c->GetMeta().GetRecordsCount());
        }
        AFL_VERIFY(itFinished)("filter", itFilter.DebugString())("count", source->GetRecordsCount());
        for (auto&& i : blobsAction.GetReadingActions()) {
            nextRead.Add(i);
        }
    }

public:
    TDefaultFetchLogic(const ui32 entityId, const std::shared_ptr<IStoragesManager>& storagesManager)
        : TBase(entityId, storagesManager) {
    }
};

}   // namespace NKikimr::NOlap::NReader::NCommon
