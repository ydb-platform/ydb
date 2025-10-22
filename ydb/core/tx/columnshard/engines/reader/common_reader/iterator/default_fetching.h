#pragma once
#include "constructor.h"

namespace NKikimr::NOlap::NReader::NCommon {

class TDefaultFetchLogic: public IKernelFetchLogic {
private:
    using TBase = IKernelFetchLogic;
    std::optional<bool> IsEmptyChunks;

    std::shared_ptr<NArrow::NAccessor::TColumnLoader> GetColumnLoader(const std::shared_ptr<NCommon::IDataSource>& source) const {
        if (auto loader = source->GetSourceSchema()->GetColumnLoaderOptional(GetEntityId())) {
            return loader;
        }
        AFL_VERIFY(IsEmptyChunks && *IsEmptyChunks);
        return source->GetContext()->GetReadMetadata()->GetResultSchema()->GetColumnLoaderVerified(GetEntityId());
    }

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
            BlobRange.reset();
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

        TPortionDataAccessor::TPreparedColumn column(std::move(chunks), GetColumnLoader(context.GetSource()));
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
            i.SetBlobData(blobs.ExtractVerified(*StorageId, *i.GetBlobRangeOptional()));
        }
    }

    virtual void DoStart(TReadActionsCollection& nextRead, TFetchingResultContext& context) override {
        auto source = context.GetSource();
        auto columnChunks = source->GetPortionAccessor().GetColumnChunksPointers(GetEntityId());
        IsEmptyChunks.emplace(columnChunks.empty());
        if (columnChunks.empty()) {
            ColumnChunks.emplace_back(source->GetRecordsCount(),
                TPortionDataAccessor::TAssembleBlobInfo(source->GetRecordsCount(), GetColumnLoader(context.GetSource())->GetDefaultValue()));
            return;
        }
        StorageId = source->GetColumnStorageId(GetEntityId());
        TBlobsAction blobsAction(source->GetContext()->GetCommonContext()->GetStoragesManager(), NBlobOperations::EConsumer::SCAN);
        auto reading = blobsAction.GetReading(*StorageId);
        auto filterPtr = context.GetAppliedFilter();
        const NArrow::TColumnFilter& cFilter = filterPtr ? *filterPtr : NArrow::TColumnFilter::BuildAllowFilter();
        auto itFilter = cFilter.GetBegin(false, source->GetRecordsCount());
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
