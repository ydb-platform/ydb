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
    virtual void DoOnDataCollected() override {
        AFL_VERIFY(!IIndexInfo::IsSpecialColumn(GetColumnId()));
        std::vector<TPortionDataAccessor::TAssembleBlobInfo> chunks;
        for (auto&& i : ColumnChunks) {
            chunks.emplace_back(i.ExtractDataVerified());
        }

        TPortionDataAccessor::TPreparedColumn column(std::move(chunks), Source->GetSourceSchema()->GetColumnLoaderVerified(GetColumnId()));
        Resources->AddVerified(GetColumnId(), column.AssembleAccessor().DetachResult(), true);
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

    virtual void DoStart(TReadActionsCollection& nextRead) override {
        auto existsAccessor = Resources->GetAccessorOptional(GetColumnId());
        if (!!existsAccessor) {
            AFL_VERIFY(existsAccessor->GetType() == NArrow::NAccessor::IChunkedArray::EType::SubColumnsPartialArray);
            Resources->Remove({ GetColumnId() });
        }
        auto columnChunks = Source->GetStageData().GetPortionAccessor().GetColumnChunksPointers(GetColumnId());
        if (columnChunks.empty()) {
            ColumnChunks.emplace_back(Source->GetRecordsCount(), TPortionDataAccessor::TAssembleBlobInfo(Source->GetRecordsCount(),
                                                                     Source->GetSourceSchema()->GetExternalDefaultValueVerified(GetColumnId())));
            return;
        }
        StorageId = Source->GetColumnStorageId(GetColumnId());
        TBlobsAction blobsAction(Source->GetContext()->GetCommonContext()->GetStoragesManager(), NBlobOperations::EConsumer::SCAN);
        auto reading = blobsAction.GetReading(*StorageId);
        auto filterPtr = Source->GetStageData().GetAppliedFilter();
        const NArrow::TColumnFilter& cFilter = filterPtr ? *filterPtr : NArrow::TColumnFilter::BuildAllowFilter();
        auto itFilter = cFilter.GetIterator(false, Source->GetRecordsCount());
        bool itFinished = false;
        for (auto&& c : columnChunks) {
            AFL_VERIFY(!itFinished);
            if (!itFilter.IsBatchForSkip(c->GetMeta().GetRecordsCount())) {
                reading->SetIsBackgroundProcess(false);
                reading->AddRange(Source->RestoreBlobRange(c->BlobRange));
                ColumnChunks.emplace_back(c->GetMeta().GetRecordsCount(), Source->RestoreBlobRange(c->BlobRange));
            } else {
                ColumnChunks.emplace_back(
                    c->GetMeta().GetRecordsCount(), TPortionDataAccessor::TAssembleBlobInfo(c->GetMeta().GetRecordsCount(),
                                                        Source->GetSourceSchema()->GetExternalDefaultValueVerified(c->GetColumnId())));
            }
            itFinished = !itFilter.Next(c->GetMeta().GetRecordsCount());
        }
        AFL_VERIFY(itFinished)("filter", itFilter.DebugString())("count", Source->GetRecordsCount());
        for (auto&& i : blobsAction.GetReadingActions()) {
            nextRead.Add(i);
        }
    }

public:
    TDefaultFetchLogic(const ui32 columnId, const std::shared_ptr<IDataSource>& source)
        : TBase(columnId, source) {
    }
};

}   // namespace NKikimr::NOlap::NReader::NCommon
