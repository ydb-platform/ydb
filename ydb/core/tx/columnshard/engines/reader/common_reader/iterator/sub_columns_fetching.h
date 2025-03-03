#pragma once
#include "constructor.h"

#include <ydb/core/formats/arrow/accessor/common/chunk_data.h>
#include <ydb/core/formats/arrow/accessor/composite/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/constructor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/fetching.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/partial.h>

namespace NKikimr::NOlap::NReader::NCommon {

class TSubColumnChunkRestoreInfo {
private:
    std::optional<TBlobRange> BlobRange;
    std::optional<TString> BlobData;
    YDB_READONLY(ui32, ColumnIdx, 0);

public:
    TSubColumnChunkRestoreInfo(const TBlobRange& range, const ui32 columnIdx)
        : BlobRange(range)
        , ColumnIdx(columnIdx) {
    }

    const std::optional<TBlobRange>& GetBlobRangeOptional() const {
        return BlobRange;
    }

    TString GetBlobDataVerified() const {
        AFL_VERIFY(!!BlobData);
        return *BlobData;
    }

    void SetBlobData(const TString& data) {
        AFL_VERIFY(!!BlobRange);
        BlobRange = std::nullopt;
        AFL_VERIFY(!BlobData);
        BlobData = data;
    }
};

class TColumnChunkRestoreInfo {
private:
    const NArrow::NAccessor::TChunkConstructionData ChunkExternalInfo;
    YDB_ACCESSOR_DEF(std::optional<NArrow::NAccessor::NSubColumns::THeaderFetchingLogic>, HeaderFetching);
    THashMap<TString, TSubColumnChunkRestoreInfo> Chunks;
    std::shared_ptr<NArrow::NAccessor::TSubColumnsPartialArray> PartialArray;
    YDB_READONLY_DEF(TBlobRange, FullChunkRange);
    YDB_ACCESSOR_DEF(std::optional<TBlobRange>, OthersReadData);
    YDB_READONLY_DEF(std::optional<TString>, OthersBlobs);

public:
    void SetOthersBlob(const TString& blob) {
        AFL_VERIFY(!!OthersReadData);
        OthersReadData = std::nullopt;
        OthersBlobs = blob;
    }

    ui32 GetRecordsCount() const {
        return ChunkExternalInfo.GetRecordsCount();
    }

    void Finish(const std::shared_ptr<NArrow::TColumnFilter>& applyFilter, const std::shared_ptr<IDataSource>& source) {
        const bool deserialize = source->IsSourceInMemory();
        if (!!OthersBlobs) {
            source->GetContext()->GetCommonContext()->GetCounters().GetSubColumns()->GetOtherCounters().OnRead(OthersBlobs->size());
            PartialArray->InitOthers(*OthersBlobs, ChunkExternalInfo, deserialize);
            OthersBlobs.reset();
        }

        AFL_VERIFY(PartialArray);
        AFL_VERIFY(!OthersReadData);
        for (auto&& i : Chunks) {
            std::shared_ptr<TColumnLoader> columnLoader = std::make_shared<TColumnLoader>(ChunkExternalInfo.GetDefaultSerializer(),
                PartialArray->GetHeader().GetAccessorConstructor(i.second.GetColumnIdx()),
                PartialArray->GetHeader().GetField(i.second.GetColumnIdx()), nullptr, 0);
            source->GetContext()->GetCommonContext()->GetCounters().GetSubColumns()->GetColumnCounters().OnRead(
                i.second.GetBlobDataVerified().size());
            std::vector<NArrow::NAccessor::TDeserializeChunkedArray::TChunk> chunks = { NArrow::NAccessor::TDeserializeChunkedArray::TChunk(
                GetRecordsCount(), i.second.GetBlobDataVerified()) };
//            const ui32 filledRecordsCount = PartialArray->GetHeader().GetColumnStats().GetColumnRecordsCount(i.second.GetColumnIdx());
            const std::shared_ptr<NArrow::NAccessor::IChunkedArray> arrOriginal =
                deserialize
                    ? columnLoader->ApplyVerified(i.second.GetBlobDataVerified(), GetRecordsCount()/*, filledRecordsCount*/)
                    : std::make_shared<NArrow::NAccessor::TDeserializeChunkedArray>(GetRecordsCount(), columnLoader, std::move(chunks), true);
            if (applyFilter) {
                PartialArray->AddColumn(i.first, applyFilter->Apply(arrOriginal));
            } else {
                PartialArray->AddColumn(i.first, arrOriginal);
            }
        }
    }

    void InitReading(const std::shared_ptr<IBlobsReadingAction>& reading, const std::vector<TString>& subColumns) {
        AFL_VERIFY(!!PartialArray);
        for (auto&& subColumnName : subColumns) {
            if (auto colIndex = PartialArray->GetHeader().GetColumnStats().GetKeyIndexOptional(subColumnName)) {
                auto colBlobRange = PartialArray->GetColumnReadRange(*colIndex);
                const TBlobRange subRange = FullChunkRange.BuildSubset(colBlobRange.GetOffset(), colBlobRange.GetSize());
                reading->AddRange(subRange);
                AddFetchData(subColumnName, subRange, *colIndex);
            } else if (!PartialArray->HasOthers() && !OthersReadData && PartialArray->IsOtherColumn(subColumnName)) {
                auto readRange = PartialArray->GetHeader().GetOthersReadRange();
                OthersReadData = FullChunkRange.BuildSubset(readRange.GetOffset(), readRange.GetSize());
                reading->AddRange(*OthersReadData);
            }
        }
    }

    const std::shared_ptr<NArrow::NAccessor::TSubColumnsPartialArray>& GetPartialArray() const {
        AFL_VERIFY(PartialArray);
        return PartialArray;
    }

    void InitPartialReader(NArrow::NAccessor::NSubColumns::TSubColumnsHeader&& header) {
        AFL_VERIFY(!PartialArray);
        PartialArray = std::make_shared<NArrow::NAccessor::TSubColumnsPartialArray>(
            std::move(header), ChunkExternalInfo.GetRecordsCount(), ChunkExternalInfo.GetColumnType());
    }

    void InitPartialReader(const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& accessor) {
        AFL_VERIFY(!PartialArray);
        AFL_VERIFY(accessor);
        AFL_VERIFY(accessor->GetType() == NArrow::NAccessor::IChunkedArray::EType::SubColumnsPartialArray)("type", accessor->GetType());
        PartialArray = std::static_pointer_cast<NArrow::NAccessor::TSubColumnsPartialArray>(accessor);
    }

    TColumnChunkRestoreInfo(const TBlobRange& fullChunkRange, const NArrow::NAccessor::TChunkConstructionData& chunkExternalInfo)
        : ChunkExternalInfo(chunkExternalInfo)
        , FullChunkRange(fullChunkRange) {
    }

    static TColumnChunkRestoreInfo BuildEmpty(const NArrow::NAccessor::TChunkConstructionData& chunkExternalInfo) {
        TColumnChunkRestoreInfo result(TBlobRange(), chunkExternalInfo);
        result.PartialArray =
            NArrow::NAccessor::TSubColumnsPartialArray::BuildEmpty(chunkExternalInfo.GetColumnType(), chunkExternalInfo.GetRecordsCount());
        return result;
    }

    THashMap<TString, TSubColumnChunkRestoreInfo>& MutableChunks() {
        return Chunks;
    }

    void AddFetchData(const TString& subColName, const TBlobRange& subRange, const ui32 colIndex) {
        const std::string_view keyName(subColName.data(), subColName.size());
        AFL_VERIFY(Chunks.emplace(subColName, TSubColumnChunkRestoreInfo(subRange, colIndex)).second);
    }

    void InitHeaderFetching(const std::shared_ptr<IBlobsStorageOperator>& storageOperator) {
        AFL_VERIFY(!HeaderFetching);
        HeaderFetching.emplace(NArrow::NAccessor::NSubColumns::THeaderFetchingLogic({ { FullChunkRange, ChunkExternalInfo } }, storageOperator));
    }
};

class TSubColumnsFetchLogic: public IKernelFetchLogic {
private:
    using TBase = IKernelFetchLogic;

    const NArrow::NAccessor::TChunkConstructionData ChunkExternalInfo;
    const std::vector<TString> SubColumns;

    std::vector<TColumnChunkRestoreInfo> ColumnChunks;
    std::optional<TString> StorageId;
    bool NeedToAddResource = false;

    virtual void DoOnDataCollected() override {
        if (NeedToAddResource) {
            NArrow::NAccessor::TCompositeChunkedArray::TBuilder compositeBuilder(ChunkExternalInfo.GetColumnType());
            for (auto&& i : ColumnChunks) {
                i.Finish(nullptr, Source);
                compositeBuilder.AddChunk(i.GetPartialArray());
            }
            Resources->AddVerified(GetColumnId(), compositeBuilder.Finish(), true);
        } else {
            ui32 pos = 0;
            for (auto&& i : ColumnChunks) {
                i.Finish(std::make_shared<NArrow::TColumnFilter>(Resources->GetAppliedFilter()->Slice(pos, i.GetRecordsCount())), Source);
                pos += i.GetRecordsCount();
            }
        }
    }

    virtual void DoOnDataReceived(TReadActionsCollection& nextRead, NBlobOperations::NRead::TCompositeReadBlobs& blobs) override {
        AFL_VERIFY(ColumnChunks.size());
        AFL_VERIFY(!!StorageId);
        TBlobsAction blobsAction(Source->GetContext()->GetCommonContext()->GetStoragesManager(), NBlobOperations::EConsumer::SCAN);
        auto reading = blobsAction.GetReading(*StorageId);
        for (auto&& i : ColumnChunks) {
            if (auto&& headerFetching = i.MutableHeaderFetching()) {
                AFL_VERIFY(!headerFetching->IsDone());
                headerFetching->OnDataFetched(blobs, nextRead);
                if (headerFetching->IsDone()) {
                    auto headers = std::move(*headerFetching).ExtractResults();
                    AFL_VERIFY(headers.size() == 1);
                    i.InitPartialReader(std::move(headers.begin()->second));
                    i.InitReading(reading, SubColumns);
                    i.MutableHeaderFetching().reset();
                }
            }
            if (!!i.GetOthersReadData()) {
                i.SetOthersBlob(blobs.Extract(*StorageId, *i.GetOthersReadData()));
            }
            for (auto&& [subColName, chunkData] : i.MutableChunks()) {
                if (!!chunkData.GetBlobRangeOptional()) {
                    chunkData.SetBlobData(blobs.Extract(*StorageId, *chunkData.GetBlobRangeOptional()));
                }
            }
        }
        nextRead.Add(reading);
    }

    virtual void DoStart(TReadActionsCollection& nextRead) override {
        auto columnChunks = Source->GetStageData().GetPortionAccessor().GetColumnChunksPointers(GetColumnId());
        AFL_VERIFY(columnChunks.size());
        StorageId = Source->GetColumnStorageId(GetColumnId());
        auto filterPtr = Source->GetStageData().GetAppliedFilter();
        const NArrow::TColumnFilter& cFilter = filterPtr ? *filterPtr : NArrow::TColumnFilter::BuildAllowFilter();
        auto itFilter = cFilter.GetIterator(false, Source->GetRecordsCount());
        bool itFinished = false;

        auto accessor = Resources->GetAccessorOptional(GetColumnId());
        NeedToAddResource = !accessor;
        std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> chunks;
        if (!NeedToAddResource) {
            if (accessor->GetType() == NArrow::NAccessor::IChunkedArray::EType::CompositeChunkedArray) {
                auto composite = std::static_pointer_cast<NArrow::NAccessor::TCompositeChunkedArray>(accessor);
                chunks = composite->GetChunks();
            } else {
                chunks.emplace_back(accessor);
            }
        }
        ui32 resChunkIdx = 0;
        for (ui32 chunkIdx = 0; chunkIdx < columnChunks.size(); ++chunkIdx) {
            auto& meta = columnChunks[chunkIdx]->GetMeta();
            AFL_VERIFY(!itFinished);
            if (!itFilter.IsBatchForSkip(meta.GetRecordsCount())) {
                const TBlobRange range = Source->RestoreBlobRange(columnChunks[chunkIdx]->BlobRange);
                ColumnChunks.emplace_back(range, ChunkExternalInfo.GetSubset(meta.GetRecordsCount()));
                if (NeedToAddResource) {
                    ColumnChunks.back().InitHeaderFetching(
                        Source->GetContext()->GetCommonContext()->GetStoragesManager()->GetOperator(*StorageId));
                    ColumnChunks.back().MutableHeaderFetching()->Start(nextRead);
                } else {
                    AFL_VERIFY(resChunkIdx < chunks.size())("chunks", chunks.size())("meta", columnChunks.size())("need", NeedToAddResource);
                    ColumnChunks.back().InitPartialReader(chunks[resChunkIdx]);
                    ++resChunkIdx;
                }
            } else {
                ColumnChunks.emplace_back(TColumnChunkRestoreInfo::BuildEmpty(ChunkExternalInfo.GetSubset(meta.GetRecordsCount())));
            }
            itFinished = !itFilter.Next(meta.GetRecordsCount());
        }
        AFL_VERIFY(NeedToAddResource || (resChunkIdx == chunks.size()));
        AFL_VERIFY(itFinished)("filter", itFilter.DebugString())("count", Source->GetRecordsCount());
    }

public:
    TSubColumnsFetchLogic(const ui32 columnId, const std::shared_ptr<IDataSource>& source, const std::vector<TString>& subColumns)
        : TBase(columnId, source)
        , ChunkExternalInfo(Source->GetSourceSchema()->GetColumnLoaderVerified(GetColumnId())->BuildAccessorContext(Source->GetRecordsCount()))
        , SubColumns(subColumns) {
        const auto loader = Source->GetSourceSchema()->GetColumnLoaderVerified(GetColumnId());
        AFL_VERIFY(loader->GetAccessorConstructor()->GetType() == NArrow::NAccessor::IChunkedArray::EType::SubColumnsArray)
        ("type", loader->GetAccessorConstructor()->GetType());
    }
};

}   // namespace NKikimr::NOlap::NReader::NCommon
