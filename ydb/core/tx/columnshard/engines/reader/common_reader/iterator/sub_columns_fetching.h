#pragma once
#include "constructor.h"

#include <ydb/core/formats/arrow/accessor/common/chunk_data.h>
#include <ydb/core/formats/arrow/accessor/composite/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/constructor.h>
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
    THashMap<TString, TSubColumnChunkRestoreInfo> Chunks;
    YDB_ACCESSOR_DEF(std::optional<TBlobRange>, HeaderRange);
    std::shared_ptr<NArrow::NAccessor::TSubColumnsPartialArray> PartialArray;
    YDB_READONLY(bool, NeedToAddResource, false);
    const TBlobRange FullChunkRange;
    YDB_ACCESSOR_DEF(std::optional<TBlobRange>, OthersReadData);

public:
    void InitOthers(const TString& blob) {
        AFL_VERIFY(!!OthersReadData);
        PartialArray->InitOthers(blob, ChunkExternalInfo);
        OthersReadData.reset();
    }

    ui32 GetRecordsCount() const {
        return ChunkExternalInfo.GetRecordsCount();
    }

    void Finish() {
        AFL_VERIFY(PartialArray);
        AFL_VERIFY(!HeaderRange);
        AFL_VERIFY(!OthersReadData);
        for (auto&& i : Chunks) {
            std::shared_ptr<TColumnLoader> columnLoader = std::make_shared<TColumnLoader>(ChunkExternalInfo.GetDefaultSerializer(),
                PartialArray->GetHeader().GetAccessorConstructor(i.second.GetColumnIdx()),
                PartialArray->GetHeader().GetField(i.second.GetColumnIdx()), nullptr, 0);
            std::vector<NArrow::NAccessor::TDeserializeChunkedArray::TChunk> chunks = { NArrow::NAccessor::TDeserializeChunkedArray::TChunk(
                GetRecordsCount(), i.second.GetBlobDataVerified()) };
            PartialArray->AddColumn(i.first,
                std::make_shared<NArrow::NAccessor::TDeserializeChunkedArray>(GetRecordsCount(), columnLoader, std::move(chunks), true));
        }
    }

    void InitReading(const std::shared_ptr<IBlobsReadingAction>& reading, const std::vector<TString>& subColumns) {
        AFL_VERIFY(!HeaderRange);
        if (!!PartialArray) {
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
        } else {
            HeaderRange = FullChunkRange.BuildSubset(0, std::min<ui32>(FullChunkRange.GetSize(), 4096));
            reading->AddRange(*HeaderRange);
        }
    }

    const std::shared_ptr<NArrow::NAccessor::TSubColumnsPartialArray>& GetPartialArray() const {
        AFL_VERIFY(PartialArray);
        return PartialArray;
    }

    void InitPartialReader(const TString& blob) {
        AFL_VERIFY(!!HeaderRange);
        AFL_VERIFY(!PartialArray);
        HeaderRange = std::nullopt;
        NeedToAddResource = true;
        PartialArray = NArrow::NAccessor::NSubColumns::TConstructor::BuildPartialReader(blob, ChunkExternalInfo).DetachResult();
    }

    void InitPartialReader(
        const ui32 columnId, const ui32 positionStart, const std::shared_ptr<NArrow::NAccessor::TAccessorsCollection>& resources) {
        AFL_VERIFY(!HeaderRange);
        AFL_VERIFY(!PartialArray);
        NeedToAddResource = false;
        auto columnAccessor = resources->GetAccessorVerified(columnId);
        auto partialArray = columnAccessor->GetArraySlow(positionStart, columnAccessor);
        AFL_VERIFY(partialArray.GetArray()->GetType() == NArrow::NAccessor::IChunkedArray::EType::SubColumnsPartialArray);
        PartialArray = std::static_pointer_cast<NArrow::NAccessor::TSubColumnsPartialArray>(partialArray.GetArray());
    }

    TColumnChunkRestoreInfo(const TBlobRange& fullChunkRange, const NArrow::NAccessor::TChunkConstructionData& chunkExternalInfo)
        : ChunkExternalInfo(chunkExternalInfo)
        , FullChunkRange(fullChunkRange) {
    }

    static TColumnChunkRestoreInfo BuildEmpty(const NArrow::NAccessor::TChunkConstructionData& chunkExternalInfo) {
        TColumnChunkRestoreInfo result(TBlobRange(), chunkExternalInfo);
        result.PartialArray = NArrow::NAccessor::TSubColumnsPartialArray::BuildEmpty(chunkExternalInfo.GetColumnType());
        return result;
    }

    const THashMap<TString, TSubColumnChunkRestoreInfo>& GetChunks() const {
        return Chunks;
    }

    THashMap<TString, TSubColumnChunkRestoreInfo>& MutableChunks() {
        return Chunks;
    }

    void AddFetchData(const TString& subColName, const TBlobRange& subRange, const ui32 colIndex) {
        const std::string_view keyName(subColName.data(), subColName.size());
        AFL_VERIFY(Chunks.emplace(subColName, TSubColumnChunkRestoreInfo(subRange, colIndex)).second);
    }
};

class TSubColumnsFetchLogic: public IKernelFetchLogic {
private:
    using TBase = IKernelFetchLogic;

    const NArrow::NAccessor::TChunkConstructionData ChunkExternalInfo;
    const std::vector<TString> SubColumns;

    std::vector<TColumnChunkRestoreInfo> ColumnChunks;
    std::optional<TString> StorageId;
    virtual void DoOnDataCollected() override {
        std::optional<bool> needToAddResource;
        for (auto&& i : ColumnChunks) {
            if (!needToAddResource) {
                needToAddResource = i.GetNeedToAddResource();
            } else {
                AFL_VERIFY(needToAddResource == i.GetNeedToAddResource());
            }
        }
        if (*needToAddResource) {
            NArrow::NAccessor::TCompositeChunkedArray::TBuilder compositeBuilder(ChunkExternalInfo.GetColumnType());
            for (auto&& i : ColumnChunks) {
                i.Finish();
                compositeBuilder.AddChunk(i.GetPartialArray());
            }
            Resources->AddVerified(GetColumnId(), compositeBuilder.Finish());
        } else {
            for (auto&& i : ColumnChunks) {
                i.Finish();
            }
        }
    }

    virtual void DoOnDataReceived(TReadActionsCollection& nextRead, NBlobOperations::NRead::TCompositeReadBlobs& blobs) override {
        AFL_VERIFY(ColumnChunks.size());
        AFL_VERIFY(!!StorageId);
        TBlobsAction blobsAction(Source->GetContext()->GetCommonContext()->GetStoragesManager(), NBlobOperations::EConsumer::SCAN);
        auto reading = blobsAction.GetReading(*StorageId);
        for (auto&& i : ColumnChunks) {
            if (!!i.GetHeaderRange()) {
                auto blob = blobs.Extract(*StorageId, *i.GetHeaderRange());
                const auto fullHeader = NArrow::NAccessor::NSubColumns::TConstructor::GetFullHeaderSize(blob);
                if (!fullHeader.IsFail() && *fullHeader <= i.GetHeaderRange()->GetSize()) {
                    i.InitPartialReader(blob);
                    i.InitReading(reading, SubColumns);
                } else {
                    ui32 size = 0;
                    if (fullHeader.IsFail()) {
                        size = NArrow::NAccessor::NSubColumns::TConstructor::GetHeaderSize(blob).DetachResult();
                    } else {
                        size = *fullHeader;
                    }
                    const TBlobRange headerRange = i.GetHeaderRange()->ExtendRange(size);
                    reading->AddRange(headerRange);
                    i.SetHeaderRange(headerRange);
                }
            } else {
                if (!!i.GetOthersReadData()) {
                    i.InitOthers(blobs.Extract(*StorageId, *i.GetOthersReadData()));
                }
                for (auto&& [subColName, chunkData] : i.MutableChunks()) {
                    if (!!chunkData.GetBlobRangeOptional()) {
                        chunkData.SetBlobData(blobs.Extract(*StorageId, *chunkData.GetBlobRangeOptional()));
                    }
                }
            }
        }
        nextRead.Add(reading);
    }

    virtual void DoStart(TReadActionsCollection& nextRead) override {
        auto columnChunks = Source->GetStageData().GetPortionAccessor().GetColumnChunksPointers(GetColumnId());
        AFL_VERIFY(columnChunks.size());
        StorageId = Source->GetColumnStorageId(GetColumnId());
        TBlobsAction blobsAction(Source->GetContext()->GetCommonContext()->GetStoragesManager(), NBlobOperations::EConsumer::SCAN);
        auto reading = blobsAction.GetReading(*StorageId);
        reading->SetIsBackgroundProcess(false);
        auto filterPtr = Source->GetStageData().GetAppliedFilter();
        const NArrow::TColumnFilter& cFilter = filterPtr ? *filterPtr : NArrow::TColumnFilter::BuildAllowFilter();
        auto itFilter = cFilter.GetIterator(false, Source->GetRecordsCount());
        bool itFinished = false;

        const bool hasColumn = Resources->HasColumn(GetColumnId());
        ui32 posCurrent = 0;
        for (auto&& c : columnChunks) {
            AFL_VERIFY(!itFinished);
            if (!itFilter.IsBatchForSkip(c->GetMeta().GetRecordsCount())) {
                const TBlobRange range = Source->RestoreBlobRange(c->BlobRange);
                ColumnChunks.emplace_back(range, ChunkExternalInfo.GetSubset(c->GetMeta().GetRecordsCount()));
                if (hasColumn) {
                    ColumnChunks.back().InitPartialReader(GetColumnId(), posCurrent, Resources);
                }
                ColumnChunks.back().InitReading(reading, SubColumns);
            } else {
                ColumnChunks.emplace_back(TColumnChunkRestoreInfo::BuildEmpty(ChunkExternalInfo));
            }
            itFinished = !itFilter.Next(c->GetMeta().GetRecordsCount());
            posCurrent += c->GetMeta().GetRecordsCount();
        }
        AFL_VERIFY(itFinished)("filter", itFilter.DebugString())("count", Source->GetRecordsCount());
        for (auto&& i : blobsAction.GetReadingActions()) {
            nextRead.Add(i);
        }
    }

public:
    TSubColumnsFetchLogic(const ui32 columnId, const std::shared_ptr<IDataSource>& source, const std::vector<TString>& subColumns)
        : TBase(columnId, source)
        , ChunkExternalInfo(Source->GetSourceSchema()->GetColumnLoaderVerified(GetColumnId())->BuildAccessorContext(Source->GetRecordsCount()))
        , SubColumns(subColumns)
    {
    }
};

}   // namespace NKikimr::NOlap::NReader::NCommon
