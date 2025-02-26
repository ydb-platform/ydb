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
    YDB_READONLY_DEF(TBlobRange, FullChunkRange);
    YDB_ACCESSOR_DEF(std::optional<TBlobRange>, OthersReadData);
    YDB_READONLY_DEF(std::optional<TString>, OthersBlobs);
    YDB_ACCESSOR_DEF(TString, SavedBlob);

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
        AFL_VERIFY(!HeaderRange);
        AFL_VERIFY(!OthersReadData);
        for (auto&& i : Chunks) {
            std::shared_ptr<TColumnLoader> columnLoader = std::make_shared<TColumnLoader>(ChunkExternalInfo.GetDefaultSerializer(),
                PartialArray->GetHeader().GetAccessorConstructor(i.second.GetColumnIdx()),
                PartialArray->GetHeader().GetField(i.second.GetColumnIdx()), nullptr, 0);
            source->GetContext()->GetCommonContext()->GetCounters().GetSubColumns()->GetColumnCounters().OnRead(
                i.second.GetBlobDataVerified().size());
            std::vector<NArrow::NAccessor::TDeserializeChunkedArray::TChunk> chunks = { NArrow::NAccessor::TDeserializeChunkedArray::TChunk(
                GetRecordsCount(), i.second.GetBlobDataVerified()) };
            const std::shared_ptr<NArrow::NAccessor::IChunkedArray> arrOriginal = deserialize
                    ? columnLoader->ApplyVerified(i.second.GetBlobDataVerified(), GetRecordsCount())
                    : std::make_shared<NArrow::NAccessor::TDeserializeChunkedArray>(GetRecordsCount(), columnLoader, std::move(chunks), true);
            if (applyFilter) {
                PartialArray->AddColumn(i.first, applyFilter->Apply(arrOriginal));
            } else {
                PartialArray->AddColumn(i.first, arrOriginal);
            }
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
        PartialArray = NArrow::NAccessor::NSubColumns::TConstructor::BuildPartialReader(blob, ChunkExternalInfo).DetachResult();
//        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("columns", PartialArray->GetHeader().GetColumnStats().DebugJson().GetStringRobust())(
//            "others", PartialArray->GetHeader().GetOtherStats().DebugJson().GetStringRobust());
    }

    void InitPartialReader(
        const ui32 columnId, const ui32 positionStart, const std::shared_ptr<NArrow::NAccessor::TAccessorsCollection>& resources) {
        AFL_VERIFY(!HeaderRange);
        AFL_VERIFY(!PartialArray);
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
        result.PartialArray =
            NArrow::NAccessor::TSubColumnsPartialArray::BuildEmpty(chunkExternalInfo.GetColumnType(), chunkExternalInfo.GetRecordsCount());
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
            for (auto&& i : ColumnChunks) {
                i.Finish(Resources->GetAppliedFilter(), Source);
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
                const TString readBlob = blobs.Extract(*StorageId, *i.GetHeaderRange());
                const TString blob = i.GetSavedBlob() ? (i.GetSavedBlob() + readBlob) : readBlob;
                const auto fullHeader = NArrow::NAccessor::NSubColumns::TConstructor::GetFullHeaderSize(blob);
                if (!fullHeader.IsFail() && *fullHeader <= blob.size()) {
                    i.SetSavedBlob(Default<TString>());
                    i.InitPartialReader(blob);
                    i.InitReading(reading, SubColumns);
                } else {
                    i.SetSavedBlob(blob);
                    ui32 size = 0;
                    if (fullHeader.IsFail()) {
                        size = NArrow::NAccessor::NSubColumns::TConstructor::GetHeaderSize(blob).DetachResult();
                    } else {
                        size = *fullHeader;
                    }
                    AFL_VERIFY(blob.size() < size)("blob", blob.size())("size", size);
                    const TBlobRange headerRange = i.GetFullChunkRange().BuildSubset(blob.size(), size - blob.size());
                    reading->AddRange(headerRange);
                    i.SetHeaderRange(headerRange);
                }
            } else {
                if (!!i.GetOthersReadData()) {
                    i.SetOthersBlob(blobs.Extract(*StorageId, *i.GetOthersReadData()));
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

        NeedToAddResource = !Resources->HasColumn(GetColumnId());
        ui32 posCurrent = 0;
        for (auto&& c : columnChunks) {
            AFL_VERIFY(!itFinished);
            if (!itFilter.IsBatchForSkip(c->GetMeta().GetRecordsCount())) {
                const TBlobRange range = Source->RestoreBlobRange(c->BlobRange);
                ColumnChunks.emplace_back(range, ChunkExternalInfo.GetSubset(c->GetMeta().GetRecordsCount()));
                if (!NeedToAddResource) {
                    ColumnChunks.back().InitPartialReader(GetColumnId(), posCurrent, Resources);
                }
                ColumnChunks.back().InitReading(reading, SubColumns);
            } else {
                ColumnChunks.emplace_back(TColumnChunkRestoreInfo::BuildEmpty(ChunkExternalInfo.GetSubset(c->GetMeta().GetRecordsCount())));
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
        , SubColumns(subColumns) {
        const auto loader = Source->GetSourceSchema()->GetColumnLoaderVerified(GetColumnId());
        AFL_VERIFY(loader->GetAccessorConstructor()->GetType() == NArrow::NAccessor::IChunkedArray::EType::SubColumnsArray)
        ("type", loader->GetAccessorConstructor()->GetType());
    }
};

}   // namespace NKikimr::NOlap::NReader::NCommon
