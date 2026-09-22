#pragma once
#include "constructor.h"

#include <ydb/core/formats/arrow/accessor/common/additional_data.h>
#include <ydb/core/formats/arrow/accessor/common/chunk_data.h>
#include <ydb/core/formats/arrow/accessor/composite/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/constructor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/partial.h>
#include <ydb/core/tx/columnshard/engines/reader/tracing/data_source_probes.h>

namespace NKikimr::NOlap::NReader::NCommon {

LWTRACE_USING(YDB_CS_DATA_SOURCE);

class TSubColumnChunkRestoreInfo {
private:
    std::optional<TBlobRange> BlobRange;
    std::optional<TString> BlobData;
    YDB_READONLY(ui32, ColumnIdx, 0);

public:
    TSubColumnChunkRestoreInfo(const TBlobRange& range, const ui32 columnIdx)
        : BlobRange(range)
        , ColumnIdx(columnIdx)
    {
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

<<<<<<< HEAD
    void Finish(const std::shared_ptr<NArrow::TColumnFilter>& applyFilter, const std::shared_ptr<IDataSource>& source) {
        const bool deserialize = source->IsSourceInMemory();
=======
    // Dictionary-only chunk: rebuild the partial array with the distinct values of the single requested key column as
    // its rows (records count = dictionary size). Nulls are represented by one null value when the column is not
    // fully populated in this chunk, so `DISTINCT` still observes NULL.
    TConclusionStatus FinishDictionaryOnly(const TSubColumnChunkRestoreInfo& chunk, const IDataSource& source) {
        const auto& header = PartialArray->GetHeader();
        const ui32 colIdx = chunk.GetColumnIdx();
        const TString subColumnName = header.GetColumnStats().GetColumnNameString(colIdx);
        const auto constructor = header.GetAccessorConstructor(colIdx, PartialArray->GetSettings().GetEncodingParams());
        const auto field = header.GetField(colIdx);
        auto additionalData =
            NArrow::NAccessor::BuildAdditionalAccessorData(header.GetAddressesProto().GetKeyColumns(colIdx).GetAdditionalAccessorData());
        const NArrow::NAccessor::TChunkConstructionData info =
            NArrow::NAccessor::TChunkConstructionData(GetRecordsCount(), nullptr, field->type(), ChunkExternalInfo.GetDefaultSerializer())
                .WithAdditionalAccessorData(additionalData);
        const TString& blob = chunk.GetBlobDataVerified();
        source.GetContext()->GetCommonContext()->GetCounters().GetSubColumns()->GetColumnCounters().OnRead(blob.size());
        auto valuesConclusion = NArrow::NAccessor::NSubColumns::BuildDictionaryOnlyValues(constructor, blob, info);
        if (valuesConclusion.IsFail()) {
            return TConclusionStatus::Fail(TStringBuilder() << "sub column '" << subColumnName
                                                            << "' dictionary-only restore failed: " << valuesConclusion.GetErrorMessage());
        }
        if (!valuesConclusion.GetResult()) {
            return TConclusionStatus::Fail(TStringBuilder() << "sub column '" << subColumnName << "' is not dictionary encoded");
        }
        std::shared_ptr<arrow::Array> values = valuesConclusion.DetachResult();
        if (header.GetColumnStats().GetColumnRecordsCount(colIdx) < GetRecordsCount() && values->null_count() == 0) {
            // Some rows have no value for this key: the dictionary itself carries no null variant, add one.
            auto nullValue = NArrow::TStatusValidator::GetValid(arrow::MakeArrayOfNull(values->type(), 1));
            values = NArrow::TStatusValidator::GetValid(arrow::Concatenate({ values, nullValue }));
        }
        const ui32 dictLen = values->length();
        NArrow::NAccessor::TPartialColumnsData columnsData;
        columnsData.AddColumn(colIdx, std::make_shared<NArrow::NAccessor::TTrivialArray>(values));
        // `header` is a reference into the old PartialArray: copy it before the old array is released.
        const NArrow::NAccessor::NSubColumns::TSubColumnsHeader headerCopy = header;
        const auto settings = PartialArray->GetSettings();
        PartialArray = std::make_shared<NArrow::NAccessor::TSubColumnsPartialArray>(
            headerCopy, std::move(columnsData), std::nullopt, ChunkExternalInfo.GetColumnType(), dictLen, settings);
        return TConclusionStatus::Success();
    }

    // Returns true when the chunk was restored in dictionary-only mode (records are dictionary values, not rows).
    TConclusion<bool> Finish(const std::shared_ptr<NArrow::TColumnFilter>& applyFilter, const IDataSource& source) {
        const bool deserialize = source.IsSourceInMemory();
>>>>>>> 64bd6afc4f1 (Fix races in scans in columnshards (#53382))
        if (!!OthersBlobs) {
            source.GetContext()->GetCommonContext()->GetCounters().GetSubColumns()->GetOtherCounters().OnRead(OthersBlobs->size());
            PartialArray->InitOthers(*OthersBlobs, ChunkExternalInfo, applyFilter, !!applyFilter || deserialize);
            OthersBlobs.reset();
        }

        AFL_VERIFY(PartialArray);
        AFL_VERIFY(!HeaderRange);
        AFL_VERIFY(!OthersReadData);
        for (auto&& i : Chunks) {
            std::shared_ptr<TColumnLoader> columnLoader = std::make_shared<TColumnLoader>(ChunkExternalInfo.GetDefaultSerializer(),
                PartialArray->GetHeader().GetAccessorConstructor(i.second.GetColumnIdx()),
                PartialArray->GetHeader().GetField(i.second.GetColumnIdx()), nullptr, 0);
            auto additionalData = NArrow::NAccessor::BuildAdditionalAccessorData(
                PartialArray->GetHeader().GetAddressesProto().GetKeyColumns(i.second.GetColumnIdx()).GetAdditionalAccessorData());
            source.GetContext()->GetCommonContext()->GetCounters().GetSubColumns()->GetColumnCounters().OnRead(
                i.second.GetBlobDataVerified().size());
            const std::shared_ptr<NArrow::NAccessor::IChunkedArray> arrOriginal =
                deserialize ? columnLoader->ApplyVerified(i.second.GetBlobDataVerified(), GetRecordsCount(), std::nullopt, additionalData)
                            : std::make_shared<NArrow::NAccessor::TDeserializeChunkedArray>(
                                  GetRecordsCount(), columnLoader, i.second.GetBlobDataVerified(), true, additionalData);
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

    void InitPartialReader(const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& accessor) {
        AFL_VERIFY(!HeaderRange);
        AFL_VERIFY(!PartialArray);
        AFL_VERIFY(accessor);
        AFL_VERIFY(accessor->GetType() == NArrow::NAccessor::IChunkedArray::EType::SubColumnsPartialArray)("type", accessor->GetType());
        PartialArray = std::static_pointer_cast<NArrow::NAccessor::TSubColumnsPartialArray>(accessor);
    }

    TColumnChunkRestoreInfo(const TBlobRange& fullChunkRange, const NArrow::NAccessor::TChunkConstructionData& chunkExternalInfo)
        : ChunkExternalInfo(chunkExternalInfo)
        , FullChunkRange(fullChunkRange)
    {
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
    IDataSource& Source;
    const std::shared_ptr<ISnapshotSchema> SourceSchema;

    std::vector<TColumnChunkRestoreInfo> ColumnChunks;
    std::optional<TString> StorageId;
    bool NeedToAddResource = false;

    virtual void DoOnDataCollected(TFetchingResultContext& context) override {
        if (NeedToAddResource) {
            NArrow::NAccessor::TCompositeChunkedArray::TBuilder compositeBuilder(ChunkExternalInfo.GetColumnType());
            for (auto&& i : ColumnChunks) {
                i.Finish(nullptr, context.GetSource());
                compositeBuilder.AddChunk(i.GetPartialArray());
            }
            context.GetAccessors().AddVerified(GetEntityId(), compositeBuilder.Finish(), true);
<<<<<<< HEAD
=======
            if (usedDictionaryOnly) {
                const NArrow::TColumnFilter& filter = context.GetAccessors().GetFilter();
                AFL_VERIFY(NCommon::IsDictionaryOnlyFetchCompatible(filter))("filter", filter.DebugString());
                context.GetSource().MutableStageData().MarkDictionaryOnlyFetch(GetEntityId());
                context.GetSource().GetContext()->GetCommonContext()->GetCounters().OnDictionaryOnlyOptimization();
            }
>>>>>>> 64bd6afc4f1 (Fix races in scans in columnshards (#53382))
        } else {
            ui32 pos = 0;
            for (auto&& i : ColumnChunks) {
                const auto& appliedFilter = context.GetAccessors().GetAppliedFilter();
                if (appliedFilter) {
                    i.Finish(std::make_shared<NArrow::TColumnFilter>(appliedFilter->Slice(pos, i.GetRecordsCount())), context.GetSource());
                } else {
                    i.Finish(nullptr, context.GetSource());
                }
                pos += i.GetRecordsCount();
            }
        }
    }

    virtual void DoOnDataReceived(TReadActionsCollection& nextRead, NBlobOperations::NRead::TCompositeReadBlobs& blobs) override {
        AFL_VERIFY(ColumnChunks.size());
        AFL_VERIFY(!!StorageId);
        TBlobsAction blobsAction(StoragesManager, NBlobOperations::EConsumer::SCAN);
        auto reading = blobsAction.GetReading(*StorageId);
        reading->SetIsBackgroundProcess(false);
        ui32 chunkIndex = 0;
        for (auto&& i : ColumnChunks) {
            if (!!i.GetHeaderRange()) {
                const auto headerStart = TInstant::Now();
                const TString readBlob = blobs.ExtractVerified(*StorageId, *i.GetHeaderRange());
                const TString blob = i.GetSavedBlob() ? (i.GetSavedBlob() + readBlob) : readBlob;
                const auto fullHeader = NArrow::NAccessor::NSubColumns::TConstructor::GetFullHeaderSize(blob);
                if (!fullHeader.IsFail() && *fullHeader <= blob.size()) {
                    i.SetSavedBlob(Default<TString>());
<<<<<<< HEAD
                    i.InitPartialReader(blob);
                    i.InitReading(reading, SubColumns);
=======
                    auto conclusion = i.InitPartialReader(blob);
                    if (conclusion.IsFail()) {
                        Source.GetContext()->GetCommonContext()->AbortWithError(conclusion.GetErrorMessage());
                        return;
                    }
>>>>>>> 64bd6afc4f1 (Fix races in scans in columnshards (#53382))
                    const auto headerDuration = TInstant::Now() - headerStart;
                    auto columnLoader = SourceSchema->GetColumnLoaderVerified(GetEntityId());
                    TString columnName = columnLoader->GetField() ? TString(columnLoader->GetField()->name()) : TString("unknown");
                    const ui64 blobBytes = blob.size();
                    const ui64 rawBytes = i.GetPartialArray()->GetHeader().GetHeaderSize();
                    LWTRACK(SubColumnsHeaderRead, Source.GetDataSourceOrbit(), Source.GetRawPathId(), Source.GetTabletId(), Source.GetTxId(),
                        Source.GetSourceId(), GetEntityId(), columnName, headerDuration, chunkIndex, blobBytes, rawBytes);
                    Source.AddBytesRead(blobBytes);
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
                    const auto dataStart = TInstant::Now();
                    i.SetOthersBlob(blobs.ExtractVerified(*StorageId, *i.GetOthersReadData()));
                    const auto dataDuration = TInstant::Now() - dataStart;
                    auto columnLoader = SourceSchema->GetColumnLoaderVerified(GetEntityId());
                    TString columnName = columnLoader->GetField() ? TString(columnLoader->GetField()->name()) : TString("unknown");
                    const ui64 blobBytes = i.GetOthersBlobs()->size();
                    const ui64 rawBytes = i.GetPartialArray()->GetHeader().GetOthersSize();
                    LWTRACK(SubColumnsDataRead, Source.GetDataSourceOrbit(), Source.GetRawPathId(), Source.GetTabletId(), Source.GetTxId(),
                        Source.GetSourceId(), GetEntityId(), columnName, dataDuration, "others", chunkIndex, blobBytes, rawBytes);
                    Source.AddBytesRead(blobBytes);
                }
                for (auto&& [subColName, chunkData] : i.MutableChunks()) {
                    if (!!chunkData.GetBlobRangeOptional()) {
                        const auto dataStart = TInstant::Now();
                        chunkData.SetBlobData(blobs.ExtractVerified(*StorageId, *chunkData.GetBlobRangeOptional()));
                        const auto dataDuration = TInstant::Now() - dataStart;
<<<<<<< HEAD
                        if (auto source = Source.lock()) {
                            auto columnLoader = source->GetSourceSchema()->GetColumnLoaderVerified(GetEntityId());
                            TString columnName = columnLoader->GetField() ? TString(columnLoader->GetField()->name()) : TString("unknown");
                            const ui64 blobBytes = chunkData.GetBlobDataVerified().size();
                            const ui32 colIndex = i.GetPartialArray()->GetHeader().GetColumnStats().GetKeyIndexVerified(subColName);
                            const ui64 rawBytes = i.GetPartialArray()->GetHeader().GetColumnStats().GetColumnSize(colIndex);
                            LWTRACK(SubColumnsDataRead, source->GetDataSourceOrbit(), source->GetRawPathId(), source->GetTabletId(),
                                source->GetTxId(), source->GetSourceId(), GetEntityId(), columnName, dataDuration, subColName, chunkIndex,
                                blobBytes, rawBytes);
                            source->AddBytesRead(blobBytes);
                        }
=======
                        auto columnLoader = SourceSchema->GetColumnLoaderVerified(GetEntityId());
                        TString columnName = columnLoader->GetField() ? TString(columnLoader->GetField()->name()) : TString("unknown");
                        const ui64 blobBytes = chunkData.GetBlobDataVerified().size();
                        const ui64 rawBytes = i.GetPartialArray()->GetHeader().GetColumnStats().GetColumnSize(columnIndex);
                        LWTRACK(SubColumnsDataRead, Source.GetDataSourceOrbit(), Source.GetRawPathId(), Source.GetTabletId(), Source.GetTxId(),
                            Source.GetSourceId(), GetEntityId(), columnName, dataDuration,
                            i.GetPartialArray()->GetHeader().GetColumnStats().GetColumnNameString(columnIndex), chunkIndex, blobBytes, rawBytes);
                        Source.AddBytesRead(blobBytes);
>>>>>>> 64bd6afc4f1 (Fix races in scans in columnshards (#53382))
                    }
                }
            }
            ++chunkIndex;
        }
        nextRead.Add(reading);
    }

    virtual void DoStart(TReadActionsCollection& nextRead, TFetchingResultContext& context) override {
        auto& source = context.GetSource();
        auto columnChunks = source.GetPortionAccessor().GetColumnChunksPointers(GetEntityId());
        AFL_VERIFY(columnChunks.size());
        StorageId = source.GetColumnStorageId(GetEntityId());
        TBlobsAction blobsAction(StoragesManager, NBlobOperations::EConsumer::SCAN);
        auto reading = blobsAction.GetReading(*StorageId);
        reading->SetIsBackgroundProcess(false);
        auto filterPtr = context.GetAppliedFilter();
        const NArrow::TColumnFilter& cFilter = filterPtr ? *filterPtr : NArrow::TColumnFilter::BuildAllowFilter();
        auto itFilter = cFilter.GetBegin(false, context.GetRecordsCount());
        bool itFinished = false;

        auto accessor = context.GetAccessors().GetAccessorOptional(GetEntityId());
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
<<<<<<< HEAD
                const TBlobRange range = source->RestoreBlobRange(columnChunks[chunkIdx]->BlobRange);
                ColumnChunks.emplace_back(range, ChunkExternalInfo.GetSubset(meta.GetRecordsCount()));
=======
                const TBlobRange range = source.RestoreBlobRange(columnChunks[chunkIdx]->BlobRange);
                ColumnChunks.emplace_back(range, ChunkExternalInfo.GetSubset(meta.GetRecordsCount()), Settings);
>>>>>>> 64bd6afc4f1 (Fix races in scans in columnshards (#53382))
                if (!NeedToAddResource) {
                    AFL_VERIFY(resChunkIdx < chunks.size())("chunks", chunks.size())("meta", columnChunks.size())("need", NeedToAddResource);
                    ColumnChunks.back().InitPartialReader(chunks[resChunkIdx]);
                    ++resChunkIdx;
                }
                ColumnChunks.back().InitReading(reading, SubColumns);
            } else {
                ColumnChunks.emplace_back(TColumnChunkRestoreInfo::BuildEmpty(ChunkExternalInfo.GetSubset(meta.GetRecordsCount())));
            }
            itFinished = !itFilter.Next(meta.GetRecordsCount());
        }
        AFL_VERIFY(NeedToAddResource || (resChunkIdx == chunks.size()));
        AFL_VERIFY(itFinished)("filter", itFilter.DebugString())("count", context.GetRecordsCount());
        for (auto&& i : blobsAction.GetReadingActions()) {
            nextRead.Add(i);
        }
    }

public:
<<<<<<< HEAD
    TSubColumnsFetchLogic(const ui32 columnId, const std::shared_ptr<IDataSource>& source, const std::vector<TString>& subColumns)
        : TBase(columnId, source->GetContext()->GetCommonContext()->GetStoragesManager())
        , ChunkExternalInfo(source->GetSourceSchema()->GetColumnLoaderVerified(GetEntityId())->BuildAccessorContext(source->GetRecordsCount()))
        , SubColumns(subColumns)
        , Source(source)
=======
    TSubColumnsFetchLogic(const ui32 columnId, IDataSource& source, const std::shared_ptr<ISnapshotSchema>& sourceSchema,
        const ui32 recordsCount, const std::vector<TString>& subColumns, const bool dictionaryOnly = false)
        : TBase(columnId, source.GetContext()->GetCommonContext()->GetStoragesManager())
        , ChunkExternalInfo(sourceSchema->GetColumnLoaderVerified(GetEntityId())->BuildAccessorContext(recordsCount))
        , Settings(GetSettings(sourceSchema->GetColumnLoaderVerified(GetEntityId())->GetAccessorConstructor()))
        , SubColumns(subColumns)
        , Source(source)
        , SourceSchema(sourceSchema)
        , DictionaryOnly(dictionaryOnly)
>>>>>>> 64bd6afc4f1 (Fix races in scans in columnshards (#53382))
    {
        const auto loader = sourceSchema->GetColumnLoaderVerified(GetEntityId());
        AFL_VERIFY(loader->GetAccessorConstructor()->GetType() == NArrow::NAccessor::IChunkedArray::EType::SubColumnsArray)(
            "type", loader->GetAccessorConstructor()->GetType());
    }

<<<<<<< HEAD
    TSubColumnsFetchLogic(const ui32 columnId, const std::shared_ptr<ISnapshotSchema>& sourceSchema,
        const std::shared_ptr<IStoragesManager>& storages, const ui32 recordsCount, const std::vector<TString>& subColumns)
        : TBase(columnId, storages)
        , ChunkExternalInfo(sourceSchema->GetColumnLoaderVerified(GetEntityId())->BuildAccessorContext(recordsCount))
        , SubColumns(subColumns)
        , Source()
=======
    TSubColumnsFetchLogic(const ui32 columnId, IDataSource& source, const std::vector<TString>& subColumns, const bool dictionaryOnly = false)
        : TSubColumnsFetchLogic(columnId, source, source.GetSourceSchema(), source.GetRecordsCount(), subColumns, dictionaryOnly)
>>>>>>> 64bd6afc4f1 (Fix races in scans in columnshards (#53382))
    {
    }
};

}   // namespace NKikimr::NOlap::NReader::NCommon
