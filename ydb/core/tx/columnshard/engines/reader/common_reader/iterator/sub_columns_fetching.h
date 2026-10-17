#pragma once
#include "constructor.h"
#include "dictionary_fetching.h"

#include <ydb/core/formats/arrow/accessor/common/additional_data.h>
#include <ydb/core/formats/arrow/accessor/common/chunk_data.h>
#include <ydb/core/formats/arrow/accessor/composite/accessor.h>
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/constructor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/dense_encoding/constructors.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/partial.h>
#include <ydb/core/tx/columnshard/engines/reader/tracing/data_source_probes.h>

#include <ydb/library/actors/core/log.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/concatenate.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/util.h>

namespace NKikimr::NOlap::NReader::NCommon {

LWTRACE_USING(YDB_CS_DATA_SOURCE);

class TSubColumnChunkRestoreInfo {
private:
    std::optional<TBlobRange> BlobRange;
    std::optional<TString> BlobData;
    YDB_READONLY(ui32, ColumnIdx, 0);
    // Only the dictionary prefix of the key column is read; the restored array holds the distinct values, not rows.
    YDB_READONLY(bool, DictionaryOnly, false);

public:
    TSubColumnChunkRestoreInfo(const TBlobRange& range, const ui32 columnIdx, const bool dictionaryOnly = false)
        : BlobRange(range)
        , ColumnIdx(columnIdx)
        , DictionaryOnly(dictionaryOnly)
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
    const NArrow::NAccessor::NSubColumns::TSettings Settings;
    THashMap<ui32, TSubColumnChunkRestoreInfo> Chunks;
    YDB_ACCESSOR_DEF(std::optional<TBlobRange>, HeaderRange);
    std::shared_ptr<NArrow::NAccessor::TSubColumnsPartialArray> PartialArray;
    YDB_READONLY_DEF(TBlobRange, FullChunkRange);
    YDB_ACCESSOR_DEF(std::optional<TBlobRange>, OthersReadData);
    YDB_READONLY_DEF(std::optional<TString>, OthersBlobs);
    YDB_ACCESSOR_DEF(TString, SavedBlob);
    // True after the data-range InitReading (header-only InitReading leaves this false).
    YDB_READONLY(bool, ReadingInitialized, false);

public:
    void SetOthersBlob(const TString& blob) {
        AFL_VERIFY(!!OthersReadData);
        OthersReadData = std::nullopt;
        OthersBlobs = blob;
    }

    ui32 GetRecordsCount() const {
        return ChunkExternalInfo.GetRecordsCount();
    }

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
        if (!!OthersBlobs) {
            source.GetContext()->GetCommonContext()->GetCounters().GetSubColumns()->GetOtherCounters().OnRead(OthersBlobs->size());
            PartialArray->InitOthers(*OthersBlobs, ChunkExternalInfo, applyFilter, !!applyFilter || deserialize);
            OthersBlobs.reset();
        }

        AFL_VERIFY(PartialArray);
        AFL_VERIFY(!HeaderRange);
        AFL_VERIFY(!OthersReadData);
        if (Chunks.size() == 1 && Chunks.begin()->second.GetDictionaryOnly()) {
            AFL_VERIFY(!applyFilter);
            AFL_VERIFY(!PartialArray->HasOthers());
            auto conclusion = FinishDictionaryOnly(Chunks.begin()->second, source);
            if (conclusion.IsFail()) {
                return conclusion;
            }
            return true;
        }
        for (auto&& i : Chunks) {
            AFL_VERIFY(!i.second.GetDictionaryOnly());
            std::shared_ptr<TColumnLoader> columnLoader = std::make_shared<TColumnLoader>(ChunkExternalInfo.GetDefaultSerializer(),
                PartialArray->GetHeader().GetAccessorConstructor(i.second.GetColumnIdx(), PartialArray->GetSettings().GetEncodingParams()),
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
                PartialArray->AddColumn(i.second.GetColumnIdx(), applyFilter->Apply(arrOriginal));
            } else {
                PartialArray->AddColumn(i.second.GetColumnIdx(), arrOriginal);
            }
        }
        return false;
    }

    // Dictionary blob prefix size of key column `colIndex` when it is dictionary encoded in this chunk, nullopt otherwise.
    std::optional<ui32> GetDictionaryPrefixSize(const ui32 colIndex) const {
        const auto& header = PartialArray->GetHeader();
        if (header.GetColumnStats().GetAccessorType(colIndex) != NArrow::NAccessor::IChunkedArray::EType::Dictionary) {
            return std::nullopt;
        }
        const auto additionalData =
            NArrow::NAccessor::BuildAdditionalAccessorData(header.GetAddressesProto().GetKeyColumns(colIndex).GetAdditionalAccessorData());
        const auto* dictData = dynamic_cast<const NArrow::NAccessor::TDictionaryAccessorData*>(additionalData.get());
        if (!dictData || !dictData->DictionaryBlobSize) {
            return std::nullopt;
        }
        return dictData->DictionaryBlobSize;
    }

    bool HasPartialArray() const {
        return !!PartialArray;
    }

    // True when this chunk stores `subColumnName` as a dedicated dictionary-encoded key column whose prefix we can
    // restore without the row-aligned index. Chunks that keep the key in "others" or as a plain/sparsed column cannot.
    bool CanRestoreDictionaryOnly(const TString& subColumnName) const {
        AFL_VERIFY(PartialArray);
        if (PartialArray->HasSubColumnData(subColumnName)) {
            return false;
        }
        auto pathResult = NArrow::NAccessor::NSubColumns::ResolveBestPath(PartialArray->GetHeader().GetColumnStats(),
            PartialArray->GetHeader().GetOtherStats(), NArrow::NAccessor::NSubColumns::ToJsonPath(subColumnName));
        if (pathResult.IsFail()) {
            return false;
        }
        const auto path = pathResult.DetachResult();
        if (!path || !path->IsColumn) {
            return false;
        }
        const auto dictPrefix = GetDictionaryPrefixSize(path->Path.ColumnIndex);
        if (!dictPrefix) {
            return false;
        }
        const auto colBlobRange = PartialArray->GetColumnReadRange(path->Path.ColumnIndex);
        return *dictPrefix <= colBlobRange.GetSize();
    }

    // `dictionaryOnly`: read only the dictionary prefix of the single requested key column when this chunk stores it
    // dictionary encoded (DISTINCT needs the distinct values, not the rows). Chunks that store the key differently
    // (plain/sparsed column, or in "others") fall back to the regular row-aligned read.
    void InitReading(const std::shared_ptr<IBlobsReadingAction>& reading, const std::vector<TString>& subColumns, const bool dictionaryOnly) {
        AFL_VERIFY(!HeaderRange);
        if (!!PartialArray) {
            ReadingInitialized = true;
            for (auto&& subColumnName : subColumns) {
                auto pathResult = NArrow::NAccessor::NSubColumns::ResolveBestPath(PartialArray->GetHeader().GetColumnStats(),
                    PartialArray->GetHeader().GetOtherStats(), NArrow::NAccessor::NSubColumns::ToJsonPath(subColumnName));
                AFL_VERIFY(pathResult.IsSuccess())("subColumnName", subColumnName)("error", pathResult.GetErrorMessage());
                const auto path = pathResult.DetachResult();
                if (path && path->IsColumn) {
                    if (Chunks.contains(path->Path.ColumnIndex)) {
                        continue;
                    }
                    auto colBlobRange = PartialArray->GetColumnReadRange(path->Path.ColumnIndex);
                    std::optional<ui32> dictPrefix;
                    if (dictionaryOnly && subColumns.size() == 1 && !PartialArray->HasSubColumnData(subColumnName)) {
                        dictPrefix = GetDictionaryPrefixSize(path->Path.ColumnIndex);
                    }
                    if (dictPrefix && *dictPrefix <= colBlobRange.GetSize()) {
                        const TBlobRange subRange = FullChunkRange.BuildSubset(colBlobRange.GetOffset(), *dictPrefix);
                        reading->AddRange(subRange);
                        AddFetchData(subRange, path->Path.ColumnIndex, /*dictionaryOnly=*/true);
                    } else {
                        const TBlobRange subRange = FullChunkRange.BuildSubset(colBlobRange.GetOffset(), colBlobRange.GetSize());
                        reading->AddRange(subRange);
                        AddFetchData(subRange, path->Path.ColumnIndex);
                    }
                } else if (!PartialArray->HasOthers() && !OthersReadData && path) {
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

    TConclusionStatus InitPartialReader(const TString& blob) {
        AFL_VERIFY(!!HeaderRange);
        AFL_VERIFY(!PartialArray);
        HeaderRange = std::nullopt;
        auto conclusion = NArrow::NAccessor::NSubColumns::TConstructor::BuildPartialReader(blob, ChunkExternalInfo, Settings);
        if (conclusion.IsFail()) {
            return conclusion;
        }
        PartialArray = conclusion.DetachResult();
        //        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("columns", PartialArray->GetHeader().GetColumnStats().DebugJson().GetStringRobust())(
        //            "others", PartialArray->GetHeader().GetOtherStats().DebugJson().GetStringRobust());
        return TConclusionStatus::Success();
    }

    void InitPartialReader(const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& accessor) {
        AFL_VERIFY(!HeaderRange);
        AFL_VERIFY(!PartialArray);
        AFL_VERIFY(accessor);
        AFL_VERIFY(accessor->GetType() == NArrow::NAccessor::IChunkedArray::EType::SubColumnsPartialArray)("type", accessor->GetType());
        PartialArray = std::static_pointer_cast<NArrow::NAccessor::TSubColumnsPartialArray>(accessor);
    }

    TColumnChunkRestoreInfo(const TBlobRange& fullChunkRange, const NArrow::NAccessor::TChunkConstructionData& chunkExternalInfo,
        const NArrow::NAccessor::NSubColumns::TSettings& settings)
        : ChunkExternalInfo(chunkExternalInfo)
        , Settings(settings)
        , FullChunkRange(fullChunkRange)
    {
    }

    static TColumnChunkRestoreInfo BuildEmpty(
        const NArrow::NAccessor::TChunkConstructionData& chunkExternalInfo, const NArrow::NAccessor::NSubColumns::TSettings& settings) {
        TColumnChunkRestoreInfo result(TBlobRange(), chunkExternalInfo, settings);
        result.PartialArray = NArrow::NAccessor::TSubColumnsPartialArray::BuildEmpty(
            chunkExternalInfo.GetColumnType(), chunkExternalInfo.GetRecordsCount(), settings);
        return result;
    }

    const THashMap<ui32, TSubColumnChunkRestoreInfo>& GetChunks() const {
        return Chunks;
    }

    THashMap<ui32, TSubColumnChunkRestoreInfo>& MutableChunks() {
        return Chunks;
    }

    void AddFetchData(const TBlobRange& subRange, const ui32 colIndex, const bool dictionaryOnly = false) {
        const auto insertResult = Chunks.try_emplace(colIndex, subRange, colIndex, dictionaryOnly);
        AFL_VERIFY(insertResult.second);
    }
};

class TSubColumnsFetchLogic: public IKernelFetchLogic {
private:
    using TBase = IKernelFetchLogic;

    const NArrow::NAccessor::TChunkConstructionData ChunkExternalInfo;
    const NArrow::NAccessor::NSubColumns::TSettings Settings;
    const std::vector<TString> SubColumns;
    IDataSource& Source;
    const std::shared_ptr<ISnapshotSchema> SourceSchema;

    std::vector<TColumnChunkRestoreInfo> ColumnChunks;
    std::optional<TString> StorageId;
    bool NeedToAddResource = false;
    // Requested by the SSA optimizer (DISTINCT over a single sub-column): read only the dictionary values of the key
    // column where a chunk stores it dictionary encoded. Rows of such chunks are dictionary entries, so the resulting
    // accessor is not row-aligned with the portion (marked via MarkDictionaryOnlyFetch).
    bool DictionaryOnly = false;

    static const NArrow::NAccessor::NSubColumns::TSettings& GetSettings(const NArrow::NAccessor::TConstructorContainer& accessor) {
        const auto* subColumnsAccessor = dynamic_cast<const NArrow::NAccessor::NSubColumns::TConstructor*>(accessor.GetObjectPtr().get());
        AFL_VERIFY(subColumnsAccessor);
        return subColumnsAccessor->GetSettings();
    }

    // Dictionary-only restore is valid only when every real physical chunk of this sub-column is dictionary-encoded.
    // Mixed encodings would concatenate dictionary-entry rows with portion-aligned rows.
    bool AllChunksSupportDictionaryOnly() const {
        if (!DictionaryOnly || SubColumns.size() != 1) {
            return false;
        }
        bool anyRealChunk = false;
        for (const auto& chunk : ColumnChunks) {
            if (!chunk.GetFullChunkRange().GetSize()) {
                continue;
            }
            if (!chunk.HasPartialArray()) {
                return false;
            }
            anyRealChunk = true;
            if (!chunk.CanRestoreDictionaryOnly(SubColumns[0])) {
                return false;
            }
        }
        return anyRealChunk;
    }

    virtual TConclusionStatus DoOnDataCollected(TFetchingResultContext& context) override {
        if (NeedToAddResource) {
            NArrow::NAccessor::TCompositeChunkedArray::TBuilder compositeBuilder(ChunkExternalInfo.GetColumnType());
            // Mixed encodings across physical chunks of one sub-column cannot share a dictionary-only row space.
            // Either every restored chunk is dictionary-only, or none is (full row-aligned read).
            bool usedDictionaryOnly = DictionaryOnly;
            for (auto&& i : ColumnChunks) {
                auto conclusion = i.Finish(nullptr, context.GetSource());
                if (conclusion.IsFail()) {
                    return conclusion;
                }
                if (!i.GetFullChunkRange().GetSize()) {
                    AFL_VERIFY(!*conclusion)("entity", GetEntityId());
                } else if (usedDictionaryOnly) {
                    AFL_VERIFY(*conclusion)("entity", GetEntityId());
                } else {
                    AFL_VERIFY(!*conclusion)("entity", GetEntityId());
                }
                compositeBuilder.AddChunk(i.GetPartialArray());
            }
            context.GetAccessors().AddVerified(GetEntityId(), compositeBuilder.Finish(), true);
            if (usedDictionaryOnly) {
                const NArrow::TColumnFilter& filter = context.GetAccessors().GetFilter();
                AFL_VERIFY(NCommon::IsDictionaryOnlyFetchCompatible(filter))("filter", filter.DebugString());
                context.GetSource().MutableStageData().MarkDictionaryOnlyFetch(GetEntityId());
                context.GetSource().GetContext()->GetCommonContext()->GetCounters().OnDictionaryOnlyOptimization();
            }
        } else {
            AFL_VERIFY(!DictionaryOnly);
            ui32 pos = 0;
            for (auto&& i : ColumnChunks) {
                const auto& appliedFilter = context.GetAccessors().GetAppliedFilter();
                TConclusion<bool> conclusion = false;
                if (appliedFilter) {
                    conclusion =
                        i.Finish(std::make_shared<NArrow::TColumnFilter>(appliedFilter->Slice(pos, i.GetRecordsCount())), context.GetSource());
                } else {
                    conclusion = i.Finish(nullptr, context.GetSource());
                }
                if (conclusion.IsFail()) {
                    return conclusion;
                }
                AFL_VERIFY(!*conclusion);
                pos += i.GetRecordsCount();
            }
        }
        return TConclusionStatus::Success();
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
                    auto conclusion = i.InitPartialReader(blob);
                    if (conclusion.IsFail()) {
                        Source.GetContext()->GetCommonContext()->AbortWithError(conclusion.GetErrorMessage());
                        return;
                    }
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
                for (auto&& [columnIndex, chunkData] : i.MutableChunks()) {
                    if (!!chunkData.GetBlobRangeOptional()) {
                        const auto dataStart = TInstant::Now();
                        chunkData.SetBlobData(blobs.ExtractVerified(*StorageId, *chunkData.GetBlobRangeOptional()));
                        const auto dataDuration = TInstant::Now() - dataStart;
                        auto columnLoader = SourceSchema->GetColumnLoaderVerified(GetEntityId());
                        TString columnName = columnLoader->GetField() ? TString(columnLoader->GetField()->name()) : TString("unknown");
                        const ui64 blobBytes = chunkData.GetBlobDataVerified().size();
                        const ui64 rawBytes = i.GetPartialArray()->GetHeader().GetColumnStats().GetColumnSize(columnIndex);
                        LWTRACK(SubColumnsDataRead, Source.GetDataSourceOrbit(), Source.GetRawPathId(), Source.GetTabletId(), Source.GetTxId(),
                            Source.GetSourceId(), GetEntityId(), columnName, dataDuration,
                            i.GetPartialArray()->GetHeader().GetColumnStats().GetColumnNameString(columnIndex), chunkIndex, blobBytes, rawBytes);
                        Source.AddBytesRead(blobBytes);
                    }
                }
            }
            ++chunkIndex;
        }
        bool headersPending = false;
        for (const auto& chunk : ColumnChunks) {
            if (!!chunk.GetHeaderRange()) {
                headersPending = true;
                break;
            }
        }
        if (!headersPending) {
            const bool useDict = AllChunksSupportDictionaryOnly();
            if (DictionaryOnly && !useDict) {
                DictionaryOnly = false;
            }
            for (auto&& i : ColumnChunks) {
                if (!i.GetReadingInitialized() && i.GetFullChunkRange().GetSize()) {
                    i.InitReading(reading, SubColumns, useDict);
                }
            }
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
        if (!NeedToAddResource || SubColumns.size() != 1 || !NCommon::IsDictionaryOnlyFetchCompatible(cFilter)) {
            // Existing (row-aligned) accessor or several keys of the column: dictionary values cannot replace rows.
            DictionaryOnly = false;
        }
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
                const TBlobRange range = source.RestoreBlobRange(columnChunks[chunkIdx]->BlobRange);
                ColumnChunks.emplace_back(range, ChunkExternalInfo.GetSubset(meta.GetRecordsCount()), Settings);
                if (!NeedToAddResource) {
                    AFL_VERIFY(resChunkIdx < chunks.size())("chunks", chunks.size())("meta", columnChunks.size())("need", NeedToAddResource);
                    ColumnChunks.back().InitPartialReader(chunks[resChunkIdx]);
                    ++resChunkIdx;
                }
                ColumnChunks.back().InitReading(reading, SubColumns, DictionaryOnly);
            } else {
                ColumnChunks.emplace_back(TColumnChunkRestoreInfo::BuildEmpty(ChunkExternalInfo.GetSubset(meta.GetRecordsCount()), Settings));
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
    TSubColumnsFetchLogic(const ui32 columnId, IDataSource& source, const std::shared_ptr<ISnapshotSchema>& sourceSchema,
        const ui32 recordsCount, const std::vector<TString>& subColumns, const bool dictionaryOnly = false)
        : TBase(columnId, source.GetContext()->GetCommonContext()->GetStoragesManager())
        , ChunkExternalInfo(sourceSchema->GetColumnLoaderVerified(GetEntityId())->BuildAccessorContext(recordsCount))
        , Settings(GetSettings(sourceSchema->GetColumnLoaderVerified(GetEntityId())->GetAccessorConstructor()))
        , SubColumns(subColumns)
        , Source(source)
        , SourceSchema(sourceSchema)
        , DictionaryOnly(dictionaryOnly)
    {
        const auto loader = sourceSchema->GetColumnLoaderVerified(GetEntityId());
        AFL_VERIFY(loader->GetAccessorConstructor()->GetType() == NArrow::NAccessor::IChunkedArray::EType::SubColumnsArray)(
            "type", loader->GetAccessorConstructor()->GetType());
    }

    TSubColumnsFetchLogic(const ui32 columnId, IDataSource& source, const std::vector<TString>& subColumns, const bool dictionaryOnly = false)
        : TSubColumnsFetchLogic(columnId, source, source.GetSourceSchema(), source.GetRecordsCount(), subColumns, dictionaryOnly)
    {
    }
};

}   // namespace NKikimr::NOlap::NReader::NCommon
