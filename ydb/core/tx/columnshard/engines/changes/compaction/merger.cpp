#include "merger.h"

#include "abstract/merger.h"
#include "plain/logic.h"
#include "sparsed/logic.h"

#include <ydb/core/formats/arrow/reader/merger.h>
#include <ydb/core/formats/arrow/serializer/native.h>
#include <ydb/core/tx/columnshard/splitter/batch_slice.h>

#include <ydb/library/formats/arrow/simple_builder/array.h>
#include <ydb/library/formats/arrow/simple_builder/filler.h>
#include <ydb/library/formats/arrow/splitter/stats.h>

namespace NKikimr::NOlap::NCompaction {

class TColumnSplitInfo {
private:
    YDB_READONLY(ui32, ColumnId, 0);
    YDB_READONLY(ui32, RecordsCount, 0);
    YDB_READONLY_DEF(std::vector<i64>, ChunkRecordsCount);
    YDB_READONLY_DEF(std::vector<std::shared_ptr<IPortionDataChunk>>, ResultChunks);
    ui32 ChunksReady = 0;
    const std::optional<NArrow::NSplitter::TSimpleSerializationStat> Stats;
    const NSplitter::TSplitSettings Settings;

public:
    TColumnSplitInfo(
        const ui32 columnId, const std::optional<NArrow::NSplitter::TSimpleSerializationStat>& stats, const NSplitter::TSplitSettings& settings)
        : ColumnId(columnId)
        , Stats(stats)
        , Settings(settings) {
    }

    void SetChunks(std::vector<i64>&& recordsCount) {
        AFL_VERIFY(RecordsCount == 0);
        AFL_VERIFY(ChunkRecordsCount.empty());
        for (auto&& i : recordsCount) {
            RecordsCount += i;
        }
        ChunkRecordsCount = std::move(recordsCount);
        AFL_VERIFY(RecordsCount);
    }

    void AddColumnChunks(const std::vector<std::shared_ptr<IPortionDataChunk>>& chunks) {
        ResultChunks.insert(ResultChunks.end(), chunks.begin(), chunks.end());
        AFL_VERIFY(ChunksReady <= ChunkRecordsCount.size());
        ui32 checkRecordsCount = 0;
        for (auto&& i : chunks) {
            checkRecordsCount += i->GetRecordsCountVerified();
            if (chunks.size() > 1) {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_COMPACTION)("settings", Settings.DebugString())(
                    "stats", Stats ? Stats->DebugString() : TString("no_stats"))("column_id", ColumnId)("packed", i->GetPackedSize());
            }
        }
        AFL_VERIFY(checkRecordsCount == ChunkRecordsCount[ChunksReady])("check", checkRecordsCount)("split", ChunkRecordsCount[ChunksReady]);
        ++ChunksReady;
    }
};

class TPortionSplitInfo {
private:
    std::map<ui32, TColumnSplitInfo> Columns;
    YDB_READONLY(ui32, RecordsCount, 0);
    TMergingChunkContext MergingContext;

public:
    TMergingChunkContext& MutableMergingContext() {
        return MergingContext;
    }

    const TMergingChunkContext& GetMergingContext() const {
        return MergingContext;
    }

    const TColumnSplitInfo& GetColumnInfo(const ui32 columnId) const {
        auto it = Columns.find(columnId);
        AFL_VERIFY(it != Columns.end());
        return it->second;
    }

    explicit TPortionSplitInfo(const ui32 recordsCount, const std::shared_ptr<arrow::RecordBatch>& remapper)
        : RecordsCount(recordsCount)
        , MergingContext(remapper) {
    }

    TGeneralSerializedSlice BuildSlice(const std::shared_ptr<TFilteredSnapshotSchema>& resultFiltered,
        const std::shared_ptr<NArrow::NSplitter::TSerializationStats>& stats,
        const std::shared_ptr<NColumnShard::TSplitterCounters>& counters) const {
        std::shared_ptr<TDefaultSchemaDetails> schemaDetails(new TDefaultSchemaDetails(resultFiltered, stats));
        THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>> portionColumns;
        for (auto&& [cId, cPage] : Columns) {
            portionColumns.emplace(cId, cPage.GetResultChunks());
        }
        return TGeneralSerializedSlice(portionColumns, schemaDetails, counters);
    }

    void AddColumnChunks(const ui32 columnId, const std::vector<std::shared_ptr<IPortionDataChunk>>& chunks) {
        auto it = Columns.find(columnId);
        AFL_VERIFY(it != Columns.end());
        it->second.AddColumnChunks(chunks);
    }

    void AddColumnSplitting(const ui32 columnId, std::vector<i64>&& chunks,
        const std::optional<NArrow::NSplitter::TSimpleSerializationStat>& stats, const NSplitter::TSplitSettings& settings) {
        auto it = Columns.emplace(columnId, TColumnSplitInfo(columnId, stats, settings));
        AFL_VERIFY(it.second);
        it.first->second.SetChunks(std::move(chunks));
        AFL_VERIFY(RecordsCount == it.first->second.GetRecordsCount())("new", it.first->second.GetRecordsCount())("control", RecordsCount);
    }
};

class TSplittedBatch {
private:
    enum class ESplittingType : ui32 {
        Stats = 0,
        RecordsCount
    };

    YDB_READONLY_DEF(std::shared_ptr<arrow::RecordBatch>, Remapper);
    YDB_READONLY_DEF(std::vector<TPortionSplitInfo>, Portions);
    const std::shared_ptr<NArrow::NSplitter::TSerializationStats> Stats;
    const std::shared_ptr<TFilteredSnapshotSchema> ResultFiltered;
    const NSplitter::TSplitSettings Settings;
    std::optional<ESplittingType> SplittingType;

public:
    ui32 GetRecordsCount() const {
        return Remapper->num_rows();
    }

    std::vector<TPortionSplitInfo>& MutablePortions() {
        return Portions;
    }

    std::vector<TGeneralSerializedSlice> BuildSlices(const std::shared_ptr<NColumnShard::TSplitterCounters>& counters) const {
        std::vector<TGeneralSerializedSlice> result;
        bool needWarnLog = false;
        for (auto&& i : Portions) {
            result.emplace_back(i.BuildSlice(ResultFiltered, Stats, counters));
            if (Portions.size() > 1 && (result.back().GetPackedSize() < 0.5 * Settings.GetMaxPortionSize() ||
                                           result.back().GetPackedSize() > 2 * (ui64)Settings.GetMaxPortionSize())) {
                needWarnLog = true;
            }
        }
        if (needWarnLog) {
            auto batchStats = Stats->GetStatsForColumns(ResultFiltered->GetColumnIds(), false);
            for (auto&& i : result) {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_COMPACTION)("p_size", i.GetPackedSize())(
                    "expected_size", batchStats ? batchStats->PredictPackedSize(i.GetRecordsCount()) : 0)(
                    "s_type", SplittingType ? (ui32)*SplittingType : 999999)("settings", Settings.DebugString())("count", Portions.size())(
                    "r_count", i.GetRecordsCount())("b_stats", batchStats ? batchStats->DebugString() : TString("NO"));
            }
        }
        return result;
    }

    TSplittedBatch(std::shared_ptr<arrow::RecordBatch>&& remapper, const std::shared_ptr<NArrow::NSplitter::TSerializationStats>& stats,
        const std::shared_ptr<TFilteredSnapshotSchema>& resultFiltered, const NSplitter::TSplitSettings& settings)
        : Remapper(std::move(remapper))
        , Stats(stats)
        , ResultFiltered(resultFiltered)
        , Settings(settings) {
        AFL_VERIFY(Remapper);
        const std::vector<i64> recordsCount = [&]() {
            auto batchStatsOpt = stats->GetStatsForColumns(resultFiltered->GetColumnIds(), false);
            if (batchStatsOpt) {
                SplittingType = ESplittingType::Stats;
                return batchStatsOpt->SplitRecordsForBlobSize(Remapper->num_rows(), settings.GetExpectedPortionSize());
            } else {
                SplittingType = ESplittingType::RecordsCount;
                return NArrow::NSplitter::TSimilarPacker::SplitWithExpected(Remapper->num_rows(), settings.GetExpectedPortionRecordsCount());
            }
        }();
        ui32 recordsCountCursor = 0;
        for (auto&& p : recordsCount) {
            AFL_VERIFY(recordsCountCursor + p <= Remapper->num_rows())("cursor", recordsCountCursor)("size", p)("length", Remapper->num_rows());
            Portions.emplace_back(p, Remapper->Slice(recordsCountCursor, p));
            recordsCountCursor += p;
            for (auto&& c : resultFiltered->GetColumnIds()) {
                std::optional<NArrow::NSplitter::TSimpleSerializationStat> colStatsOpt = stats->GetColumnInfo(c);
                std::vector<i64> chunks;
                if (!colStatsOpt) {
                    auto loader = resultFiltered->GetColumnLoaderOptional(c);
                    if (loader) {
                        colStatsOpt = loader->TryBuildColumnStat();
                    }
                }
                if (!colStatsOpt) {
                    AFL_WARN(NKikimrServices::TX_COLUMNSHARD_COMPACTION)("event", "incorrect_case_stat")("stat", Stats->DebugString())(
                        "column_id", c)("schema", resultFiltered->DebugString());
                    chunks = NArrow::NSplitter::TSimilarPacker::SplitWithExpected(p, settings.GetExpectedRecordsCountOnPage());
                } else {
                    chunks = colStatsOpt->SplitRecords(
                        p, settings.GetExpectedRecordsCountOnPage(), settings.GetExpectedBlobPage(), settings.GetMaxBlobSize() * 0.9);
                }
                Portions.back().AddColumnSplitting(c, std::move(chunks), colStatsOpt, settings);
            }
        }
        AFL_VERIFY(recordsCountCursor == Remapper->num_rows())("cursor", recordsCountCursor)("length", Remapper->num_rows());
    }
};

class TSplitTopology {
private:
    const std::shared_ptr<NArrow::NSplitter::TSerializationStats> Stats;
    const std::shared_ptr<TFilteredSnapshotSchema> ResultFiltered;
    YDB_READONLY_DEF(std::vector<TSplittedBatch>, SplittedBatches);

public:
    std::vector<TSplittedBatch>& MutableSplittedBatches() {
        return SplittedBatches;
    }

    TSplitTopology(
        const std::shared_ptr<NArrow::NSplitter::TSerializationStats>& stats, const std::shared_ptr<TFilteredSnapshotSchema>& resultFiltered)
        : Stats(stats)
        , ResultFiltered(resultFiltered) {
    }

    void FillRemapping(std::vector<std::shared_ptr<arrow::RecordBatch>>&& remapping, const NSplitter::TSplitSettings& settings) {
        AFL_VERIFY(SplittedBatches.empty());
        for (auto&& i : remapping) {
            SplittedBatches.emplace_back(std::move(i), Stats, ResultFiltered, settings);
        }
    }
};

std::vector<TWritePortionInfoWithBlobsResult> TMerger::Execute(const std::shared_ptr<NArrow::NSplitter::TSerializationStats>& stats,
    const NArrow::NMerger::TIntervalPositions& checkPoints, const std::shared_ptr<TFilteredSnapshotSchema>& resultFiltered,
    const TInternalPathId pathId, const std::optional<ui64> shardingActualVersion) {
    AFL_VERIFY(Batches.size() == Filters.size());
    TSplitTopology splitInfo(stats, resultFiltered);
    {
        arrow::FieldVector indexFields;
        indexFields.emplace_back(IColumnMerger::PortionIdField);
        indexFields.emplace_back(IColumnMerger::PortionRecordIndexField);
        if (resultFiltered->HasColumnId((ui32)IIndexInfo::ESpecialColumn::DELETE_FLAG)) {
            IIndexInfo::AddDeleteFields(indexFields);
        }
        IIndexInfo::AddSnapshotFields(indexFields);
        auto dataSchema = std::make_shared<arrow::Schema>(indexFields);
        NArrow::NMerger::TMergePartialStream mergeStream(
            resultFiltered->GetIndexInfo().GetReplaceKey(), dataSchema, false, IIndexInfo::GetSnapshotColumnNames(), std::nullopt);

        ui32 idx = 0;
        for (auto&& batch : Batches) {
            {
                NArrow::NConstruction::IArrayBuilder::TPtr column =
                    std::make_shared<NArrow::NConstruction::TSimpleArrayConstructor<NArrow::NConstruction::TIntConstFiller<arrow::UInt16Type>>>(
                        IColumnMerger::PortionIdFieldName, idx);
                batch->AddField(IColumnMerger::PortionIdField, column->BuildArray(batch->num_rows())).Validate();
            }
            {
                NArrow::NConstruction::IArrayBuilder::TPtr column =
                    std::make_shared<NArrow::NConstruction::TSimpleArrayConstructor<NArrow::NConstruction::TIntSeqFiller<arrow::UInt32Type>>>(
                        IColumnMerger::PortionRecordIndexFieldName);
                batch->AddField(IColumnMerger::PortionRecordIndexField, column->BuildArray(batch->num_rows())).Validate();
            }
            mergeStream.AddSource(batch, Filters[idx], NArrow::NMerger::TIterationOrder::Forward(0));
            ++idx;
        }
        auto batchResults = mergeStream.DrainAllParts(checkPoints, indexFields);
        NSplitter::TSplitSettings settings;
        settings.SetMaxPortionSize(PortionExpectedSize);
        splitInfo.FillRemapping(std::move(batchResults), NYDBTest::TControllers::GetColumnShardController()->GetBlobSplitSettings(settings));
    }

    using TColumnData = std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>;
    THashMap<ui32, TColumnData> columnsData;
    {
        ui32 batchIdx = 0;
        for (auto&& p : Batches) {
            ui32 columnIdx = 0;
            for (auto&& i : p->GetSchema()->GetFields()) {
                const std::optional<ui32> columnId = resultFiltered->GetIndexInfo().GetColumnIdOptional(i->name());
                if (columnId) {
                    auto it = columnsData.find(*columnId);
                    if (it == columnsData.end()) {
                        it = columnsData.emplace(*columnId, TColumnData(Batches.size())).first;
                    }
                    it->second[batchIdx] = p->GetColumnVerified(columnIdx);
                }
                ++columnIdx;
            }
            ++batchIdx;
        }
    }

    TMergingContext mergingContext(Batches);

    for (auto&& [columnId, columnData] : columnsData) {
        if (columnId == (ui32)IIndexInfo::ESpecialColumn::WRITE_ID &&
            (!HasAppData() || !AppDataVerified().FeatureFlags.GetEnableInsertWriteIdSpecialColumnCompatibility())) {
            continue;
        }
        const TString& columnName = resultFiltered->GetIndexInfo().GetColumnName(columnId);
        NActors::TLogContextGuard logGuard(NActors::TLogContextBuilder::Build()("field_name", columnName));
        auto columnInfo = stats->GetColumnInfo(columnId);

        TColumnMergeContext commonContext(
            columnId, resultFiltered, NSplitter::TSplitSettings().GetExpectedUnpackColumnChunkRawSize(), columnInfo);
        if (OptimizationWritingPackMode) {
            commonContext.MutableSaver().AddSerializerWithBorder(
                100, std::make_shared<NArrow::NSerialization::TNativeSerializer>(arrow::Compression::type::UNCOMPRESSED));
            commonContext.MutableSaver().AddSerializerWithBorder(
                Max<ui32>(), std::make_shared<NArrow::NSerialization::TNativeSerializer>(arrow::Compression::type::LZ4_FRAME));
        }

        THolder<IColumnMerger> merger =
            IColumnMerger::TFactory::MakeHolder(commonContext.GetLoader()->GetAccessorConstructor().GetClassName(), commonContext);
        AFL_VERIFY(!!merger)("problem", "cannot create merger")(
            "class_name", commonContext.GetLoader()->GetAccessorConstructor().GetClassName());
        merger->Start(std::move(columnData), mergingContext);

        for (auto&& batchResult : splitInfo.MutableSplittedBatches()) {
            for (auto&& portion : batchResult.MutablePortions()) {
                ui32 cursor = 0;
                for (auto&& c : portion.GetColumnInfo(columnId).GetChunkRecordsCount()) {
                    TChunkMergeContext context(Context.Counters, portion.MutableMergingContext().Slice(cursor, c));
                    cursor += c;
                    portion.AddColumnChunks(columnId, merger->Execute(context, mergingContext).GetChunks());
                }
                AFL_VERIFY(cursor == portion.GetRecordsCount());
            }
        }
    }

    const auto groups =
        resultFiltered->GetIndexInfo().GetEntityGroupsByStorageId(IStoragesManager::DefaultStorageId, *SaverContext.GetStoragesManager());
    std::vector<TWritePortionInfoWithBlobsResult> result;
    for (auto&& columnChunks : splitInfo.GetSplittedBatches()) {
        auto batchResult = columnChunks.GetRemapper();
        std::vector<TGeneralSerializedSlice> packs = columnChunks.BuildSlices(Context.Counters.SplitterCounters);
        std::shared_ptr<TDefaultSchemaDetails> schemaDetails(new TDefaultSchemaDetails(resultFiltered, stats));
        ui32 recordIdx = 0;
        for (auto&& i : packs) {
            TGeneralSerializedSlice slicePrimary(std::move(i));
            auto dataWithSecondary =
                resultFiltered->GetIndexInfo()
                    .AppendIndexes(slicePrimary.GetPortionChunksToHash(), SaverContext.GetStoragesManager(), slicePrimary.GetRecordsCount())
                    .DetachResult();
            TGeneralSerializedSlice slice(dataWithSecondary.GetExternalData(), schemaDetails, Context.Counters.SplitterCounters);

            auto b = batchResult->Slice(recordIdx, slice.GetRecordsCount());
            const ui32 deletionsCount = IIndexInfo::CalcDeletions(b, false);
            auto constructor = TWritePortionInfoWithBlobsConstructor::BuildByBlobs(slice.GroupChunksByBlobs(groups),
                dataWithSecondary.GetSecondaryInplaceData(), pathId, resultFiltered->GetVersion(), resultFiltered->GetSnapshot(),
                SaverContext.GetStoragesManager(), EPortionType::Compacted);
///?????
            NArrow::TFirstLastSpecialKeys primaryKeys(slice.GetFirstLastPKBatch(resultFiltered->GetIndexInfo().GetReplaceKey()));
            NArrow::TMinMaxSpecialKeys snapshotKeys(b, TIndexInfo::ArrowSchemaSnapshot());
            constructor.GetPortionConstructor().MutablePortionConstructor().AddMetadata(
                *resultFiltered, deletionsCount, primaryKeys, snapshotKeys);
            constructor.GetPortionConstructor().MutablePortionConstructor().MutableMeta().SetTierName(IStoragesManager::DefaultStorageId);
            if (shardingActualVersion) {
                constructor.GetPortionConstructor().MutablePortionConstructor().SetShardingVersion(*shardingActualVersion);
            }
            result.emplace_back(std::move(constructor));
            recordIdx += slice.GetRecordsCount();
        }
    }
    return result;
}

}   // namespace NKikimr::NOlap::NCompaction
