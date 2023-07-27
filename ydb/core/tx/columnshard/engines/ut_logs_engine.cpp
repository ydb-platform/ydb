#include <library/cpp/testing/unittest/registar.h>
#include "column_engine_logs.h"
#include "predicate/predicate.h"

#include <ydb/core/tx/columnshard/columnshard_ut_common.h>


namespace NKikimr {

using namespace NOlap;
namespace NTypeIds = NScheme::NTypeIds;
using TTypeId = NScheme::TTypeId;
using TTypeInfo = NScheme::TTypeInfo;

namespace {

class TTestDbWrapper : public IDbWrapper {
public:
    struct TIndex {
        THashMap<ui64, std::vector<TGranuleRecord>> Granules; // pathId -> granule
        THashMap<ui64, std::vector<TColumnRecord>> Columns; // granule -> columns
        THashMap<ui32, ui64> Counters;
    };

    void Insert(const TInsertedData& data) override {
        Inserted.emplace(TWriteId{data.WriteTxId}, data);
    }

    void Commit(const TInsertedData& data) override {
        Committed[data.PathId].emplace(data);
    }

    void Abort(const TInsertedData& data) override {
        Aborted.emplace(TWriteId{data.WriteTxId}, data);
    }

    void EraseInserted(const TInsertedData& data) override {
        Inserted.erase(TWriteId{data.WriteTxId});
    }

    void EraseCommitted(const TInsertedData& data) override {
        Committed[data.PathId].erase(data);
    }

    void EraseAborted(const TInsertedData& data) override {
        Aborted.erase(TWriteId{data.WriteTxId});
    }

    bool Load(TInsertTableAccessor& accessor,
              const TInstant&) override {
        for (auto&& i : Inserted) {
            accessor.AddInserted(std::move(i.second), true);
        }
        for (auto&& i : Aborted) {
            accessor.AddAborted(std::move(i.second), true);
        }
        for (auto&& i : Committed) {
            for (auto&& c: i.second) {
                auto copy = c;
                accessor.AddCommitted(std::move(copy), true);
            }
        }
        return true;
    }

    void WriteGranule(ui32 index, const IColumnEngine&, const TGranuleRecord& row) override {
        auto& granules = Indices[index].Granules[row.PathId];

        bool replaced = false;
        for (auto& rec : granules) {
            if (rec == row) {
                rec = row;
                replaced = true;
                break;
            }
        }
        if (!replaced) {
            granules.push_back(row);
        }
    }

    void EraseGranule(ui32 index, const IColumnEngine&, const TGranuleRecord& row) override {
        auto& pathGranules = Indices[index].Granules[row.PathId];

        std::vector<TGranuleRecord> filtered;
        filtered.reserve(pathGranules.size());
        for (const TGranuleRecord& rec : pathGranules) {
            if (rec.Granule != row.Granule) {
                filtered.push_back(rec);
            }
        }
        pathGranules.swap(filtered);
    }

    bool LoadGranules(ui32 index, const IColumnEngine&, const std::function<void(const TGranuleRecord&)>& callback) override {
        auto& granules = Indices[index].Granules;
        for (auto& [pathId, vec] : granules) {
            for (const auto& rec : vec) {
                callback(rec);
            }
        }
        return true;
    }

    void WriteColumn(ui32 index, const TColumnRecord& row) override {
        auto& columns = Indices[index].Columns[row.Granule];

        bool replaced = false;
        for (auto& rec : columns) {
            if (rec == row) {
                rec = row;
                replaced = true;
                break;
            }
        }
        if (!replaced) {
            columns.push_back(row);
        }
    }

    void EraseColumn(ui32 index, const TColumnRecord& row) override {
        auto& columns = Indices[index].Columns[row.Granule];

        std::vector<TColumnRecord> filtered;
        filtered.reserve(columns.size());
        for (auto& rec : columns) {
            if (rec != row) {
                filtered.push_back(rec);
            }
        }
        columns.swap(filtered);
    }

    bool LoadColumns(ui32 index, const std::function<void(const TColumnRecord&)>& callback) override {
        auto& columns = Indices[index].Columns;
        for (auto& [granule, vec] : columns) {
            for (const auto& rec : vec) {
                callback(rec);
            }
        }
        return true;
    }

    void WriteCounter(ui32 index, ui32 counterId, ui64 value) override {
        auto& counters = Indices[index].Counters;
        counters[counterId] = value;
    }

    bool LoadCounters(ui32 index, const std::function<void(ui32 id, ui64 value)>& callback) override {
        auto& counters = Indices[index].Counters;
        for (auto& [id, value] : counters) {
            callback(id, value);
        }
        return true;
    }

private:
    THashMap<TWriteId, TInsertedData> Inserted;
    THashMap<ui64, TSet<TInsertedData>> Committed;
    THashMap<TWriteId, TInsertedData> Aborted;
    THashMap<ui32, TIndex> Indices;
};

static const std::vector<std::pair<TString, TTypeInfo>> testColumns = {
    // PK
    {"timestamp", TTypeInfo(NTypeIds::Timestamp) },
    {"resource_type", TTypeInfo(NTypeIds::Utf8) },
    {"resource_id", TTypeInfo(NTypeIds::Utf8) },
    {"uid", TTypeInfo(NTypeIds::Utf8) },
    //
    {"message", TTypeInfo(NTypeIds::Utf8) }
};

static const std::vector<std::pair<TString, TTypeInfo>> testKey = {
    {"timestamp", TTypeInfo(NTypeIds::Timestamp) },
    {"resource_type", TTypeInfo(NTypeIds::Utf8) },
    {"resource_id", TTypeInfo(NTypeIds::Utf8) },
    {"uid", TTypeInfo(NTypeIds::Utf8) }
};

template <typename TKeyDataType>
class TBuilder {
public:
    using TTraits = typename arrow::TypeTraits<TKeyDataType>;
    using TCType = std::conditional_t<arrow::has_c_type<TKeyDataType>::value, typename TTraits::CType, std::string>;

    struct TRow {
        TCType Timestamp;
        std::string ResourceType;
        std::string ResourceId;
        std::string Uid;
        std::string Message;
    };

    TBuilder()
        : Schema(NArrow::MakeArrowSchema(testColumns))
    {
        auto status = arrow::RecordBatchBuilder::Make(Schema, arrow::default_memory_pool(), &BatchBuilder);
        Y_VERIFY(status.ok());
    }

    bool AddRow(const TRow& row) {
        bool ok = true;
        ok = ok && BatchBuilder->GetFieldAs<typename TTraits::BuilderType>(0)->Append(row.Timestamp).ok();
        ok = ok && BatchBuilder->GetFieldAs<arrow::TypeTraits<arrow::StringType>::BuilderType>(1)->Append(row.ResourceType).ok();
        ok = ok && BatchBuilder->GetFieldAs<arrow::TypeTraits<arrow::StringType>::BuilderType>(2)->Append(row.ResourceId).ok();
        ok = ok && BatchBuilder->GetFieldAs<arrow::TypeTraits<arrow::StringType>::BuilderType>(3)->Append(row.Uid).ok();
        ok = ok && BatchBuilder->GetFieldAs<arrow::TypeTraits<arrow::StringType>::BuilderType>(4)->Append(row.Message).ok();
        return ok;
    }

    std::shared_ptr<arrow::RecordBatch> Finish() {
        std::shared_ptr<arrow::RecordBatch> batch;
        auto status = BatchBuilder->Flush(&batch);
        Y_VERIFY(status.ok());
        return batch;
    }

private:
    std::shared_ptr<arrow::Schema> Schema = NArrow::MakeArrowSchema(testColumns);
    std::unique_ptr<arrow::RecordBatchBuilder> BatchBuilder;
};

TBlobRange MakeBlobRange(ui32 step, ui32 blobSize) {
    // tabletId, generation, step, channel, blobSize, cookie
    return TBlobRange(TUnifiedBlobId(11111, TLogoBlobID(100500, 42, step, 3, blobSize, 0)), 0, blobSize);
}

TString MakeTestBlob(i64 start = 0, i64 end = 100) {
    TBuilder<arrow::TimestampType> builder;
    for (i64 ts = start; ts < end; ++ts) {
        TString str = ToString(ts);
        TString sortedStr = Sprintf("%05ld", (long)ts);
        builder.AddRow({ts, sortedStr, str, str, str});
    }
    auto batch = builder.Finish();
    return NArrow::SerializeBatchNoCompression(batch);
}

void AddIdsToBlobs(const std::vector<TString>& srcBlobs, std::vector<TPortionInfo>& portions,
                   THashMap<TBlobRange, TString>& blobs, ui32& step) {
    ui32 pos = 0;
    for (auto& portion : portions) {
        for (auto& rec : portion.Records) {
            rec.BlobRange = MakeBlobRange(++step, srcBlobs[pos].size());
            //UNIT_ASSERT(rec.Valid());
            blobs[rec.BlobRange] = srcBlobs[pos];
            ++pos;
        }
    }
}

TCompactionLimits TestLimits() {
    TCompactionLimits limits;
    limits.GranuleBlobSplitSize = 1024;
    limits.GranuleSizeForOverloadPrevent = 400 * 1024;
    limits.GranuleOverloadSize = 800 * 1024;
    return limits;
}

bool Insert(TColumnEngineForLogs& engine, TTestDbWrapper& db, TSnapshot snap,
            std::vector<TInsertedData>&& dataToIndex, THashMap<TBlobRange, TString>& blobs, ui32& step) {
    std::shared_ptr<TInsertColumnEngineChanges> changes = engine.StartInsert(TestLimits(), std::move(dataToIndex));
    if (!changes) {
        return false;
    }

    changes->Blobs.insert(blobs.begin(), blobs.end());
    changes->StartEmergency();

    NOlap::TConstructionContext context(engine.GetVersionedIndex(), NColumnShard::TIndexationCounters("Indexation"));
    std::vector<TString> newBlobs = std::move(changes->ConstructBlobs(context).DetachResult());

    UNIT_ASSERT_VALUES_EQUAL(changes->AppendedPortions.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(newBlobs.size(), testColumns.size() + 2); // add 2 columns: planStep, txId

    AddIdsToBlobs(newBlobs, changes->AppendedPortions, blobs, step);

    const bool result = engine.ApplyChanges(db, changes, snap);
    changes->AbortEmergency();
    return result;
}

struct TExpected {
    ui32 SrcPortions;
    ui32 NewPortions;
    ui32 NewGranules;
};

bool Compact(TColumnEngineForLogs& engine, TTestDbWrapper& db, TSnapshot snap, THashMap<TBlobRange, TString>&& blobs, ui32& step,
             const TExpected& expected) {
    auto compactionInfo = engine.Compact(TestLimits(), {});
    UNIT_ASSERT(!!compactionInfo);

    std::shared_ptr<TCompactColumnEngineChanges> changes = engine.StartCompaction(std::move(compactionInfo), TestLimits());
    UNIT_ASSERT(changes->IsSplit());
    UNIT_ASSERT_VALUES_EQUAL(changes->SwitchedPortions.size(), expected.SrcPortions);
    changes->SetBlobs(std::move(blobs));
    changes->StartEmergency();
    NOlap::TConstructionContext context(engine.GetVersionedIndex(), NColumnShard::TIndexationCounters("Compaction"));
    std::vector<TString> newBlobs = std::move(changes->ConstructBlobs(context).DetachResult());

    UNIT_ASSERT_VALUES_EQUAL(changes->AppendedPortions.size(), expected.NewPortions);
    AddIdsToBlobs(newBlobs, changes->AppendedPortions, changes->Blobs, step);

    UNIT_ASSERT_VALUES_EQUAL(changes->GetTmpGranuleIds().size(), expected.NewGranules);

    const bool result = engine.ApplyChanges(db, changes, snap);
    changes->AbortEmergency();
    return result;
}

bool Cleanup(TColumnEngineForLogs& engine, TTestDbWrapper& db, TSnapshot snap, ui32 expectedToDrop) {
    THashSet<ui64> pathsToDrop;
    std::shared_ptr<TCleanupColumnEngineChanges> changes = engine.StartCleanup(snap, TestLimits(), pathsToDrop, 1000);
    UNIT_ASSERT(changes);
    UNIT_ASSERT_VALUES_EQUAL(changes->PortionsToDrop.size(), expectedToDrop);


    changes->StartEmergency();
    const bool result = engine.ApplyChanges(db, changes, snap);
    changes->AbortEmergency();
    return result;
}

bool Ttl(TColumnEngineForLogs& engine, TTestDbWrapper& db,
         const THashMap<ui64, NOlap::TTiering>& pathEviction, ui32 expectedToDrop) {
    std::shared_ptr<TTTLColumnEngineChanges> changes = engine.StartTtl(pathEviction);
    UNIT_ASSERT(changes);
    UNIT_ASSERT_VALUES_EQUAL(changes->PortionsToDrop.size(), expectedToDrop);


    changes->StartEmergency();
    const bool result = engine.ApplyChanges(db, changes, TSnapshot(1,0));
    changes->AbortEmergency();
    return result;
}

std::shared_ptr<TPredicate> MakePredicate(int64_t ts, NArrow::EOperation op) {
    auto type = arrow::timestamp(arrow::TimeUnit::MICRO);
    auto res = arrow::MakeArrayFromScalar(arrow::TimestampScalar(ts, type), 1);

    std::vector<std::shared_ptr<arrow::Field>> fields = { std::make_shared<arrow::Field>("timestamp", type) };
    return std::make_shared<TPredicate>(op, arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(std::move(fields)), 1, { *res }));
}

std::shared_ptr<TPredicate> MakeStrPredicate(const std::string& key, NArrow::EOperation op) {
    auto type = arrow::utf8();
    auto res = arrow::MakeArrayFromScalar(arrow::StringScalar(key), 1);

    std::vector<std::shared_ptr<arrow::Field>> fields = { std::make_shared<arrow::Field>("resource_type", type) };
    return std::make_shared<TPredicate>(op, arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(std::move(fields)), 1, { *res }));
}

} // namespace


Y_UNIT_TEST_SUITE(TColumnEngineTestLogs) {
    void WriteLoadRead(const std::vector<std::pair<TString, TTypeInfo>>& ydbSchema,
                       const std::vector<std::pair<TString, TTypeInfo>>& key) {
        TTestDbWrapper db;
        TIndexInfo tableInfo = NColumnShard::BuildTableInfo(ydbSchema, key);

        std::vector<ui64> paths = {1, 2};

        TString testBlob = MakeTestBlob();

        std::vector<TBlobRange> blobRanges;
        blobRanges.push_back(MakeBlobRange(1, testBlob.size()));
        blobRanges.push_back(MakeBlobRange(2, testBlob.size()));

        // PlanStep, TxId, PathId, DedupId, BlobId, Data, [Metadata]
        TInstant writeTime = TInstant::Now();

        // load
        TColumnEngineForLogs engine(0);
        TSnapshot indexSnaphot(1, 1);
        engine.UpdateDefaultSchema(indexSnaphot, TIndexInfo(tableInfo));
        THashSet<TUnifiedBlobId> lostBlobs;
        engine.Load(db, lostBlobs);

        std::vector<TInsertedData> dataToIndex = {
            {1, 2, paths[0], "", blobRanges[0].BlobId, "", writeTime, indexSnaphot},
            {2, 1, paths[0], "", blobRanges[1].BlobId, "", writeTime, indexSnaphot}
        };

        // write

        ui32 step = 1000;
        THashMap<TBlobRange, TString> blobs;
        blobs[blobRanges[0]] = testBlob;
        blobs[blobRanges[1]] = testBlob;
        Insert(engine, db, TSnapshot(1, 2), std::move(dataToIndex), blobs, step);
       
        // selects

        auto lastSchema = engine.GetVersionedIndex().GetLastSchema();
        UNIT_ASSERT_EQUAL(lastSchema->GetSnapshot(), indexSnaphot);
        const TIndexInfo& indexInfo = lastSchema->GetIndexInfo();
        THashSet<ui32> oneColumnId = { indexInfo.GetColumnId(testColumns[0].first) };
        THashSet<ui32> columnIds;
        for (auto& [column, typeId] : testColumns) {
            columnIds.insert(indexInfo.GetColumnId(column));
        }

        { // select from snap before insert
            ui64 planStep = 1;
            ui64 txId = 0;
            auto selectInfo = engine.Select(paths[0], TSnapshot(planStep, txId), columnIds, NOlap::TPKRangesFilter(false));
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->Granules.size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->Portions.size(), 0);
        }

        { // select from snap between insert (greater txId)
            ui64 planStep = 1;
            ui64 txId = 2;
            auto selectInfo = engine.Select(paths[0], TSnapshot(planStep, txId), columnIds, NOlap::TPKRangesFilter(false));
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->Granules.size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->Portions.size(), 0);
        }

        { // select from snap after insert (greater planStep)
            ui64 planStep = 2;
            ui64 txId = 1;
            auto selectInfo = engine.Select(paths[0], TSnapshot(planStep, txId), oneColumnId, NOlap::TPKRangesFilter(false));
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->Granules.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->Portions.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->Portions[0].NumRecords(), 1);
        }

        { // select another pathId
            ui64 planStep = 2;
            ui64 txId = 1;
            auto selectInfo = engine.Select(paths[1], TSnapshot(planStep, txId), oneColumnId, NOlap::TPKRangesFilter(false));
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->Granules.size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->Portions.size(), 0);
        }
    }

    Y_UNIT_TEST(IndexWriteLoadRead) {
        WriteLoadRead(testColumns, testKey);
    }

    Y_UNIT_TEST(IndexWriteLoadReadStrPK) {
        std::vector<std::pair<TString, TTypeInfo>> key = {
            {"resource_type", TTypeInfo(NTypeIds::Utf8) },
            {"resource_id", TTypeInfo(NTypeIds::Utf8) },
            {"uid", TTypeInfo(NTypeIds::Utf8) },
            {"timestamp", TTypeInfo(NTypeIds::Timestamp) }
        };

        WriteLoadRead(testColumns, key);
    }

    void ReadWithPredicates(const std::vector<std::pair<TString, TTypeInfo>>& ydbSchema,
                            const std::vector<std::pair<TString, TTypeInfo>>& key) {
        TTestDbWrapper db;
        TIndexInfo tableInfo = NColumnShard::BuildTableInfo(ydbSchema, key);

        TSnapshot indexSnapshot(1, 1);
        TColumnEngineForLogs engine(0, TestLimits());
        engine.UpdateDefaultSchema(indexSnapshot, TIndexInfo(tableInfo));
        THashSet<TUnifiedBlobId> lostBlobs;
        engine.Load(db, lostBlobs);

        ui64 pathId = 1;
        ui32 step = 1000;

        // insert
        ui64 planStep = 1;

        THashMap<TBlobRange, TString> blobs;
        ui64 numRows = 1000;
        ui64 rowPos = 0;
        for (ui64 txId = 1; txId <= 20; ++txId, rowPos += numRows) {
            TString testBlob = MakeTestBlob(rowPos, rowPos + numRows);
            auto blobRange = MakeBlobRange(++step, testBlob.size());
            blobs[blobRange] = testBlob;

            // PlanStep, TxId, PathId, DedupId, BlobId, Data, [Metadata]
            std::vector<TInsertedData> dataToIndex;
            dataToIndex.push_back(
                TInsertedData{planStep, txId, pathId, "", blobRange.BlobId, "", TInstant::Now(), indexSnapshot});

            bool ok = Insert(engine, db, TSnapshot(planStep, txId), std::move(dataToIndex), blobs, step);
            UNIT_ASSERT(ok);
        }

        // compact
        planStep = 2;

        bool ok = Compact(engine, db, TSnapshot(planStep, 1), std::move(blobs), step, {20, 4, 4});
        UNIT_ASSERT(ok);

        // read

        // TODO: read old snapshot

        planStep = 3;

        const TIndexInfo& indexInfo = engine.GetVersionedIndex().GetLastSchema()->GetIndexInfo();
        THashSet<ui32> oneColumnId = { indexInfo.GetColumnId(key[0].first) };

        { // full scan
            ui64 txId = 1;
            auto selectInfo = engine.Select(pathId, TSnapshot(planStep, txId), oneColumnId, NOlap::TPKRangesFilter(false));
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->Granules.size(), 4);
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->Portions.size(), 4);
        }

        // predicates

        {
            ui64 txId = 1;
            std::shared_ptr<TPredicate> gt10k = MakePredicate(10000, NArrow::EOperation::Greater);
            if (key[0].second == TTypeInfo(NTypeIds::Utf8)) {
                gt10k = MakeStrPredicate("10000", NArrow::EOperation::Greater);
            }
            NOlap::TPKRangesFilter pkFilter(false);
            Y_VERIFY(pkFilter.Add(gt10k, nullptr, nullptr));
            auto selectInfo = engine.Select(pathId, TSnapshot(planStep, txId), oneColumnId, pkFilter);
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->Granules.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->Portions.size(), 2);
        }

        {
            ui64 txId = 1;
            std::shared_ptr<TPredicate> lt10k = MakePredicate(9999, NArrow::EOperation::Less); // TODO: better border checks
            if (key[0].second == TTypeInfo(NTypeIds::Utf8)) {
                lt10k = MakeStrPredicate("09999", NArrow::EOperation::Less);
            }
            NOlap::TPKRangesFilter pkFilter(false);
            Y_VERIFY(pkFilter.Add(nullptr, lt10k, nullptr));
            auto selectInfo = engine.Select(pathId, TSnapshot(planStep, txId), oneColumnId, pkFilter);
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->Granules.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->Portions.size(), 2);
        }
    }

    Y_UNIT_TEST(IndexReadWithPredicates) {
        ReadWithPredicates(testColumns, testKey);
    }

    Y_UNIT_TEST(IndexReadWithPredicatesStrPK) {
        std::vector<std::pair<TString, TTypeInfo>> key = {
            {"resource_type", TTypeInfo(NTypeIds::Utf8) },
            {"resource_id", TTypeInfo(NTypeIds::Utf8) },
            {"uid", TTypeInfo(NTypeIds::Utf8) },
            {"timestamp", TTypeInfo(NTypeIds::Timestamp) }
        };

        ReadWithPredicates(testColumns, key);
    }

    Y_UNIT_TEST(IndexWriteOverload) {
        TTestDbWrapper db;
        TIndexInfo tableInfo = NColumnShard::BuildTableInfo(testColumns, testKey);;

        ui64 pathId = 1;
        ui32 step = 1000;

        // inserts
        ui64 planStep = 1;

        TColumnEngineForLogs engine(0, TestLimits());
        TSnapshot indexSnapshot(1, 1);
        engine.UpdateDefaultSchema(indexSnapshot, TIndexInfo(tableInfo));
        THashSet<TUnifiedBlobId> lostBlobs;
        engine.Load(db, lostBlobs);

        THashMap<TBlobRange, TString> blobs;
        ui64 numRows = 1000;
        ui64 rowPos = 0;
        bool overload = false;
        for (ui64 txId = 1; txId <= 100; ++txId, rowPos += numRows) {
            TString testBlob = MakeTestBlob(rowPos, rowPos + numRows);
            auto blobRange = MakeBlobRange(++step, testBlob.size());
            blobs[blobRange] = testBlob;

            // PlanStep, TxId, PathId, DedupId, BlobId, Data, [Metadata]
            std::vector<TInsertedData> dataToIndex;
            dataToIndex.push_back(
                TInsertedData{planStep, txId, pathId, "", blobRange.BlobId, "", TInstant::Now(), indexSnapshot});

            bool ok = Insert(engine, db, TSnapshot(planStep, txId), std::move(dataToIndex), blobs, step);
            // first overload returns ok: it's a postcondition
            if (!overload) {
                UNIT_ASSERT(ok);
            } else {
                UNIT_ASSERT(!ok);
                break;
            }
            overload = engine.GetOverloadedGranules(pathId);
        }
        UNIT_ASSERT(overload);

        { // check it's overloaded after reload
            TColumnEngineForLogs tmpEngine(0, TestLimits());
            tmpEngine.UpdateDefaultSchema(TSnapshot::Zero(), TIndexInfo(tableInfo));
            tmpEngine.Load(db, lostBlobs);
            UNIT_ASSERT(tmpEngine.GetOverloadedGranules(pathId));
        }

        // compact
        planStep = 2;

        bool ok = Compact(engine, db, TSnapshot(planStep, 1), std::move(blobs), step, {23, 5, 5});
        UNIT_ASSERT(ok);

        // success write after compaction
        planStep = 3;

        for (ui64 txId = 1; txId <= 2; ++txId, rowPos += numRows) {
            TString testBlob = MakeTestBlob(rowPos, rowPos + numRows);
            auto blobRange = MakeBlobRange(++step, testBlob.size());
            blobs[blobRange] = testBlob;

            // PlanStep, TxId, PathId, DedupId, BlobId, Data, [Metadata]
            std::vector<TInsertedData> dataToIndex;
            dataToIndex.push_back(
                TInsertedData{planStep, txId, pathId, "", blobRange.BlobId, "", TInstant::Now(), indexSnapshot});

            bool ok = Insert(engine, db, TSnapshot(planStep, txId), std::move(dataToIndex), blobs, step);
            bool overload = engine.GetOverloadedGranules(pathId);
            UNIT_ASSERT(ok);
            UNIT_ASSERT(!overload);
        }

        { // check it's not overloaded after reload
            TColumnEngineForLogs tmpEngine(0, TestLimits());
            tmpEngine.UpdateDefaultSchema(TSnapshot::Zero(), TIndexInfo(tableInfo));
            tmpEngine.Load(db, lostBlobs);
            UNIT_ASSERT(!tmpEngine.GetOverloadedGranules(pathId));
        }
    }

    Y_UNIT_TEST(IndexTtl) {
        TTestDbWrapper db;
        TIndexInfo tableInfo = NColumnShard::BuildTableInfo(testColumns, testKey);;

        ui64 pathId = 1;
        ui32 step = 1000;

        // insert
        ui64 planStep = 1;

        TSnapshot indexSnapshot(1, 1);
        TColumnEngineForLogs engine(0, TestLimits());
        engine.UpdateDefaultSchema(indexSnapshot, TIndexInfo(tableInfo));
        THashSet<TUnifiedBlobId> lostBlobs;
        engine.Load(db, lostBlobs);

        THashMap<TBlobRange, TString> blobs;
        ui64 numRows = 1000;
        ui64 rowPos = 0;
        for (ui64 txId = 1; txId <= 20; ++txId, rowPos += numRows) {
            TString testBlob = MakeTestBlob(rowPos, rowPos + numRows);
            auto blobRange = MakeBlobRange(++step, testBlob.size());
            blobs[blobRange] = testBlob;

            // PlanStep, TxId, PathId, DedupId, BlobId, Data, [Metadata]
            std::vector<TInsertedData> dataToIndex;
            dataToIndex.push_back(
                TInsertedData{planStep, txId, pathId, "", blobRange.BlobId, "", TInstant::Now(), indexSnapshot});

            bool ok = Insert(engine, db, TSnapshot(planStep, txId), std::move(dataToIndex), blobs, step);
            UNIT_ASSERT(ok);
        }

        // compact
        planStep = 2;

        bool ok = Compact(engine, db, TSnapshot(planStep, 1), std::move(blobs), step, {20, 4, 4});
        UNIT_ASSERT(ok);

        // read
        planStep = 3;

        const TIndexInfo& indexInfo = engine.GetVersionedIndex().GetLastSchema()->GetIndexInfo();
        THashSet<ui32> oneColumnId = { indexInfo.GetColumnId(testColumns[0].first) };

        { // full scan
            ui64 txId = 1;
            auto selectInfo = engine.Select(pathId, TSnapshot(planStep, txId), oneColumnId, NOlap::TPKRangesFilter(false));
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->Portions.size(), 4);
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->Granules.size(), 4);
        }

        // Cleanup
        Cleanup(engine, db, TSnapshot(planStep, 1), 20);

        { // full scan
            ui64 txId = 1;
            auto selectInfo = engine.Select(pathId, TSnapshot(planStep, txId), oneColumnId, NOlap::TPKRangesFilter(false));
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->Portions.size(), 4);
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->Granules.size(), 4);
        }

        // TTL
        std::shared_ptr<arrow::DataType> ttlColType = arrow::timestamp(arrow::TimeUnit::MICRO);
        THashMap<ui64, NOlap::TTiering> pathTtls;
        NOlap::TTiering tiering;
        tiering.Ttl = NOlap::TTierInfo::MakeTtl(TInstant::MicroSeconds(10000), "timestamp");
        pathTtls.emplace(pathId, std::move(tiering));
        Ttl(engine, db, pathTtls, 2);

        // read + load + read

        { // full scan
            ui64 txId = 1;
            auto selectInfo = engine.Select(pathId, TSnapshot(planStep, txId), oneColumnId, NOlap::TPKRangesFilter(false));
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->Portions.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->Granules.size(), 2);
        }

        // load
        engine.Load(db, lostBlobs);
        UNIT_ASSERT_VALUES_EQUAL(engine.GetTotalStats().EmptyGranules, 1);

        { // full scan
            ui64 txId = 1;
            auto selectInfo = engine.Select(pathId, TSnapshot(planStep, txId), oneColumnId, NOlap::TPKRangesFilter(false));
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->Portions.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->Granules.size(), 2);
        }
    }
}

}
