#include "helper.h"
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/predicate/predicate.h>
#include <ydb/core/tx/columnshard/engines/changes/cleanup_portions.h>
#include <ydb/core/tx/columnshard/engines/changes/indexation.h>
#include <ydb/core/tx/columnshard/engines/changes/ttl.h>

#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction.h>
#include <ydb/core/tx/columnshard/engines/portions/write_with_blobs.h>
#include <ydb/core/tx/columnshard/blobs_action/bs/storage.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/data_sharing/manager/shared_blobs.h>
#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>
#include <ydb/core/tx/columnshard/background_controller.h>
#include <ydb/core/tx/columnshard/engines/changes/abstract/abstract.h>
#include <ydb/core/tx/columnshard/test_helper/helper.h>
#include <ydb/core/tx/columnshard/engines/insert_table/insert_table.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr {

using namespace NOlap;
namespace NTypeIds = NScheme::NTypeIds;
using TTypeId = NScheme::TTypeId;
using TTypeInfo = NScheme::TTypeInfo;
using TDefaultTestsController = NKikimr::NYDBTest::NColumnShard::TController;
using namespace NKikimr::NOlap::NEngines::NTest;

namespace {

std::shared_ptr<NDataLocks::TManager> EmptyDataLocksManager = std::make_shared<NDataLocks::TManager>();

class TTestDbWrapper : public IDbWrapper {
private:
    std::map<TPortionAddress, std::map<TChunkAddress, TColumnChunkLoadContext>> LoadContexts;
public:
    struct TIndex {
        THashMap<ui64, THashMap<ui64, TPortionInfoConstructor>> Columns; // pathId -> portions
        THashMap<ui32, ui64> Counters;
    };

    virtual TConclusion<THashMap<ui64, std::map<TSnapshot, TGranuleShardingInfo>>> LoadGranulesShardingInfo() override {
        THashMap<ui64, std::map<TSnapshot, TGranuleShardingInfo>> result;
        return result;
    }

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

    virtual void WritePortion(const NOlap::TPortionInfo& /*portion*/) override {

    }
    virtual void ErasePortion(const NOlap::TPortionInfo& /*portion*/) override {

    }
    virtual bool LoadPortions(const std::function<void(NOlap::TPortionInfoConstructor&&, const NKikimrTxColumnShard::TIndexPortionMeta&)>& /*callback*/) override {
        return true;
    }

    void WriteColumn(const TPortionInfo& portion, const TColumnRecord& row, const ui32 firstPKColumnId) override {
        auto rowProto = row.GetMeta().SerializeToProto();
        if (firstPKColumnId == row.GetColumnId() && row.GetChunkIdx() == 0) {
            *rowProto.MutablePortionMeta() = portion.GetMeta().SerializeToProto();
        }

        auto& data = Indices[0].Columns[portion.GetPathId()];
        NOlap::TColumnChunkLoadContext loadContext(row.GetAddress(), portion.RestoreBlobRange(row.BlobRange), rowProto);
        auto itInsertInfo = LoadContexts[portion.GetAddress()].emplace(row.GetAddress(), loadContext);
        if (!itInsertInfo.second) {
            itInsertInfo.first->second = loadContext;
        }
        auto it = data.find(portion.GetPortion());
        if (it == data.end()) {
            it = data.emplace(portion.GetPortion(), TPortionInfoConstructor(portion, false, true)).first;
        } else {
            Y_ABORT_UNLESS(portion.GetPathId() == it->second.GetPathId() && portion.GetPortion() == it->second.GetPortionIdVerified());
        }
        it->second.SetMinSnapshotDeprecated(portion.GetMinSnapshotDeprecated());
        if (portion.HasRemoveSnapshot()) {
            if (!it->second.HasRemoveSnapshot()) {
                it->second.SetRemoveSnapshot(portion.GetRemoveSnapshotVerified());
            }
        } else {
            AFL_VERIFY(!it->second.HasRemoveSnapshot());
        }

        bool replaced = false;
        for (auto& rec : it->second.MutableRecords()) {
            if (rec.IsEqualTest(row)) {
                rec = row;
                replaced = true;
                break;
            }
        }
        if (!replaced) {
            it->second.MutableRecords().emplace_back(row);
        }
    }

    void EraseColumn(const TPortionInfo& portion, const TColumnRecord& row) override {
        auto& data = Indices[0].Columns[portion.GetPathId()];
        auto it = data.find(portion.GetPortion());
        Y_ABORT_UNLESS(it != data.end());
        auto& portionLocal = it->second;

        std::vector<TColumnRecord> filtered;
        for (auto& rec : portionLocal.GetRecords()) {
            if (!rec.IsEqualTest(row)) {
                filtered.push_back(rec);
            }
        }
        portionLocal.MutableRecords().swap(filtered);
    }

    bool LoadColumns(const std::function<void(NOlap::TPortionInfoConstructor&&, const TColumnChunkLoadContext&)>& callback) override {
        auto& columns = Indices[0].Columns;
        for (auto& [pathId, portions] : columns) {
            for (auto& [portionId, portionLocal] : portions) {
                auto copy = portionLocal;
                copy.MutableRecords().clear();
                for (const auto& rec : portionLocal.GetRecords()) {
                    auto itContextLoader = LoadContexts[copy.GetAddress()].find(rec.GetAddress());
                    Y_ABORT_UNLESS(itContextLoader != LoadContexts[copy.GetAddress()].end());
                    auto address = copy.GetAddress();
                    callback(std::move(copy), itContextLoader->second);
                    LoadContexts[address].erase(itContextLoader);
                }
            }
        }
        return true;
    }

    virtual void WriteIndex(const TPortionInfo& /*portion*/, const TIndexChunk& /*row*/) override {}
    virtual void EraseIndex(const TPortionInfo& /*portion*/, const TIndexChunk& /*row*/) override {}
    virtual bool LoadIndexes(const std::function<void(const ui64 /*pathId*/, const ui64 /*portionId*/, const TIndexChunkLoadContext&)>& /*callback*/) override { return true; }

    void WriteCounter(ui32 counterId, ui64 value) override {
        auto& counters = Indices[0].Counters;
        counters[counterId] = value;
    }

    bool LoadCounters(const std::function<void(ui32 id, ui64 value)>& callback) override {
        auto& counters = Indices[0].Counters;
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

static const std::vector<NArrow::NTest::TTestColumn> testColumns = {
    // PK
    NArrow::NTest::TTestColumn("timestamp", TTypeInfo(NTypeIds::Timestamp) ),
    NArrow::NTest::TTestColumn("resource_type", TTypeInfo(NTypeIds::Utf8) ),
    NArrow::NTest::TTestColumn("resource_id", TTypeInfo(NTypeIds::Utf8) ),
    NArrow::NTest::TTestColumn("uid", TTypeInfo(NTypeIds::Utf8) ),
    //
    NArrow::NTest::TTestColumn("message", TTypeInfo(NTypeIds::Utf8) )
};

static const std::vector<NArrow::NTest::TTestColumn> testKey = {
    NArrow::NTest::TTestColumn("timestamp", TTypeInfo(NTypeIds::Timestamp) ),
    NArrow::NTest::TTestColumn("resource_type", TTypeInfo(NTypeIds::Utf8) ),
    NArrow::NTest::TTestColumn("resource_id", TTypeInfo(NTypeIds::Utf8) ),
    NArrow::NTest::TTestColumn("uid", TTypeInfo(NTypeIds::Utf8) )
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
        Y_ABORT_UNLESS(status.ok());
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
        Y_ABORT_UNLESS(status.ok());
        return batch;
    }

private:
    std::shared_ptr<arrow::Schema> Schema = NArrow::MakeArrowSchema(testColumns);
    std::unique_ptr<arrow::RecordBatchBuilder> BatchBuilder;
};

TUnifiedBlobId MakeUnifiedBlobId(ui32 step, ui32 blobSize) {
    return TUnifiedBlobId(11111, TLogoBlobID(100500, 42, step, 3, blobSize, 0));
}

TBlobRange MakeBlobRange(ui32 step, ui32 blobSize) {
    // tabletId, generation, step, channel, blobSize, cookie
    return TBlobRange(MakeUnifiedBlobId(step, blobSize), 0, blobSize);
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

void AddIdsToBlobs(std::vector<TWritePortionInfoWithBlobs>& portions, NBlobOperations::NRead::TCompositeReadBlobs& blobs, ui32& step) {
    for (auto& portion : portions) {
        for (auto& rec : portion.GetPortionConstructor().MutableRecords()) {
            rec.BlobRange.BlobIdx = portion.GetPortionConstructor().RegisterBlobId(MakeUnifiedBlobId(++step, portion.GetBlobFullSizeVerified(rec.ColumnId, rec.Chunk)));
            TString data = portion.GetBlobByRangeVerified(rec.ColumnId, rec.Chunk);
            blobs.Add(IStoragesManager::DefaultStorageId, portion.GetPortionConstructor().RestoreBlobRange(rec.BlobRange), std::move(data));
        }
    }
}

bool Insert(TColumnEngineForLogs& engine, TTestDbWrapper& db, TSnapshot snap,
            std::vector<TInsertedData>&& dataToIndex, NBlobOperations::NRead::TCompositeReadBlobs& blobs, ui32& step) {

    for (ui32 i = 0; i < dataToIndex.size(); ++i) {
        // Commited data always has nonzero planstep (for WriteLoadRead tests)
        dataToIndex[i].PlanStep = i + 1;
    };
    std::shared_ptr<TInsertColumnEngineChanges> changes = engine.StartInsert(std::move(dataToIndex));
    if (!changes) {
        return false;
    }

    changes->Blobs = std::move(blobs);
    blobs.Clear();
    changes->StartEmergency();

    NOlap::TConstructionContext context(engine.GetVersionedIndex(), NColumnShard::TIndexationCounters("Indexation"), snap);
    Y_ABORT_UNLESS(changes->ConstructBlobs(context).Ok());

    UNIT_ASSERT_VALUES_EQUAL(changes->AppendedPortions.size(), 1);
    ui32 blobsCount = 0;
    for (auto&& i : changes->AppendedPortions) {
        blobsCount += i.GetBlobs().size();
    }
    UNIT_ASSERT_VALUES_EQUAL(blobsCount, 1); // add 2 columns: planStep, txId

    AddIdsToBlobs(changes->AppendedPortions, blobs, step);

    const bool result = engine.ApplyChangesOnTxCreate(changes, snap) && engine.ApplyChangesOnExecute(db, changes, snap);

    NOlap::TWriteIndexContext contextExecute(nullptr, db, engine);
    changes->WriteIndexOnExecute(nullptr, contextExecute);
    NOlap::TWriteIndexCompleteContext contextComplete(NActors::TActivationContext::AsActorContext(), 0, 0, TDuration::Zero(), engine);
    changes->WriteIndexOnComplete(nullptr, contextComplete);
    changes->AbortEmergency("testing");
    return result;
}

struct TExpected {
    ui32 SrcPortions;
    ui32 NewPortions;
    ui32 NewGranules;
};

bool Compact(TColumnEngineForLogs& engine, TTestDbWrapper& db, TSnapshot snap, NBlobOperations::NRead::TCompositeReadBlobs&& blobs, ui32& step,
             const TExpected& /*expected*/, THashMap<TBlobRange, TString>* blobsPool = nullptr) {
    std::shared_ptr<TCompactColumnEngineChanges> changes = dynamic_pointer_cast<TCompactColumnEngineChanges>(engine.StartCompaction(EmptyDataLocksManager));
    UNIT_ASSERT(changes);
    //    UNIT_ASSERT_VALUES_EQUAL(changes->SwitchedPortions.size(), expected.SrcPortions);
    changes->Blobs = std::move(blobs);
    changes->StartEmergency();
    NOlap::TConstructionContext context(engine.GetVersionedIndex(), NColumnShard::TIndexationCounters("Compaction"), NOlap::TSnapshot(step, 1));
    Y_ABORT_UNLESS(changes->ConstructBlobs(context).Ok());

    //    UNIT_ASSERT_VALUES_EQUAL(changes->AppendedPortions.size(), expected.NewPortions);
    AddIdsToBlobs(changes->AppendedPortions, changes->Blobs, step);

    //    UNIT_ASSERT_VALUES_EQUAL(changes->GetTmpGranuleIds().size(), expected.NewGranules);

    const bool result = engine.ApplyChangesOnTxCreate(changes, snap) && engine.ApplyChangesOnExecute(db, changes, snap);
    NOlap::TWriteIndexContext contextExecute(nullptr, db, engine);
    changes->WriteIndexOnExecute(nullptr, contextExecute);
    NOlap::TWriteIndexCompleteContext contextComplete(NActors::TActivationContext::AsActorContext(), 0, 0, TDuration::Zero(), engine);
    changes->WriteIndexOnComplete(nullptr, contextComplete);
    if (blobsPool) {
        for (auto&& i : changes->AppendedPortions) {
            for (auto&& r : i.GetPortionResult().GetRecords()) {
                Y_ABORT_UNLESS(blobsPool->emplace(i.GetPortionResult().RestoreBlobRange(r.BlobRange), i.GetBlobByRangeVerified(r.ColumnId, r.Chunk)).second);
            }
        }
    }
    changes->AbortEmergency("testing");
    return result;
}

bool Cleanup(TColumnEngineForLogs& engine, TTestDbWrapper& db, TSnapshot snap, ui32 expectedToDrop) {
    THashSet<ui64> pathsToDrop;
    std::shared_ptr<TCleanupPortionsColumnEngineChanges> changes = engine.StartCleanupPortions(snap, pathsToDrop, EmptyDataLocksManager);
    UNIT_ASSERT(changes || !expectedToDrop);
    if (!expectedToDrop && !changes) {
        return true;
    }
    UNIT_ASSERT_VALUES_EQUAL(changes->PortionsToDrop.size(), expectedToDrop);


    changes->StartEmergency();
    const bool result = engine.ApplyChangesOnTxCreate(changes, snap) && engine.ApplyChangesOnExecute(db, changes, snap);
    NOlap::TWriteIndexContext contextExecute(nullptr, db, engine);
    changes->WriteIndexOnExecute(nullptr, contextExecute);
    NOlap::TWriteIndexCompleteContext contextComplete(NActors::TActivationContext::AsActorContext(), 0, 0, TDuration::Zero(), engine);
    changes->WriteIndexOnComplete(nullptr, contextComplete);
    changes->AbortEmergency("testing");
    return result;
}

bool Ttl(TColumnEngineForLogs& engine, TTestDbWrapper& db,
         const THashMap<ui64, NOlap::TTiering>& pathEviction, ui32 expectedToDrop) {
    std::vector<std::shared_ptr<TTTLColumnEngineChanges>> vChanges = engine.StartTtl(pathEviction, EmptyDataLocksManager, 512 * 1024 * 1024);
    AFL_VERIFY(vChanges.size() == 1)("count", vChanges.size());
    auto changes = vChanges.front();
    UNIT_ASSERT_VALUES_EQUAL(changes->GetPortionsToRemove().size(), expectedToDrop);


    changes->StartEmergency();
    const bool result = engine.ApplyChangesOnTxCreate(changes, TSnapshot(1, 1)) && engine.ApplyChangesOnExecute(db, changes, TSnapshot(1, 1));
    NOlap::TWriteIndexContext contextExecute(nullptr, db, engine);
    changes->WriteIndexOnExecute(nullptr, contextExecute);
    NOlap::TWriteIndexCompleteContext contextComplete(NActors::TActivationContext::AsActorContext(), 0, 0, TDuration::Zero(), engine);
    changes->WriteIndexOnComplete(nullptr, contextComplete);
    changes->AbortEmergency("testing");
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

std::shared_ptr<NKikimr::NOlap::IStoragesManager> InitializeStorageManager() {
    return NKikimr::NOlap::TTestStoragesManager::GetInstance();
}

std::shared_ptr<NKikimr::NOlap::IStoragesManager> CommonStoragesManager = InitializeStorageManager();

Y_UNIT_TEST_SUITE(TColumnEngineTestLogs) {
    void WriteLoadRead(const std::vector<NArrow::NTest::TTestColumn>& ydbSchema,
                       const std::vector<NArrow::NTest::TTestColumn>& key) {
        TTestDbWrapper db;
        TIndexInfo tableInfo = NColumnShard::BuildTableInfo(ydbSchema, key);

        std::vector<ui64> paths = {1, 2};

        TString testBlob = MakeTestBlob();

        std::vector<TBlobRange> blobRanges;
        blobRanges.push_back(MakeBlobRange(1, testBlob.size()));
        blobRanges.push_back(MakeBlobRange(2, testBlob.size()));

        // PlanStep, TxId, PathId, DedupId, BlobId, Data, [Metadata]
        // load
        TSnapshot indexSnaphot(1, 1);
        TColumnEngineForLogs engine(0, CommonStoragesManager, indexSnaphot, TIndexInfo(tableInfo));
        for (auto&& i : paths) {
            engine.RegisterTable(i);
        }
        engine.Load(db);

        std::vector<TInsertedData> dataToIndex = {
            TInsertedData(2, paths[0], "", blobRanges[0].BlobId, TLocalHelper::GetMetaProto(), 0, {}),
            TInsertedData(1, paths[0], "", blobRanges[1].BlobId, TLocalHelper::GetMetaProto(), 0, {})
        };

        // write

        ui32 step = 1000;
        {
            NBlobOperations::NRead::TCompositeReadBlobs blobs;
            TString str1 = testBlob;
            blobs.Add(IStoragesManager::DefaultStorageId, blobRanges[0], std::move(str1));
            str1 = testBlob;
            blobs.Add(IStoragesManager::DefaultStorageId, blobRanges[1], std::move(str1));
            Insert(engine, db, TSnapshot(1, 2), std::move(dataToIndex), blobs, step);
        }

        // selects

        auto lastSchema = engine.GetVersionedIndex().GetLastSchema();
        UNIT_ASSERT_EQUAL(lastSchema->GetSnapshot(), indexSnaphot);
        const TIndexInfo& indexInfo = lastSchema->GetIndexInfo();
        THashSet<ui32> oneColumnId = { indexInfo.GetColumnId(testColumns[0].GetName()) };
        THashSet<ui32> columnIds;
        for (auto& c : testColumns) {
            columnIds.insert(indexInfo.GetColumnId(c.GetName()));
        }

        { // select from snap before insert
            ui64 planStep = 1;
            ui64 txId = 0;
            auto selectInfo = engine.Select(paths[0], TSnapshot(planStep, txId), NOlap::TPKRangesFilter(false));
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->PortionsOrderedPK.size(), 0);
        }

        { // select from snap between insert (greater txId)
            ui64 planStep = 1;
            ui64 txId = 2;
            auto selectInfo = engine.Select(paths[0], TSnapshot(planStep, txId), NOlap::TPKRangesFilter(false));
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->PortionsOrderedPK.size(), 0);
        }

        { // select from snap after insert (greater planStep)
            ui64 planStep = 2;
            ui64 txId = 1;
            auto selectInfo = engine.Select(paths[0], TSnapshot(planStep, txId), NOlap::TPKRangesFilter(false));
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->PortionsOrderedPK.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->PortionsOrderedPK[0]->NumChunks(), columnIds.size() + TIndexInfo::GetSpecialColumnNames().size());
        }

        { // select another pathId
            ui64 planStep = 2;
            ui64 txId = 1;
            auto selectInfo = engine.Select(paths[1], TSnapshot(planStep, txId), NOlap::TPKRangesFilter(false));
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->PortionsOrderedPK.size(), 0);
        }
    }

    Y_UNIT_TEST(IndexWriteLoadRead) {
        WriteLoadRead(testColumns, testKey);
    }

    Y_UNIT_TEST(IndexWriteLoadReadStrPK) {
        std::vector<NArrow::NTest::TTestColumn> key = {
            NArrow::NTest::TTestColumn("resource_type", TTypeInfo(NTypeIds::Utf8) ),
            NArrow::NTest::TTestColumn("resource_id", TTypeInfo(NTypeIds::Utf8) ),
            NArrow::NTest::TTestColumn("uid", TTypeInfo(NTypeIds::Utf8) ),
            NArrow::NTest::TTestColumn("timestamp", TTypeInfo(NTypeIds::Timestamp) )
        };

        WriteLoadRead(testColumns, key);
    }

    void ReadWithPredicates(const std::vector<NArrow::NTest::TTestColumn>& ydbSchema,
                            const std::vector<NArrow::NTest::TTestColumn>& key) {
        TTestDbWrapper db;
        TIndexInfo tableInfo = NColumnShard::BuildTableInfo(ydbSchema, key);

        ui64 pathId = 1;
        ui32 step = 1000;

        TSnapshot indexSnapshot(1, 1);
        TColumnEngineForLogs engine(0, CommonStoragesManager, indexSnapshot, TIndexInfo(tableInfo));
        engine.RegisterTable(pathId);
        engine.Load(db);

        // insert
        ui64 planStep = 1;

        ui64 numRows = 1000;
        ui64 rowPos = 0;
        for (ui64 txId = 1; txId <= 20; ++txId, rowPos += numRows) {
            TString testBlob = MakeTestBlob(rowPos, rowPos + numRows);
            auto blobRange = MakeBlobRange(++step, testBlob.size());
            NBlobOperations::NRead::TCompositeReadBlobs blobs;
            TString str1 = testBlob;
            blobs.Add(IStoragesManager::DefaultStorageId, blobRange, std::move(str1));

            // PlanStep, TxId, PathId, DedupId, BlobId, Data, [Metadata]
            std::vector<TInsertedData> dataToIndex;
            dataToIndex.push_back(
                TInsertedData(txId, pathId, "", blobRange.BlobId, TLocalHelper::GetMetaProto(), 0, {}));

            bool ok = Insert(engine, db, TSnapshot(planStep, txId), std::move(dataToIndex), blobs, step);
            UNIT_ASSERT(ok);
        }

        // compact
        planStep = 2;

//        bool ok = Compact(engine, db, TSnapshot(planStep, 1), std::move(blobs), step, {20, 4, 4});
//        UNIT_ASSERT(ok);

        // read

        // TODO: read old snapshot

        planStep = 3;

        const TIndexInfo& indexInfo = engine.GetVersionedIndex().GetLastSchema()->GetIndexInfo();
        THashSet<ui32> oneColumnId = { indexInfo.GetColumnId(key[0].GetName()) };

        { // full scan
            ui64 txId = 1;
            auto selectInfo = engine.Select(pathId, TSnapshot(planStep, txId), NOlap::TPKRangesFilter(false));
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->PortionsOrderedPK.size(), 20);
        }

        // predicates

        {
            ui64 txId = 1;
            std::shared_ptr<TPredicate> gt10k = MakePredicate(10000, NArrow::EOperation::Greater);
            if (key[0].GetType() == TTypeInfo(NTypeIds::Utf8)) {
                gt10k = MakeStrPredicate("10000", NArrow::EOperation::Greater);
            }
            NOlap::TPKRangesFilter pkFilter(false);
            Y_ABORT_UNLESS(pkFilter.Add(gt10k, nullptr, nullptr));
            auto selectInfo = engine.Select(pathId, TSnapshot(planStep, txId), pkFilter);
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->PortionsOrderedPK.size(), 10);
        }

        {
            ui64 txId = 1;
            std::shared_ptr<TPredicate> lt10k = MakePredicate(8999, NArrow::EOperation::Less); // TODO: better border checks
            if (key[0].GetType() == TTypeInfo(NTypeIds::Utf8)) {
                lt10k = MakeStrPredicate("08999", NArrow::EOperation::Less);
            }
            NOlap::TPKRangesFilter pkFilter(false);
            Y_ABORT_UNLESS(pkFilter.Add(nullptr, lt10k, nullptr));
            auto selectInfo = engine.Select(pathId, TSnapshot(planStep, txId), pkFilter);
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->PortionsOrderedPK.size(), 9);
        }
    }

    Y_UNIT_TEST(IndexReadWithPredicates) {
        ReadWithPredicates(testColumns, testKey);
    }

    Y_UNIT_TEST(IndexReadWithPredicatesStrPK) {
        std::vector<NArrow::NTest::TTestColumn> key = {
            NArrow::NTest::TTestColumn("resource_type", TTypeInfo(NTypeIds::Utf8) ),
            NArrow::NTest::TTestColumn("resource_id", TTypeInfo(NTypeIds::Utf8) ),
            NArrow::NTest::TTestColumn("uid", TTypeInfo(NTypeIds::Utf8) ),
            NArrow::NTest::TTestColumn("timestamp", TTypeInfo(NTypeIds::Timestamp) )
        };

        ReadWithPredicates(testColumns, key);
    }

    Y_UNIT_TEST(IndexWriteOverload) {
        TTestDbWrapper db;
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TIndexInfo tableInfo = NColumnShard::BuildTableInfo(testColumns, testKey);;

        ui64 pathId = 1;
        ui32 step = 1000;

        // inserts
        ui64 planStep = 1;

        TSnapshot indexSnapshot(1, 1);
        TColumnEngineForLogs engine(0, CommonStoragesManager, indexSnapshot, TIndexInfo(tableInfo));
        engine.RegisterTable(pathId);
        engine.Load(db);

        ui64 numRows = 1000;
        ui64 rowPos = 0;
        NBlobOperations::NRead::TCompositeReadBlobs blobsAll;
        for (ui64 txId = 1; txId <= 100; ++txId, rowPos += numRows) {
            TString testBlob = MakeTestBlob(rowPos, rowPos + numRows);
            auto blobRange = MakeBlobRange(++step, testBlob.size());
            NBlobOperations::NRead::TCompositeReadBlobs blobs;
            blobs.Add(IStoragesManager::DefaultStorageId, blobRange, std::move(testBlob));

            // PlanStep, TxId, PathId, DedupId, BlobId, Data, [Metadata]
            std::vector<TInsertedData> dataToIndex;
            dataToIndex.push_back(
                TInsertedData(txId, pathId, "", blobRange.BlobId, TLocalHelper::GetMetaProto(), 0, {}));

            bool ok = Insert(engine, db, TSnapshot(planStep, txId), std::move(dataToIndex), blobs, step);
            blobsAll.Merge(std::move(blobs));
            UNIT_ASSERT(ok);
        }

        { // check it's overloaded after reload
            TColumnEngineForLogs tmpEngine(0, CommonStoragesManager, TSnapshot::Zero(), TIndexInfo(tableInfo));
            tmpEngine.RegisterTable(pathId);
            tmpEngine.Load(db);
        }

        // compact
        planStep = 2;

        bool ok = Compact(engine, db, TSnapshot(planStep, 1), std::move(blobsAll), step, {23, 5, 5});
        UNIT_ASSERT(ok);

        // success write after compaction
        planStep = 3;

        for (ui64 txId = 1; txId <= 2; ++txId, rowPos += numRows) {
            TString testBlob = MakeTestBlob(rowPos, rowPos + numRows);
            auto blobRange = MakeBlobRange(++step, testBlob.size());
            NBlobOperations::NRead::TCompositeReadBlobs blobs;
            blobs.Add(IStoragesManager::DefaultStorageId, blobRange, std::move(testBlob));

            // PlanStep, TxId, PathId, DedupId, BlobId, Data, [Metadata]
            std::vector<TInsertedData> dataToIndex;
            dataToIndex.push_back(
                TInsertedData(txId, pathId, "", blobRange.BlobId, TLocalHelper::GetMetaProto(), 0, {}));

            bool ok = Insert(engine, db, TSnapshot(planStep, txId), std::move(dataToIndex), blobs, step);
            UNIT_ASSERT(ok);
        }

        { // check it's not overloaded after reload
            TColumnEngineForLogs tmpEngine(0, CommonStoragesManager, TSnapshot::Zero(), TIndexInfo(tableInfo));
            tmpEngine.RegisterTable(pathId);
            tmpEngine.Load(db);
        }
    }

    Y_UNIT_TEST(IndexTtl) {
        TTestDbWrapper db;
        TIndexInfo tableInfo = NColumnShard::BuildTableInfo(testColumns, testKey);

        ui64 pathId = 1;
        ui32 step = 1000;

        // insert
        ui64 planStep = 1;
        TSnapshot indexSnapshot(1, 1);
        {
            TColumnEngineForLogs engine(0, CommonStoragesManager, indexSnapshot, TIndexInfo(tableInfo));
            engine.RegisterTable(pathId);
            engine.Load(db);

            ui64 numRows = 1000;
            ui64 rowPos = 0;
            for (ui64 txId = 1; txId <= 20; ++txId, rowPos += numRows) {
                TString testBlob = MakeTestBlob(rowPos, rowPos + numRows);
                auto blobRange = MakeBlobRange(++step, testBlob.size());
                NBlobOperations::NRead::TCompositeReadBlobs blobs;
                TString str1 = testBlob;
                blobs.Add(IStoragesManager::DefaultStorageId, blobRange, std::move(str1));

                // PlanStep, TxId, PathId, DedupId, BlobId, Data, [Metadata]
                std::vector<TInsertedData> dataToIndex;
                dataToIndex.push_back(
                    TInsertedData(txId, pathId, "", blobRange.BlobId, TLocalHelper::GetMetaProto(), 0, {}));

                bool ok = Insert(engine, db, TSnapshot(planStep, txId), std::move(dataToIndex), blobs, step);
                UNIT_ASSERT(ok);
            }

            // compact
            planStep = 2;

            //        bool ok = Compact(engine, db, TSnapshot(planStep, 1), std::move(blobs), step, {20, 4, 4});
            //        UNIT_ASSERT(ok);

                    // read
            planStep = 3;

            const TIndexInfo& indexInfo = engine.GetVersionedIndex().GetLastSchema()->GetIndexInfo();
            THashSet<ui32> oneColumnId = {indexInfo.GetColumnId(testColumns[0].GetName())};

            { // full scan
                ui64 txId = 1;
                auto selectInfo = engine.Select(pathId, TSnapshot(planStep, txId), NOlap::TPKRangesFilter(false));
                UNIT_ASSERT_VALUES_EQUAL(selectInfo->PortionsOrderedPK.size(), 20);
            }

            // Cleanup
            Cleanup(engine, db, TSnapshot(planStep, 1), 0);

            { // full scan
                ui64 txId = 1;
                auto selectInfo = engine.Select(pathId, TSnapshot(planStep, txId), NOlap::TPKRangesFilter(false));
                UNIT_ASSERT_VALUES_EQUAL(selectInfo->PortionsOrderedPK.size(), 20);
            }

            // TTL
            std::shared_ptr<arrow::DataType> ttlColType = arrow::timestamp(arrow::TimeUnit::MICRO);
            THashMap<ui64, NOlap::TTiering> pathTtls;
            NOlap::TTiering tiering;
            AFL_VERIFY(tiering.Add(NOlap::TTierInfo::MakeTtl(TDuration::MicroSeconds(TInstant::Now().MicroSeconds() - 10000), "timestamp")));
            pathTtls.emplace(pathId, std::move(tiering));
            Ttl(engine, db, pathTtls, 10);

            // read + load + read

            { // full scan
                ui64 txId = 1;
                auto selectInfo = engine.Select(pathId, TSnapshot(planStep, txId), NOlap::TPKRangesFilter(false));
                UNIT_ASSERT_VALUES_EQUAL(selectInfo->PortionsOrderedPK.size(), 10);
            }
        }
        {
            // load
            TColumnEngineForLogs engine(0, CommonStoragesManager, indexSnapshot, TIndexInfo(tableInfo));
            engine.RegisterTable(pathId);
            engine.Load(db);

            const TIndexInfo& indexInfo = engine.GetVersionedIndex().GetLastSchema()->GetIndexInfo();
            THashSet<ui32> oneColumnId = {indexInfo.GetColumnId(testColumns[0].GetName())};

            { // full scan
                ui64 txId = 1;
                auto selectInfo = engine.Select(pathId, TSnapshot(planStep, txId), NOlap::TPKRangesFilter(false));
                UNIT_ASSERT_VALUES_EQUAL(selectInfo->PortionsOrderedPK.size(), 10);
            }
        }
    }
}

}
