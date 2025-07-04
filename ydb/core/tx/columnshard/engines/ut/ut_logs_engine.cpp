#include "helper.h"

#include <ydb/core/tx/columnshard/background_controller.h>
#include <ydb/core/tx/columnshard/blobs_action/bs/storage.h>
#include <ydb/core/tx/columnshard/blobs_action/counters/storage.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/data_locks/manager/manager.h>
#include <ydb/core/tx/columnshard/data_sharing/manager/shared_blobs.h>
#include <ydb/core/tx/columnshard/engines/changes/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/changes/cleanup_portions.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction.h>
#include <ydb/core/tx/columnshard/engines/changes/ttl.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/portions/write_with_blobs.h>
#include <ydb/core/tx/columnshard/engines/predicate/predicate.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/helper.h>

#include <ydb/library/arrow_kernels/operations.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

using namespace NOlap;
namespace NTypeIds = NScheme::NTypeIds;
using TTypeId = NScheme::TTypeId;
using TTypeInfo = NScheme::TTypeInfo;
using TDefaultTestsController = NKikimr::NYDBTest::NColumnShard::TController;
using namespace NKikimr::NOlap::NEngines::NTest;

namespace {

std::shared_ptr<NDataLocks::TManager> EmptyDataLocksManager = std::make_shared<NDataLocks::TManager>();

class TTestDbWrapper: public IDbWrapper {
private:
    std::map<TPortionAddress, TColumnChunkLoadContextV2> LoadContexts;

public:
    virtual void WriteColumns(const NOlap::TPortionInfo& portion, const NKikimrTxColumnShard::TIndexPortionAccessor& proto) override {
        auto it = LoadContexts.find(portion.GetAddress());
        if (it == LoadContexts.end()) {
            LoadContexts.emplace(portion.GetAddress(), TColumnChunkLoadContextV2(portion.GetPathId(), portion.GetPortionId(), proto));
        } else {
            it->second = TColumnChunkLoadContextV2(portion.GetPathId(), portion.GetPortionId(), proto);
        }
    }

    virtual const IBlobGroupSelector* GetDsGroupSelector() const override {
        return &Default<TFakeGroupSelector>();
    }

    struct TIndex {
        THashMap<TInternalPathId, THashMap<ui64, TPortionAccessorConstructor>> Columns;   // pathId -> portions
        THashMap<ui32, ui64> Counters;
    };

    virtual TConclusion<THashMap<TInternalPathId, std::map<TSnapshot, TGranuleShardingInfo>>> LoadGranulesShardingInfo() override {
        THashMap<TInternalPathId, std::map<TSnapshot, TGranuleShardingInfo>> result;
        return result;
    }

    virtual void WritePortion(const NOlap::TPortionInfo& portion) override {
        auto it = Portions.find(portion.GetPortionId());
        if (it == Portions.end()) {
            Portions.emplace(portion.GetPortionId(), portion.MakeCopy());
        } else {
            it->second = portion.MakeCopy();
        }
    }
    virtual void ErasePortion(const NOlap::TPortionInfo& portion) override {
        AFL_VERIFY(Portions.erase(portion.GetPortionId()));
    }
    virtual bool LoadPortions(const std::optional<TInternalPathId> pathId,
        const std::function<void(std::unique_ptr<NOlap::TPortionInfoConstructor>&&, const NKikimrTxColumnShard::TIndexPortionMeta&)>& callback) override {
        for (auto&& i : Portions) {
            if (!pathId || *pathId == i.second->GetPathId()) {
                callback(i.second->BuildConstructor(false, false),
                    i.second->GetMeta().SerializeToProto(i.second->GetPortionType() == NOlap::EPortionType::Compacted
                                                             ? NPortion::EProduced::SPLIT_COMPACTED
                                                             : NPortion::EProduced::INSERTED));
            }
        }
        return true;
    }

    void WriteColumn(const TPortionInfo& portion, const TColumnRecord& row, const ui32 firstPKColumnId) override {
        auto rowProto = row.GetMeta().SerializeToProto();
        if (firstPKColumnId == row.GetColumnId() && row.GetChunkIdx() == 0) {
            *rowProto.MutablePortionMeta() = portion.GetMeta().SerializeToProto(portion.GetPortionType() == NOlap::EPortionType::Compacted
                                                                                    ? NPortion::EProduced::SPLIT_COMPACTED
                                                                                    : NPortion::EProduced::INSERTED);
        }

        auto& data = Indices[0].Columns[portion.GetPathId()];
        auto it = data.find(portion.GetPortionId());
        if (it == data.end()) {
            it = data.emplace(portion.GetPortionId(), portion.BuildConstructor(true, true)).first;
        } else {
            Y_ABORT_UNLESS(portion.GetPathId() == it->second.MutablePortionConstructor().GetPathId() &&
                           portion.GetPortionId() == it->second.MutablePortionConstructor().GetPortionIdVerified());
        }
        it->second.MutablePortionConstructor().SetSchemaVersion(portion.GetSchemaVersionVerified());
        if (portion.HasRemoveSnapshot()) {
            if (!it->second.MutablePortionConstructor().HasRemoveSnapshot()) {
                it->second.MutablePortionConstructor().SetRemoveSnapshot(portion.GetRemoveSnapshotVerified());
            }
        } else {
            AFL_VERIFY(!it->second.MutablePortionConstructor().HasRemoveSnapshot());
        }

        bool replaced = false;
        for (auto& rec : it->second.TestMutableRecords()) {
            if (rec.IsEqualTest(row)) {
                rec = row;
                replaced = true;
                break;
            }
        }
        if (!replaced) {
            it->second.TestMutableRecords().emplace_back(row);
        }
    }

    void EraseColumn(const TPortionInfo& portion, const TColumnRecord& row) override {
        auto& data = Indices[0].Columns[portion.GetPathId()];
        auto it = data.find(portion.GetPortionId());
        Y_ABORT_UNLESS(it != data.end());
        auto& portionLocal = it->second;

        std::vector<TColumnRecord> filtered;
        for (auto& rec : portionLocal.GetRecords()) {
            if (!rec.IsEqualTest(row)) {
                filtered.push_back(rec);
            }
        }
        portionLocal.TestMutableRecords().swap(filtered);
    }

    bool LoadColumns(const std::optional<TInternalPathId> reqPathId, const std::function<void(TColumnChunkLoadContextV2&&)>& callback) override {
        auto& columns = Indices[0].Columns;
        for (auto& [pathId, portions] : columns) {
            if (pathId && *reqPathId != pathId) {
                continue;
            }
            for (auto& [portionId, portionLocal] : portions) {
//                auto copy = portionLocal.MakeCopy();
//                copy->TestMutableRecords().clear();
                auto it = LoadContexts.find(portionLocal.GetPortionConstructor().GetAddress());
                AFL_VERIFY(it != LoadContexts.end());
                callback(std::move(it->second));
                LoadContexts.erase(it);
            }
        }
        return true;
    }

    virtual void WriteIndex(const TPortionInfo& /*portion*/, const TIndexChunk& /*row*/) override {
    }
    virtual void EraseIndex(const TPortionInfo& /*portion*/, const TIndexChunk& /*row*/) override {
    }
    virtual bool LoadIndexes(const std::optional<TInternalPathId> /*reqPathId*/,
        const std::function<void(const TInternalPathId /*pathId*/, const ui64 /*portionId*/, TIndexChunkLoadContext&&)>& /*callback*/) override {
        return true;
    }

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
    THashMap<ui64, std::shared_ptr<NOlap::TPortionInfo>> Portions;
    THashMap<ui32, TIndex> Indices;
};

static const std::vector<NArrow::NTest::TTestColumn> testColumns = {
    // PK
    NArrow::NTest::TTestColumn("timestamp", TTypeInfo(NTypeIds::Timestamp)),
    NArrow::NTest::TTestColumn("resource_type", TTypeInfo(NTypeIds::Utf8)), NArrow::NTest::TTestColumn("resource_id", TTypeInfo(NTypeIds::Utf8)),
    NArrow::NTest::TTestColumn("uid", TTypeInfo(NTypeIds::Utf8)),
    //
    NArrow::NTest::TTestColumn("message", TTypeInfo(NTypeIds::Utf8))
};

static const std::vector<NArrow::NTest::TTestColumn> testKey = { NArrow::NTest::TTestColumn("timestamp", TTypeInfo(NTypeIds::Timestamp)),
    NArrow::NTest::TTestColumn("resource_type", TTypeInfo(NTypeIds::Utf8)), NArrow::NTest::TTestColumn("resource_id", TTypeInfo(NTypeIds::Utf8)),
    NArrow::NTest::TTestColumn("uid", TTypeInfo(NTypeIds::Utf8)) };

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
        : Schema(NArrow::MakeArrowSchema(testColumns)) {
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

TString MakeTestBlob(i64 start = 0, i64 end = 100, ui32 step = 1) {
    TBuilder<arrow::TimestampType> builder;
    for (i64 ts = start; ts < end; ts += step) {
        TString str = ToString(ts);
        TString sortedStr = Sprintf("%05ld", (long)ts);
        builder.AddRow({ ts, sortedStr, str, str, str });
    }
    auto batch = builder.Finish();
    return NArrow::SerializeBatchNoCompression(batch);
}

void AddIdsToBlobs(std::vector<TWritePortionInfoWithBlobsResult>& portions, NBlobOperations::NRead::TCompositeReadBlobs& blobs, ui32& step) {
    for (auto& portion : portions) {
        THashMap<TUnifiedBlobId, TString> blobsData;
        for (auto& b : portion.MutableBlobs()) {
            const auto blobId = MakeUnifiedBlobId(++step, b.GetSize());
            b.RegisterBlobId(portion, blobId);
            blobsData.emplace(blobId, b.GetResultBlob());
        }
        for (auto&& rec : portion.GetPortionConstructor().GetRecords()) {
            auto range = portion.GetPortionConstructor().RestoreBlobRangeSlow(rec.BlobRange, rec.GetAddress());
            auto it = blobsData.find(range.BlobId);
            AFL_VERIFY(it != blobsData.end());
            const TString& data = it->second;
            AFL_VERIFY(range.Offset + range.Size <= data.size());
            blobs.Add(IStoragesManager::DefaultStorageId, range, data.substr(range.Offset, range.Size));
        }
    }
}

bool Insert(TColumnEngineForLogs& engine, TTestDbWrapper& db, TSnapshot snap, std::vector<TCommittedData>&& dataToIndex,
    NBlobOperations::NRead::TCompositeReadBlobs& blobs, ui32& step) {
    std::shared_ptr<TInsertColumnEngineChanges> changes = engine.StartInsert(std::move(dataToIndex));
    if (!changes) {
        return false;
    }

    changes->Blobs = std::move(blobs);
    blobs.Clear();
    changes->StartEmergency();

    NOlap::TConstructionContext context(engine.GetVersionedIndex(), NColumnShard::TIndexationCounters("Indexation"), snap);
    Y_ABORT_UNLESS(changes->ConstructBlobs(context).Ok());

    UNIT_ASSERT_VALUES_EQUAL(changes->GetAppendedPortions().size(), 1);
    ui32 blobsCount = 0;
    for (auto&& i : changes->GetAppendedPortions()) {
        blobsCount += i.GetBlobs().size();
    }
    AFL_VERIFY(blobsCount == 5 || blobsCount == 1)("count", blobsCount);

    AddIdsToBlobs(changes->MutableAppendedPortions(), blobs, step);

    const bool result = engine.ApplyChangesOnTxCreate(changes, snap) && engine.ApplyChangesOnExecute(db, changes, snap);

    NOlap::TWriteIndexContext contextExecute(nullptr, db, engine, snap);
    changes->WriteIndexOnExecute(nullptr, contextExecute);
    NOlap::TWriteIndexCompleteContext contextComplete(NActors::TActivationContext::AsActorContext(), 0, 0, TDuration::Zero(), engine, snap);
    changes->WriteIndexOnComplete(nullptr, contextComplete);
    changes->AbortEmergency("testing");
    return result;
}

struct TExpected {
    ui32 SrcPortions;
    ui32 NewPortions;
    ui32 NewGranules;
};

class TTestCompactionAccessorsSubscriber: public NOlap::IDataAccessorRequestsSubscriber {
private:
    std::shared_ptr<TColumnEngineChanges> Changes;
    const std::shared_ptr<NOlap::TVersionedIndex> VersionedIndex;
    virtual const std::shared_ptr<const TAtomicCounter>& DoGetAbortionFlag() const override {
        return Default<std::shared_ptr<const TAtomicCounter>>();
    }

    virtual void DoOnRequestsFinished(TDataAccessorsResult&& result) override {
        const TDataAccessorsInitializationContext context(VersionedIndex);
        Changes->SetFetchedDataAccessors(std::move(result), context);
    }

public:
    TTestCompactionAccessorsSubscriber(
        const std::shared_ptr<TColumnEngineChanges>& changes, const std::shared_ptr<NOlap::TVersionedIndex>& versionedIndex)
        : Changes(changes)
        , VersionedIndex(versionedIndex) {
    }
};

bool Compact(TColumnEngineForLogs& engine, TTestDbWrapper& db, TSnapshot snap, NBlobOperations::NRead::TCompositeReadBlobs&& blobs, ui32& step,
    const TExpected& /*expected*/, THashMap<TBlobRange, TString>* blobsPool = nullptr) {
    std::shared_ptr<TCompactColumnEngineChanges> changes =
        dynamic_pointer_cast<TCompactColumnEngineChanges>(engine.StartCompaction(EmptyDataLocksManager));
    UNIT_ASSERT(changes);
    //    UNIT_ASSERT_VALUES_EQUAL(changes->SwitchedPortions.size(), expected.SrcPortions);
    changes->StartEmergency();
    {
        auto request = std::make_shared<TDataAccessorsRequest>(NGeneralCache::TPortionsMetadataCachePolicy::EConsumer::GENERAL_COMPACTION);
        for (const auto& portion : changes->GetPortionsToAccess()) {
            request->AddPortion(portion);
        }
        request->RegisterSubscriber(
            std::make_shared<TTestCompactionAccessorsSubscriber>(changes, std::make_shared<NOlap::TVersionedIndex>(engine.GetVersionedIndex())));
        engine.FetchDataAccessors(request);
    }
    changes->Blobs = std::move(blobs);
    NOlap::TConstructionContext context(engine.GetVersionedIndex(), NColumnShard::TIndexationCounters("Compaction"), NOlap::TSnapshot(step, 1));
    Y_ABORT_UNLESS(changes->ConstructBlobs(context).Ok());

    //    UNIT_ASSERT_VALUES_EQUAL(changes->AppendedPortions.size(), expected.NewPortions);
    AddIdsToBlobs(changes->MutableAppendedPortions(), changes->Blobs, step);

    //    UNIT_ASSERT_VALUES_EQUAL(changes->GetTmpGranuleIds().size(), expected.NewGranules);

    const bool result = engine.ApplyChangesOnTxCreate(changes, snap) && engine.ApplyChangesOnExecute(db, changes, snap);
    NOlap::TWriteIndexContext contextExecute(nullptr, db, engine, snap);
    changes->WriteIndexOnExecute(nullptr, contextExecute);
    NOlap::TWriteIndexCompleteContext contextComplete(NActors::TActivationContext::AsActorContext(), 0, 0, TDuration::Zero(), engine, snap);
    changes->WriteIndexOnComplete(nullptr, contextComplete);
    if (blobsPool) {
        for (auto&& i : changes->GetAppendedPortions()) {
            for (auto&& r : i.GetPortionResult().TestGetRecords()) {
                Y_ABORT_UNLESS(blobsPool
                                   ->emplace(i.GetPortionResult().GetPortionInfo().RestoreBlobRange(r.BlobRange),
                                       i.GetBlobByRangeVerified(r.ColumnId, r.Chunk))
                                   .second);
            }
        }
    }
    changes->AbortEmergency("testing");
    return result;
}

bool Cleanup(TColumnEngineForLogs& engine, TTestDbWrapper& db, TSnapshot snap, ui32 expectedToDrop) {
    THashSet<TInternalPathId> pathsToDrop;
    std::shared_ptr<TCleanupPortionsColumnEngineChanges> changes = engine.StartCleanupPortions(snap, pathsToDrop, EmptyDataLocksManager);
    UNIT_ASSERT(changes || !expectedToDrop);
    if (!expectedToDrop && !changes) {
        return true;
    }
    UNIT_ASSERT_VALUES_EQUAL(changes->GetPortionsToDrop().size(), expectedToDrop);

    changes->StartEmergency();
    const bool result = engine.ApplyChangesOnTxCreate(changes, snap) && engine.ApplyChangesOnExecute(db, changes, snap);
    NOlap::TWriteIndexContext contextExecute(nullptr, db, engine, snap);
    changes->WriteIndexOnExecute(nullptr, contextExecute);
    NOlap::TWriteIndexCompleteContext contextComplete(NActors::TActivationContext::AsActorContext(), 0, 0, TDuration::Zero(), engine, snap);
    changes->WriteIndexOnComplete(nullptr, contextComplete);
    changes->AbortEmergency("testing");
    return result;
}

namespace {
class TTestMetadataAccessorsSubscriber: public NOlap::IDataAccessorRequestsSubscriber {
private:
    std::shared_ptr<IMetadataAccessorResultProcessor> Processor;
    TColumnEngineForLogs& Engine;

    virtual const std::shared_ptr<const TAtomicCounter>& DoGetAbortionFlag() const override {
        return Default<std::shared_ptr<const TAtomicCounter>>();
    }
    virtual void DoOnRequestsFinished(TDataAccessorsResult&& result) override {
        Processor->ApplyResult(
            NOlap::NResourceBroker::NSubscribe::TResourceContainer<NOlap::TDataAccessorsResult>::BuildForTest(std::move(result)), Engine);
    }

public:
    TTestMetadataAccessorsSubscriber(const std::shared_ptr<IMetadataAccessorResultProcessor>& processor, TColumnEngineForLogs& engine)
        : Processor(processor)
        , Engine(engine) {
    }
};

}   // namespace

bool Ttl(TColumnEngineForLogs& engine, TTestDbWrapper& db, const THashMap<TInternalPathId, NOlap::TTiering>& pathEviction, ui32 expectedToDrop) {
    engine.StartActualization(pathEviction);
    std::vector<NOlap::TCSMetadataRequest> requests = engine.CollectMetadataRequests();
    for (auto&& i : requests) {
        i.GetRequest()->RegisterSubscriber(std::make_shared<TTestMetadataAccessorsSubscriber>(i.GetProcessor(), engine));
        engine.FetchDataAccessors(i.GetRequest());
    }

    std::vector<std::shared_ptr<TTTLColumnEngineChanges>> vChanges = engine.StartTtl(pathEviction, EmptyDataLocksManager, 512 * 1024 * 1024);
    AFL_VERIFY(vChanges.size() == 1)("count", vChanges.size());
    auto changes = vChanges.front();
    UNIT_ASSERT_VALUES_EQUAL(changes->GetPortionsToRemove().GetSize(), expectedToDrop);

    changes->StartEmergency();
    {
        auto request = std::make_shared<TDataAccessorsRequest>(NGeneralCache::TPortionsMetadataCachePolicy::EConsumer::GENERAL_COMPACTION);
        for (const auto& portion : changes->GetPortionsToAccess()) {
            request->AddPortion(portion);
        }
        request->RegisterSubscriber(
            std::make_shared<TTestCompactionAccessorsSubscriber>(changes, std::make_shared<NOlap::TVersionedIndex>(engine.GetVersionedIndex())));
        engine.FetchDataAccessors(request);
    }
    const bool result = engine.ApplyChangesOnTxCreate(changes, TSnapshot(1, 1)) && engine.ApplyChangesOnExecute(db, changes, TSnapshot(1, 1));
    NOlap::TWriteIndexContext contextExecute(nullptr, db, engine, TSnapshot(1, 1));
    changes->WriteIndexOnExecute(nullptr, contextExecute);
    NOlap::TWriteIndexCompleteContext contextComplete(
        NActors::TActivationContext::AsActorContext(), 0, 0, TDuration::Zero(), engine, TSnapshot(1, 1));
    changes->WriteIndexOnComplete(nullptr, contextComplete);
    changes->AbortEmergency("testing");
    return result;
}

std::shared_ptr<TPredicate> MakePredicate(int64_t ts, NKikimr::NKernels::EOperation op) {
    auto type = arrow::timestamp(arrow::TimeUnit::MICRO);
    auto res = arrow::MakeArrayFromScalar(arrow::TimestampScalar(ts, type), 1);

    std::vector<std::shared_ptr<arrow::Field>> fields = { std::make_shared<arrow::Field>("timestamp", type) };
    return std::make_shared<TPredicate>(op, arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(std::move(fields)), 1, { *res }));
}

std::shared_ptr<TPredicate> MakeStrPredicate(const std::string& key, NKikimr::NKernels::EOperation op) {
    auto type = arrow::utf8();
    auto res = arrow::MakeArrayFromScalar(arrow::StringScalar(key), 1);

    std::vector<std::shared_ptr<arrow::Field>> fields = { std::make_shared<arrow::Field>("resource_type", type) };
    return std::make_shared<TPredicate>(op, arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(std::move(fields)), 1, { *res }));
}

}   // namespace

std::shared_ptr<NKikimr::NOlap::IStoragesManager> InitializeStorageManager() {
    return NKikimr::NOlap::TTestStoragesManager::GetInstance();
}

std::shared_ptr<NKikimr::NOlap::IStoragesManager> CommonStoragesManager = InitializeStorageManager();

Y_UNIT_TEST_SUITE(TColumnEngineTestLogs) {
    void WriteLoadRead(const std::vector<NArrow::NTest::TTestColumn>& ydbSchema, const std::vector<NArrow::NTest::TTestColumn>& key) {
        TTestBasicRuntime runtime;
        TTestDbWrapper db;
        TIndexInfo tableInfo = NColumnShard::BuildTableInfo(ydbSchema, key);

        std::vector<TInternalPathId> paths = { 
            TInternalPathId::FromRawValue(1),
            TInternalPathId::FromRawValue(2)
        };

        TString testBlob = MakeTestBlob();

        std::vector<TBlobRange> blobRanges;
        blobRanges.push_back(MakeBlobRange(1, testBlob.size()));
        blobRanges.push_back(MakeBlobRange(2, testBlob.size()));

        // PlanStep, TxId, PathId, DedupId, BlobId, Data, [Metadata]
        // load
        TSnapshot indexSnapshot(1, 1);
        TColumnEngineForLogs engine(
            0, std::make_shared<TSchemaObjectsCache>(), NDataAccessorControl::TLocalManager::BuildForTests(), CommonStoragesManager, indexSnapshot, 0, TIndexInfo(tableInfo), std::make_shared<NColumnShard::TPortionIndexStats>());
        for (auto&& i : paths) {
            engine.RegisterTable(i);
        }
        engine.TestingLoad(db);

        std::vector<TCommittedData> dataToIndex = { TCommittedData(TUserData::Build(paths[0], blobRanges[0], TLocalHelper::GetMetaProto(), 1, {}), TSnapshot(1, 2), 0, (TInsertWriteId)2),
            TCommittedData(TUserData::Build(paths[0], blobRanges[1], TLocalHelper::GetMetaProto(), 1, {}), TSnapshot(2, 1), 0, (TInsertWriteId)1) };

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
        UNIT_ASSERT_EQUAL(lastSchema->GetSnapshot(), indexSnapshot);
        const TIndexInfo& indexInfo = lastSchema->GetIndexInfo();
        THashSet<ui32> oneColumnId = { indexInfo.GetColumnIdVerified(testColumns[0].GetName()) };
        THashSet<ui32> columnIds;
        for (auto& c : testColumns) {
            columnIds.insert(indexInfo.GetColumnIdVerified(c.GetName()));
        }

        {   // select from snap before insert
            ui64 planStep = 1;
            ui64 txId = 0;
            auto selectInfo = engine.Select(paths[0], TSnapshot(planStep, txId), NOlap::TPKRangesFilter(), false);
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->Portions.size(), 0);
        }

        {   // select from snap between insert (greater txId)
            ui64 planStep = 1;
            ui64 txId = 2;
            auto selectInfo = engine.Select(paths[0], TSnapshot(planStep, txId), NOlap::TPKRangesFilter(), false);
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->Portions.size(), 0);
        }

        {   // select from snap after insert (greater planStep)
            ui64 planStep = 2;
            ui64 txId = 1;
            auto selectInfo = engine.Select(paths[0], TSnapshot(planStep, txId), NOlap::TPKRangesFilter(), false);
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->Portions.size(), 1);
        }

        {   // select another pathId
            ui64 planStep = 2;
            ui64 txId = 1;
            auto selectInfo = engine.Select(paths[1], TSnapshot(planStep, txId), NOlap::TPKRangesFilter(), false);
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->Portions.size(), 0);
        }
    }

    Y_UNIT_TEST(IndexWriteLoadRead) {
        WriteLoadRead(testColumns, testKey);
    }

    Y_UNIT_TEST(IndexWriteLoadReadStrPK) {
        std::vector<NArrow::NTest::TTestColumn> key = { NArrow::NTest::TTestColumn("resource_type", TTypeInfo(NTypeIds::Utf8)),
            NArrow::NTest::TTestColumn("resource_id", TTypeInfo(NTypeIds::Utf8)), NArrow::NTest::TTestColumn("uid", TTypeInfo(NTypeIds::Utf8)),
            NArrow::NTest::TTestColumn("timestamp", TTypeInfo(NTypeIds::Timestamp)) };

        WriteLoadRead(testColumns, key);
    }

    void ReadWithPredicates(const std::vector<NArrow::NTest::TTestColumn>& ydbSchema, const std::vector<NArrow::NTest::TTestColumn>& key) {
        TTestBasicRuntime runtime;
        TTestDbWrapper db;
        TIndexInfo tableInfo = NColumnShard::BuildTableInfo(ydbSchema, key);

        const auto& pathId =  TInternalPathId::FromRawValue(1);
        ui32 step = 1000;

        TSnapshot indexSnapshot(1, 1);
        TColumnEngineForLogs engine(
            0, std::make_shared<TSchemaObjectsCache>(), NDataAccessorControl::TLocalManager::BuildForTests(), CommonStoragesManager, indexSnapshot, 0, TIndexInfo(tableInfo), std::make_shared<NColumnShard::TPortionIndexStats>());
        engine.RegisterTable(pathId);
        engine.TestingLoad(db);

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
            std::vector<TCommittedData> dataToIndex;
            TSnapshot ss(planStep, txId);
            dataToIndex.push_back(
                TCommittedData(TUserData::Build(pathId, blobRange, TLocalHelper::GetMetaProto(), 1, {}), ss, 0, (TInsertWriteId)txId));

            bool ok = Insert(engine, db, ss, std::move(dataToIndex), blobs, step);
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
        THashSet<ui32> oneColumnId = { indexInfo.GetColumnIdVerified(key[0].GetName()) };

        {   // full scan
            ui64 txId = 1;
            auto selectInfo = engine.Select(pathId, TSnapshot(planStep, txId), NOlap::TPKRangesFilter(), false);
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->Portions.size(), 20);
        }

        // predicates

        {
            ui64 txId = 1;
            std::shared_ptr<TPredicate> gt10k = MakePredicate(10000, NKikimr::NKernels::EOperation::Greater);
            if (key[0].GetType() == TTypeInfo(NTypeIds::Utf8)) {
                gt10k = MakeStrPredicate("10000", NKikimr::NKernels::EOperation::Greater);
            }
            NOlap::TPKRangesFilter pkFilter;
            Y_ABORT_UNLESS(pkFilter.Add(gt10k, nullptr, indexInfo.GetReplaceKey()));
            auto selectInfo = engine.Select(pathId, TSnapshot(planStep, txId), pkFilter, false);
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->Portions.size(), 10);
        }

        {
            ui64 txId = 1;
            std::shared_ptr<TPredicate> lt10k = MakePredicate(8999, NKikimr::NKernels::EOperation::Less);   // TODO: better border checks
            if (key[0].GetType() == TTypeInfo(NTypeIds::Utf8)) {
                lt10k = MakeStrPredicate("08999", NKikimr::NKernels::EOperation::Less);
            }
            NOlap::TPKRangesFilter pkFilter;
            Y_ABORT_UNLESS(pkFilter.Add(nullptr, lt10k, indexInfo.GetReplaceKey()));
            auto selectInfo = engine.Select(pathId, TSnapshot(planStep, txId), pkFilter, false);
            UNIT_ASSERT_VALUES_EQUAL(selectInfo->Portions.size(), 9);
        }
    }

    Y_UNIT_TEST(IndexReadWithPredicates) {
        ReadWithPredicates(testColumns, testKey);
    }

    Y_UNIT_TEST(IndexReadWithPredicatesStrPK) {
        std::vector<NArrow::NTest::TTestColumn> key = { NArrow::NTest::TTestColumn("resource_type", TTypeInfo(NTypeIds::Utf8)),
            NArrow::NTest::TTestColumn("resource_id", TTypeInfo(NTypeIds::Utf8)), NArrow::NTest::TTestColumn("uid", TTypeInfo(NTypeIds::Utf8)),
            NArrow::NTest::TTestColumn("timestamp", TTypeInfo(NTypeIds::Timestamp)) };

        ReadWithPredicates(testColumns, key);
    }

    Y_UNIT_TEST(IndexWriteOverload) {
        TTestBasicRuntime runtime;
        TTestDbWrapper db;
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TIndexInfo tableInfo = NColumnShard::BuildTableInfo(testColumns, testKey);
        ;

        const auto& pathId =  TInternalPathId::FromRawValue(1);
        ui32 step = 1000;

        // inserts
        ui64 planStep = 1;

        TSnapshot indexSnapshot(1, 1);
        TColumnEngineForLogs engine(
            0, std::make_shared<TSchemaObjectsCache>(), NDataAccessorControl::TLocalManager::BuildForTests(), CommonStoragesManager, indexSnapshot, 0, TIndexInfo(tableInfo), std::make_shared<NColumnShard::TPortionIndexStats>());
        engine.RegisterTable(pathId);
        engine.TestingLoad(db);

        ui64 numRows = 1000;
        ui64 rowPos = 0;
        NBlobOperations::NRead::TCompositeReadBlobs blobsAll;
        for (ui64 txId = 1; txId <= 100; ++txId, rowPos += numRows) {
            TString testBlob = MakeTestBlob(rowPos, rowPos + numRows);
            auto blobRange = MakeBlobRange(++step, testBlob.size());
            NBlobOperations::NRead::TCompositeReadBlobs blobs;
            blobs.Add(IStoragesManager::DefaultStorageId, blobRange, std::move(testBlob));

            // PlanStep, TxId, PathId, DedupId, BlobId, Data, [Metadata]
            std::vector<TCommittedData> dataToIndex;
            TSnapshot ss(planStep, txId);
            dataToIndex.push_back(
                TCommittedData(TUserData::Build(pathId, blobRange, TLocalHelper::GetMetaProto(), 1, {}), ss, 0, (TInsertWriteId)txId));

            bool ok = Insert(engine, db, ss, std::move(dataToIndex), blobs, step);
            blobsAll.Merge(std::move(blobs));
            UNIT_ASSERT(ok);
        }

        {   // check it's overloaded after reload
            TColumnEngineForLogs tmpEngine(
                0, std::make_shared<TSchemaObjectsCache>(), NDataAccessorControl::TLocalManager::BuildForTests(), CommonStoragesManager, TSnapshot::Zero(), 0, TIndexInfo(tableInfo), std::make_shared<NColumnShard::TPortionIndexStats>());
            tmpEngine.RegisterTable(pathId);
            tmpEngine.TestingLoad(db);
        }

        // compact
        planStep = 2;

        bool ok = Compact(engine, db, TSnapshot(planStep, 1), std::move(blobsAll), step, { 23, 5, 5 });
        UNIT_ASSERT(ok);

        // success write after compaction
        planStep = 3;

        for (ui64 txId = 1; txId <= 2; ++txId, rowPos += numRows) {
            TString testBlob = MakeTestBlob(rowPos, rowPos + numRows);
            auto blobRange = MakeBlobRange(++step, testBlob.size());
            NBlobOperations::NRead::TCompositeReadBlobs blobs;
            blobs.Add(IStoragesManager::DefaultStorageId, blobRange, std::move(testBlob));

            // PlanStep, TxId, PathId, DedupId, BlobId, Data, [Metadata]
            std::vector<TCommittedData> dataToIndex;
            TSnapshot ss(planStep, txId);
            dataToIndex.push_back(
                TCommittedData(TUserData::Build(pathId, blobRange, TLocalHelper::GetMetaProto(), 1, {}), ss, 0, TInsertWriteId(txId)));

            bool ok = Insert(engine, db, ss, std::move(dataToIndex), blobs, step);
            UNIT_ASSERT(ok);
        }

        {   // check it's not overloaded after reload
            TColumnEngineForLogs tmpEngine(
                0, std::make_shared<TSchemaObjectsCache>(), NDataAccessorControl::TLocalManager::BuildForTests(), CommonStoragesManager, TSnapshot::Zero(), 0, TIndexInfo(tableInfo), std::make_shared<NColumnShard::TPortionIndexStats>());
            tmpEngine.RegisterTable(pathId);
            tmpEngine.TestingLoad(db);
        }
    }

    Y_UNIT_TEST(IndexTtl) {
        TTestBasicRuntime runtime;
        TTestDbWrapper db;
        TIndexInfo tableInfo = NColumnShard::BuildTableInfo(testColumns, testKey);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        csDefaultControllerGuard->SetOverrideTasksActualizationLag(TDuration::Zero());

        const auto pathId = TInternalPathId::FromRawValue(1);
        ui32 step = 1000;

        // insert
        ui64 planStep = 1;
        TSnapshot indexSnapshot(1, 1);
        {
            TColumnEngineForLogs engine(
                0, std::make_shared<TSchemaObjectsCache>(), NDataAccessorControl::TLocalManager::BuildForTests(), CommonStoragesManager, indexSnapshot, 0, TIndexInfo(tableInfo), std::make_shared<NColumnShard::TPortionIndexStats>());
            engine.RegisterTable(pathId);
            engine.TestingLoad(db);

            const ui64 numRows = 1000;
            const ui64 txCount = 20;
            const ui64 tsIncrement = 1;
            const auto blobTsRange = numRows * tsIncrement;
            const auto gap = TDuration::Hours(1);   //much longer than blobTsRange*txCount
            auto blobStartTs = (TInstant::Now() - gap).MicroSeconds();
            for (ui64 txId = 1; txId <= txCount; ++txId) {
                TString testBlob = MakeTestBlob(blobStartTs, blobStartTs + blobTsRange, tsIncrement);
                auto blobRange = MakeBlobRange(++step, testBlob.size());
                NBlobOperations::NRead::TCompositeReadBlobs blobs;
                TString str1 = testBlob;
                blobs.Add(IStoragesManager::DefaultStorageId, blobRange, std::move(str1));

                // PlanStep, TxId, PathId, DedupId, BlobId, Data, [Metadata]
                TSnapshot ss(planStep, txId);
                std::vector<TCommittedData> dataToIndex;
                dataToIndex.push_back(
                    TCommittedData(TUserData::Build(pathId, blobRange, TLocalHelper::GetMetaProto(), 1, {}), ss, 0, TInsertWriteId(txId)));

                bool ok = Insert(engine, db, ss, std::move(dataToIndex), blobs, step);
                UNIT_ASSERT(ok);
                blobStartTs += blobTsRange;
                if (txId == txCount / 2) {
                    //Make a gap.
                    //NB After this gap, some rows may be in the future at the point of setting TTL
                    blobStartTs += gap.MicroSeconds();
                }
            }

            // compact
            planStep = 2;

            //        bool ok = Compact(engine, db, TSnapshot(planStep, 1), std::move(blobs), step, {20, 4, 4});
            //        UNIT_ASSERT(ok);

            // read
            planStep = 3;

            const TIndexInfo& indexInfo = engine.GetVersionedIndex().GetLastSchema()->GetIndexInfo();
            THashSet<ui32> oneColumnId = { indexInfo.GetColumnIdVerified(testColumns[0].GetName()) };

            {   // full scan
                ui64 txId = 1;
                auto selectInfo = engine.Select(pathId, TSnapshot(planStep, txId), NOlap::TPKRangesFilter(), false);
                UNIT_ASSERT_VALUES_EQUAL(selectInfo->Portions.size(), 20);
            }

            // Cleanup
            Cleanup(engine, db, TSnapshot(planStep, 1), 0);

            {   // full scan
                ui64 txId = 1;
                auto selectInfo = engine.Select(pathId, TSnapshot(planStep, txId), NOlap::TPKRangesFilter(), false);
                UNIT_ASSERT_VALUES_EQUAL(selectInfo->Portions.size(), 20);
            }

            // TTL
            std::shared_ptr<arrow::DataType> ttlColType = arrow::timestamp(arrow::TimeUnit::MICRO);
            THashMap<TInternalPathId, NOlap::TTiering> pathTtls;
            NOlap::TTiering tiering;
            AFL_VERIFY(tiering.Add(NOlap::TTierInfo::MakeTtl(gap, "timestamp")));
            pathTtls.emplace(pathId, std::move(tiering));
            Ttl(engine, db, pathTtls, txCount / 2);

            // read + load + read

            {   // full scan
                ui64 txId = 1;
                auto selectInfo = engine.Select(pathId, TSnapshot(planStep, txId), NOlap::TPKRangesFilter(), false);
                UNIT_ASSERT_VALUES_EQUAL(selectInfo->Portions.size(), 10);
            }
        }
        {
            // load
            TColumnEngineForLogs engine(
                0, std::make_shared<TSchemaObjectsCache>(), NDataAccessorControl::TLocalManager::BuildForTests(), CommonStoragesManager, indexSnapshot, 0, TIndexInfo(tableInfo), std::make_shared<NColumnShard::TPortionIndexStats>());
            engine.RegisterTable(pathId);
            engine.TestingLoad(db);

            const TIndexInfo& indexInfo = engine.GetVersionedIndex().GetLastSchema()->GetIndexInfo();
            THashSet<ui32> oneColumnId = { indexInfo.GetColumnIdVerified(testColumns[0].GetName()) };

            {   // full scan
                ui64 txId = 1;
                auto selectInfo = engine.Select(pathId, TSnapshot(planStep, txId), NOlap::TPKRangesFilter(), false);
                UNIT_ASSERT_VALUES_EQUAL(selectInfo->Portions.size(), 10);
            }
        }
    }
}

}   // namespace NKikimr
