#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/protos/long_tx_service_config.pb.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/engines/changes/cleanup_portions.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction.h>
#include <ydb/core/tx/columnshard/engines/changes/ttl.h>
#include <ydb/core/tx/columnshard/engines/changes/with_appended.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/objects_cache.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/operations/write_data.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/core/tx/columnshard/test_helper/shard_reader.h>
#include <ydb/core/tx/columnshard/test_helper/test_combinator.h>
#include <ydb/core/tx/long_tx_service/public/snapshot_registry.h>

#include <ydb/library/actors/protos/unittests.pb.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <arrow/api.h>
#include <arrow/ipc/reader.h>
#include <util/string/join.h>
#include <util/string/printf.h>

namespace NKikimr {

using namespace NColumnShard;
using namespace Tests;
using namespace NTxUT;

using TDefaultTestsController = NKikimr::NYDBTest::NColumnShard::TController;

namespace {

// Update a column in a RecordBatch to a constant value (seconds since epoch).
// Copied from ut_columnshard_schema.cpp.
std::shared_ptr<arrow::RecordBatch> UpdateColumn(std::shared_ptr<arrow::RecordBatch> batch, TString columnName, i64 seconds) {
    std::string name(columnName.c_str(), columnName.size());
    auto schema = batch->schema();
    int pos = schema->GetFieldIndex(name);
    UNIT_ASSERT(pos >= 0);
    auto colType = batch->GetColumnByName(name)->type_id();
    std::shared_ptr<arrow::Array> array;
    if (colType == arrow::Type::TIMESTAMP) {
        auto scalar = arrow::TimestampScalar(seconds * 1000 * 1000, arrow::timestamp(arrow::TimeUnit::MICRO));
        UNIT_ASSERT_VALUES_EQUAL(scalar.value, seconds * 1000 * 1000);
        auto res = arrow::MakeArrayFromScalar(scalar, batch->num_rows());
        UNIT_ASSERT(res.ok());
        array = *res;
    } else if (colType == arrow::Type::UINT16) {
        TInstant date(TInstant::Seconds(seconds));
        auto res = arrow::MakeArrayFromScalar(arrow::UInt16Scalar(date.Days()), batch->num_rows());
        UNIT_ASSERT(res.ok());
        array = *res;
    } else if (colType == arrow::Type::UINT32) {
        auto res = arrow::MakeArrayFromScalar(arrow::UInt32Scalar(seconds), batch->num_rows());
        UNIT_ASSERT(res.ok());
        array = *res;
    } else if (colType == arrow::Type::UINT64) {
        auto res = arrow::MakeArrayFromScalar(arrow::UInt64Scalar(seconds), batch->num_rows());
        UNIT_ASSERT(res.ok());
        array = *res;
    }
    UNIT_ASSERT(array);
    auto columns = batch->columns();
    columns[pos] = array;
    return arrow::RecordBatch::Make(schema, batch->num_rows(), columns);
}

template <bool GenerateInternalPathId>
class TTruncatePathIdController: public TDefaultTestsController {
public:
    bool IsForcedGenerateInternalPathId() const override {
        return GenerateInternalPathId;
    }
};

constexpr auto TruncateTestMaxReadStaleness = TDuration::Seconds(1);

void SetupTruncateTestRuntime(TTestBasicRuntime& runtime) {
    TTester::Setup(runtime);
    // Use local scan snapshot guard so SetOverrideMaxReadStaleness controls the cleanup floor.
    runtime.GetAppData().FeatureFlags.SetEnableSnapshotsLocking(false);
}

auto RegisterTruncateTestController() {
    auto guard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
    guard->SetOverrideMaxReadStaleness(TruncateTestMaxReadStaleness);
    guard->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Cleanup);
    return guard;
}

const TColumnShard* WaitForShard(TDefaultTestsController& controller, TTestBasicRuntime& runtime) {
    const TInstant deadline = TInstant::Now() + TDuration::Seconds(5);
    while (controller.GetShardActualsCount() == 0 && TInstant::Now() < deadline) {
        runtime.SimulateSleep(TDuration::MilliSeconds(50));
    }
    UNIT_ASSERT_VALUES_EQUAL(controller.GetShardActualsCount(), 1);
    return controller.GetTheOnlyShard();
}

bool IsInPathsToDrop(const TColumnShard& shard, const TInternalPathId& pathId) {
    for (const auto& [_, pathIds] : shard.GetTablesManager().GetPathsToDrop()) {
        if (pathIds.contains(pathId)) {
            return true;
        }
    }
    return false;
}

void AssertPathsToDropState(const TColumnShard& shard, const TInternalPathId& pathId, const bool expectedPresent) {
    UNIT_ASSERT_VALUES_EQUAL(IsInPathsToDrop(shard, pathId), expectedPresent);
}

void AdvanceShardPlanStep(
    TTestBasicRuntime& runtime, TActorId& sender, ui64& txId, int& writeId, const ui64 pathId, const TestTableDescription& testTable) {
    std::vector<ui64> writeIds;
    UNIT_ASSERT(WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 0, 1 }, testTable.Schema), testTable.Schema, true, &writeIds));
    const auto planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
    PlanCommit(runtime, sender, planStep, txId);
}

bool HasPortionsRemovedAt(const TColumnShard& shard, const TInternalPathId pathId, const NOlap::TSnapshot& snapshot) {
    const auto& granule = shard.GetTablesManager().GetPrimaryIndexAsVerified<NOlap::TColumnEngineForLogs>().GetGranuleVerified(pathId);
    for (const auto& [_, portion] : granule.GetPortions()) {
        if (portion->IsRemovedFor(snapshot)) {
            return true;
        }
    }
    return false;
}

bool WaitForTruncatedPortionsCleanup(TDefaultTestsController& controller, TTestBasicRuntime& runtime, const TActorId& sender,
    const TInternalPathId pathId, const NOlap::TSnapshot& snapshot, const std::function<void()>& advancePlanStep) {
    const TInstant end = TInstant::Now() + TDuration::Seconds(60);
    while (TInstant::Now() < end) {
        Wakeup(runtime, sender, TTestTxConfig::TxTablet0);
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new NColumnShard::TEvPrivate::TEvPingSnapshotsUsage());
        advancePlanStep();
        runtime.SimulateSleep(TDuration::Seconds(1));
        Y_UNUSED(controller.WaitCleaning(TDuration::Seconds(1), &runtime));
        if (const auto* shard = controller.GetAnyShard()) {
            if (!HasPortionsRemovedAt(*shard, pathId, snapshot)) {
                return true;
            }
        }
    }
    return false;
}

bool CheckTableInfoV1RowExists(TTestBasicRuntime& runtime, ui64 tabletId, ui64 internalPathId, ui64 schemeShardLocalPathId) {
    TActorId sender = runtime.AllocateEdgeActor();
    const TString query = Sprintf(R"___(
        (
            (let key '('('PathId (Uint64 '%lu)) '('SchemeShardLocalPathId (Uint64 '%lu))))
            (let select '('PathId))
            (return (AsList (SetResult 'Result (SelectRow 'TableInfoV1 key select))))
        )
    )___", internalPathId, schemeShardLocalPathId);

    auto evTx = new TEvTablet::TEvLocalMKQL;
    evTx->Record.MutableProgram()->MutableProgram()->SetText(query);
    ForwardToTablet(runtime, tabletId, sender, evTx);

    auto event = runtime.GrabEdgeEvent<TEvTablet::TEvLocalMKQLResponse>(sender);
    UNIT_ASSERT(event);
    UNIT_ASSERT_VALUES_EQUAL(event->Get()->Record.GetStatus(), NKikimrProto::OK);
    const auto& result = event->Get()->Record.GetExecutionEngineEvaluatedResponse();
    return result.GetValue().GetStruct(0).GetOptional().HasOptional();
}

enum class EPreparedCommitMode {
    Simple,
    Primary,
    Secondary
};

void CheckPreparedCommitAfterTruncate(EPreparedCommitMode mode, bool reboot) {
    const bool sync = mode != EPreparedCommitMode::Simple;
    const bool secondary = mode == EPreparedCommitMode::Secondary;
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);
    auto controller = RegisterTruncateTestController();
    TActorId sender = runtime.AllocateEdgeActor();
    TestTableDescription table{};
    Y_UNUSED(PrepareTablet(runtime, 1, table.Schema));
    std::vector<ui64> oldIds;
    UNIT_ASSERT(WriteData(runtime, sender, 10, 1, MakeTestBlob({ 0, 100 }, table.Schema), table.Schema, true, &oldIds));
    const auto oldStep = ProposeCommit(runtime, sender, 11, oldIds);
    PlanCommit(runtime, sender, oldStep, 11);
    constexpr ui64 lockId = 103;
    constexpr ui64 commitTxId = 20;
    constexpr ui64 truncateTxId = 30;
    constexpr ui64 peer = TTestTxConfig::TxTablet0 + 100;
    bool votedAbort = false;
    bool scanWithLock = sync;
    runtime.SetEventFilter([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
        if (scanWithLock && ev->GetTypeRewrite() == TEvDataShard::TEvKqpScan::EventType) {
            auto& record = ev->Get<TEvDataShard::TEvKqpScan>()->Record;
            record.SetLockTxId(lockId);
            record.SetLockMode(NKikimrDataEvents::OPTIMISTIC);
        }
        if (secondary && ev->GetTypeRewrite() == TEvPipeCache::TEvForward::EventType) {
            auto* forward = ev->Get<TEvPipeCache::TEvForward>();
            if (forward->TabletId == peer) {
                if (auto* readSet = dynamic_cast<TEvTxProcessing::TEvReadSet*>(forward->Ev.Get())) {
                    NKikimrTx::TReadSetData data;
                    UNIT_ASSERT(data.ParseFromString(readSet->Record.GetReadSet()));
                    UNIT_ASSERT(data.GetDecision() == NKikimrTx::TReadSetData::DECISION_ABORT);
                    votedAbort = true;
                }
                return true;
            }
        }
        return false;
    });
    if (sync) {
        TShardReader reader(runtime, TTestTxConfig::TxTablet0, 1, NOlap::TSnapshot(oldStep, 11));
        reader.SetReplyColumnIds(TTestSchema::ExtractIds(table.Schema));
        auto rows = reader.ReadAll();
        UNIT_ASSERT(rows);
        UNIT_ASSERT_VALUES_EQUAL(rows->num_rows(), 100);
        UNIT_ASSERT(!reader.IsError());
    }
    scanWithLock = false;
    std::vector<ui64> ids;
    UNIT_ASSERT(WriteData(runtime, sender, 12, 1, MakeTestBlob({ 100, 110 }, table.Schema), table.Schema, true, &ids,
        NEvWrite::EModificationType::Upsert, lockId));
    const auto* lock = WaitForShard(*controller.operator->(), runtime)->GetOperationsManager().GetLockOptional(lockId);
    UNIT_ASSERT(lock);
    UNIT_ASSERT(!lock->IsBroken());
    auto request = std::make_unique<NEvents::TDataEvents::TEvWrite>(commitTxId, NKikimrDataEvents::TEvWrite::MODE_PREPARE);
    auto* locks = request->Record.MutableLocks();
    locks->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
    auto* protoLock = locks->AddLocks();
    protoLock->SetLockId(lockId);
    protoLock->SetGeneration(lock->GetGeneration());
    protoLock->SetCounter(lock->GetInternalGenerationCounter());
    if (sync) {
        locks->SetArbiterColumnShard(secondary ? peer : TTestTxConfig::TxTablet0);
        locks->AddSendingShards(TTestTxConfig::TxTablet0);
        locks->AddReceivingShards(TTestTxConfig::TxTablet0);
        if (secondary) {
            locks->AddSendingShards(peer);
            locks->AddReceivingShards(peer);
        }
    }
    ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, request.release());
    auto prepared = runtime.GrabEdgeEvent<NEvents::TDataEvents::TEvWriteResult>(sender);
    UNIT_ASSERT_VALUES_EQUAL(prepared->Get()->Record.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED);
    UNIT_ASSERT_VALUES_EQUAL(prepared->Get()->Record.GetTxId(), commitTxId);
    const ui64 commitMinStep = prepared->Get()->Record.GetMinStep();
    // TRUNCATE must prepare and complete while commitTxId is still prepared.
    const auto truncateStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(1, 1), truncateTxId);
    PlanSchemaTx(runtime, sender, { truncateStep, truncateTxId });
    lock = WaitForShard(*controller.operator->(), runtime)->GetOperationsManager().GetLockOptional(lockId);
    UNIT_ASSERT(lock);
    UNIT_ASSERT_VALUES_EQUAL(lock->IsBroken(), sync);
    if (reboot) {
        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
    }
    const TPlanStep commitStep{ Max(Max(commitMinStep, truncateStep.Val()), runtime.GetTimeProvider()->Now().MilliSeconds()) + 1 };
    PlanWriteTx(runtime, sender, { commitStep, commitTxId }, false);
    if (secondary) {
        for (ui32 i = 0; i < 100 && !votedAbort; ++i) {
            runtime.SimulateSleep(TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT(votedAbort);
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender,
            new TEvTxProcessing::TEvReadSetAck(0, commitTxId, TTestTxConfig::TxTablet0, peer, peer, 0));
        NKikimrTx::TReadSetData decision;
        decision.SetDecision(NKikimrTx::TReadSetData::DECISION_ABORT);
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender,
            new TEvTxProcessing::TEvReadSet(commitStep.Val(), commitTxId, peer, TTestTxConfig::TxTablet0, peer, decision.SerializeAsString()));
    }
    auto result = runtime.GrabEdgeEvent<NEvents::TDataEvents::TEvWriteResult>(sender);
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetTxId(), commitTxId);
    const bool aborted = sync;
    UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(),
        aborted ? NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN : NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
    TShardReader reader(runtime, TTestTxConfig::TxTablet0, 1, { commitStep, commitTxId });
    reader.SetReplyColumnIds(TTestSchema::ExtractIds(table.Schema));
    auto rows = reader.ReadAll();
    UNIT_ASSERT(!reader.IsError());
    UNIT_ASSERT_VALUES_EQUAL(rows ? rows->num_rows() : 0, aborted ? 0 : 10);
}

void CheckPreparedSyncCommitRetryDuringTruncate(bool secondary, bool reboot) {
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);
    auto controller = RegisterTruncateTestController();
    TActorId sender = runtime.AllocateEdgeActor();
    TestTableDescription table{};
    Y_UNUSED(PrepareTablet(runtime, 1, table.Schema));
    constexpr ui64 lockId = 103;
    constexpr ui64 commitTxId = 20;
    constexpr ui64 peer = TTestTxConfig::TxTablet0 + 100;
    std::vector<ui64> ids;
    UNIT_ASSERT(WriteData(
        runtime, sender, 10, 1, MakeTestBlob({ 0, 100 }, table.Schema), table.Schema, true, &ids, NEvWrite::EModificationType::Upsert, lockId));
    const auto* lock = WaitForShard(*controller.operator->(), runtime)->GetOperationsManager().GetLockOptional(lockId);
    UNIT_ASSERT(lock && !lock->IsBroken());
    NKikimrDataEvents::TEvWrite request;
    request.SetTxId(commitTxId);
    request.SetTxMode(NKikimrDataEvents::TEvWrite::MODE_PREPARE);
    auto* locks = request.MutableLocks();
    locks->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
    auto* protoLock = locks->AddLocks();
    protoLock->SetLockId(lockId);
    protoLock->SetGeneration(lock->GetGeneration());
    protoLock->SetCounter(lock->GetInternalGenerationCounter());
    locks->SetArbiterColumnShard(secondary ? peer : TTestTxConfig::TxTablet0);
    locks->AddSendingShards(TTestTxConfig::TxTablet0);
    locks->AddReceivingShards(TTestTxConfig::TxTablet0);
    if (secondary) {
        locks->AddSendingShards(peer);
        locks->AddReceivingShards(peer);
    }
    const auto propose = [&](const NKikimrDataEvents::TEvWrite& record, NKikimrDataEvents::TEvWriteResult::EStatus expected) {
        auto ev = std::make_unique<NEvents::TDataEvents::TEvWrite>();
        ev->Record.CopyFrom(record);
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, ev.release());
        auto result = runtime.GrabEdgeEvent<NEvents::TDataEvents::TEvWriteResult>(sender);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetTxId(), record.GetTxId());
        UNIT_ASSERT_VALUES_EQUAL_C(result->Get()->Record.GetStatus(), expected, result->Get()->Record.DebugString());
        return result->Get()->Record;
    };
    const auto prepared = propose(request, NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED);
    Y_UNUSED(ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(1, 1), 30));
    if (reboot) {
        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        const auto* recovered = WaitForShard(*controller.operator->(), runtime)->GetOperationsManager().GetLockOptional(lockId);
        UNIT_ASSERT(recovered && recovered->IsBroken());
    }
    auto differentLock = request;
    differentLock.MutableLocks()->MutableLocks(0)->SetLockId(lockId + 1);
    propose(differentLock, NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
    auto differentKind = request;
    differentKind.MutableLocks()->ClearSendingShards();
    differentKind.MutableLocks()->ClearReceivingShards();
    differentKind.MutableLocks()->ClearArbiterColumnShard();
    propose(differentKind, NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
    auto freshCommit = request;
    freshCommit.SetTxId(commitTxId + 1);
    propose(freshCommit, NKikimrDataEvents::TEvWriteResult::STATUS_SCHEME_CHANGED);
    for (ui32 retry = 0; retry < 2; ++retry) {
        const auto result = propose(request, NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED);
        UNIT_ASSERT_VALUES_EQUAL(result.GetMinStep(), prepared.GetMinStep());
        UNIT_ASSERT_VALUES_EQUAL(result.GetMaxStep(), prepared.GetMaxStep());
    }
}

}   // namespace

Y_UNIT_TEST_SUITE(TruncateTable) {
    Y_UNIT_TEST(EmptyTable) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema);

        ui64 txId = 10;
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
    }

    // Write, truncate, then check both sides of the truncate boundary:
    // a read strictly before the truncate snapshot still sees the old data, a read exactly at
    // the truncate snapshot sees an empty table.
    Y_UNIT_TEST(WithData) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema);

        ui64 txId = 10;
        int writeId = 10;
        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        const auto snapshotBeforeTruncate = NOlap::TSnapshot(planStep, txId);
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto truncateSnapshot = NOlap::TSnapshot(planStep, txId);

        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
    }

    Y_UNIT_TEST_DUO(TruncateEarlierThanBackgroundRemoval, SamePlanStep) {
        TTestBasicRuntime runtime;
        SetupTruncateTestRuntime(runtime);
        auto controller = RegisterTruncateTestController();
        controller->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
        TActorId sender = runtime.AllocateEdgeActor();
        TestTableDescription table{};
        Y_UNUSED(PrepareTablet(runtime, 1, table.Schema));
        ui64 txId = 10;
        std::vector<ui64> writeIds;
        UNIT_ASSERT(WriteData(runtime, sender, 10, 1, MakeTestBlob({ 0, 100 }, table.Schema), table.Schema, true, &writeIds));
        const auto commitStep = ProposeCommit(runtime, sender, ++txId, writeIds);
        PlanCommit(runtime, sender, commitStep, txId);
        const NOlap::TSnapshot before(commitStep, txId);

        const auto* shard = WaitForShard(*controller.operator->(), runtime);
        const auto pathId = *shard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(1), false);
        const auto truncateStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(1, 1), ++txId);
        const NOlap::TSnapshot truncate(truncateStep, txId);
        const NOlap::TSnapshot physicalRemove(truncateStep.Val() + (SamePlanStep ? 0 : TDuration::Hours(1).MilliSeconds()), Max<ui64>());
        auto& engine = shard->GetTablesManager().MutablePrimaryIndexAsVerified<NOlap::TColumnEngineForLogs>();
        const auto& granule = engine.GetGranuleVerified(pathId);
        UNIT_ASSERT(!granule.GetPortions().empty());
        // Model the published input of a background task whose removal is later than TRUNCATE.
        // Queue it before truncate to exercise relocation of an existing GC entry.
        for (const auto& [_, portion] : granule.GetPortions()) {
            engine.ModifyPortionOnComplete(portion, [&](const auto& info) {
                info->SetRemoveSnapshot(physicalRemove);
            });
            engine.AddCleanupPortion(portion);
        }
        PlanSchemaTx(runtime, sender, truncate);
        for (const auto& [_, portion] : granule.GetPortions()) {
            UNIT_ASSERT_VALUES_EQUAL(portion->GetRemoveSnapshotVerified(), truncate);
        }
        for (const auto& [snapshot, expected] : std::vector<std::pair<NOlap::TSnapshot, ui64>>{ { before, 100 }, { truncate, 0 } }) {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, 1, snapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(table.Schema));
            const auto rows = reader.ReadAll();
            UNIT_ASSERT(!reader.IsError());
            UNIT_ASSERT_VALUES_EQUAL(rows ? rows->num_rows() : 0, expected);
        }
        controller->SetOverrideUsedSnapshotLivetime(TDuration::Zero());
        controller->EnableBackground(NKikimr::NYDBTest::ICSController::EBackground::Cleanup);
        ui64 cleanupStep = truncateStep.Val() + 10000;
        const auto advance = [&] {
            UNIT_ASSERT(cleanupStep < physicalRemove.GetPlanStep() || SamePlanStep);
            PlanCommit(runtime, sender, TPlanStep{ cleanupStep }, TSet<ui64>{});
            cleanupStep += 10000;
        };
        UNIT_ASSERT(WaitForTruncatedPortionsCleanup(*controller.operator->(), runtime, sender, pathId, truncate, advance));
        UNIT_ASSERT(granule.GetPortions().empty());
        UNIT_ASSERT(granule.GetTruncateSnapshots().empty());
        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        const auto& recovered = WaitForShard(*controller.operator->(), runtime)
                                    ->GetTablesManager()
                                    .GetPrimaryIndexAsVerified<NOlap::TColumnEngineForLogs>()
                                    .GetGranuleVerified(pathId);
        UNIT_ASSERT(recovered.GetPortions().empty());
        UNIT_ASSERT(recovered.GetTruncateSnapshots().empty());
    }

    Y_UNIT_TEST(TruncateHistoryPersistsOnlyBoundaries) {
        TTestBasicRuntime runtime;
        SetupTruncateTestRuntime(runtime);
        auto controller = RegisterTruncateTestController();
        TActorId sender = runtime.AllocateEdgeActor();
        TestTableDescription table{};
        Y_UNUSED(PrepareTablet(runtime, 1, table.Schema));
        ui64 txId = 10;
        auto step = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(1, 1), ++txId);
        PlanSchemaTx(runtime, sender, { step, txId });
        const auto pathId =
            *WaitForShard(*controller.operator->(), runtime)->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(1),
                false);
        const NOlap::TSnapshot truncateSnapshot(step, txId);
        const auto readHistory = [&](ui64 alias) {
            auto query = new TEvTablet::TEvLocalMKQL;
            query->Record.MutableProgram()->MutableProgram()->SetText(Sprintf(R"___(
                (
                    (let key '('('PathId (Uint64 '%lu)) '('SchemeShardLocalPathId (Uint64 '%lu))))
                    (return (AsList (SetResult 'Result (SelectRow 'TableInfoV1 key '('TruncateSnapshots)))))
                )
            )___", pathId.GetRawValue(), alias));
            ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, query);
            auto response = runtime.GrabEdgeEvent<TEvTablet::TEvLocalMKQLResponse>(sender);
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetStatus(), NKikimrProto::OK);
            const auto& value = response->Get()->Record.GetExecutionEngineEvaluatedResponse().GetValue();
            const auto& bytes = value.GetStruct(0).GetOptional().GetOptional().GetStruct(0).GetOptional().GetBytes();
            NKikimrColumnShardProto::TTruncateSnapshots history;
            UNIT_ASSERT_C(history.ParseFromString(bytes), value.DebugString());
            UNIT_ASSERT_VALUES_EQUAL(history.SnapshotsSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(history.GetSnapshots(0).GetPlanStep(), truncateSnapshot.GetPlanStep());
            UNIT_ASSERT_VALUES_EQUAL(history.GetSnapshots(0).GetTxId(), truncateSnapshot.GetTxId());
            return bytes;
        };
        const TString initial = readHistory(1);
        step = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(1, 2, 2), ++txId);
        PlanSchemaTx(runtime, sender, { step, txId });
        UNIT_ASSERT_VALUES_EQUAL(readHistory(1), initial);
        UNIT_ASSERT_VALUES_EQUAL(readHistory(2), initial);
        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        const auto* shard = WaitForShard(*controller.operator->(), runtime);
        const auto& history = shard->GetTablesManager().GetTable(pathId).GetTruncateSnapshots();
        const auto& granule = shard->GetTablesManager().GetPrimaryIndexAsVerified<NOlap::TColumnEngineForLogs>().GetGranuleVerified(pathId);
        UNIT_ASSERT(&history == &granule.GetTruncateSnapshots());
        UNIT_ASSERT_VALUES_EQUAL(history.size(), 1);
        UNIT_ASSERT(history.begin()->second == NOlap::ETruncateState::MarkedPortions);
        UNIT_ASSERT_VALUES_EQUAL(readHistory(1), initial);
        UNIT_ASSERT_VALUES_EQUAL(readHistory(2), initial);
    }

    Y_UNIT_TEST(TruncateAndInsert) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema);

        ui64 txId = 10;
        int writeId = 10;
        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        const auto snapshotBeforeTruncate = NOlap::TSnapshot(planStep, txId);

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 200, 250 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 50);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
    }

    Y_UNIT_TEST(TruncateAbsentTable) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        Y_UNUSED(PrepareTablet(runtime, pathId, testTable.Schema));

        ui64 txId = 10;
        ProposeSchemaTxFail(runtime, sender, TTestSchema::TruncateTableTxBody(111, 1), ++txId);
    }

    // Each truncate interval has distinct data. Time-travel and the persistent truncate history
    // must survive reboot without changing table identity.
    Y_UNIT_TEST_DUO(MultipleTruncatesTimeTravel, Reboot) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema);

        const auto initialPathId = WaitForShard(*csDefaultControllerGuard.operator->(), runtime)
                                       ->GetTablesManager()
                                       .ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(pathId), false);
        UNIT_ASSERT(initialPathId);
        ui64 txId = 10;
        int writeId = 10;
        ui32 schemaRound = 0;

        auto writeAndCommit = [&](ui64 from, ui64 to) -> NOlap::TSnapshot {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ from, to }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
            return NOlap::TSnapshot(planStep, txId);
        };
        auto truncate = [&]() -> NOlap::TSnapshot {
            planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, ++schemaRound), ++txId);
            PlanSchemaTx(runtime, sender, { planStep, txId });
            return NOlap::TSnapshot(planStep, txId);
        };

        const auto g0Snapshot = writeAndCommit(0, 100);
        const auto t1 = truncate();
        const auto g1Snapshot = writeAndCommit(200, 230);
        const auto t2 = truncate();
        const auto g2Snapshot = writeAndCommit(300, 320);
        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }
        const auto* shard = WaitForShard(*csDefaultControllerGuard.operator->(), runtime);
        UNIT_ASSERT_VALUES_EQUAL(shard->GetTablesManager().GetTables().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            *shard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(pathId), false), *initialPathId);
        const auto& granule =
            shard->GetTablesManager().GetPrimaryIndexAsVerified<NOlap::TColumnEngineForLogs>().GetGranuleVerified(*initialPathId);
        UNIT_ASSERT_VALUES_EQUAL(granule.GetTruncateSnapshots().size(), 2);

        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, g0Snapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, g1Snapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 30);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, g2Snapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 20);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, t1);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, t2);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
    }

    // TRUNCATE preserves TTL settings, which must continue to expire new rows.
    Y_UNIT_TEST(TruncatePreservesTtl) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        csControllerGuard->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
        csControllerGuard->SetOverrideTasksActualizationLag(TDuration::Zero());
        csControllerGuard->SetOverrideCompactionActualizationLag(TDuration::Zero());
        csControllerGuard->SetOverrideOptimizerFreshnessCheckDuration(TDuration::Zero());
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        Y_UNUSED(PrepareTablet(runtime, pathId, testTable.Schema));

        const auto ttlDuration = TDuration::Seconds(3600);
        auto specials = TTestSchema::TTableSpecials().SetTtl(ttlDuration);
        specials.SetTtlColumn(TTestSchema::DefaultTtlColumn);
        const auto alterBody =
            TTestSchema::AlterTableTxBody(pathId, /*standalone=*/true, /*version=*/1, testTable.Schema, testTable.Pk, specials);
        ui64 txId = 10;
        auto planStep = ProposeSchemaTx(runtime, sender, alterBody, ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        auto& csController = *csControllerGuard.operator->();
        const auto* shard = csController.GetTheOnlyShard();

        {
            const auto internalPathId = shard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(pathId), false);
            UNIT_ASSERT(internalPathId);
            const auto ttl = shard->GetTablesManager().GetTableTtl(*internalPathId);
            UNIT_ASSERT(ttl.has_value());
            UNIT_ASSERT_VALUES_EQUAL(ttl->GetEvictColumnName(), TTestSchema::DefaultTtlColumn);
            const auto& tiers = ttl->GetOrderedTiers();
            UNIT_ASSERT_VALUES_EQUAL(tiers.size(), 1);
            const auto& tier = *tiers.begin();
            UNIT_ASSERT_VALUES_EQUAL(tier.Get().GetEvictColumnName(), TTestSchema::DefaultTtlColumn);
            UNIT_ASSERT_VALUES_EQUAL(tier.Get().GetEvictDuration(), ttlDuration);
        }

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 2), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        shard = csController.GetTheOnlyShard();

        {
            const auto newInternalPathId = shard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(pathId), false);
            UNIT_ASSERT(newInternalPathId);
            const auto ttl = shard->GetTablesManager().GetTableTtl(*newInternalPathId);
            UNIT_ASSERT(ttl.has_value());
            UNIT_ASSERT_VALUES_EQUAL(ttl->GetEvictColumnName(), TTestSchema::DefaultTtlColumn);
            const auto& tiers = ttl->GetOrderedTiers();
            UNIT_ASSERT_VALUES_EQUAL(tiers.size(), 1);
            const auto& tier = *tiers.begin();
            UNIT_ASSERT_VALUES_EQUAL(tier.Get().GetEvictColumnName(), TTestSchema::DefaultTtlColumn);
            UNIT_ASSERT_VALUES_EQUAL(tier.Get().GetEvictDuration(), ttlDuration);
        }

        const auto now = TAppData::TimeProvider->Now().Seconds();
        const auto staleTs = now - 7200;
        const auto freshTs = now - 1800;
        std::vector<ui64> writeIds;
        const auto arrowSchema = NArrow::MakeArrowSchema(testTable.Schema);
        auto writeWithTtlTs = [&](const ui64 writeId, const std::pair<ui64, ui64> range, const i64 ts) {
            const TString blob = MakeTestBlob(range, testTable.Schema);
            auto batch = NArrow::DeserializeBatch(blob, arrowSchema);
            UNIT_ASSERT(batch);
            batch = UpdateColumn(batch, TTestSchema::DefaultTtlColumn, ts);
            const TString data = NArrow::SerializeBatchNoCompression(batch);
            UNIT_ASSERT(WriteData(runtime, sender, writeId, pathId, data, testTable.Schema, true, &writeIds));
        };
        writeWithTtlTs(100, { 0, 1 }, staleTs);
        writeWithTtlTs(101, { 1, 2 }, freshTs);
        planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
        PlanCommit(runtime, sender, planStep, txId);
        const auto dataSnapshot = NOlap::TSnapshot(planStep, txId);
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, dataSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 2);
            UNIT_ASSERT(!reader.IsError());
        }

        // TTL eviction commits at a fresh plan step, so it is invisible at dataSnapshot (MVCC).
        // Advance the plan step with an empty commit and read the latest state at that step
        // (TxId = Max<ui64>()) to observe eviction of the stale row.
        auto readLatestRowCount = [&]() -> ui64 {
            planStep = planStep + 1;
            PlanCommit(runtime, sender, planStep, TSet<ui64>{});
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, Max<ui64>()));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(reader.IsCorrectlyFinished());
            return rb ? rb->num_rows() : 0;
        };
        ui64 evictedRowCount = 0;
        csController.WaitCondition(TDuration::Seconds(30), [&] {
            runtime.SimulateSleep(TDuration::MilliSeconds(200));
            evictedRowCount = readLatestRowCount();
            return evictedRowCount == 1;
        });
        UNIT_ASSERT_VALUES_EQUAL(evictedRowCount, 1);
    }

    // When TTL is removed via ALTER and then TRUNCATE is performed, the table
    // must keep TTL disabled. With Reboot=true, exercises the
    // InitFromDB → AddVersionFromProto path with the nullopt case.
    Y_UNIT_TEST_DUO(TruncateAfterTtlRemoved, Reboot) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        Y_UNUSED(PrepareTablet(runtime, pathId, testTable.Schema));

        ui64 txId = 10;

        // Step 1: Set TTL via ALTER.
        const auto ttlDuration = TDuration::Seconds(3600);
        auto specials = TTestSchema::TTableSpecials().SetTtl(ttlDuration);
        specials.SetTtlColumn(TTestSchema::DefaultTtlColumn);
        {
            const auto alterBody =
                TTestSchema::AlterTableTxBody(pathId, /*standalone=*/true, /*version=*/1, testTable.Schema, testTable.Pk, specials);
            auto planStep = ProposeSchemaTx(runtime, sender, alterBody, ++txId);
            PlanSchemaTx(runtime, sender, { planStep, txId });
        }

        auto& csController = *csControllerGuard.operator->();
        const auto* shard = csController.GetTheOnlyShard();

        // Verify TTL is set.
        {
            const auto internalPathId = shard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(pathId), false);
            UNIT_ASSERT(internalPathId);
            const auto ttl = shard->GetTablesManager().GetTableTtl(*internalPathId);
            UNIT_ASSERT(ttl.has_value());
        }

        // Step 2: Remove TTL via ALTER (empty TTableSpecials → Disabled).
        {
            const auto alterBody = TTestSchema::AlterTableTxBody(
                pathId, /*standalone=*/true, /*version=*/2, testTable.Schema, testTable.Pk, TTestSchema::TTableSpecials{});
            auto planStep = ProposeSchemaTx(runtime, sender, alterBody, ++txId);
            PlanSchemaTx(runtime, sender, { planStep, txId });
        }

        // Verify TTL is removed.
        {
            const auto internalPathId = shard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(pathId), false);
            UNIT_ASSERT(internalPathId);
            const auto ttl = shard->GetTablesManager().GetTableTtl(*internalPathId);
            UNIT_ASSERT(!ttl.has_value());
        }

        // Step 3: Optionally restart the tablet to force InitFromDB reload.
        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
            shard = csController.GetTheOnlyShard();
            const auto internalPathId = shard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(pathId), false);
            UNIT_ASSERT(internalPathId);
            const auto ttl = shard->GetTablesManager().GetTableTtl(*internalPathId);
            UNIT_ASSERT(!ttl.has_value());
        }

        // Step 4: TRUNCATE.
        {
            auto planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 3), ++txId);
            PlanSchemaTx(runtime, sender, { planStep, txId });
        }

        // Verify that TTL remains disabled after truncate.
        {
            const auto newInternalPathId = shard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(pathId), false);
            UNIT_ASSERT(newInternalPathId);
            const auto ttl = shard->GetTablesManager().GetTableTtl(*newInternalPathId);
            UNIT_ASSERT(!ttl.has_value());
        }
    }

    // TTL must survive schema-only ALTER (e.g. ADD COLUMN) that does not carry TTL settings.
    // Before the fix, AddVersionFromProto added nullopt for versions without TTL settings,
    // so GetTableTtl(Max) resolved the latest version as no-TTL and tiering was lost after reboot.
    Y_UNIT_TEST_DUO(TtlSurvivesSchemaOnlyAlter, Reboot) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        Y_UNUSED(PrepareTablet(runtime, pathId, testTable.Schema));

        ui64 txId = 10;

        // Step 1: Set TTL via ALTER.
        const auto ttlDuration = TDuration::Seconds(3600);
        auto specials = TTestSchema::TTableSpecials().SetTtl(ttlDuration);
        specials.SetTtlColumn(TTestSchema::DefaultTtlColumn);
        {
            const auto alterBody =
                TTestSchema::AlterTableTxBody(pathId, /*standalone=*/true, /*version=*/1, testTable.Schema, testTable.Pk, specials);
            auto planStep = ProposeSchemaTx(runtime, sender, alterBody, ++txId);
            PlanSchemaTx(runtime, sender, { planStep, txId });
        }

        auto& csController = *csControllerGuard.operator->();
        const auto* shard = csController.GetTheOnlyShard();

        // Verify TTL is set.
        {
            const auto internalPathId = shard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(pathId), false);
            UNIT_ASSERT(internalPathId);
            const auto ttl = shard->GetTablesManager().GetTableTtl(*internalPathId);
            UNIT_ASSERT(ttl.has_value());
            UNIT_ASSERT_VALUES_EQUAL(ttl->GetEvictColumnName(), TTestSchema::DefaultTtlColumn);
        }

        // Step 2: Schema-only ALTER (ADD COLUMN) without TTL settings (carry-over).
        // setTtlSettings=false means the proto has no TtlSettings field → carry-over.
        {
            auto schemaWithNewColumn = testTable.Schema;
            schemaWithNewColumn.push_back(NArrow::NTest::TTestColumn("new_column", NScheme::TTypeInfo(NScheme::NTypeIds::Int32)));
            const auto alterBody = TTestSchema::AlterTableTxBody(pathId, /*standalone=*/true, /*version=*/2, schemaWithNewColumn, testTable.Pk,
                TTestSchema::TTableSpecials{}, /*setTtlSettings=*/false);
            auto planStep = ProposeSchemaTx(runtime, sender, alterBody, ++txId);
            PlanSchemaTx(runtime, sender, { planStep, txId });
        }

        // Verify TTL is still active (not lost due to schema-only ALTER).
        {
            const auto internalPathId = shard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(pathId), false);
            UNIT_ASSERT(internalPathId);
            const auto ttl = shard->GetTablesManager().GetTableTtl(*internalPathId);
            UNIT_ASSERT(ttl.has_value());
            UNIT_ASSERT_VALUES_EQUAL(ttl->GetEvictColumnName(), TTestSchema::DefaultTtlColumn);
        }

        // Step 3: Optionally restart the tablet to force InitFromDB reload.
        // This is where the bug manifested: AddVersionFromProto added nullopt for version 2,
        // so GetTableTtl(Max) resolved to no-TTL after reboot.
        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }
        shard = csController.GetTheOnlyShard();
        const auto internalPathId = shard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(pathId), false);
        UNIT_ASSERT(internalPathId);
        const auto ttl = shard->GetTablesManager().GetTableTtl(*internalPathId);
        UNIT_ASSERT(ttl.has_value());
        UNIT_ASSERT_VALUES_EQUAL(ttl->GetEvictColumnName(), TTestSchema::DefaultTtlColumn);
    }

    // ALTER after TRUNCATE updates the same table while preserving pre-truncate time-travel.
    Y_UNIT_TEST(TruncateThenAlter) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema);

        ui64 txId = 10;
        int writeId = 10;
        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        const auto snapshotBeforeTruncate = NOlap::TSnapshot(planStep, txId);

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto truncateSnapshot = NOlap::TSnapshot(planStep, txId);

        auto& csController = *csControllerGuard.operator->();
        const auto* shard = csController.GetTheOnlyShard();
        const auto newInternalPathId = shard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(pathId), false);
        UNIT_ASSERT(newInternalPathId);
        UNIT_ASSERT(!shard->GetTablesManager().GetTableTtl(*newInternalPathId).has_value());

        auto specials = TTestSchema::TTableSpecials().SetTtl(TDuration::Seconds(3600));
        specials.SetTtlColumn(TTestSchema::DefaultTtlColumn);
        const auto alterBody =
            TTestSchema::AlterTableTxBody(pathId, /*standalone=*/true, /*version=*/2, testTable.Schema, testTable.Pk, specials);
        planStep = ProposeSchemaTx(runtime, sender, alterBody, ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        shard = csController.GetTheOnlyShard();
        {
            const auto resolved = shard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(pathId), false);
            UNIT_ASSERT(resolved);
            UNIT_ASSERT_VALUES_EQUAL(*resolved, *newInternalPathId);
            UNIT_ASSERT(shard->GetTablesManager().GetTableTtl(*resolved).has_value());
        }

        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 200, 250 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 50);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
    }

    // MOVE after TRUNCATE renames the SS path while preserving the table's truncate history.
    // Time-travel therefore works on dst, not on src. Reboot must preserve that mapping.
    Y_UNIT_TEST_DUO(TruncateThenMove, Reboot) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 srcPathId = 1;
        const ui64 dstPathId = 2;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, srcPathId, testTable.Schema);

        ui64 txId = 10;
        int writeId = 10;
        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(
                WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        const auto snapshotBeforeTruncate = NOlap::TSnapshot(planStep, txId);

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(srcPathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::MoveTableTxBody(srcPathId, dstPathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto moveSnapshot = NOlap::TSnapshot(planStep, txId);

        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }

        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, moveSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, moveSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }

        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(WriteData(
                runtime, sender, writeId++, dstPathId, MakeTestBlob({ 200, 250 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 50);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(reader.IsError());
        }
    }

    // COPY after TRUNCATE captures the empty table. The copy is pinned at its CopyVersion,
    // so later writes to the source are NOT visible on the copy.
    Y_UNIT_TEST(TruncateThenCopy) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 srcPathId = 1;
        const ui64 dstPathId = 2;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, srcPathId, testTable.Schema);

        ui64 txId = 10;
        int writeId = 10;
        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(
                WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        const auto snapshotBeforeTruncate = NOlap::TSnapshot(planStep, txId);

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(srcPathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto truncateSnapshot = NOlap::TSnapshot(planStep, txId);
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(srcPathId, dstPathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto copySnapshot = NOlap::TSnapshot(planStep, txId);

        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, copySnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, copySnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }

        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(WriteData(
                runtime, sender, writeId++, srcPathId, MakeTestBlob({ 200, 250 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 50);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            // The copy is pinned at CopyVersion, taken while the table was empty after truncate,
            // so the later source write is invisible on dst.
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }

        ProposeSchemaTxFail(runtime, sender, TTestSchema::TruncateTableTxBody(dstPathId, 1), ++txId);
    }

    Y_UNIT_TEST(TruncateAndDrop) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema);

        ui64 txId = 10;
        int writeId = 10;
        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        const auto snapshotBeforeTruncate = NOlap::TSnapshot(planStep, txId);

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto truncateSnapshot = NOlap::TSnapshot(planStep, txId);
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::DropTableTxBody(pathId, 2), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
    }

    Y_UNIT_TEST(TruncateReadOnlyTableFails) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 srcPathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, srcPathId, testTable.Schema);

        ui64 txId = 10;
        int writeId = 10;
        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(
                WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        const ui64 dstPathId = 2;
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(srcPathId, dstPathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        ProposeSchemaTxFail(runtime, sender, TTestSchema::TruncateTableTxBody(dstPathId, 1), ++txId);
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
    }

    // Copies and time-travel retain truncated portions across reboot. Once both copies are
    // dropped and snapshots expire, GC removes those portions while retaining the live source.
    Y_UNIT_TEST(TruncateCopySourceRetention) {
        TTestBasicRuntime runtime;
        SetupTruncateTestRuntime(runtime);
        auto csControllerGuard = RegisterTruncateTestController();
        auto& csController = *csControllerGuard.operator->();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 srcPathId = 1;
        const ui64 copyPathIdA = 2;
        const ui64 copyPathIdB = 3;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, srcPathId, testTable.Schema);

        ui64 txId = 10;
        int writeId = 10;
        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(
                WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        const auto snapshotBeforeTruncate = NOlap::TSnapshot(planStep, txId);

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(srcPathId, copyPathIdA, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(srcPathId, copyPathIdB, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(srcPathId, 2), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto truncateSnapshot = NOlap::TSnapshot(planStep, txId);

        const auto* shard = WaitForShard(csController, runtime);
        UNIT_ASSERT(shard);
        const auto oldInternalPathId =
            shard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(copyPathIdA), false);
        UNIT_ASSERT(oldInternalPathId);

        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, copyPathIdA, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, copyPathIdB, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, copyPathIdA, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
        UNIT_ASSERT(CheckTableInfoV1RowExists(runtime, TTestTxConfig::TxTablet0, oldInternalPathId->GetRawValue(), srcPathId));

        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);

        const auto* restartedShard = WaitForShard(csController, runtime);
        UNIT_ASSERT(restartedShard);
        {
            const auto recoveredOld =
                restartedShard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(copyPathIdA), false);
            UNIT_ASSERT(recoveredOld);
            UNIT_ASSERT_VALUES_EQUAL(*recoveredOld, *oldInternalPathId);
            const auto recoveredSource =
                restartedShard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(srcPathId), false);
            UNIT_ASSERT(recoveredSource);
            UNIT_ASSERT_VALUES_EQUAL(*recoveredSource, *oldInternalPathId);
            UNIT_ASSERT(restartedShard->GetTablesManager()
                            .GetPrimaryIndexAsVerified<NOlap::TColumnEngineForLogs>()
                            .GetGranuleVerified(*oldInternalPathId)
                            .GetTruncateSnapshots()
                            .contains(truncateSnapshot));
        }

        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, copyPathIdA, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, copyPathIdB, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::DropTableTxBody(copyPathIdA, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, copyPathIdB, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
        UNIT_ASSERT(restartedShard->GetTablesManager().HasTable(*oldInternalPathId, true));

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::DropTableTxBody(copyPathIdB, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        UNIT_ASSERT(!restartedShard->GetTablesManager().GetTable(*oldInternalPathId, true).IsDropped());

        // Expire read snapshots and allow GC to remove truncated portions.
        csControllerGuard->SetOverrideUsedSnapshotLivetime(TDuration::Zero());
        csControllerGuard->EnableBackground(NKikimr::NYDBTest::ICSController::EBackground::Cleanup);
        auto advancePlanStep = [&] {
            AdvanceShardPlanStep(runtime, sender, txId, writeId, srcPathId, testTable);
        };
        UNIT_ASSERT(WaitForTruncatedPortionsCleanup(csController, runtime, sender, *oldInternalPathId, truncateSnapshot, advancePlanStep));

        {
            const auto* finalizedShard = csController.GetAnyShard();
            UNIT_ASSERT(finalizedShard);
            UNIT_ASSERT(finalizedShard->GetTablesManager().HasTable(*oldInternalPathId));
        }
        {
            // After GC removes truncated portions, the read-staleness floor has advanced past
            // snapshotBeforeTruncate, so the time-travel read is rejected ("Snapshot too old").
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(reader.IsError());
        }
    }

    // An active copy scan must retain its portions across source TRUNCATE and copy DROP.
    Y_UNIT_TEST(ActiveCopyScanSurvivesTruncateAndDrop) {
        TTestBasicRuntime runtime;
        SetupTruncateTestRuntime(runtime);
        auto controllerGuard = RegisterTruncateTestController();
        auto& controller = *controllerGuard.operator->();
        controllerGuard->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        TActorId sender = runtime.AllocateEdgeActor();

        constexpr ui64 srcPathId = 1;
        constexpr ui64 copyPathId = 2;
        TestTableDescription testTable{};
        Y_UNUSED(PrepareTablet(runtime, srcPathId, testTable.Schema));

        ui64 txId = 10;
        int writeId = 10;
        std::vector<ui64> writeIds;
        UNIT_ASSERT(
            WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds));
        auto planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
        PlanCommit(runtime, sender, planStep, txId);

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(srcPathId, copyPathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto copySnapshot = NOlap::TSnapshot(planStep, txId);
        const auto* shard = WaitForShard(controller, runtime);
        const auto oldInternalPathId = shard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(copyPathId), false);
        UNIT_ASSERT(oldInternalPathId);

        TShardReader activeScan(runtime, TTestTxConfig::TxTablet0, copyPathId, copySnapshot);
        activeScan.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
        UNIT_ASSERT_C(activeScan.InitializeScanner(), "copy scan must start before TRUNCATE");

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(srcPathId, 2), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto truncateSnapshot = NOlap::TSnapshot(planStep, txId);
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::DropTableTxBody(copyPathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        shard = WaitForShard(controller, runtime);
        UNIT_ASSERT(!shard->GetTablesManager().GetTable(*oldInternalPathId, true).IsDropped());
        AssertPathsToDropState(*shard, *oldInternalPathId, false);

        controllerGuard->SetOverrideUsedSnapshotLivetime(TDuration::Zero());
        controllerGuard->EnableBackground(NKikimr::NYDBTest::ICSController::EBackground::Cleanup);
        for (ui32 i = 0; i < 5; ++i) {
            AdvanceShardPlanStep(runtime, sender, txId, writeId, srcPathId, testTable);
            Wakeup(runtime, sender, TTestTxConfig::TxTablet0);
            ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new NColumnShard::TEvPrivate::TEvPingSnapshotsUsage());
            runtime.SimulateSleep(TDuration::Seconds(1));
            Y_UNUSED(controller.WaitCleaning(TDuration::Seconds(1), &runtime));
            shard = WaitForShard(controller, runtime);
            UNIT_ASSERT_C(HasPortionsRemovedAt(*shard, *oldInternalPathId, truncateSnapshot),
                "GC must retain truncated portions while the copy scan is active");
        }

        activeScan.Ack();
        const auto rows = activeScan.ContinueReadAll();
        UNIT_ASSERT(activeScan.IsCorrectlyFinished());
        UNIT_ASSERT(rows);
        UNIT_ASSERT_VALUES_EQUAL(rows->num_rows(), 100);

        auto advancePlanStep = [&] {
            AdvanceShardPlanStep(runtime, sender, txId, writeId, srcPathId, testTable);
        };
        UNIT_ASSERT(WaitForTruncatedPortionsCleanup(controller, runtime, sender, *oldInternalPathId, truncateSnapshot, advancePlanStep));
        UNIT_ASSERT(controller.GetAnyShard()->GetTablesManager().HasTable(*oldInternalPathId, true));
    }

    // A live copy retains the shared table after source DROP. Expired source scans must
    // fail normally while the copy remains readable.
    Y_UNIT_TEST(DroppedSourceWithLiveCopyRejectsLateScanAfterGc) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        runtime.GetAppData(0).FeatureFlags.SetEnableSnapshotsLocking(true);
        auto& longTx = runtime.GetAppData(0).LongTxServiceConfig;
        longTx.SetLocalSnapshotPromotionTimeSeconds(1);
        longTx.SetMaxClockSkewMs(1000);
        longTx.SetSnapshotsExchangeIntervalSeconds(1);
        longTx.SetSnapshotsRegistryUpdateIntervalSeconds(1);

        auto controllerGuard = RegisterTruncateTestController();
        auto& controller = *controllerGuard.operator->();
        controllerGuard->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        controllerGuard->SetOverrideUsedSnapshotLivetime(TDuration::Zero());
        TActorId sender = runtime.AllocateEdgeActor();

        constexpr ui64 srcPathId = 1;
        constexpr ui64 copyPathId = 2;
        TestTableDescription testTable{};
        Y_UNUSED(PrepareTablet(runtime, srcPathId, testTable.Schema));

        ui64 txId = 10;
        int writeId = 10;
        std::vector<ui64> writeIds;
        UNIT_ASSERT(
            WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds));
        auto planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
        PlanCommit(runtime, sender, planStep, txId);

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(srcPathId, copyPathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto copySnapshot = NOlap::TSnapshot(planStep, txId);
        const auto* shard = WaitForShard(controller, runtime);
        const auto oldInternalPathId = shard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(copyPathId), false);
        UNIT_ASSERT(oldInternalPathId);

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(srcPathId, 2), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto truncateSnapshot = NOlap::TSnapshot(planStep, txId);
        shard = WaitForShard(controller, runtime);
        const auto newInternalPathId = shard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(srcPathId), false);
        UNIT_ASSERT(newInternalPathId);
        UNIT_ASSERT_VALUES_EQUAL(*oldInternalPathId, *newInternalPathId);

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::DropTableTxBody(srcPathId, 3), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        shard = WaitForShard(controller, runtime);
        AssertPathsToDropState(*shard, *newInternalPathId, false);
        AssertPathsToDropState(*shard, *oldInternalPathId, false);

        controllerGuard->EnableBackground(NKikimr::NYDBTest::ICSController::EBackground::Cleanup);
        ui64 nextPlanStep = planStep.Val() + 10000;
        for (ui32 i = 0; i < 5; ++i) {
            runtime.SimulateSleep(TDuration::Seconds(1));
            auto registryBuilder = CreateImmutableSnapshotRegistryBuilder();
            registryBuilder->SetOldestCollectionTime(runtime.GetCurrentTime());
            runtime.GetAppData(0).SnapshotRegistryHolder->Set(std::move(*registryBuilder).Build());
            PlanCommit(runtime, sender, TPlanStep{ nextPlanStep++ }, TSet<ui64>{});
            Wakeup(runtime, sender, TTestTxConfig::TxTablet0);
            Y_UNUSED(controller.WaitCleaning(TDuration::Seconds(1), &runtime));
        }

        shard = WaitForShard(controller, runtime);
        UNIT_ASSERT_C(shard->GetTablesManager().HasTable(*newInternalPathId, true), "the copy must retain the shared table");
        UNIT_ASSERT(shard->GetTablesManager().HasTable(*oldInternalPathId, true));
        const auto src = TSchemeShardLocalPathId::FromRawValue(srcPathId);
        UNIT_ASSERT(!shard->GetTablesManager().ResolveInternalPathId(src, false));

        TShardReader lateScan(runtime, TTestTxConfig::TxTablet0, srcPathId, truncateSnapshot);
        lateScan.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
        UNIT_ASSERT(!lateScan.ReadAll());
        UNIT_ASSERT_C(lateScan.IsError(), "late scan of dropped source must fail without crashing the tablet");

        // A path with neither history nor a live mapping must also fail without aborting the tablet.
        TShardReader missingScan(runtime, TTestTxConfig::TxTablet0, 3, copySnapshot);
        missingScan.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
        UNIT_ASSERT(!missingScan.ReadAll());
        UNIT_ASSERT(missingScan.IsError());

        TShardReader copyScan(runtime, TTestTxConfig::TxTablet0, copyPathId, copySnapshot);
        copyScan.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
        const auto rows = copyScan.ReadAll();
        UNIT_ASSERT(copyScan.IsCorrectlyFinished());
        UNIT_ASSERT(rows);
        UNIT_ASSERT_VALUES_EQUAL(rows->num_rows(), 100);
    }

    // Second TRUNCATE of the source while a copy of its initial contents is still alive.
    Y_UNIT_TEST(TruncateSourceTwiceWithLiveCopy) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 srcPathId = 1;
        const ui64 dstPathId = 2;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, srcPathId, testTable.Schema);

        ui64 txId = 10;
        int writeId = 10;
        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(
                WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        const auto g0Snapshot = NOlap::TSnapshot(planStep, txId);

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(srcPathId, dstPathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(srcPathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto t1 = NOlap::TSnapshot(planStep, txId);

        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(WriteData(
                runtime, sender, writeId++, srcPathId, MakeTestBlob({ 200, 220 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        const auto g1Snapshot = NOlap::TSnapshot(planStep, txId);

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(srcPathId, 2), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto t2 = NOlap::TSnapshot(planStep, txId);

        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, t2);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, t2);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, g0Snapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, g1Snapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 20);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, t1);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
    }

    Y_UNIT_TEST(TruncateSourceAfterDropCopySucceeds) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 srcPathId = 1;
        const ui64 dstPathId = 2;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, srcPathId, testTable.Schema);

        ui64 txId = 10;
        int writeId = 10;
        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(
                WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(srcPathId, dstPathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::DropTableTxBody(dstPathId, 2), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(srcPathId, 3), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });

        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, dstPathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(reader.IsError());
        }
    }

    Y_UNIT_TEST(TruncateSeqNoCheck) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema);

        ui64 txId = 10;
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 5), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        ProposeSchemaTxFail(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 3), ++txId);
        ProposeSchemaTxFail(runtime, sender, TTestSchema::DropTableTxBody(pathId, 4), ++txId);
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 6), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
    }

    Y_UNIT_TEST_DUO(CommitPlannedBeforeTruncate, Reboot) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto controller = RegisterTruncateTestController();
        TActorId sender = runtime.AllocateEdgeActor();
        TestTableDescription table{};
        Y_UNUSED(PrepareTablet(runtime, 1, table.Schema));
        std::vector<ui64> writeIds;
        UNIT_ASSERT(WriteData(runtime, sender, 10, 1, MakeTestBlob({ 0, 100 }, table.Schema), table.Schema, true, &writeIds));
        const auto commitMinStep = ProposeCommit(runtime, sender, 11, writeIds);
        // PREPARED must arrive before the outstanding write is planned.
        const auto truncateMinStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(1, 1), 12);
        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
            ForwardToTablet(
                runtime, TTestTxConfig::TxTablet0, sender, new TEvColumnShard::TEvProposeTransaction(NKikimrTxColumnShard::TX_KIND_SCHEMA, 0,
                                                               sender, 12, TTestSchema::TruncateTableTxBody(1, 1), 0, 0));
            auto result = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(sender);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetTxId(), 12);
            UNIT_ASSERT(result->Get()->Record.GetStatus() == NKikimrTxColumnShard::PREPARED);
        }
        const TPlanStep commitStep{ Max(commitMinStep.Val(), runtime.GetTimeProvider()->Now().MilliSeconds()) + 1 };
        PlanCommit(runtime, sender, commitStep, 11);
        const TPlanStep truncateStep{ Max(commitStep.Val() + 1, truncateMinStep.Val()) };
        PlanSchemaTx(runtime, sender, { truncateStep, 12 });
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, 1, { commitStep, 11 });
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(table.Schema));
            const auto rows = reader.ReadAll();
            UNIT_ASSERT(rows);
            UNIT_ASSERT_VALUES_EQUAL(rows->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, 1, { truncateStep, 12 });
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(table.Schema));
            UNIT_ASSERT(!reader.ReadAll());
            UNIT_ASSERT(!reader.IsError());
        }
        // Complete must reopen the table for new writes.
        std::vector<ui64> nextIds;
        UNIT_ASSERT(WriteData(runtime, sender, 13, 1, MakeTestBlob({ 200, 250 }, table.Schema), table.Schema, true, &nextIds));
        const auto nextStep = ProposeCommit(runtime, sender, 14, nextIds);
        PlanCommit(runtime, sender, nextStep, 14);
        TShardReader reader(runtime, TTestTxConfig::TxTablet0, 1, { nextStep, 14 });
        reader.SetReplyColumnIds(TTestSchema::ExtractIds(table.Schema));
        const auto rows = reader.ReadAll();
        UNIT_ASSERT(rows);
        UNIT_ASSERT_VALUES_EQUAL(rows->num_rows(), 50);
        UNIT_ASSERT(!reader.IsError());
    }

    Y_UNIT_TEST_DUO(TruncateDoesNotWaitForUnrelatedPreparedCommit, Reboot) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto controller = RegisterTruncateTestController();
        TActorId sender = runtime.AllocateEdgeActor();
        TestTableDescription table{};
        Y_UNUSED(PrepareTablet(runtime, 1, table.Schema, 1, table.Standalone));
        NKikimrTxColumnShard::TSchemaTxBody createTable;
        UNIT_ASSERT(createTable.ParseFromString(TTestSchema::CreateTableTxBody(2, table.Standalone, table.Schema, table.Pk)));
        createTable.MutableSeqNo()->SetRound(2);
        const auto createStep = ProposeSchemaTx(runtime, sender, createTable.SerializeAsString(), 10);
        PlanSchemaTx(runtime, sender, { createStep, 10 });
        std::vector<ui64> writeIds;
        UNIT_ASSERT(WriteData(runtime, sender, 11, 2, MakeTestBlob({ 0, 100 }, table.Schema), table.Schema, true, &writeIds));
        const auto commitMinStep = ProposeCommit(runtime, sender, 12, writeIds);
        const auto truncateStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(1, 1), 13);
        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }
        PlanSchemaTx(runtime, sender, { truncateStep, 13 });
        const auto* shard = WaitForShard(*controller.operator->(), runtime);
        UNIT_ASSERT(shard->GetProgressTxController().GetTxOperator(12, ETxOperatorStatus::Any, true));
        const TPlanStep commitStep{ Max(Max(commitMinStep.Val(), truncateStep.Val()), runtime.GetTimeProvider()->Now().MilliSeconds()) + 1 };
        PlanCommit(runtime, sender, commitStep, 12);
        TShardReader reader(runtime, TTestTxConfig::TxTablet0, 2, { commitStep, 12 });
        reader.SetReplyColumnIds(TTestSchema::ExtractIds(table.Schema));
        const auto rows = reader.ReadAll();
        UNIT_ASSERT(rows);
        UNIT_ASSERT_VALUES_EQUAL(rows->num_rows(), 100);
        UNIT_ASSERT(!reader.IsError());
    }

    Y_UNIT_TEST_DUO(TruncateAbortsPreparedPrimaryReadWrite, Reboot) {
        CheckPreparedCommitAfterTruncate(EPreparedCommitMode::Primary, Reboot);
    }

    Y_UNIT_TEST_DUO(TruncateAbortsPreparedSecondaryReadWrite, Reboot) {
        CheckPreparedCommitAfterTruncate(EPreparedCommitMode::Secondary, Reboot);
    }

    Y_UNIT_TEST_DUO(PreparedWriteCommitAfterTruncate, Reboot) {
        CheckPreparedCommitAfterTruncate(EPreparedCommitMode::Simple, Reboot);
    }

    Y_UNIT_TEST_DUO(TruncateWaitsForPlannedCommitDecision, Reboot) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto controller = RegisterTruncateTestController();
        TActorId sender = runtime.AllocateEdgeActor();
        TestTableDescription table{};
        Y_UNUSED(PrepareTablet(runtime, 1, table.Schema));
        constexpr ui64 lockId = 103;
        constexpr ui64 commitTxId = 20;
        constexpr ui64 truncateTxId = 30;
        constexpr ui64 peer = TTestTxConfig::TxTablet0 + 100;
        std::vector<ui64> ids;
        UNIT_ASSERT(WriteData(runtime, sender, 10, 1, MakeTestBlob({ 0, 100 }, table.Schema), table.Schema, true, &ids,
            NEvWrite::EModificationType::Upsert, lockId));
        auto* lock = WaitForShard(*controller.operator->(), runtime)->GetOperationsManager().GetLockOptional(lockId);
        UNIT_ASSERT(lock);
        auto request = std::make_unique<NEvents::TDataEvents::TEvWrite>(commitTxId, NKikimrDataEvents::TEvWrite::MODE_PREPARE);
        auto* locks = request->Record.MutableLocks();
        locks->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
        auto* protoLock = locks->AddLocks();
        protoLock->SetLockId(lockId);
        protoLock->SetGeneration(lock->GetGeneration());
        protoLock->SetCounter(lock->GetInternalGenerationCounter());
        locks->SetArbiterColumnShard(TTestTxConfig::TxTablet0);
        locks->AddSendingShards(TTestTxConfig::TxTablet0);
        locks->AddReceivingShards(TTestTxConfig::TxTablet0);
        locks->AddReceivingShards(peer);
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, request.release());
        auto prepared = runtime.GrabEdgeEvent<NEvents::TDataEvents::TEvWriteResult>(sender);
        UNIT_ASSERT_VALUES_EQUAL(prepared->Get()->Record.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED);
        bool committedDecision = false;
        runtime.SetEventFilter([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvPipeCache::TEvForward::EventType) {
                auto* forward = ev->Get<TEvPipeCache::TEvForward>();
                if (forward->TabletId == peer) {
                    if (auto* readSet = dynamic_cast<TEvTxProcessing::TEvReadSet*>(forward->Ev.Get())) {
                        NKikimrTx::TReadSetData data;
                        UNIT_ASSERT(data.ParseFromString(readSet->Record.GetReadSet()));
                        UNIT_ASSERT(data.GetDecision() == NKikimrTx::TReadSetData::DECISION_COMMIT);
                        committedDecision = true;
                    }
                    return true;
                }
            }
            return false;
        });
        const TPlanStep commitStep{ prepared->Get()->Record.GetMinStep() };
        PlanWriteTx(runtime, sender, { commitStep, commitTxId }, false);
        for (ui32 i = 0; i < 100 && !committedDecision; ++i) {
            runtime.SimulateSleep(TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT(committedDecision);
        // Delay the peer's ACK: TRUNCATE can prepare, but it must not overtake this COMMIT.
        const auto pathId =
            WaitForShard(*controller.operator->(), runtime)->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(1),
                false);
        UNIT_ASSERT(pathId);
        const auto truncateMinStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(1, 1), truncateTxId);
        if (Reboot) {
            committedDecision = false;
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
            lock = WaitForShard(*controller.operator->(), runtime)->GetOperationsManager().GetLockOptional(lockId);
            UNIT_ASSERT(lock);
            UNIT_ASSERT(lock->IsBroken());
            // Recovery must resend the saved COMMIT, despite the recovered broken lock.
            for (ui32 i = 0; i < 100 && !committedDecision; ++i) {
                runtime.SimulateSleep(TDuration::MilliSeconds(10));
            }
            UNIT_ASSERT(committedDecision);
        }
        const TPlanStep truncateStep{ Max(commitStep.Val() + 1, truncateMinStep.Val()) };
        PlanSchemaTxStepOnly(runtime, sender, { truncateStep, truncateTxId });
        runtime.SimulateSleep(TDuration::MilliSeconds(50));
        const auto* shard = WaitForShard(*controller.operator->(), runtime);
        UNIT_ASSERT(shard->GetProgressTxController().GetTxOperator(commitTxId, ETxOperatorStatus::Any, true));
        UNIT_ASSERT(shard->GetProgressTxController().GetTxOperator(truncateTxId, ETxOperatorStatus::Any, true));
        UNIT_ASSERT(shard->GetTablesManager().GetTable(*pathId).GetTruncateSnapshots().empty());
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender,
            new TEvTxProcessing::TEvReadSetAck(commitStep.Val(), commitTxId, TTestTxConfig::TxTablet0, peer, peer, 0));
        auto result = runtime.GrabEdgeEvent<NEvents::TDataEvents::TEvWriteResult>(sender);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_COMPLETED);
        WaitSchemaTxCompletion(runtime, sender, truncateTxId);
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, 1, { commitStep, commitTxId });
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(table.Schema));
            const auto rows = reader.ReadAll();
            UNIT_ASSERT(rows);
            UNIT_ASSERT_VALUES_EQUAL(rows->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
        TShardReader reader(runtime, TTestTxConfig::TxTablet0, 1, { truncateStep, truncateTxId });
        reader.SetReplyColumnIds(TTestSchema::ExtractIds(table.Schema));
        UNIT_ASSERT(!reader.ReadAll());
        UNIT_ASSERT(!reader.IsError());
    }

    Y_UNIT_TEST(WriteOnlyCommitAfterTruncatePlanSucceeds) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 srcPathId = 1;
        const ui64 copyPathId = 2;
        const ui64 lockId = 3;
        TestTableDescription testTable{};
        Y_UNUSED(PrepareTablet(runtime, srcPathId, testTable.Schema));

        ui64 txId = 10;
        int writeId = 10;
        std::vector<ui64> committedWriteIds;
        UNIT_ASSERT(WriteData(
            runtime, sender, writeId++, srcPathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &committedWriteIds));
        auto planStep = ProposeCommit(runtime, sender, ++txId, committedWriteIds);
        PlanCommit(runtime, sender, planStep, txId);

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(srcPathId, copyPathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto snapshotBeforeTruncate = NOlap::TSnapshot(planStep, txId);

        std::vector<ui64> lockedWriteIds;
        UNIT_ASSERT(WriteData(runtime, sender, writeId++, srcPathId, MakeTestBlob({ 100, 150 }, testTable.Schema), testTable.Schema, true,
            &lockedWriteIds, NEvWrite::EModificationType::Upsert, lockId));

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(srcPathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto truncateSnapshot = NOlap::TSnapshot(planStep, txId);

        // A write-only transaction commits into the same table after truncate.
        const auto commitPlanStep = ProposeCommit(runtime, sender, ++txId, lockedWriteIds, lockId);
        PlanCommit(runtime, sender, commitPlanStep, txId);
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, NOlap::TSnapshot(commitPlanStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rows = reader.ReadAll();
            UNIT_ASSERT(reader.IsCorrectlyFinished());
            UNIT_ASSERT(rows);
            UNIT_ASSERT_VALUES_EQUAL(rows->num_rows(), 50);
        }

        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, srcPathId, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            UNIT_ASSERT(!reader.ReadAll());
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, copyPathId, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
    }

    Y_UNIT_TEST_DUO(TruncatePausesNewCompactions, Reboot) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto controller = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        controller->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
        controller->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Cleanup);
        TActorId sender = runtime.AllocateEdgeActor();
        TestTableDescription table{};
        Y_UNUSED(PrepareTablet(runtime, 1, table.Schema));
        ui64 txId = 10;
        int writeId = 10;
        auto write = [&] {
            std::vector<ui64> ids;
            UNIT_ASSERT(WriteData(runtime, sender, writeId++, 1, MakeTestBlob({ 0, 100 }, table.Schema), table.Schema, true, &ids));
            const auto step = ProposeCommit(runtime, sender, ++txId, ids);
            PlanCommit(runtime, sender, step, txId);
        };
        for (ui32 i = 0; i < 4; ++i) {
            write();
        }
        const auto internalPathId =
            *WaitForShard(*controller.operator->(), runtime)->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(1),
                false);
        auto priority = [&] {
            const auto& granule = WaitForShard(*controller.operator->(), runtime)
                                      ->GetTablesManager()
                                      .GetPrimaryIndexAsVerified<NOlap::TColumnEngineForLogs>()
                                      .GetGranuleVerified(internalPathId);
            granule.ActualizeOptimizer(runtime.GetCurrentTime(), TDuration::Zero());
            return granule.GetCompactionPriority();
        };
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT(!priority().IsZero());
        const auto truncateTxId = ++txId;
        const auto step = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(1, 1), truncateTxId);
        UNIT_ASSERT(priority().IsZero());
        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
            UNIT_ASSERT(priority().IsZero());
        }
        controller->EnableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
        const auto startedBefore = controller->GetCompactionStartedCounter().Val();
        for (ui32 i = 0; i < 3; ++i) {
            Wakeup(runtime, sender, TTestTxConfig::TxTablet0);
            runtime.SimulateSleep(TDuration::Seconds(1));
        }
        UNIT_ASSERT_VALUES_EQUAL(controller->GetCompactionStartedCounter().Val(), startedBefore);
        controller->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
        PlanSchemaTx(runtime, sender, { step, truncateTxId });
        for (ui32 i = 0; i < 4; ++i) {
            write();
        }
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT(!priority().IsZero());
    }

    Y_UNIT_TEST_DUO(CompactionCancelledAfterTruncate, Reboot) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto controller = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        controller->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Cleanup);
        controller->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
        TActorId sender = runtime.AllocateEdgeActor();
        TestTableDescription table{};
        Y_UNUSED(PrepareTablet(runtime, 1, table.Schema));
        ui64 txId = 10;
        int writeId = 10;
        auto write = [&](ui64 from, ui64 to) {
            std::vector<ui64> ids;
            UNIT_ASSERT(WriteData(runtime, sender, writeId++, 1, MakeTestBlob({ from, to }, table.Schema), table.Schema, true, &ids));
            auto step = ProposeCommit(runtime, sender, ++txId, ids);
            PlanCommit(runtime, sender, step, txId);
            return NOlap::TSnapshot(step, txId);
        };
        for (ui32 i = 0; i < 4; ++i) {
            write(0, 100);
        }
        auto step = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(1, 2, 1), ++txId);
        PlanSchemaTx(runtime, sender, { step, txId });
        const auto beforeTruncate = NOlap::TSnapshot(step, txId);

        std::vector<TAutoPtr<IEventHandle>> delayedCompactions;
        std::vector<std::shared_ptr<NOlap::TColumnEngineChanges>> changesToCancel;
        runtime.SetEventFilter([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (auto* msg = TryGetPrivateEvent<NColumnShard::TEvPrivate::TEvWriteIndex>(ev)) {
                if (msg->GetPutStatus() == NKikimrProto::OK &&
                    std::dynamic_pointer_cast<NOlap::TCompactColumnEngineChanges>(msg->IndexChanges)) {
                    changesToCancel.emplace_back(msg->IndexChanges);
                    delayedCompactions.emplace_back(ev.Release());
                    return true;
                }
            }
            return false;
        });
        controller->EnableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
        for (ui32 i = 0; delayedCompactions.empty() && i < 30; ++i) {
            Wakeup(runtime, sender, TTestTxConfig::TxTablet0);
            runtime.SimulateSleep(TDuration::Seconds(1));
        }
        UNIT_ASSERT_C(!delayedCompactions.empty(), "compaction must be ready to publish before truncate");
        controller->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
        step = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(1, 2), ++txId);
        PlanSchemaTx(runtime, sender, { step, txId });
        const auto afterWrite = write(1000, 1020);

        const auto finishedBefore = controller->GetCompactionFinishedCounter().Val();
        const intptr_t delayedCount = delayedCompactions.size();
        runtime.SetEventFilter([](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>&) {
            return false;
        });
        for (auto& ev : delayedCompactions) {
            runtime.Send(ev.Release());
        }
        for (ui32 i = 0; controller->GetCompactionFinishedCounter().Val() < finishedBefore + delayedCount && i < 30; ++i) {
            runtime.SimulateSleep(TDuration::Seconds(1));
        }
        UNIT_ASSERT_VALUES_EQUAL(controller->GetCompactionFinishedCounter().Val(), finishedBefore + delayedCount);
        for (const auto& changes : changesToCancel) {
            UNIT_ASSERT_C(changes->IsAborted(), "compaction must be cancelled by truncate");
        }
        auto checkRows = [&](ui64 pathId, const NOlap::TSnapshot& snapshot, ui64 count) {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, snapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(table.Schema));
            auto rows = reader.ReadAll();
            UNIT_ASSERT(!reader.IsError());
            UNIT_ASSERT_VALUES_EQUAL(rows ? rows->num_rows() : 0, count);
        };
        checkRows(1, beforeTruncate, 100);
        checkRows(1, afterWrite, 20);
        checkRows(2, afterWrite, 100);

        step = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(1, 3, 3), ++txId);
        PlanSchemaTx(runtime, sender, { step, txId });
        step = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(1, 4), ++txId);
        PlanSchemaTx(runtime, sender, { step, txId });
        const auto afterSecondTruncate = NOlap::TSnapshot(step, txId);
        if (Reboot) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }
        checkRows(1, beforeTruncate, 100);
        checkRows(1, afterWrite, 20);
        checkRows(1, afterSecondTruncate, 0);
        checkRows(2, afterSecondTruncate, 100);
        checkRows(3, afterSecondTruncate, 20);
    }

    Y_UNIT_TEST(TruncateCancelsActualizationAndCleanupWaitsForItsLock) {
        TTestBasicRuntime runtime;
        SetupTruncateTestRuntime(runtime);
        auto controller = RegisterTruncateTestController();
        controller->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
        controller->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::TTL);
        controller->SetOverrideTasksActualizationLag(TDuration::Zero());
        controller->SetOverrideCompactionActualizationLag(TDuration::Zero());
        controller->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        TActorId sender = runtime.AllocateEdgeActor();
        TestTableDescription table{};
        Y_UNUSED(PrepareTablet(runtime, 1, table.Schema));
        auto specials = TTestSchema::TTableSpecials().SetTtl(TDuration::Seconds(3600));
        specials.SetTtlColumn(TTestSchema::DefaultTtlColumn);
        ui64 txId = 10;
        auto step = ProposeSchemaTx(runtime, sender, TTestSchema::AlterTableTxBody(1, true, 1, table.Schema, table.Pk, specials), ++txId);
        PlanSchemaTx(runtime, sender, { step, txId });
        auto batch = NArrow::DeserializeBatch(MakeTestBlob({ 0, 10 }, table.Schema), NArrow::MakeArrowSchema(table.Schema));
        batch = UpdateColumn(batch, TTestSchema::DefaultTtlColumn, TAppData::TimeProvider->Now().Seconds() - 7200);
        std::vector<ui64> writeIds;
        UNIT_ASSERT(WriteData(runtime, sender, 100, 1, NArrow::SerializeBatchNoCompression(batch), table.Schema, true, &writeIds));
        step = ProposeCommit(runtime, sender, ++txId, writeIds);
        PlanCommit(runtime, sender, step, txId);

        TAutoPtr<IEventHandle> delayed;
        std::shared_ptr<NOlap::TColumnEngineChanges> changes;
        runtime.SetEventFilter([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (auto* msg = TryGetPrivateEvent<NColumnShard::TEvPrivate::TEvWriteIndex>(ev)) {
                if (msg->GetPutStatus() == NKikimrProto::OK && std::dynamic_pointer_cast<NOlap::TTTLColumnEngineChanges>(msg->IndexChanges)) {
                    UNIT_ASSERT(!delayed);
                    changes = msg->IndexChanges;
                    delayed = ev.Release();
                    return true;
                }
            }
            return false;
        });
        controller->EnableBackground(NKikimr::NYDBTest::ICSController::EBackground::TTL);
        for (ui32 i = 0; !delayed && i < 30; ++i) {
            Wakeup(runtime, sender, TTestTxConfig::TxTablet0);
            runtime.SimulateSleep(TDuration::Seconds(1));
        }
        UNIT_ASSERT_C(delayed, "actualization must reach publication before truncate");
        controller->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::TTL);
        step = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(1, 2), ++txId);
        PlanSchemaTx(runtime, sender, { step, txId });
        const auto truncateSnapshot = NOlap::TSnapshot(step, txId);
        const auto pathId =
            *controller->GetTheOnlyShard()->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(1), false);
        controller->SetOverrideUsedSnapshotLivetime(TDuration::Zero());
        controller->EnableBackground(NKikimr::NYDBTest::ICSController::EBackground::Cleanup);
        ui64 cleanupStep = step.Val() + 10000;
        auto advance = [&] {
            PlanCommit(runtime, sender, TPlanStep{ cleanupStep }, TSet<ui64>{});
            cleanupStep += 10000;
        };
        for (ui32 i = 0; i < 3; ++i) {
            advance();
            Wakeup(runtime, sender, TTestTxConfig::TxTablet0);
            ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new NColumnShard::TEvPrivate::TEvPingSnapshotsUsage());
            runtime.SimulateSleep(TDuration::Seconds(1));
        }
        UNIT_ASSERT(HasPortionsRemovedAt(*controller->GetTheOnlyShard(), pathId, truncateSnapshot));
        UNIT_ASSERT_VALUES_EQUAL(controller->GetTheOnlyShard()->GetTablesManager().GetTable(pathId).GetTruncateSnapshots().size(), 1);
        runtime.SetEventFilter([](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>&) {
            return false;
        });
        runtime.Send(delayed.Release());
        UNIT_ASSERT(WaitForTruncatedPortionsCleanup(*controller.operator->(), runtime, sender, pathId, truncateSnapshot, advance));
        UNIT_ASSERT(changes->IsAborted());
        UNIT_ASSERT(controller->GetTheOnlyShard()->GetTablesManager().GetTable(pathId).GetTruncateSnapshots().empty());
        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        const auto* shard = WaitForShard(*controller.operator->(), runtime);
        UNIT_ASSERT(shard->GetTablesManager().GetTable(pathId).GetTruncateSnapshots().empty());
        UNIT_ASSERT(
            shard->GetTablesManager().GetPrimaryIndexAsVerified<NOlap::TColumnEngineForLogs>().GetGranuleVerified(pathId).GetPortions().empty());
    }

    Y_UNIT_TEST(TruncateBreaksSourceReadLocks) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto controller = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        controller->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Cleanup);
        TActorId sender = runtime.AllocateEdgeActor();
        TestTableDescription table{};
        Y_UNUSED(PrepareTablet(runtime, 1, table.Schema));
        std::vector<ui64> ids;
        UNIT_ASSERT(WriteData(runtime, sender, 10, 1, MakeTestBlob({ 0, 100 }, table.Schema), table.Schema, true, &ids));
        auto step = ProposeCommit(runtime, sender, 11, ids);
        PlanCommit(runtime, sender, step, 11);
        step = ProposeSchemaTx(runtime, sender, TTestSchema::CopyTableTxBody(1, 2, 1), 12);
        PlanSchemaTx(runtime, sender, { step, 12 });
        const auto readSnapshot = NOlap::TSnapshot(step, 12);

        ui64 scanLockId = 0;
        runtime.SetEventFilter([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::TEvKqpScan::EventType && scanLockId) {
                auto& record = ev->Get<TEvDataShard::TEvKqpScan>()->Record;
                record.SetLockTxId(scanLockId);
                record.SetLockMode(NKikimrDataEvents::OPTIMISTIC);
            }
            return false;
        });
        auto readWithLock = [&](ui64 pathId, ui64 lockId) {
            scanLockId = lockId;
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, readSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(table.Schema));
            UNIT_ASSERT(reader.ReadAll());
            UNIT_ASSERT(!reader.IsError());
            scanLockId = 0;
        };
        auto isBroken = [&](ui64 lockId) {
            auto* lock = WaitForShard(*controller.operator->(), runtime)->GetOperationsManager().GetLockOptional(lockId);
            UNIT_ASSERT(lock);
            return lock->IsBroken();
        };
        readWithLock(1, 101);
        readWithLock(2, 102);
        UNIT_ASSERT(!isBroken(101));
        UNIT_ASSERT(!isBroken(102));
        step = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(1, 2), 13);
        PlanSchemaTx(runtime, sender, { step, 13 });
        UNIT_ASSERT(isBroken(101));
        UNIT_ASSERT(!isBroken(102));

        // A late read of the source's old snapshot must also conflict with truncate.
        readWithLock(1, 103);
        readWithLock(2, 104);
        UNIT_ASSERT(isBroken(103));
        UNIT_ASSERT(!isBroken(104));
    }

    Y_UNIT_TEST_DUO(PreparedPrimaryCommitRetryDuringTruncate, Reboot) {
        CheckPreparedSyncCommitRetryDuringTruncate(false, Reboot);
    }

    Y_UNIT_TEST_DUO(PreparedSecondaryCommitRetryDuringTruncate, Reboot) {
        CheckPreparedSyncCommitRetryDuringTruncate(true, Reboot);
    }

    Y_UNIT_TEST_DUO(PreparedSimpleCommitRetryAfterReboot, TruncatePending) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto controller = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();
        TestTableDescription table{};
        Y_UNUSED(PrepareTablet(runtime, 1, table.Schema));
        constexpr ui64 lockId = 3;
        constexpr ui64 commitTxId = 11;
        constexpr ui64 truncateTxId = 12;
        std::vector<ui64> writeIds;
        UNIT_ASSERT(WriteData(runtime, sender, 10, 1, MakeTestBlob({ 0, 100 }, table.Schema), table.Schema, true, &writeIds,
            NEvWrite::EModificationType::Upsert, lockId));
        const auto step = ProposeCommit(runtime, sender, commitTxId, writeIds, lockId);
        if (TruncatePending) {
            ForwardToTablet(
                runtime, TTestTxConfig::TxTablet0, sender, new TEvColumnShard::TEvProposeTransaction(NKikimrTxColumnShard::TX_KIND_SCHEMA, 0,
                                                               sender, truncateTxId, TTestSchema::TruncateTableTxBody(1, 1), 0, 0));
            runtime.SimulateSleep(TDuration::MilliSeconds(50));
        }
        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        const auto* shard = WaitForShard(*controller.operator->(), runtime);
        UNIT_ASSERT(shard->GetOperationsManager().GetLockOptional(lockId)->IsBroken());
        const auto propose = [&](ui64 tx, ui64 lock, NKikimrDataEvents::TEvWriteResult::EStatus status) {
            auto request = std::make_unique<NEvents::TDataEvents::TEvWrite>(tx, NKikimrDataEvents::TEvWrite::MODE_PREPARE);
            request->Record.MutableLocks()->AddLocks()->SetLockId(lock);
            request->Record.MutableLocks()->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
            ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, request.release());
            auto result = runtime.GrabEdgeEvent<NEvents::TDataEvents::TEvWriteResult>(sender);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetTxId(), tx);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetStatus(), status);
        };
        // A mismatched retry must not replace the already prepared operation.
        propose(commitTxId, lockId + 1, NKikimrDataEvents::TEvWriteResult::STATUS_BAD_REQUEST);
        if (!TruncatePending) {
            propose(commitTxId + 2, lockId, NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN);
        }
        propose(commitTxId, lockId, NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED);
        propose(commitTxId, lockId, NKikimrDataEvents::TEvWriteResult::STATUS_PREPARED);
        const TPlanStep commitStep{ Max(step.Val(), runtime.GetTimeProvider()->Now().MilliSeconds()) + 1 };
        PlanCommit(runtime, sender, commitStep, commitTxId);
        TShardReader reader(runtime, TTestTxConfig::TxTablet0, 1, NOlap::TSnapshot(commitStep, commitTxId));
        reader.SetReplyColumnIds(TTestSchema::ExtractIds(table.Schema));
        const auto rows = reader.ReadAll();
        UNIT_ASSERT(!reader.IsError());
        UNIT_ASSERT(rows);
        UNIT_ASSERT_VALUES_EQUAL(rows->num_rows(), 100);
        if (TruncatePending) {
            auto result = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(sender);
            UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetTxId(), truncateTxId);
            UNIT_ASSERT(result->Get()->Record.GetStatus() == NKikimrTxColumnShard::PREPARED);
            PlanSchemaTx(runtime, sender, { Max(commitStep.Val() + 1, result->Get()->Record.GetMinStep()), truncateTxId });
        }
    }

    Y_UNIT_TEST(CommitIsRejectedAfterAbortWasQueued) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        const ui64 lockId = 3;
        const ui64 commitTxId = 13;
        TestTableDescription testTable{};
        Y_UNUSED(PrepareTablet(runtime, pathId, testTable.Schema));

        int writeId = 10;
        std::vector<ui64> lockedWriteIds;
        UNIT_ASSERT(WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true,
            &lockedWriteIds, NEvWrite::EModificationType::Upsert, lockId));

        const auto createTableTxBody = [&](const ui64 auxPathId, const ui32 round) {
            NKikimrTxColumnShard::TSchemaTxBody auxTx;
            UNIT_ASSERT(auxTx.ParseFromString(TTestSchema::CreateTableTxBody(auxPathId, testTable.Standalone, testTable.Schema, testTable.Pk)));
            auxTx.MutableSeqNo()->SetRound(round);
            TString body;
            Y_PROTOBUF_SUPPRESS_NODISCARD auxTx.SerializeToString(&body);
            return body;
        };
        // The response to a schema proposal is sent after the earlier write finishes in the executor.
        const auto barrierPlanStep = ProposeSchemaTx(runtime, sender, createTableTxBody(98, 2), 11);
        PlanSchemaTx(runtime, sender, { barrierPlanStep, 11 });

        auto* shard = WaitForShard(*csControllerGuard.operator->(), runtime);
        auto* lockInfo = shard->GetOperationsManager().GetLockOptional(lockId);
        UNIT_ASSERT(lockInfo);
        // Reproduce the state after rollback has queued TAbortWriteTransaction, but before it
        // executes. The commit proposal must not assign a TxId to this lock.
        lockInfo->SetNeedsAborting();
        UNIT_ASSERT(lockInfo->ReadyForAborting());
        lockInfo->SetAborting();

        auto commit = std::make_unique<NEvents::TDataEvents::TEvWrite>(commitTxId, NKikimrDataEvents::TEvWrite::MODE_PREPARE);
        commit->Record.MutableLocks()->AddLocks()->SetLockId(lockId);
        commit->Record.MutableLocks()->SetOp(NKikimrDataEvents::TKqpLocks::Commit);
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, commit.release());
        auto commitResult = runtime.GrabEdgeEvent<NEvents::TDataEvents::TEvWriteResult>(sender);
        UNIT_ASSERT(commitResult);
        UNIT_ASSERT_VALUES_EQUAL(commitResult->Get()->Record.GetTxId(), commitTxId);
        UNIT_ASSERT_VALUES_EQUAL(commitResult->Get()->Record.GetStatus(), NKikimrDataEvents::TEvWriteResult::STATUS_LOCKS_BROKEN);
        UNIT_ASSERT(!lockInfo->IsTxIdAssigned());

        std::vector<ui64> nextWriteIds;
        UNIT_ASSERT(
            WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 100, 150 }, testTable.Schema), testTable.Schema, true, &nextWriteIds));
    }

    // Path fence on TRUNCATE propose: uncommitted writes, new writes, and CommitWriteLock for a
    // lock that wrote before the fence must fail; after plan the table is empty.
    Y_UNIT_TEST(TruncateFencesWritesOnPropose) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        Y_UNUSED(PrepareTablet(runtime, pathId, testTable.Schema));

        ui64 txId = 10;
        int writeId = 10;

        std::vector<ui64> uncommittedWriteIds;
        UNIT_ASSERT(WriteData(
            runtime, sender, writeId++, pathId, MakeTestBlob({ 0, 50 }, testTable.Schema), testTable.Schema, true, &uncommittedWriteIds));

        std::vector<ui64> writeIdsBefore;
        const auto lockBefore = 1;
        UNIT_ASSERT(WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 50, 100 }, testTable.Schema), testTable.Schema, true,
            &writeIdsBefore, NEvWrite::EModificationType::Upsert, lockBefore));

        const auto truncateTxId = ++txId;
        {
            auto event = std::make_unique<TEvColumnShard::TEvProposeTransaction>(
                NKikimrTxColumnShard::TX_KIND_SCHEMA, 0, sender, truncateTxId, TTestSchema::TruncateTableTxBody(pathId, 1), 0, 0);
            ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, event.release());
        }
        runtime.SimulateSleep(TDuration::MilliSeconds(50));

        {
            std::vector<ui64> writeIdsAfter;
            UNIT_ASSERT(!WriteData(
                runtime, sender, writeId++, pathId, MakeTestBlob({ 100, 150 }, testTable.Schema), testTable.Schema, true, &writeIdsAfter));
        }
        ProposeCommitFail(runtime, sender, TTestTxConfig::TxTablet0, ++txId, writeIdsBefore, lockBefore);

        auto ev = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(sender);
        UNIT_ASSERT(ev);
        const auto& res = ev->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(res.GetTxId(), truncateTxId);
        UNIT_ASSERT_EQUAL(res.GetStatus(), NKikimrTxColumnShard::PREPARED);
        const auto planStep = TPlanStep{ res.GetMinStep() };
        PlanSchemaTx(runtime, sender, { planStep, truncateTxId });
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, truncateTxId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
    }

    Y_UNIT_TEST(TruncateInStoreTableFails) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        Y_UNUSED(PrepareTablet(runtime, pathId, testTable.Schema, 1, false));
        ui64 txId = 10;
        ProposeSchemaTxFail(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), ++txId);
    }

    Y_UNIT_TEST_DUO(TruncateSurvivesRestart, GenerateInternalPathId) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        runtime.GetAppData().ColumnShardConfig.SetGenerateInternalPathId(GenerateInternalPathId);
        auto csDefaultControllerGuard =
            NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TTruncatePathIdController<GenerateInternalPathId>>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema);

        const auto originalInternalPathId = *WaitForShard(*csDefaultControllerGuard.operator->(), runtime)
                                                 ->GetTablesManager()
                                                 .ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(pathId), false);
        if (!GenerateInternalPathId) {
            UNIT_ASSERT_VALUES_EQUAL(originalInternalPathId.GetRawValue(), pathId);
        }

        ui64 txId = 10;
        int writeId = 1;
        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 0, 50 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }

        const auto beforeTruncate = NOlap::TSnapshot(planStep, txId);
        const auto readWhileFenced = [&] {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, beforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            const auto batch = reader.ReadAll();
            UNIT_ASSERT(!reader.IsError());
            UNIT_ASSERT(batch);
            UNIT_ASSERT_VALUES_EQUAL(batch->num_rows(), 50);
        };
        const auto truncateTxId = ++txId;
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), truncateTxId);
        readWhileFenced();
        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        readWhileFenced();
        PlanSchemaTx(runtime, sender, { planStep, truncateTxId });
        UNIT_ASSERT_VALUES_EQUAL(*WaitForShard(*csDefaultControllerGuard.operator->(), runtime)
                                      ->GetTablesManager()
                                      .ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(pathId), false), originalInternalPathId);
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, truncateTxId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
    }

    Y_UNIT_TEST(TruncateRestartAfterPlan) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);
        auto csDefaultControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDefaultTestsController>();
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema);

        ui64 txId = 10;
        int writeId = 10;
        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        const auto snapshotBeforeTruncate = NOlap::TSnapshot(planStep, txId);

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto truncateSnapshot = NOlap::TSnapshot(planStep, txId);
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }

        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);

        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }
        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 200, 250 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, NOlap::TSnapshot(planStep, txId));
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 50);
            UNIT_ASSERT(!reader.IsError());
        }
    }

    // Cleanup removes truncated portions without deleting the live table. Time-travel
    // works until retention expires.
    Y_UNIT_TEST_DUO(CleanupWithPortionsRetriesAfterFailure, RebootBeforeRetry) {
        TTestBasicRuntime runtime;
        SetupTruncateTestRuntime(runtime);
        auto controller = RegisterTruncateTestController();
        controller->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
        TActorId sender = runtime.AllocateEdgeActor();
        TestTableDescription table{};
        Y_UNUSED(PrepareTablet(runtime, 1, table.Schema));
        ui64 txId = 10;
        for (int writeId = 10; writeId < 13; ++writeId) {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(WriteData(runtime, sender, writeId, 1, MakeTestBlob({ 0, 100 }, table.Schema), table.Schema, true, &writeIds));
            const auto step = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, step, txId);
        }
        const auto step = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(1, 1), ++txId);
        PlanSchemaTx(runtime, sender, { step, txId });
        const NOlap::TSnapshot truncate(step, txId);
        const auto pathId =
            *WaitForShard(*controller.operator->(), runtime)->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(1),
                false);
        std::shared_ptr<NOlap::TCleanupPortionsColumnEngineChanges> failedCleanup;
        std::set<ui64> failedPortions;
        runtime.SetEventFilter([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (!failedCleanup) {
                if (auto* msg = TryGetPrivateEvent<NColumnShard::TEvPrivate::TEvWriteIndex>(ev)) {
                    if (auto cleanup = std::dynamic_pointer_cast<NOlap::TCleanupPortionsColumnEngineChanges>(msg->IndexChanges)) {
                        UNIT_ASSERT(!cleanup->GetPortionsToDrop().empty());
                        UNIT_ASSERT(cleanup->HasTruncatesToRemove());
                        for (const auto& portion : cleanup->GetPortionsToDrop()) {
                            failedPortions.emplace(portion->GetPortionId());
                        }
                        failedCleanup = cleanup;
                        controller->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Cleanup);
                        msg->SetPutStatus(NKikimrProto::ERROR);
                    }
                }
            }
            return false;
        });
        controller->SetOverrideUsedSnapshotLivetime(TDuration::Zero());
        controller->EnableBackground(NKikimr::NYDBTest::ICSController::EBackground::Cleanup);
        ui64 planStep = step.Val() + TruncateTestMaxReadStaleness.MilliSeconds() + 10000;
        auto advance = [&] {
            PlanCommit(runtime, sender, TPlanStep{ planStep }, TSet<ui64>{});
            planStep += 10000;
        };
        for (ui32 i = 0; (!failedCleanup || !failedCleanup->IsAborted()) && i < 30; ++i) {
            advance();
            Wakeup(runtime, sender, TTestTxConfig::TxTablet0);
            ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new NColumnShard::TEvPrivate::TEvPingSnapshotsUsage());
            runtime.SimulateSleep(TDuration::Seconds(1));
        }
        UNIT_ASSERT(failedCleanup && failedCleanup->IsAborted());
        UNIT_ASSERT(!failedPortions.empty());
        if (RebootBeforeRetry) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }
        const auto* shard = WaitForShard(*controller.operator->(), runtime);
        const auto& history = shard->GetTablesManager().GetTable(pathId).GetTruncateSnapshots();
        UNIT_ASSERT_VALUES_EQUAL(history.size(), 1);
        UNIT_ASSERT(history.at(truncate) == NOlap::ETruncateState::MarkedPortions);
        const auto& granule = shard->GetTablesManager().GetPrimaryIndexAsVerified<NOlap::TColumnEngineForLogs>().GetGranuleVerified(pathId);
        for (const auto id : failedPortions) {
            auto portion = granule.GetPortionOptional(id);
            UNIT_ASSERT(portion && portion->IsRemovedFor(truncate));
        }
        bool retried = false;
        runtime.SetEventFilter([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (auto* msg = TryGetPrivateEvent<NColumnShard::TEvPrivate::TEvWriteIndex>(ev)) {
                if (auto cleanup = std::dynamic_pointer_cast<NOlap::TCleanupPortionsColumnEngineChanges>(msg->IndexChanges)) {
                    std::set<ui64> portions;
                    for (const auto& portion : cleanup->GetPortionsToDrop()) {
                        portions.emplace(portion->GetPortionId());
                    }
                    UNIT_ASSERT(portions == failedPortions);
                    UNIT_ASSERT(cleanup->HasTruncatesToRemove());
                    retried = true;
                }
            }
            return false;
        });
        controller->EnableBackground(NKikimr::NYDBTest::ICSController::EBackground::Cleanup);
        UNIT_ASSERT(WaitForTruncatedPortionsCleanup(*controller.operator->(), runtime, sender, pathId, truncate, advance));
        UNIT_ASSERT(retried);
        UNIT_ASSERT(controller->GetTheOnlyShard()->GetTablesManager().GetTable(pathId).GetTruncateSnapshots().empty());
        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        shard = WaitForShard(*controller.operator->(), runtime);
        UNIT_ASSERT(shard->GetTablesManager().GetTable(pathId).GetTruncateSnapshots().empty());
        UNIT_ASSERT(
            shard->GetTablesManager().GetPrimaryIndexAsVerified<NOlap::TColumnEngineForLogs>().GetGranuleVerified(pathId).GetPortions().empty());
    }

    Y_UNIT_TEST_DUO(EmptyTableTruncateHistoryIsCleaned, FailFirstCleanup) {
        TTestBasicRuntime runtime;
        SetupTruncateTestRuntime(runtime);
        auto controller = RegisterTruncateTestController();
        TActorId sender = runtime.AllocateEdgeActor();
        TestTableDescription table{};
        Y_UNUSED(PrepareTablet(runtime, 1, table.Schema));
        ui64 txId = 10;
        auto step = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(1, 1), ++txId);
        PlanSchemaTx(runtime, sender, { step, txId });
        step = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(1, 2), ++txId);
        PlanSchemaTx(runtime, sender, { step, txId });
        auto pathId =
            *WaitForShard(*controller.operator->(), runtime)->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(1),
                false);
        UNIT_ASSERT_VALUES_EQUAL(controller->GetTheOnlyShard()->GetTablesManager().GetTable(pathId).GetTruncateSnapshots().size(), 2);
        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        UNIT_ASSERT_VALUES_EQUAL(
            WaitForShard(*controller.operator->(), runtime)->GetTablesManager().GetTable(pathId).GetTruncateSnapshots().size(), 2);
        std::shared_ptr<NOlap::TCleanupPortionsColumnEngineChanges> failedCleanup;
        runtime.SetEventFilter([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (FailFirstCleanup && !failedCleanup) {
                if (auto* msg = TryGetPrivateEvent<NColumnShard::TEvPrivate::TEvWriteIndex>(ev)) {
                    if (auto cleanup = std::dynamic_pointer_cast<NOlap::TCleanupPortionsColumnEngineChanges>(msg->IndexChanges)) {
                        UNIT_ASSERT(cleanup->GetPortionsToAccess().empty());
                        UNIT_ASSERT(cleanup->HasTruncatesToRemove());
                        failedCleanup = cleanup;
                        controller->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Cleanup);
                        msg->SetPutStatus(NKikimrProto::ERROR);
                    }
                }
            }
            return false;
        });
        controller->SetOverrideUsedSnapshotLivetime(TDuration::Zero());
        controller->EnableBackground(NKikimr::NYDBTest::ICSController::EBackground::Cleanup);
        const auto deadline = TInstant::Now() + TDuration::Seconds(30);
        ui64 planStep = step.Val() + TruncateTestMaxReadStaleness.MilliSeconds() + 10000;
        while (
            !controller->GetTheOnlyShard()->GetTablesManager().GetTable(pathId).GetTruncateSnapshots().empty() && TInstant::Now() < deadline) {
            PlanCommit(runtime, sender, TPlanStep{ planStep }, TSet<ui64>{});
            planStep += 10000;
            Wakeup(runtime, sender, TTestTxConfig::TxTablet0);
            ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new NColumnShard::TEvPrivate::TEvPingSnapshotsUsage());
            runtime.SimulateSleep(TDuration::Seconds(1));
            if (failedCleanup && failedCleanup->IsAborted()) {
                UNIT_ASSERT_VALUES_EQUAL(controller->GetTheOnlyShard()->GetTablesManager().GetTable(pathId).GetTruncateSnapshots().size(), 2);
                runtime.SetEventFilter([](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>&) {
                    return false;
                });
                controller->EnableBackground(NKikimr::NYDBTest::ICSController::EBackground::Cleanup);
                break;
            }
        }
        if (FailFirstCleanup) {
            UNIT_ASSERT(failedCleanup && failedCleanup->IsAborted());
            while (!controller->GetTheOnlyShard()->GetTablesManager().GetTable(pathId).GetTruncateSnapshots().empty() &&
                   TInstant::Now() < deadline) {
                Wakeup(runtime, sender, TTestTxConfig::TxTablet0);
                runtime.SimulateSleep(TDuration::Seconds(1));
            }
        }
        UNIT_ASSERT(controller->GetTheOnlyShard()->GetTablesManager().GetTable(pathId).GetTruncateSnapshots().empty());
        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        const auto* shard = WaitForShard(*controller.operator->(), runtime);
        UNIT_ASSERT(shard->GetTablesManager().GetTable(pathId).GetTruncateSnapshots().empty());
        UNIT_ASSERT(
            shard->GetTablesManager().GetPrimaryIndexAsVerified<NOlap::TColumnEngineForLogs>().GetGranuleVerified(pathId).GetPortions().empty());
    }

    Y_UNIT_TEST(EmptyTableTruncateThenDropCleansHistory) {
        TTestBasicRuntime runtime;
        SetupTruncateTestRuntime(runtime);
        auto controller = RegisterTruncateTestController();
        TActorId sender = runtime.AllocateEdgeActor();
        TestTableDescription table{};
        Y_UNUSED(PrepareTablet(runtime, 1, table.Schema));
        ui64 txId = 10;
        auto step = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(1, 1), ++txId);
        PlanSchemaTx(runtime, sender, { step, txId });
        const auto pathId =
            *WaitForShard(*controller.operator->(), runtime)->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(1),
                false);
        step = ProposeSchemaTx(runtime, sender, TTestSchema::DropTableTxBody(1, 2), ++txId);
        PlanSchemaTx(runtime, sender, { step, txId });
        controller->SetOverrideUsedSnapshotLivetime(TDuration::Zero());
        ui64 planStep = step.Val() + TruncateTestMaxReadStaleness.MilliSeconds() + 10000;
        const auto advance = [&] {
            PlanCommit(runtime, sender, TPlanStep{ planStep }, TSet<ui64>{});
            planStep += 10000;
            Wakeup(runtime, sender, TTestTxConfig::TxTablet0);
            ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new NColumnShard::TEvPrivate::TEvPingSnapshotsUsage());
            runtime.SimulateSleep(TDuration::Seconds(1));
        };
        for (ui32 i = 0; i < 3; ++i) {
            advance();
        }
        // Table cleanup must not leave a truncate bucket referencing a removed granule.
        UNIT_ASSERT(controller->GetTheOnlyShard()->GetTablesManager().HasTable(pathId, true));
        UNIT_ASSERT_VALUES_EQUAL(controller->GetTheOnlyShard()->GetTablesManager().GetTable(pathId, true).GetTruncateSnapshots().size(), 1);
        controller->EnableBackground(NKikimr::NYDBTest::ICSController::EBackground::Cleanup);
        const auto deadline = TInstant::Now() + TDuration::Seconds(30);
        while (controller->GetTheOnlyShard()->GetTablesManager().HasTable(pathId, true) && TInstant::Now() < deadline) {
            advance();
        }
        UNIT_ASSERT(!controller->GetTheOnlyShard()->GetTablesManager().HasTable(pathId, true));
        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        UNIT_ASSERT(WaitForShard(*controller.operator->(), runtime)->GetTablesManager().GetTables().empty());
    }

    Y_UNIT_TEST(TruncateCleanupPreservesTable) {
        TTestBasicRuntime runtime;
        SetupTruncateTestRuntime(runtime);
        auto csControllerGuard = RegisterTruncateTestController();
        auto& csController = *csControllerGuard.operator->();
        csControllerGuard->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        TActorId sender = runtime.AllocateEdgeActor();

        const ui64 pathId = 1;
        TestTableDescription testTable{};
        auto planStep = PrepareTablet(runtime, pathId, testTable.Schema);

        ui64 txId = 10;
        int writeId = 10;
        {
            std::vector<ui64> writeIds;
            UNIT_ASSERT(
                WriteData(runtime, sender, writeId++, pathId, MakeTestBlob({ 0, 100 }, testTable.Schema), testTable.Schema, true, &writeIds));
            planStep = ProposeCommit(runtime, sender, ++txId, writeIds);
            PlanCommit(runtime, sender, planStep, txId);
        }
        const auto snapshotBeforeTruncate = NOlap::TSnapshot(planStep, txId);

        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::TruncateTableTxBody(pathId, 1), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        const auto truncateSnapshot = NOlap::TSnapshot(planStep, txId);

        const auto* shard = WaitForShard(csController, runtime);
        UNIT_ASSERT(shard);

        const auto newInternalPathId = shard->GetTablesManager().ResolveInternalPathId(TSchemeShardLocalPathId::FromRawValue(pathId), false);
        UNIT_ASSERT(newInternalPathId);
        UNIT_ASSERT_VALUES_EQUAL(shard->GetTablesManager().GetTables().size(), 1);
        AssertPathsToDropState(*shard, *newInternalPathId, false);
        UNIT_ASSERT(HasPortionsRemovedAt(*shard, *newInternalPathId, truncateSnapshot));
        {
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(rb);
            UNIT_ASSERT_VALUES_EQUAL(rb->num_rows(), 100);
            UNIT_ASSERT(!reader.IsError());
        }

        // Expire the time-travel snapshot and enable cleanup of truncated portions.
        csControllerGuard->SetOverrideUsedSnapshotLivetime(TDuration::Zero());
        csControllerGuard->EnableBackground(NKikimr::NYDBTest::ICSController::EBackground::Cleanup);
        auto advancePlanStep = [&] {
            AdvanceShardPlanStep(runtime, sender, txId, writeId, pathId, testTable);
        };
        UNIT_ASSERT(WaitForTruncatedPortionsCleanup(csController, runtime, sender, *newInternalPathId, truncateSnapshot, advancePlanStep));

        {
            const auto* finalizedShard = csController.GetAnyShard();
            UNIT_ASSERT(finalizedShard);
            const auto& tables = finalizedShard->GetTablesManager().GetTables();
            UNIT_ASSERT_VALUES_EQUAL(tables.size(), 1);
            UNIT_ASSERT(tables.contains(*newInternalPathId));
            UNIT_ASSERT(!HasPortionsRemovedAt(*finalizedShard, *newInternalPathId, truncateSnapshot));
            UNIT_ASSERT(tables.at(*newInternalPathId).GetTruncateSnapshots().empty());
            UNIT_ASSERT(finalizedShard->GetTablesManager()
                            .GetPrimaryIndexAsVerified<NOlap::TColumnEngineForLogs>()
                            .GetGranuleVerified(*newInternalPathId)
                            .GetTruncateSnapshots()
                            .empty());
        }
        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        UNIT_ASSERT(WaitForShard(csController, runtime)->GetTablesManager().GetTable(*newInternalPathId).GetTruncateSnapshots().empty());
        {
            // GC advanced the read-staleness floor past the truncate snapshot, so this read is rejected.
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, truncateSnapshot);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(reader.IsError());
        }
        {
            // Likewise, snapshotBeforeTruncate is below the floor after GC removed truncated portions.
            TShardReader reader(runtime, TTestTxConfig::TxTablet0, pathId, snapshotBeforeTruncate);
            reader.SetReplyColumnIds(TTestSchema::ExtractIds(testTable.Schema));
            auto rb = reader.ReadAll();
            UNIT_ASSERT(!rb);
            UNIT_ASSERT(reader.IsError());
        }

        // Final DROP must remove the durable truncate history together with the table.
        planStep = ProposeSchemaTx(runtime, sender, TTestSchema::DropTableTxBody(pathId, 2), ++txId);
        PlanSchemaTx(runtime, sender, { planStep, txId });
        ui64 nextPlanStep = planStep.Val() + 10000;
        const auto deadline = TInstant::Now() + TDuration::Seconds(60);
        while (csController.GetTheOnlyShard()->GetTablesManager().HasTable(*newInternalPathId, true) && TInstant::Now() < deadline) {
            PlanCommit(runtime, sender, TPlanStep{ nextPlanStep }, TSet<ui64>{});
            nextPlanStep += 10000;
            Wakeup(runtime, sender, TTestTxConfig::TxTablet0);
            ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new NColumnShard::TEvPrivate::TEvPingSnapshotsUsage());
            runtime.SimulateSleep(TDuration::Seconds(1));
            Y_UNUSED(csController.WaitCleaning(TDuration::Seconds(1), &runtime));
        }
        UNIT_ASSERT(!csController.GetTheOnlyShard()->GetTablesManager().HasTable(*newInternalPathId, true));
        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        UNIT_ASSERT(WaitForShard(csController, runtime)->GetTablesManager().GetTables().empty());
    }
}
}   // namespace NKikimr
