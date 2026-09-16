#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/columnshard/flow_control_manager/flow_control_manager_events.h>
#include <ydb/core/tx/columnshard/flow_control_manager/flow_control_manager_service.h>
#include <ydb/core/tx/columnshard/flow_control_manager/flow_control_manager_types.h>
#include <ydb/core/tx/columnshard/overload_manager/overload_manager_events.h>
#include <ydb/core/tx/columnshard/overload_manager/overload_manager_service.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/long_tx_service/public/types.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx.h>
#include <ydb/core/tx/tx_proxy/long_tx_write_flow_control.h>
#include <ydb/core/tx/tx_proxy/upload_rows_common_impl.h>

#include <ydb/library/aclib/user_context.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/protos/interconnect.pb.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <library/cpp/testing/unittest/registar.h>

#include <cmath>
#include <functional>

namespace NKikimr {
namespace {

using namespace NActors;
using namespace NColumnShard;
using namespace NColumnShard::NFlowControl;
using namespace NLongTxService;
using namespace NTxUT;

// FlowControl Get* applies proto defaults for every unset field, so tests that flip one
// knob via MutableFlowControl must also nail the drain params they care about.
void ApplyDrainParamsToFlowControl(
    NKikimrConfig::TColumnShardConfig::TFlowControlConfig* fc, const TFlowControlManagerServiceOperator::TDrainRateParams& p) {
    fc->SetDrainRateMin(p.RMin);
    fc->SetDrainRateMax(p.RMax);
    fc->SetDrainRateStart(p.RStart);
    fc->SetDrainAimdBeta(p.AimdBeta);
    fc->SetDrainCubicRecoveryTargetSec(p.CubicRecoveryTargetSec);
    fc->SetDrainCubicProbePercent(p.CubicProbePercent);
    fc->SetDrainRateMinBytes(p.RMinBytes);
    fc->SetDrainRateMaxBytes(p.RMaxBytes);
    fc->SetDrainRateStartBytes(p.RStartBytes);
}

class TStartLongTxWriteActor: public TActorBootstrapped<TStartLongTxWriteActor> {
public:
    explicit TStartLongTxWriteActor(TLongTxWrite longTxWrite)
        : LongTxWrite(std::move(longTxWrite))
    {
    }

    void Bootstrap(const TActorContext& ctx) {
        NTxProxy::StartLongTxWriteFlowControlled(ctx, std::move(LongTxWrite));
        PassAway();
    }

private:
    TLongTxWrite LongTxWrite;
};

class TDoLongTxWriteSameMailboxActor: public TActorBootstrapped<TDoLongTxWriteSameMailboxActor> {
public:
    TDoLongTxWriteSameMailboxActor(TLongTxWrite longTxWrite)
        : LongTxWrite(std::move(longTxWrite))
    {
    }

    void Bootstrap(const TActorContext& ctx) {
        auto tx = std::move(LongTxWrite);
        NTxProxy::DoLongTxWriteSameMailbox(ctx, tx.GetReplyTo(), tx.GetLongTxId(), tx.GetDedupId(), tx.GetDatabaseName(), tx.GetPath(),
            tx.GetNavigateResult(), tx.GetBatch(), tx.GetIssues(), tx.GetUserCtx(), /*forceNoFlowControl=*/false, tx.GetDeadline(),
            tx.GetOperationTimeout());
        PassAway();
    }

private:
    TLongTxWrite LongTxWrite;
};

class TRequestReleaseResourcesActor: public TActorBootstrapped<TRequestReleaseResourcesActor> {
public:
    enum class EMode {
        RequestOnly,
        ReleaseOnly,
    };

    TRequestReleaseResourcesActor(ui64 writesCount, ui64 writesSize, EMode mode)
        : WritesCount(writesCount)
        , WritesSize(writesSize)
        , Mode(mode)
    {
    }

    void Bootstrap(const TActorContext&) {
        switch (Mode) {
            case EMode::RequestOnly:
                Y_UNUSED(NOverload::TOverloadManagerServiceOperator::RequestResources(WritesCount, WritesSize));
                break;
            case EMode::ReleaseOnly:
                NOverload::TOverloadManagerServiceOperator::ReleaseResources(WritesCount, WritesSize);
                break;
        }
        PassAway();
    }

private:
    const ui64 WritesCount;
    const ui64 WritesSize;
    const EMode Mode;
};

// ---------------------------------------------------------------------------
// Test harness for TFlowControlManager. Extend helpers when adding coverage
// for new events or success paths that require ColumnShard setup.
// ---------------------------------------------------------------------------

class TFlowControlManagerTestEnv {
public:
    explicit TFlowControlManagerTestEnv(TTestBasicRuntime& runtime)
        : Runtime(runtime)
    {
        // Fast drain jitter in UTs by default; keep queue capacity high unless a test shrinks it.
        // Drain rate params: leave process defaults (or whatever the test set before construction);
        // FCM copies them at RegisterServices time.
        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), 1024);
        // Pin wait/delayed-reject percents so tests stay deterministic even if process-wide
        // defaults change; production defaults are WaitTimeoutPercent=50 / DelayedReject=20.
        TFlowControlManagerServiceOperator::SetWaitTimeoutPercent(50);
        // Delayed-reject percent controls the delay before OVERLOADED is sent from the delayed-reject
        // queue; pin it to 10% so delayed-reject tests are deterministic.
        TFlowControlManagerServiceOperator::SetDelayedRejectTimeoutPercent(10);
        TTester::Setup(Runtime);
        RegisterServices();
        ReplyTo = Runtime.AllocateEdgeActor();
    }

    ~TFlowControlManagerTestEnv() {
        Runtime.GetAppData(0).ColumnShardConfig.ClearFlowControl();
        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::MilliSeconds(50), TDuration::MilliSeconds(250), 1024);
        TFlowControlManagerServiceOperator::SetWaitTimeoutPercent(50);
        TFlowControlManagerServiceOperator::SetDelayedRejectTimeoutPercent(10);
        TFlowControlManagerServiceOperator::ResetUtOverrides();
    }

    TActorId GetReplyTo() const {
        return ReplyTo;
    }

    TLongTxWrite BuildLongTxWrite(std::shared_ptr<const NSchemeCache::TSchemeCacheNavigate> navigateResult,
        std::shared_ptr<arrow::RecordBatch> batch = nullptr, TDuration operationTimeout = TDuration::Seconds(5 * 60),
        TInstant now = TInstant::Zero()) const {
        auto issues = std::make_shared<NYql::TIssues>();
        if (!batch) {
            batch = MakeEmptyBatch();
        }
        TLongTxId longTxId;
        Y_ABORT_UNLESS(longTxId.ParseString("ydb://long-tx/01ezvvxjdk2hd4vdgjs68knvp8?node_id=1"));
        if (now == TInstant::Zero()) {
            now = Runtime.GetCurrentTime();
        }
        return TLongTxWrite(ReplyTo, longTxId, "0", "/Root", "/Root/table", std::move(navigateResult), std::move(batch), std::move(issues),
            NACLib::TUserContextBuilder().Build(), now + operationTimeout, operationTimeout);
    }

    void StartLongTxWrite(TLongTxWrite longTxWrite) {
        Runtime.Register(new TStartLongTxWriteActor(std::move(longTxWrite)), 0, Runtime.GetAppData(0).UserPoolId);
    }

    void DoLongTxWriteSameMailbox(TLongTxWrite longTxWrite) {
        Runtime.Register(new TDoLongTxWriteSameMailboxActor(std::move(longTxWrite)), 0, Runtime.GetAppData(0).UserPoolId);
    }

    void SendToFlowControlManager(IEventBase* event) {
        Runtime.Send(new IEventHandle(TFlowControlManagerServiceOperator::MakeServiceId(Runtime.GetNodeId(0)), ReplyTo, event), 0, true);
    }

    TEvTryAdmitResult::TPtr TryAdmit(TVector<ui64> tabletIds, TDuration operationTimeout = TDuration::Seconds(60), ui64 batchSize = 0) {
        const TInstant now = Runtime.GetCurrentTime();
        SendToFlowControlManager(new TEvTryAdmit(std::move(tabletIds), now + operationTimeout, operationTimeout, batchSize));
        return Runtime.GrabEdgeEventRethrow<TEvTryAdmitResult>(ReplyTo);
    }

    void SeedTabletLocation(ui64 tabletId, ui32 nodeId) {
        SendToFlowControlManager(new TEvTabletLocationUpdated(tabletId, nodeId));
    }

    // Monotonic seed for TEvNodeOverloadStatus. OM now publishes with generations based on
    // Now().GetValue(), so a hard-coded 1 is treated as stale by FCM's LastGeneration watermark.
    ui64 LastSeededOverloadGeneration = 0;

    // generation == 0 (default) picks the next value above both the wall clock and prior seeds.
    // Returns the generation actually sent so callers can reuse it.
    ui64 SeedNodeOverloadStatus(ui32 nodeId, NKikimrTxColumnShard::TEvNodeOverloadStatus::EStatus status, ui64 generation = 0) {
        if (generation == 0) {
            generation = Max(TInstant::Now().GetValue(), LastSeededOverloadGeneration) + 1;
        }
        LastSeededOverloadGeneration = Max(LastSeededOverloadGeneration, generation);
        // FCM takes the publishing node from Sender, not from the payload. ReplyTo lives on the
        // local test node, so a fake service Sender is what lets tests heat node 42.
        const TActorId sender(nodeId, TStringBuf("seedFcmStat"));
        Runtime.Send(new IEventHandle(TFlowControlManagerServiceOperator::MakeServiceId(Runtime.GetNodeId(0)), sender,
                         new TEvNodeOverloadStatus(status, generation)), 0, true);
        return generation;
    }

    // Emulate TShardWriter reporting a terminal per-request write outcome. This is the
    // only feedback that moves the drain rate, so tests drive it directly.
    void SendWriteOutcome(ui64 tabletId, ui32 nodeId, bool overloaded, ui32 retries = 0) {
        SendWriteOutcome(tabletId, nodeId, overloaded ? EWriteOutcome::Overloaded : EWriteOutcome::Ok, retries);
    }

    void SendWriteOutcome(ui64 tabletId, ui32 nodeId, EWriteOutcome outcome, ui32 retries = 0) {
        SendToFlowControlManager(new TEvWriteOutcome(tabletId, nodeId, outcome, retries));
        Runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(1));
    }

    void EnableSchedulesForAllActors() {
        // FCM is registered in the env ctor before this hook; enable it explicitly for ContinueDrain / jitter.
        Runtime.EnableScheduleForActor(TFlowControlManagerServiceOperator::MakeServiceId(Runtime.GetNodeId(0)), true);
        Runtime.SetRegistrationObserverFunc([](TTestActorRuntimeBase& runtime, const TActorId& /*parentId*/, const TActorId& actorId) {
            runtime.EnableScheduleForActor(actorId, true);
        });
        Runtime.SetScheduledEventFilter([](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event, TDuration, TInstant&) {
            return !runtime.IsScheduleForActorEnabled(event->GetRecipientRewrite());
        });
    }

    TEvents::TEvCompleted::TPtr WaitCompleted(TDuration advance = TDuration::Zero()) {
        if (advance) {
            Runtime.AdvanceCurrentTime(advance);
            Runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));
        }
        return Runtime.GrabEdgeEventRethrow<TEvents::TEvCompleted>(ReplyTo);
    }

    // Read a FlowControl derivative counter value by its short name (as passed to
    // GetDeriviative, e.g. "FlowControl/WaitQueue/TimedOut/Count"). Mirrors the
    // subgroup layout from TCommonCountersOwner (module_id=CSFlowControlManager,
    // "Deriviative/" prefix) so tests can assert on FCM signals directly.
    i64 ReadFcmDeriviative(const TString& name) const {
        auto sub = Runtime.GetDynamicCounters(0)->GetSubgroup("module_id", "CSFlowControlManager");
        return sub->GetCounter(TString("Deriviative/") + name, true)->Val();
    }

    // Read a FlowControl value/gauge counter by its short name (e.g.
    // "FlowControl/Drain/RefillRate").
    i64 ReadFcmValue(const TString& name) const {
        auto sub = Runtime.GetDynamicCounters(0)->GetSubgroup("module_id", "CSFlowControlManager");
        return sub->GetCounter(TString("Value/") + name, false)->Val();
    }

private:
    static std::shared_ptr<arrow::RecordBatch> MakeEmptyBatch() {
        auto schema = arrow::schema({ arrow::field("id", arrow::uint64(), false) });
        return NArrow::MakeEmptyBatch(schema);
    }

    void RegisterServices() {
        const auto counters = Runtime.GetDynamicCounters(0);
        auto& appData = Runtime.GetAppData(0);
        {
            auto actor = NOverload::TOverloadManagerServiceOperator::CreateService(counters);
            const auto actorId = Runtime.Register(actor.release(), 0, appData.UserPoolId, TMailboxType::Revolving, 0);
            Runtime.EnableScheduleForActor(actorId, true);
            Runtime.RegisterService(NOverload::TOverloadManagerServiceOperator::MakeServiceId(), actorId, 0);
        }
        {
            auto actor = TFlowControlManagerServiceOperator::CreateService(counters);
            const auto actorId = Runtime.Register(actor.release(), 0, appData.UserPoolId, TMailboxType::Revolving, 0);
            Runtime.EnableScheduleForActor(actorId, true);
            Runtime.RegisterService(TFlowControlManagerServiceOperator::MakeServiceId(Runtime.GetNodeId(0)), actorId, 0);
        }
    }

    TTestBasicRuntime& Runtime;
    TActorId ReplyTo;
};

std::shared_ptr<NSchemeCache::TSchemeCacheNavigate> MakeNavigateWithSchemeError() {
    auto navigate = std::make_shared<NSchemeCache::TSchemeCacheNavigate>();
    navigate->ErrorCount = 1;
    return navigate;
}

std::shared_ptr<NSchemeCache::TSchemeCacheNavigate> MakeNavigateWithoutColumnTableSplitter() {
    auto navigate = std::make_shared<NSchemeCache::TSchemeCacheNavigate>();
    NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.Kind = NSchemeCache::TSchemeCacheNavigate::KindTable;
    entry.Status = NSchemeCache::TSchemeCacheNavigate::EStatus::Ok;
    navigate->ResultSet.push_back(std::move(entry));
    return navigate;
}

constexpr ui64 TestTableId = 1;
constexpr ui64 TestSchemaVersion = 1;

std::shared_ptr<NSchemeCache::TSchemeCacheNavigate> MakeNavigateForSingleColumnShard(ui64 shardTabletId) {
    auto navigate = std::make_shared<NSchemeCache::TSchemeCacheNavigate>();
    NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.Kind = NSchemeCache::TSchemeCacheNavigate::KindColumnTable;
    entry.Status = NSchemeCache::TSchemeCacheNavigate::EStatus::Ok;
    entry.TableId = TTableId(TTestTxConfig::DomainUid, TestTableId, TestSchemaVersion);

    const std::vector<NArrow::NTest::TTestColumn> schema = {
        NArrow::NTest::TTestColumn("id", TTypeInfo(NTypeIds::Uint64)),
    };
    auto columnTableInfo = MakeIntrusive<NSchemeCache::TSchemeCacheNavigate::TColumnTableInfo>();
    auto* schemaProto = columnTableInfo->Description.MutableSchema();
    TTestSchema::InitSchema(schema, schema, {}, schemaProto);
    schemaProto->SetVersion(TestSchemaVersion);

    auto* sharding = columnTableInfo->Description.MutableSharding();
    sharding->AddColumnShards(shardTabletId);
    auto* hashSharding = sharding->MutableHashSharding();
    hashSharding->AddColumns("id");
    hashSharding->SetFunction(NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_CONSISTENCY_64);

    entry.ColumnTableInfo = columnTableInfo;
    navigate->ResultSet.push_back(std::move(entry));
    return navigate;
}

std::shared_ptr<arrow::RecordBatch> MakeHappyPathBatch() {
    return MakeTestBatch<arrow::UInt64Type>({ "id" }, std::vector<uint64_t>{ 42 });
}

using TFakeShardWriteResultFactory = std::function<std::unique_ptr<NEvents::TDataEvents::TEvWriteResult>(ui64 shardTabletId)>;

std::unique_ptr<NEvents::TDataEvents::TEvWriteResult> MakeFakeShardWriteCompleted(ui64 shardTabletId) {
    return NEvents::TDataEvents::TEvWriteResult::BuildCompleted(shardTabletId);
}

std::unique_ptr<NEvents::TDataEvents::TEvWriteResult> MakeFakeShardWriteOverloaded(ui64 shardTabletId) {
    auto result = std::make_unique<NEvents::TDataEvents::TEvWriteResult>();
    result->Record.SetOrigin(shardTabletId);
    result->Record.SetStatus(NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED);
    return result;
}

void DisableShardWriteRetries(TTestBasicRuntime& runtime) {
    runtime.GetAppData(0).ColumnShardConfig.SetProxyMaxRetriesPerShard(0);
    runtime.GetAppData(0).FeatureFlags.SetEnableCsOverloadsSubscriptionRetries(false);
}

void InstallFakeShardWriteResponder(
    TTestBasicRuntime& runtime, ui64 shardTabletId, TFakeShardWriteResultFactory factory, bool* writeObserved = nullptr) {
    runtime.SetEventFilter([shardTabletId, writeObserved, factory = std::move(factory)](TTestActorRuntimeBase& rt, TAutoPtr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvPipeCache::TEvForward::EventType) {
            const auto* forward = ev->Get<TEvPipeCache::TEvForward>();
            if (forward->TabletId == shardTabletId && forward->Ev && forward->Ev->Type() == NEvents::TDataEvents::TEvWrite::EventType) {
                if (writeObserved) {
                    *writeObserved = true;
                }
                // Prefer a real node id so FCM tablet→node learning matches remoted service addressing.
                // Must use (nodeId, poolId, localId, hint) — TActorId(ui32, ui64) is the raw (x1,x2) ctor.
                const ui32 tabletNodeId = ev->Sender.NodeId() ? ev->Sender.NodeId() : rt.GetNodeId(0);
                rt.Schedule(new IEventHandle(ev->Sender, TActorId(tabletNodeId, 0, shardTabletId, 0), factory(shardTabletId).release()),
                    TDuration::Zero());
                return true;
            }
        }
        return false;
    });
}

void InstallFakeShardDeliveryProblem(TTestBasicRuntime& runtime, ui64 shardTabletId, bool* writeObserved = nullptr) {
    runtime.SetEventFilter([shardTabletId, writeObserved](TTestActorRuntimeBase& rt, TAutoPtr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvPipeCache::TEvForward::EventType) {
            const auto* forward = ev->Get<TEvPipeCache::TEvForward>();
            if (forward->TabletId == shardTabletId && forward->Ev && forward->Ev->Type() == NEvents::TDataEvents::TEvWrite::EventType) {
                if (writeObserved) {
                    *writeObserved = true;
                }
                rt.Schedule(new IEventHandle(ev->Sender, ev->Sender, new TEvPipeCache::TEvDeliveryProblem(shardTabletId, /*notDelivered=*/true)),
                    TDuration::Zero());
                return true;
            }
        }
        return false;
    });
}

void SeedOverloadManagerNodes(TTestBasicRuntime& runtime, const TActorId& sender, const std::vector<ui32>& nodeIds) {
    auto nodes = MakeIntrusive<TIntrusiveVector<TEvInterconnect::TNodeInfo>>();
    for (ui32 nodeId : nodeIds) {
        nodes->emplace_back(TEvInterconnect::TNodeInfo(nodeId, "::", "localhost", "localhost", 1234, TNodeLocation()));
    }
    runtime.Send(new IEventHandle(NOverload::TOverloadManagerServiceOperator::MakeServiceId(), sender, new TEvInterconnect::TEvNodesInfo(nodes)),
        0, true);
}

Y_UNIT_TEST_SUITE(TFlowControlManager) {
    Y_UNIT_TEST(StartLongTxWriteRepliesOnNavigateError) {
        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);

        env.StartLongTxWrite(env.BuildLongTxWrite(MakeNavigateWithSchemeError()));

        const auto completed = env.WaitCompleted();
        UNIT_ASSERT_VALUES_EQUAL((Ydb::StatusIds::StatusCode)completed->Get()->Status, Ydb::StatusIds::SCHEME_ERROR);
    }

    Y_UNIT_TEST(StartLongTxWriteRepliesWhenSplitterIsMissing) {
        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);

        env.StartLongTxWrite(env.BuildLongTxWrite(MakeNavigateWithoutColumnTableSplitter()));

        const auto completed = env.WaitCompleted();
        UNIT_ASSERT_VALUES_EQUAL((Ydb::StatusIds::StatusCode)completed->Get()->Status, Ydb::StatusIds::BAD_REQUEST);
    }

    Y_UNIT_TEST(StartLongTxWriteRepliesSuccess) {
        TTestBasicRuntime runtime;
        const ui64 shardTabletId = TTestTxConfig::TxTablet0;
        bool writeObserved = false;
        InstallFakeShardWriteResponder(runtime, shardTabletId, MakeFakeShardWriteCompleted, &writeObserved);

        TFlowControlManagerTestEnv env(runtime);
        env.StartLongTxWrite(env.BuildLongTxWrite(MakeNavigateForSingleColumnShard(shardTabletId), MakeHappyPathBatch()));

        const auto completed = env.WaitCompleted();
        UNIT_ASSERT(writeObserved);
        UNIT_ASSERT_VALUES_EQUAL((Ydb::StatusIds::StatusCode)completed->Get()->Status, Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(StartLongTxWriteRepliesWhenShardOverloaded) {
        TTestBasicRuntime runtime;
        const ui64 shardTabletId = TTestTxConfig::TxTablet0;

        bool writeObserved = false;
        InstallFakeShardWriteResponder(runtime, shardTabletId, MakeFakeShardWriteOverloaded, &writeObserved);

        TFlowControlManagerTestEnv env(runtime);
        DisableShardWriteRetries(runtime);
        env.StartLongTxWrite(env.BuildLongTxWrite(MakeNavigateForSingleColumnShard(shardTabletId), MakeHappyPathBatch()));

        const auto completed = env.WaitCompleted();
        UNIT_ASSERT(writeObserved);
        UNIT_ASSERT_VALUES_EQUAL((Ydb::StatusIds::StatusCode)completed->Get()->Status, Ydb::StatusIds::OVERLOADED);
    }

    Y_UNIT_TEST(GatesWhenTabletMappedToHotNode) {
        TTestBasicRuntime runtime;
        const ui64 shardTabletId = TTestTxConfig::TxTablet0;
        const ui32 hotNodeId = 42;

        bool writeObserved = false;
        InstallFakeShardWriteResponder(runtime, shardTabletId, MakeFakeShardWriteCompleted, &writeObserved);

        TFlowControlManagerTestEnv env(runtime);
        // Queue disabled → gated requests reject immediately.
        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), /*maxWaitQueueSize=*/0);
        env.SeedTabletLocation(shardTabletId, hotNodeId);
        env.SeedNodeOverloadStatus(hotNodeId, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);

        env.StartLongTxWrite(env.BuildLongTxWrite(MakeNavigateForSingleColumnShard(shardTabletId), MakeHappyPathBatch()));

        const auto completed = env.WaitCompleted();
        UNIT_ASSERT(!writeObserved);
        UNIT_ASSERT_VALUES_EQUAL((Ydb::StatusIds::StatusCode)completed->Get()->Status, Ydb::StatusIds::OVERLOADED);
    }

    Y_UNIT_TEST(WaitThenAllowOnReady) {
        TTestBasicRuntime runtime;
        const ui64 shardTabletId = TTestTxConfig::TxTablet0;
        const ui32 hotNodeId = 42;

        bool writeObserved = false;
        InstallFakeShardWriteResponder(runtime, shardTabletId, MakeFakeShardWriteCompleted, &writeObserved);

        TFlowControlManagerTestEnv env(runtime);
        env.EnableSchedulesForAllActors();
        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), /*maxWaitQueueSize=*/1024);
        env.SeedTabletLocation(shardTabletId, hotNodeId);
        env.SeedNodeOverloadStatus(hotNodeId, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);

        env.StartLongTxWrite(
            env.BuildLongTxWrite(MakeNavigateForSingleColumnShard(shardTabletId), MakeHappyPathBatch(), TDuration::Seconds(60)));

        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(20));
        UNIT_ASSERT(!writeObserved);

        env.SeedNodeOverloadStatus(hotNodeId, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY);

        const auto completed = env.WaitCompleted();
        UNIT_ASSERT(writeObserved);
        UNIT_ASSERT_VALUES_EQUAL((Ydb::StatusIds::StatusCode)completed->Get()->Status, Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(WaitDeadlineRejects) {
        TTestBasicRuntime runtime;
        const ui64 shardTabletId = TTestTxConfig::TxTablet0;
        const ui32 hotNodeId = 42;
        const TDuration operationTimeout = TDuration::MilliSeconds(200);

        bool writeObserved = false;
        InstallFakeShardWriteResponder(runtime, shardTabletId, MakeFakeShardWriteCompleted, &writeObserved);

        TFlowControlManagerTestEnv env(runtime);
        env.EnableSchedulesForAllActors();
        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), /*maxWaitQueueSize=*/1024);
        env.SeedTabletLocation(shardTabletId, hotNodeId);
        env.SeedNodeOverloadStatus(hotNodeId, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);

        env.StartLongTxWrite(env.BuildLongTxWrite(MakeNavigateForSingleColumnShard(shardTabletId), MakeHappyPathBatch(), operationTimeout));

        // Default WaitTimeoutPercent=50 ⇒ wait deadline = Timeout/2 = 100ms.
        const auto completed = env.WaitCompleted(operationTimeout / 2 + TDuration::MilliSeconds(20));
        UNIT_ASSERT(!writeObserved);
        UNIT_ASSERT_VALUES_EQUAL((Ydb::StatusIds::StatusCode)completed->Get()->Status, Ydb::StatusIds::OVERLOADED);
    }

    // A queued waiter aborted because its WaitDeadline expired must be counted as a
    // timeout, distinctly from a client-initiated cancel. Driven at the FCM protocol
    // level (TEvCancelWait carries the DeadlineExpired flag) so the assertion does not
    // depend on helper/write scheduling order.
    Y_UNIT_TEST(WaitDeadlineTimeoutIncrementsTimedOutCounter) {
        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        env.EnableSchedulesForAllActors();
        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), /*maxWaitQueueSize=*/1024);

        const ui64 shardTabletId = TTestTxConfig::TxTablet0;
        const ui32 hotNodeId = 42;
        env.SeedTabletLocation(shardTabletId, hotNodeId);
        env.SeedNodeOverloadStatus(hotNodeId, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);

        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/WaitQueue/TimedOut/Count"), 0);

        const auto admit = env.TryAdmit({ shardTabletId });
        UNIT_ASSERT_VALUES_EQUAL((int)admit->Get()->GetDecision(), (int)EAdmitDecision::Wait);
        const ui64 waiterId = admit->Get()->GetWaiterId();
        UNIT_ASSERT(waiterId != 0);
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/WaitQueue/Enqueued/Count"), 1);

        // The helper's wait-deadline timer fired while the request was still queued.
        env.SendToFlowControlManager(new TEvCancelWait(waiterId, /*deadlineExpired=*/true));
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(10));

        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/WaitQueue/TimedOut/Count"), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/WaitQueue/Cancelled/Count"), 0);
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/WaitQueue/RejectedDeadline/Count"), 0);
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/WaitQueue/Drained/Count"), 0);
    }

    // The same cancel without the DeadlineExpired flag is a client cancel, not a timeout.
    Y_UNIT_TEST(ClientCancelIncrementsCancelledCounter) {
        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        env.EnableSchedulesForAllActors();
        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), /*maxWaitQueueSize=*/1024);

        const ui64 shardTabletId = TTestTxConfig::TxTablet0;
        const ui32 hotNodeId = 42;
        env.SeedTabletLocation(shardTabletId, hotNodeId);
        env.SeedNodeOverloadStatus(hotNodeId, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);

        const auto admit = env.TryAdmit({ shardTabletId });
        UNIT_ASSERT_VALUES_EQUAL((int)admit->Get()->GetDecision(), (int)EAdmitDecision::Wait);

        env.SendToFlowControlManager(new TEvCancelWait(admit->Get()->GetWaiterId(), /*deadlineExpired=*/false));
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(10));

        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/WaitQueue/Cancelled/Count"), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/WaitQueue/TimedOut/Count"), 0);
    }

    // Sole in-flight DrainWaiter times out after a sibling was enqueued: without wakeups on
    // enqueue / cancel / stale DrainWaiter, the sibling would sit forever with Tokens full.
    Y_UNIT_TEST(DrainResumesAfterTimedOutScheduledWaiterWhenSiblingEnqueued) {
        TFlowControlManagerServiceOperator::TDrainRateParams drain;
        drain.RMin = 1.0;
        drain.RMax = 0.0;
        drain.RStart = 50.0;
        drain.CubicProbePercent = 0.0;
        TFlowControlManagerServiceOperator::SetDrainRateParams(drain);

        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        env.EnableSchedulesForAllActors();
        // Fixed jitter so the first waiter is DrainScheduled but not yet Allowed.
        TFlowControlManagerServiceOperator::SetWaitQueueParams(
            TDuration::MilliSeconds(100), TDuration::MilliSeconds(100), /*maxWaitQueueSize=*/1024);

        const ui64 tabletA = TTestTxConfig::TxTablet0;
        const ui32 nodeA = 42;
        env.SeedTabletLocation(tabletA, nodeA);
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);

        const auto admitA = env.TryAdmit({ tabletA });
        UNIT_ASSERT_VALUES_EQUAL((int)admitA->Get()->GetDecision(), (int)EAdmitDecision::Wait);
        const ui64 waiterA = admitA->Get()->GetWaiterId();

        // A becomes DrainScheduled (token reserved) with 100ms jitter; no sibling yet ⇒ no ContinueDrain.
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/WaitQueue/Drained/Count"), 0);

        // Sibling enqueues while A is still only scheduled; must be Kick-drained somehow.
        const auto admitB = env.TryAdmit({ tabletA });
        UNIT_ASSERT_VALUES_EQUAL((int)admitB->Get()->GetDecision(), (int)EAdmitDecision::Wait);

        // Time out A before its DrainWaiter fires (refunds token; must not drop the drain chain).
        env.SendToFlowControlManager(new TEvCancelWait(waiterA, /*deadlineExpired=*/true));
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/WaitQueue/TimedOut/Count"), 1);

        // Let jitter elapse: stale DrainWaiter for A + B's Allow.
        runtime.AdvanceCurrentTime(TDuration::MilliSeconds(100));
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));

        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/WaitQueue/Drained/Count"), 1);
        {
            const auto drained = runtime.GrabEdgeEventRethrow<TEvTryAdmitResult>(env.GetReplyTo());
            UNIT_ASSERT_VALUES_EQUAL((int)drained->Get()->GetDecision(), (int)EAdmitDecision::Allow);
        }
    }

    // Proto/config often nails DrainRateStartBytes=0 (message default) meaning "unset". Applying
    // that as a real start cold-starts FCM at EffectiveRMinBytes after restart and freezes drain.
    Y_UNIT_TEST(ZeroConfigStartRatesFallBackToDefaults) {
        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        TFlowControlManagerServiceOperator::ResetUtOverrides();

        auto* fc = runtime.GetAppData(0).ColumnShardConfig.MutableFlowControl();
        fc->SetDrainRateStart(0);
        fc->SetDrainRateStartBytes(0);
        fc->SetMaxWaitQueueSize(1024);

        const auto params = TFlowControlManagerServiceOperator::GetDrainRateParams();
        UNIT_ASSERT_C(params.RStart > 0.0, TStringBuilder() << "RStart=" << params.RStart);
        UNIT_ASSERT_C(params.RStartBytes >= 1'000'000.0, TStringBuilder() << "RStartBytes=" << params.RStartBytes);

        runtime.GetAppData(0).ColumnShardConfig.ClearFlowControl();
    }

    // After a cut to the unset-bound floor, a queued large batch must still drain: soft cap
    // raises to BatchSize and ContinueDrain must not park for deficit/rate hours.
    Y_UNIT_TEST(FloorRateDrainsWhenSoftCapCoversBatch) {
        TFlowControlManagerServiceOperator::TDrainRateParams drain;
        drain.RMin = 0.0;   // unset → EffectiveRMin = 1
        drain.RMax = 0.0;
        drain.RStart = 1.0;
        drain.CubicProbePercent = 0.0;
        drain.RMinBytes = 0.0;   // unset → EffectiveRMinBytes = 1 MB/s
        drain.RMaxBytes = 0.0;
        drain.RStartBytes = 1'000'000.0;
        TFlowControlManagerServiceOperator::SetDrainRateParams(drain);

        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        env.EnableSchedulesForAllActors();
        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), /*maxWaitQueueSize=*/1024);

        const ui64 tabletA = TTestTxConfig::TxTablet0;
        const ui32 nodeA = 42;
        env.SeedTabletLocation(tabletA, nodeA);
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);

        // Batch > RStartBytes: empty→non-empty must seed TokensBytes to BatchSize (soft-cap raise).
        constexpr ui64 batchSize = 2'000'000;
        const auto admit = env.TryAdmit({ tabletA }, TDuration::Seconds(60), batchSize);
        UNIT_ASSERT_VALUES_EQUAL((int)admit->Get()->GetDecision(), (int)EAdmitDecision::Wait);
        UNIT_ASSERT_C(env.ReadFcmValue("FlowControl/Drain/TokensBytes") >= static_cast<i64>(batchSize),
            TStringBuilder() << "TokensBytes=" << env.ReadFcmValue("FlowControl/Drain/TokensBytes"));

        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));

        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/WaitQueue/Drained/Count"), 1);
        {
            const auto drained = runtime.GrabEdgeEventRethrow<TEvTryAdmitResult>(env.GetReplyTo());
            UNIT_ASSERT_VALUES_EQUAL((int)drained->Get()->GetDecision(), (int)EAdmitDecision::Allow);
        }
    }

    // Compaction / OM hot: empty→non-empty HotNodes applies a full AimdBeta cut and clamps tokens.
    Y_UNIT_TEST(HotNodeEmptyToNonEmptyCutsDrainRate) {
        TFlowControlManagerServiceOperator::TDrainRateParams drain;
        drain.RMin = 10.0;
        drain.RMax = 0.0;
        drain.RStart = 100.0;
        drain.AimdBeta = 0.5;
        drain.CubicProbePercent = 0.0;
        drain.RMinBytes = 1'000'000.0;
        drain.RStartBytes = 100'000'000.0;
        TFlowControlManagerServiceOperator::SetDrainRateParams(drain);

        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        env.EnableSchedulesForAllActors();
        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), 1024);

        const ui64 tabletA = TTestTxConfig::TxTablet0;
        const ui32 nodeA = 42;
        env.SeedTabletLocation(tabletA, nodeA);

        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmValue("FlowControl/Drain/RefillRate"), 100);
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/Drain/RateCut/Count"), 0);

        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(10));

        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/Drain/RateCut/Count"), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmValue("FlowControl/Drain/RefillRate"), 50);
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmValue("FlowControl/Drain/RefillRateBytes"), 50'000'000);
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmValue("FlowControl/HotNodes/Count"), 1);

        // A second hot node while already non-empty must not cut again (the rate may still
        // be drifting down through the continuous while-hot decay).
        env.SeedNodeOverloadStatus(/*nodeB=*/43, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/Drain/RateCut/Count"), 1);
        UNIT_ASSERT_LE(env.ReadFcmValue("FlowControl/Drain/RefillRate"), 50);
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmValue("FlowControl/HotNodes/Count"), 2);
    }

    // Staying hot is evidence the single edge cut was not enough: the rate must keep
    // decaying for as long as the pressure lasts, down to the configured floor.
    Y_UNIT_TEST(HotNodeKeepsDecayingWhileHot) {
        TFlowControlManagerServiceOperator::TDrainRateParams drain;
        drain.RMin = 1.0;
        drain.RMax = 0.0;
        drain.RStart = 100.0;
        drain.AimdBeta = 0.5;
        drain.CubicRecoveryTargetSec = 10.0;   // decay tau = K/10 = 1s
        drain.CubicProbePercent = 0.0;
        drain.RMinBytes = 1'000'000.0;
        drain.RStartBytes = 100'000'000.0;
        TFlowControlManagerServiceOperator::SetDrainRateParams(drain);

        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        env.EnableSchedulesForAllActors();
        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), 1024);

        const ui64 tabletA = TTestTxConfig::TxTablet0;
        const ui32 nodeA = 42;
        env.SeedTabletLocation(tabletA, nodeA);

        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmValue("FlowControl/Drain/RefillRate"), 50);

        // Two decay taus at β=0.5 ⇒ ×0.25 (the hot tick keeps integrating on its own).
        runtime.AdvanceCurrentTime(TDuration::Seconds(2));
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));
        const i64 decayed = env.ReadFcmValue("FlowControl/Drain/RefillRate");
        UNIT_ASSERT_C(decayed <= 15 && decayed >= 10, TStringBuilder() << "expected ~12, got " << decayed);
        UNIT_ASSERT_C(env.ReadFcmDeriviative("FlowControl/Drain/RateDecay/Count") > 0, "no decay recorded");
        // Still a single cut: decay is not counted as an AIMD cut.
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/Drain/RateCut/Count"), 1);

        // Sustained pressure walks the rate all the way down to the floor and stops there.
        runtime.AdvanceCurrentTime(TDuration::Seconds(10));
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmValue("FlowControl/Drain/RefillRate"), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmValue("FlowControl/Drain/RefillRateBytes"), 1'000'000);
    }

    // Hot-edge cut must not CUBIC-recover to the pre-cut peak (that recreates the sawtooth).
    Y_UNIT_TEST(HotCutDoesNotRecoverToPreCutPeak) {
        TFlowControlManagerServiceOperator::TDrainRateParams drain;
        drain.RMin = 1.0;
        drain.RMax = 0.0;
        drain.RStart = 100.0;
        drain.AimdBeta = 0.8;   // weak cut — previously undone by CUBIC recovery to Wmax
        drain.CubicRecoveryTargetSec = 2.0;
        drain.CubicProbePercent = 0.0;
        drain.RMinBytes = 1'000'000.0;
        drain.RStartBytes = 100'000'000.0;
        TFlowControlManagerServiceOperator::SetDrainRateParams(drain);

        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        env.EnableSchedulesForAllActors();
        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), 1024);

        const ui64 tabletA = TTestTxConfig::TxTablet0;
        const ui32 nodeA = 42;
        env.SeedTabletLocation(tabletA, nodeA);

        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(10));
        const i64 afterCut = env.ReadFcmValue("FlowControl/Drain/RefillRate");
        UNIT_ASSERT_VALUES_EQUAL(afterCut, 80);

        UNIT_ASSERT_VALUES_EQUAL((int)env.TryAdmit({ tabletA })->Get()->GetDecision(), (int)EAdmitDecision::Wait);
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(20));
        {
            const auto drained = runtime.GrabEdgeEventRethrow<TEvTryAdmitResult>(env.GetReplyTo());
            UNIT_ASSERT_VALUES_EQUAL((int)drained->Get()->GetDecision(), (int)EAdmitDecision::Allow);
        }

        // The node was hot for a while before READY, so the rate sits at or below the cut.
        const i64 beforeGrowth = env.ReadFcmValue("FlowControl/Drain/RefillRate");
        UNIT_ASSERT_LE(beforeGrowth, afterCut);

        runtime.AdvanceCurrentTime(TDuration::MilliSeconds(2500));
        const ui64 growTarget = Max<ui64>(1, static_cast<ui64>(std::ceil(static_cast<double>(afterCut))));
        for (ui64 i = 0; i < growTarget; ++i) {
            env.SendWriteOutcome(tabletA, nodeA, /*overloaded=*/false);
        }

        // Must stay where the hot pressure left it (probe% = 0), not climb back toward 100.
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmValue("FlowControl/Drain/RefillRate"), beforeGrowth);
    }

    // While HotNodes is non-empty, clean write outcomes must not CUBIC-probe the rate.
    Y_UNIT_TEST(HotNodeFreezesCubicGrowth) {
        TFlowControlManagerServiceOperator::TDrainRateParams drain;
        drain.RMin = 1.0;
        drain.RMax = 0.0;
        drain.RStart = 10.0;
        drain.CubicProbePercent = 50.0;
        drain.CubicRecoveryTargetSec = 1.0;
        TFlowControlManagerServiceOperator::SetDrainRateParams(drain);

        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        env.EnableSchedulesForAllActors();
        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), 1024);

        const ui64 tabletA = TTestTxConfig::TxTablet0;
        const ui32 nodeA = 42;
        env.SeedTabletLocation(tabletA, nodeA);
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);
        // empty→hot cut 10→5; open a cohort by draining after READY, then go hot again.
        UNIT_ASSERT_VALUES_EQUAL((int)env.TryAdmit({ tabletA })->Get()->GetDecision(), (int)EAdmitDecision::Wait);
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(20));
        {
            const auto drained = runtime.GrabEdgeEventRethrow<TEvTryAdmitResult>(env.GetReplyTo());
            UNIT_ASSERT_VALUES_EQUAL((int)drained->Get()->GetDecision(), (int)EAdmitDecision::Allow);
        }
        const i64 rateAfterDrain = env.ReadFcmValue("FlowControl/Drain/RefillRate");

        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(10));
        const i64 rateWhileHot = env.ReadFcmValue("FlowControl/Drain/RefillRate");
        UNIT_ASSERT_C(rateWhileHot <= rateAfterDrain, TStringBuilder() << rateWhileHot << " vs " << rateAfterDrain);

        runtime.AdvanceCurrentTime(TDuration::Seconds(2));
        for (int i = 0; i < 32; ++i) {
            env.SendWriteOutcome(tabletA, nodeA, /*overloaded=*/false);
        }

        // Clean outcomes while hot buy nothing: no growth, and the rate only decays further.
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/Drain/RateGrow/Count"), 0);
        UNIT_ASSERT_LE(env.ReadFcmValue("FlowControl/Drain/RefillRate"), rateWhileHot);
    }

    Y_UNIT_TEST(WaitTimeoutPercentFromConfig) {
        TTestBasicRuntime runtime;
        const ui64 tabletA = TTestTxConfig::TxTablet0;
        const ui32 nodeA = 42;
        const TDuration operationTimeout = TDuration::MilliSeconds(1000);

        TFlowControlManagerTestEnv env(runtime);
        env.EnableSchedulesForAllActors();
        // Config wins over UT atomics for wait percent.
        runtime.GetAppData(0).ColumnShardConfig.MutableFlowControl()->SetWaitTimeoutPercent(10);
        runtime.GetAppData(0).ColumnShardConfig.MutableFlowControl()->SetMaxWaitQueueSize(1024);
        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), 1024);

        env.SeedTabletLocation(tabletA, nodeA);
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);

        const TInstant now = runtime.GetCurrentTime();
        const auto admit = env.TryAdmit({ tabletA }, operationTimeout);
        UNIT_ASSERT_VALUES_EQUAL((int)admit->Get()->GetDecision(), (int)EAdmitDecision::Wait);
        // Max wait = 10% of 1000ms = 100ms ⇒ WaitDeadline = now + 100ms.
        const TInstant expected = now + operationTimeout * 10 / 100;
        UNIT_ASSERT_VALUES_EQUAL(admit->Get()->GetWaitDeadline().MilliSeconds(), expected.MilliSeconds());

        // Request whose wait window already elapsed → RejectNow.
        const TInstant startedAgo = runtime.GetCurrentTime() - TDuration::MilliSeconds(200);
        env.SendToFlowControlManager(new TEvTryAdmit(TVector<ui64>{ tabletA }, startedAgo + operationTimeout, operationTimeout));
        const auto late = runtime.GrabEdgeEventRethrow<TEvTryAdmitResult>(env.GetReplyTo());
        UNIT_ASSERT_VALUES_EQUAL((int)late->Get()->GetDecision(), (int)EAdmitDecision::RejectNow);

        runtime.GetAppData(0).ColumnShardConfig.ClearFlowControl();
    }

    Y_UNIT_TEST(QueueFullRejects) {
        TTestBasicRuntime runtime;
        const ui64 shardTabletId = TTestTxConfig::TxTablet0;
        const ui32 hotNodeId = 42;

        bool writeObserved = false;
        InstallFakeShardWriteResponder(runtime, shardTabletId, MakeFakeShardWriteCompleted, &writeObserved);

        TFlowControlManagerTestEnv env(runtime);
        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), /*maxWaitQueueSize=*/1);
        env.SeedTabletLocation(shardTabletId, hotNodeId);
        env.SeedNodeOverloadStatus(hotNodeId, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);

        // First request occupies the only wait slot (stays queued).
        env.StartLongTxWrite(
            env.BuildLongTxWrite(MakeNavigateForSingleColumnShard(shardTabletId), MakeHappyPathBatch(), TDuration::Seconds(60)));
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(20));

        // Second request must be rejected immediately (queue full).
        const auto replyTo2 = runtime.AllocateEdgeActor();
        {
            auto issues = std::make_shared<NYql::TIssues>();
            TLongTxId longTxId;
            Y_ABORT_UNLESS(longTxId.ParseString("ydb://long-tx/01ezvvxjdk2hd4vdgjs68knvp8?node_id=1"));
            const TInstant now = runtime.GetCurrentTime();
            auto tx = TLongTxWrite(replyTo2, longTxId, "1", "/Root", "/Root/table", MakeNavigateForSingleColumnShard(shardTabletId),
                MakeHappyPathBatch(), std::move(issues),
                NACLib::TUserContextBuilder().Build(), now + TDuration::Seconds(60), TDuration::Seconds(60));
            env.StartLongTxWrite(std::move(tx));
        }

        const auto completed2 = runtime.GrabEdgeEventRethrow<TEvents::TEvCompleted>(replyTo2);
        UNIT_ASSERT(!writeObserved);
        UNIT_ASSERT_VALUES_EQUAL((Ydb::StatusIds::StatusCode)completed2->Get()->Status, Ydb::StatusIds::OVERLOADED);
    }

    Y_UNIT_TEST(NoJumpWhileWaitersOnSameDestination) {
        // Drain params must be set before FCM is constructed.
        TFlowControlManagerServiceOperator::TDrainRateParams drain;
        drain.RMin = 1.0;
        drain.RMax = 1.0;
        drain.RStart = 1.0;
        TFlowControlManagerServiceOperator::SetDrainRateParams(drain);

        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        env.EnableSchedulesForAllActors();
        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), 1024);

        const ui64 tabletA = TTestTxConfig::TxTablet0;
        const ui32 nodeA = 42;
        env.SeedTabletLocation(tabletA, nodeA);
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);

        // Three waiters on A; burst=1 so READY drains only one immediately.
        for (int i = 0; i < 3; ++i) {
            const auto admit = env.TryAdmit({ tabletA });
            UNIT_ASSERT_VALUES_EQUAL((int)admit->Get()->GetDecision(), (int)EAdmitDecision::Wait);
        }

        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(20));
        // One Allow from drain (consumed by edge actor as TryAdmitResult).
        {
            const auto drained = runtime.GrabEdgeEventRethrow<TEvTryAdmitResult>(env.GetReplyTo());
            UNIT_ASSERT_VALUES_EQUAL((int)drained->Get()->GetDecision(), (int)EAdmitDecision::Allow);
        }

        // Node A is cool but still has waiters → new admit must Wait (no jump).
        const auto noJump = env.TryAdmit({ tabletA });
        UNIT_ASSERT_VALUES_EQUAL((int)noJump->Get()->GetDecision(), (int)EAdmitDecision::Wait);
    }

    Y_UNIT_TEST(CoolOtherDestinationAllowsDespiteWaitersOnA) {
        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), 1024);

        const ui64 tabletA = TTestTxConfig::TxTablet0;
        const ui64 tabletB = TTestTxConfig::TxTablet1;
        const ui32 nodeA = 42;
        const ui32 nodeB = 43;
        env.SeedTabletLocation(tabletA, nodeA);
        env.SeedTabletLocation(tabletB, nodeB);
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);

        const auto waitA = env.TryAdmit({ tabletA });
        UNIT_ASSERT_VALUES_EQUAL((int)waitA->Get()->GetDecision(), (int)EAdmitDecision::Wait);

        // B is cool and has no waiters → Allow even while A has a queue.
        const auto allowB = env.TryAdmit({ tabletB });
        UNIT_ASSERT_VALUES_EQUAL((int)allowB->Get()->GetDecision(), (int)EAdmitDecision::Allow);
    }

    Y_UNIT_TEST(MultiDestWaiterBlocksSharedDestination) {
        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), 1024);

        const ui64 tabletA = TTestTxConfig::TxTablet0;
        const ui64 tabletB = TTestTxConfig::TxTablet1;
        const ui32 nodeA = 42;
        const ui32 nodeB = 43;
        env.SeedTabletLocation(tabletA, nodeA);
        env.SeedTabletLocation(tabletB, nodeB);
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);

        // Multi-dest waiter increments counts on both A and B.
        const auto waitAB = env.TryAdmit({ tabletA, tabletB });
        UNIT_ASSERT_VALUES_EQUAL((int)waitAB->Get()->GetDecision(), (int)EAdmitDecision::Wait);

        // B-only must wait while multi-dest waiter is still queued.
        const auto waitB = env.TryAdmit({ tabletB });
        UNIT_ASSERT_VALUES_EQUAL((int)waitB->Get()->GetDecision(), (int)EAdmitDecision::Wait);
    }

    Y_UNIT_TEST(PacedDrainDoesNotReleaseAllAtOnce) {
        TFlowControlManagerServiceOperator::TDrainRateParams drain;
        drain.RMin = 1.0;
        drain.RMax = 1.0;
        drain.RStart = 1.0;
        drain.CubicProbePercent = 0.0;   // do not grow during the test
        TFlowControlManagerServiceOperator::SetDrainRateParams(drain);

        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        env.EnableSchedulesForAllActors();
        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), 1024);

        const ui64 tabletA = TTestTxConfig::TxTablet0;
        const ui32 nodeA = 42;
        env.SeedTabletLocation(tabletA, nodeA);
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);

        for (int i = 0; i < 5; ++i) {
            UNIT_ASSERT_VALUES_EQUAL((int)env.TryAdmit({ tabletA })->Get()->GetDecision(), (int)EAdmitDecision::Wait);
        }

        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(20));

        // Exactly one drain Allow before time advances enough for another token.
        {
            const auto drained = runtime.GrabEdgeEventRethrow<TEvTryAdmitResult>(env.GetReplyTo());
            UNIT_ASSERT_VALUES_EQUAL((int)drained->Get()->GetDecision(), (int)EAdmitDecision::Allow);
        }

        // Without advancing ~1s, no further drain Allow should be pending.
        {
            TAutoPtr<IEventHandle> handle;
            const auto* drained = runtime.GrabEdgeEventRethrow<TEvTryAdmitResult>(handle, TDuration::MilliSeconds(50));
            UNIT_ASSERT(!drained);
        }

        // Advance sim time so the token bucket refills, then nudge drain scheduling.
        runtime.AdvanceCurrentTime(TDuration::Seconds(1));
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));
        {
            const auto drained = runtime.GrabEdgeEventRethrow<TEvTryAdmitResult>(env.GetReplyTo());
            UNIT_ASSERT_VALUES_EQUAL((int)drained->Get()->GetDecision(), (int)EAdmitDecision::Allow);
        }
    }

    Y_UNIT_TEST(OverloadAfterDrainCutsRate) {
        TFlowControlManagerServiceOperator::TDrainRateParams drain;
        drain.RMin = 10.0;
        drain.RMax = 100.0;
        drain.RStart = 100.0;
        drain.AimdBeta = 0.5;
        drain.CubicProbePercent = 0.0;
        TFlowControlManagerServiceOperator::SetDrainRateParams(drain);

        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        env.EnableSchedulesForAllActors();
        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), 1024);

        const ui64 tabletA = TTestTxConfig::TxTablet0;
        const ui32 nodeA = 42;
        env.SeedTabletLocation(tabletA, nodeA);
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);

        for (int i = 0; i < 3; ++i) {
            UNIT_ASSERT_VALUES_EQUAL((int)env.TryAdmit({ tabletA })->Get()->GetDecision(), (int)EAdmitDecision::Wait);
        }

        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(20));
        {
            const auto drained = runtime.GrabEdgeEventRethrow<TEvTryAdmitResult>(env.GetReplyTo());
            UNIT_ASSERT_VALUES_EQUAL((int)drained->Get()->GetDecision(), (int)EAdmitDecision::Allow);
        }

        // OVERLOADED marks the node hot (gates admits) and cuts rate when HotNodes goes
        // empty→non-empty; further READY still drains the remaining waiters.
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY);
        runtime.AdvanceCurrentTime(TDuration::MilliSeconds(100));
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));
        {
            const auto drained = runtime.GrabEdgeEventRethrow<TEvTryAdmitResult>(env.GetReplyTo());
            UNIT_ASSERT_VALUES_EQUAL((int)drained->Get()->GetDecision(), (int)EAdmitDecision::Allow);
        }
    }

    Y_UNIT_TEST(DrainRateDoesNotGrowWithoutTraffic) {
        // ProbePercent > 0 but no traffic: growth is decided only by completed cohorts of
        // per-request write outcomes, so elapsed wall-clock time and repeated drain
        // scheduling must never raise the rate on their own.
        TFlowControlManagerServiceOperator::TDrainRateParams drain;
        drain.RMin = 10.0;
        drain.RMax = 500.0;
        drain.RStart = 10.0;
        drain.CubicProbePercent = 50.0;
        TFlowControlManagerServiceOperator::SetDrainRateParams(drain);

        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        env.EnableSchedulesForAllActors();
        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), 1024);

        const ui64 tabletA = TTestTxConfig::TxTablet0;
        const ui32 nodeA = 42;
        env.SeedTabletLocation(tabletA, nodeA);

        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/Drain/RateGrow/Count"), 0);

        // No waiters in queue. Repeatedly poke ScheduleDrainEligible via READY + time.
        for (int i = 0; i < 5; ++i) {
            env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY);
            runtime.AdvanceCurrentTime(TDuration::MilliSeconds(50));
            runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(10));
        }

        // Empty queue ⇒ no growth despite elapsed time and repeated scheduling.
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/Drain/RateGrow/Count"), 0);
    }

    // FlowControl config applied AFTER the FCM actor is constructed (dynamic config) must
    // take effect on the drain-rate bounds. The actor seeds RMin/RMax at construction; a
    // later, lower DrainRateMax must clamp the live RefillRate down on the next drain cycle.
    Y_UNIT_TEST(DrainRateMaxFromConfigAppliedLive) {
        // Seed a HIGH process-wide RMax and a high start, as if config was not yet merged
        // when the actor was built.
        TFlowControlManagerServiceOperator::TDrainRateParams drain;
        drain.RMin = 20.0;
        drain.RMax = 500.0;
        drain.RStart = 200.0;
        drain.CubicProbePercent = 0.0;
        TFlowControlManagerServiceOperator::SetDrainRateParams(drain);

        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        env.EnableSchedulesForAllActors();
        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), 1024);

        const ui64 tabletA = TTestTxConfig::TxTablet0;
        const ui32 nodeA = 42;
        env.SeedTabletLocation(tabletA, nodeA);

        // Now a lower DrainRateMax arrives via ColumnShardConfig.FlowControl (config wins
        // over the process atomics, mirroring WaitTimeoutPercentFromConfig).
        auto* fc = runtime.GetAppData(0).ColumnShardConfig.MutableFlowControl();
        fc->SetDrainRateMin(20);
        fc->SetDrainRateMax(100);
        fc->SetDrainRateStart(50);
        fc->SetDrainCubicProbePercent(0);
        fc->SetDrainAimdBeta(0.5);

        // Drive a drain cycle (RefillTokens -> SyncDrainBounds) via STATUS_READY.
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY);
        runtime.AdvanceCurrentTime(TDuration::MilliSeconds(50));
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(10));

        // RefillRate must be clamped from the seeded 200 down to the config RMax of 100.
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmValue("FlowControl/Drain/RefillRate"), 100);
    }

    // Proto DrainRateMax / DrainRateMaxBytes default to 0 = no limit. A partial FlowControl
    // YAML (queue knobs only) must keep that no-limit semantics (not invent a rate ceiling).
    Y_UNIT_TEST(UnsetDrainRateMaxDoesNotCapFromProtoDefault) {
        TFlowControlManagerServiceOperator::TDrainRateParams drain;
        drain.RMin = 0.0;
        drain.RMax = 0.0;   // no limit
        drain.RStart = 200.0;
        drain.CubicProbePercent = 0.0;
        TFlowControlManagerServiceOperator::SetDrainRateParams(drain);

        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        env.EnableSchedulesForAllActors();

        // Partial config like production: only queue knobs, no drain_rate_max.
        auto* fc = runtime.GetAppData(0).ColumnShardConfig.MutableFlowControl();
        fc->SetMaxWaitQueueSize(500);
        fc->SetWaitTimeoutPercent(10);
        fc->SetMaxDelayedRejectQueueSize(5000);
        fc->SetDelayedRejectTimeoutPercent(10);
        UNIT_ASSERT(!fc->HasDrainRateMax());
        UNIT_ASSERT(!fc->HasDrainRateMaxBytes());
        UNIT_ASSERT_VALUES_EQUAL(fc->GetDrainRateMax(), 0.0);
        UNIT_ASSERT_VALUES_EQUAL(fc->GetDrainRateMaxBytes(), 0.0);

        const ui64 tabletA = TTestTxConfig::TxTablet0;
        const ui32 nodeA = 42;
        env.SeedTabletLocation(tabletA, nodeA);

        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY);
        runtime.AdvanceCurrentTime(TDuration::MilliSeconds(50));
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(10));

        // Live rate seeded at 200 stays: proto RMax default 0 means no ceiling.
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmValue("FlowControl/Drain/RefillRate"), 200);
    }

    // Shared CUBIC probe / beta from FlowControl apply to BOTH count and bytes buckets.
    Y_UNIT_TEST(SharedConfigCubicAppliesToBothBuckets) {
        TFlowControlManagerServiceOperator::TDrainRateParams drain;
        // RMin* = RStart*: OVERLOADED gating must not Aimd-cut either bucket (hot empty→non-empty).
        drain.RMin = 100.0;
        drain.RMax = 0.0;
        drain.RStart = 100.0;
        drain.RMinBytes = 100'000'000.0;
        drain.RStartBytes = 100'000'000.0;
        drain.CubicProbePercent = 1.0;   // overridden by config via SyncDrainBounds
        TFlowControlManagerServiceOperator::SetDrainRateParams(drain);

        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        env.EnableSchedulesForAllActors();
        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), 1024);

        // Config must carry the full drain knobs: Get* fills unset fields from proto defaults.
        drain.CubicProbePercent = 10.0;   // +10% of Wmax both buckets
        drain.AimdBeta = 0.5;
        ApplyDrainParamsToFlowControl(runtime.GetAppData(0).ColumnShardConfig.MutableFlowControl(), drain);

        const ui64 tabletA = TTestTxConfig::TxTablet0;
        const ui32 nodeA = 42;
        env.SeedTabletLocation(tabletA, nodeA);
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);
        UNIT_ASSERT_VALUES_EQUAL((int)env.TryAdmit({ tabletA })->Get()->GetDecision(), (int)EAdmitDecision::Wait);
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(20));
        {
            const auto drained = runtime.GrabEdgeEventRethrow<TEvTryAdmitResult>(env.GetReplyTo());
            UNIT_ASSERT_VALUES_EQUAL((int)drained->Get()->GetDecision(), (int)EAdmitDecision::Allow);
        }
        // Growth needs a quiet window after the node cooled down (default K=10s ⇒ 2s).
        runtime.AdvanceCurrentTime(TDuration::Seconds(3));
        for (int i = 0; i < 100; ++i) {
            env.SendWriteOutcome(tabletA, nodeA, /*overloaded=*/false);
        }
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/Drain/RateGrow/Count"), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmValue("FlowControl/Drain/RefillRate"), 110);
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmValue("FlowControl/Drain/RefillRateBytes"), 110'000'000);

        runtime.GetAppData(0).ColumnShardConfig.ClearFlowControl();
    }

    // Explicit bytes max from config clamps RefillRateBytes on the next SyncDrainBounds.
    Y_UNIT_TEST(DrainRateMaxBytesFromConfigAppliedLive) {
        TFlowControlManagerServiceOperator::TDrainRateParams drain;
        drain.RMin = 0.0;
        drain.RMax = 0.0;
        drain.RStart = 50.0;
        drain.RStartBytes = 200'000'000.0;
        drain.CubicProbePercent = 0.0;
        TFlowControlManagerServiceOperator::SetDrainRateParams(drain);

        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        env.EnableSchedulesForAllActors();

        drain.RMaxBytes = 100'000'000.0;
        ApplyDrainParamsToFlowControl(runtime.GetAppData(0).ColumnShardConfig.MutableFlowControl(), drain);

        const ui32 nodeA = 42;
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY);
        runtime.AdvanceCurrentTime(TDuration::MilliSeconds(50));
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(10));

        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmValue("FlowControl/Drain/RefillRateBytes"), 100'000'000);
    }

    // Growth needs a full clean cohort AND a quiet window since the last hot node: write
    // outcomes arrive per shard write, so they cannot be the only clock.
    Y_UNIT_TEST(CohortAllOkGrowsRateAfterQuietWindow) {
        TFlowControlManagerServiceOperator::TDrainRateParams drain;
        drain.RMin = 1.0;
        drain.RMax = 100.0;
        drain.RStart = 1.0;   // cohort target = ceil(1.0) = 1
        drain.CubicProbePercent = 5.0;
        TFlowControlManagerServiceOperator::SetDrainRateParams(drain);

        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        env.EnableSchedulesForAllActors();
        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), 1024);

        const ui64 tabletA = TTestTxConfig::TxTablet0;
        const ui32 nodeA = 42;
        env.SeedTabletLocation(tabletA, nodeA);
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);

        UNIT_ASSERT_VALUES_EQUAL((int)env.TryAdmit({ tabletA })->Get()->GetDecision(), (int)EAdmitDecision::Wait);

        // Release the waiter so a cohort opens (target 1).
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(20));
        {
            const auto drained = runtime.GrabEdgeEventRethrow<TEvTryAdmitResult>(env.GetReplyTo());
            UNIT_ASSERT_VALUES_EQUAL((int)drained->Get()->GetDecision(), (int)EAdmitDecision::Allow);
        }

        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/Drain/RateGrow/Count"), 0);

        // Past the hot cooldown (default K=10s ⇒ 2s) a clean cohort probes +5% of Wmax.
        runtime.AdvanceCurrentTime(TDuration::Seconds(3));
        env.SendWriteOutcome(tabletA, nodeA, /*overloaded=*/false);

        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/Drain/Outcome/Ok/Count"), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/Drain/RateGrow/Count"), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/Drain/CohortAborted/Count"), 0);
        // Probe 5% of Wmax=1 ⇒ ~1.05 (gauge rounds to 1).
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmValue("FlowControl/Drain/RefillRate"), 1);
    }

    // The same clean cohort inside the hot cooldown must not grow anything: a READY flap
    // right before the outcomes is exactly the case that used to re-inflate the rate.
    Y_UNIT_TEST(HotCooldownBlocksGrowthAfterReady) {
        TFlowControlManagerServiceOperator::TDrainRateParams drain;
        drain.RMin = 1.0;
        drain.RMax = 100.0;
        drain.RStart = 1.0;   // cohort target = ceil(1.0) = 1
        drain.CubicProbePercent = 5.0;
        TFlowControlManagerServiceOperator::SetDrainRateParams(drain);

        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        env.EnableSchedulesForAllActors();
        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), 1024);

        const ui64 tabletA = TTestTxConfig::TxTablet0;
        const ui32 nodeA = 42;
        env.SeedTabletLocation(tabletA, nodeA);
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);

        UNIT_ASSERT_VALUES_EQUAL((int)env.TryAdmit({ tabletA })->Get()->GetDecision(), (int)EAdmitDecision::Wait);
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(20));
        {
            const auto drained = runtime.GrabEdgeEventRethrow<TEvTryAdmitResult>(env.GetReplyTo());
            UNIT_ASSERT_VALUES_EQUAL((int)drained->Get()->GetDecision(), (int)EAdmitDecision::Allow);
        }

        env.SendWriteOutcome(tabletA, nodeA, /*overloaded=*/false);

        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/Drain/Outcome/Ok/Count"), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/Drain/RateGrow/Count"), 0);
        UNIT_ASSERT_C(env.ReadFcmDeriviative("FlowControl/Drain/GrowthBlocked/Count") > 0, "cooldown not applied");
    }

    // The requests/s rate must not detach from what FCM actually serves. Here the bytes
    // bucket paces the queue at ~2 releases/s while the count rate says 1000/s: the anchor
    // has to walk that meaningless number back down toward measured throughput.
    Y_UNIT_TEST(AnchorPullsRateDownToServedThroughput) {
        TFlowControlManagerServiceOperator::TDrainRateParams drain;
        drain.RMin = 1000.0;   // lowered via config once the queue has formed
        drain.RMax = 0.0;
        drain.RStart = 1000.0;
        drain.AimdBeta = 0.5;
        drain.CubicProbePercent = 0.0;
        // Pin the bytes bucket so the drain pace stays exactly 2 × 5MB batches per second.
        drain.RMinBytes = 10'000'000.0;
        drain.RStartBytes = 10'000'000.0;
        TFlowControlManagerServiceOperator::SetDrainRateParams(drain);

        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        env.EnableSchedulesForAllActors();
        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), 1024);

        const ui64 tabletA = TTestTxConfig::TxTablet0;
        const ui32 nodeA = 42;
        constexpr ui64 batchSize = 5'000'000;
        env.SeedTabletLocation(tabletA, nodeA);
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);

        for (int i = 0; i < 40; ++i) {
            const auto res = env.TryAdmit({ tabletA }, TDuration::Seconds(600), batchSize);
            UNIT_ASSERT_VALUES_EQUAL((int)res->Get()->GetDecision(), (int)EAdmitDecision::Wait);
        }
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(20));
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmValue("FlowControl/Drain/RefillRate"), 1000);

        // RMin was holding the anchor floor at the start rate; let it act.
        drain.RMin = 1.0;
        ApplyDrainParamsToFlowControl(runtime.GetAppData(0).ColumnShardConfig.MutableFlowControl(), drain);
        for (int i = 0; i < 100; ++i) {
            runtime.AdvanceCurrentTime(TDuration::MilliSeconds(100));
            runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(10));
        }

        const i64 served = env.ReadFcmValue("FlowControl/Drain/ServedRateCount");
        UNIT_ASSERT_C(served >= 1 && served <= 5, TStringBuilder() << "expected ~2 served req/s, got " << served);
        UNIT_ASSERT_C(env.ReadFcmDeriviative("FlowControl/Drain/AnchorGiveBack/Count") > 0, "anchor never engaged");
        const i64 rate = env.ReadFcmValue("FlowControl/Drain/RefillRate");
        UNIT_ASSERT_C(rate < 500, TStringBuilder() << "rate not pulled toward served throughput: " << rate);

        runtime.GetAppData(0).ColumnShardConfig.ClearFlowControl();
    }

    // Tokens accrue to the soft cap while admits are gated. Handing that whole budget out
    // in the instant the node reports READY is what re-overloads compaction, so the hot →
    // cool edge trims it.
    Y_UNIT_TEST(ReadyEdgeClampsCarriedOverTokens) {
        TFlowControlManagerServiceOperator::TDrainRateParams drain;
        drain.RMin = 100.0;   // = RStart: the hot-edge cut and the decay are clamped away
        drain.RMax = 0.0;
        drain.RStart = 100.0;
        drain.CubicProbePercent = 0.0;
        drain.RMinBytes = 100'000'000.0;
        drain.RStartBytes = 100'000'000.0;
        TFlowControlManagerServiceOperator::SetDrainRateParams(drain);

        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        env.EnableSchedulesForAllActors();
        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), 1024);

        const ui64 tabletA = TTestTxConfig::TxTablet0;
        const ui32 nodeA = 42;
        env.SeedTabletLocation(tabletA, nodeA);
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);

        for (int i = 0; i < 100; ++i) {
            UNIT_ASSERT_VALUES_EQUAL((int)env.TryAdmit({ tabletA })->Get()->GetDecision(), (int)EAdmitDecision::Wait);
        }

        // Sit hot long enough for the count bucket to fill to its one-second soft cap.
        runtime.AdvanceCurrentTime(TDuration::Seconds(5));
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmValue("FlowControl/Drain/RefillRate"), 100);

        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(5));

        int allowed = 0;
        for (;;) {
            TAutoPtr<IEventHandle> handle;
            const auto* drained = runtime.GrabEdgeEventRethrow<TEvTryAdmitResult>(handle, TDuration::MilliSeconds(1));
            if (!drained) {
                break;
            }
            UNIT_ASSERT_VALUES_EQUAL((int)drained->GetDecision(), (int)EAdmitDecision::Allow);
            ++allowed;
        }
        // Uncapped this dumps a full second of budget (100); the clamp keeps a quarter.
        UNIT_ASSERT_C(allowed >= 10 && allowed <= 45, TStringBuilder() << "released " << allowed << " at the READY edge");
    }

    // Post-Wmax probe adds ProbePercent of Wmax (not a fixed +N admits/s nail).
    Y_UNIT_TEST(CubicProbesAboveWmaxFractionally) {
        TFlowControlManagerServiceOperator::TDrainRateParams drain;
        // RMin* = RStart* so gating OVERLOADED does not cut either bucket.
        drain.RMin = 100.0;
        drain.RMax = 0.0;   // unset ceiling
        drain.RStart = 100.0;
        drain.CubicProbePercent = 5.0;
        drain.RMinBytes = 100'000'000.0;
        drain.RStartBytes = 100'000'000.0;
        TFlowControlManagerServiceOperator::SetDrainRateParams(drain);

        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        env.EnableSchedulesForAllActors();
        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), 1024);

        const ui64 tabletA = TTestTxConfig::TxTablet0;
        const ui32 nodeA = 42;
        env.SeedTabletLocation(tabletA, nodeA);
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(10));

        UNIT_ASSERT_VALUES_EQUAL((int)env.TryAdmit({ tabletA })->Get()->GetDecision(), (int)EAdmitDecision::Wait);
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(20));
        {
            const auto drained = runtime.GrabEdgeEventRethrow<TEvTryAdmitResult>(env.GetReplyTo());
            UNIT_ASSERT_VALUES_EQUAL((int)drained->Get()->GetDecision(), (int)EAdmitDecision::Allow);
        }

        // Growth needs a quiet window after the node cooled down (default K=10s ⇒ 2s).
        runtime.AdvanceCurrentTime(TDuration::Seconds(3));
        for (int i = 0; i < 100; ++i) {
            env.SendWriteOutcome(tabletA, nodeA, /*overloaded=*/false);
        }

        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/Drain/RateGrow/Count"), 1);
        // 100 + 5%*100 = 105; bytes 1e8 + 5%*1e8.
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmValue("FlowControl/Drain/RefillRate"), 105);
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmValue("FlowControl/Drain/RefillRateBytes"), 105'000'000);
    }

    // After a ×4 write-outcome cut, CUBIC recovers to ~Wmax in ~KTarget.
    Y_UNIT_TEST(CubicRecoversToWmaxInAboutKTarget) {
        auto runOnce = [](double wmax, double wmaxBytes) {
            constexpr double kTarget = 2.0;
            constexpr double beta = 0.25;   // ×4 cut

            TFlowControlManagerServiceOperator::TDrainRateParams drain;
            drain.RMin = wmax;   // gating OVERLOADED must not change rate
            drain.RMax = 0.0;
            drain.RStart = wmax;
            drain.RMinBytes = wmaxBytes;
            drain.RStartBytes = wmaxBytes;
            drain.AimdBeta = beta;
            drain.CubicRecoveryTargetSec = kTarget;
            drain.CubicProbePercent = 0.0;   // no post-Wmax climb
            TFlowControlManagerServiceOperator::SetDrainRateParams(drain);

            TTestBasicRuntime runtime;
            TFlowControlManagerTestEnv env(runtime);
            env.EnableSchedulesForAllActors();
            TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), 1024);

            const ui64 tabletA = TTestTxConfig::TxTablet0;
            const ui32 nodeA = 42;
            env.SeedTabletLocation(tabletA, nodeA);
            env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);
            runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(10));

            UNIT_ASSERT_VALUES_EQUAL((int)env.TryAdmit({ tabletA })->Get()->GetDecision(), (int)EAdmitDecision::Wait);
            env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY);
            runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(20));
            {
                const auto drained = runtime.GrabEdgeEventRethrow<TEvTryAdmitResult>(env.GetReplyTo());
                UNIT_ASSERT_VALUES_EQUAL((int)drained->Get()->GetDecision(), (int)EAdmitDecision::Allow);
            }

            // Drop RMin so the dirty-cohort cut can land; hot edge was clamped away above.
            // Nail the full drain knobs — Get* would otherwise fill unset fields from proto defaults.
            drain.RMin = 0.1;
            drain.RMinBytes = 1.0;
            ApplyDrainParamsToFlowControl(runtime.GetAppData(0).ColumnShardConfig.MutableFlowControl(), drain);
            const ui64 cutTarget = Max<ui64>(1, static_cast<ui64>(std::ceil(wmax)));
            for (ui64 i = 0; i < cutTarget; ++i) {
                env.SendWriteOutcome(tabletA, nodeA, /*overloaded=*/true);
            }
            const i64 afterCut = env.ReadFcmValue("FlowControl/Drain/RefillRate");
            UNIT_ASSERT_C(afterCut <= static_cast<i64>(wmax * beta + 1.0),
                TStringBuilder() << "expected ~" << (wmax * beta) << " after cut, got " << afterCut);

            // Idle time alone must not recover; then one clean cohort at t≥K lifts to Wmax.
            runtime.AdvanceCurrentTime(TDuration::MilliSeconds(static_cast<ui64>(kTarget * 1000) + 100));
            runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(10));
            UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmValue("FlowControl/Drain/RefillRate"), afterCut);

            // Re-open a cohort without lowering rate: RMin = afterCut clamps the hot cut.
            drain.RMin = static_cast<double>(afterCut);
            drain.RMinBytes = static_cast<double>(env.ReadFcmValue("FlowControl/Drain/RefillRateBytes"));
            ApplyDrainParamsToFlowControl(runtime.GetAppData(0).ColumnShardConfig.MutableFlowControl(), drain);
            env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);
            runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(10));
            UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmValue("FlowControl/Drain/RefillRate"), afterCut);
            UNIT_ASSERT_VALUES_EQUAL((int)env.TryAdmit({ tabletA })->Get()->GetDecision(), (int)EAdmitDecision::Wait);
            env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY);
            runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(20));
            {
                const auto drained = runtime.GrabEdgeEventRethrow<TEvTryAdmitResult>(env.GetReplyTo());
                UNIT_ASSERT_VALUES_EQUAL((int)drained->Get()->GetDecision(), (int)EAdmitDecision::Allow);
            }
            // Quiet window after the node cooled down (K=2s ⇒ 0.4s cooldown).
            runtime.AdvanceCurrentTime(TDuration::MilliSeconds(500));
            const ui64 growTarget = Max<ui64>(1, static_cast<ui64>(std::ceil(static_cast<double>(afterCut))));
            for (ui64 i = 0; i < growTarget; ++i) {
                env.SendWriteOutcome(tabletA, nodeA, /*overloaded=*/false);
            }

            const i64 recovered = env.ReadFcmValue("FlowControl/Drain/RefillRate");
            UNIT_ASSERT_C(
                recovered >= static_cast<i64>(wmax * 0.95), TStringBuilder() << "count Wmax=" << wmax << " recovered to " << recovered);
            const i64 recoveredBytes = env.ReadFcmValue("FlowControl/Drain/RefillRateBytes");
            UNIT_ASSERT_C(recoveredBytes >= static_cast<i64>(wmaxBytes * 0.95),
                TStringBuilder() << "bytes Wmax=" << wmaxBytes << " recovered to " << recoveredBytes);

            runtime.GetAppData(0).ColumnShardConfig.ClearFlowControl();
        };

        runOnce(/*wmax=*/4.0, /*wmaxBytes=*/4'000'000.0);
        runOnce(/*wmax=*/40.0, /*wmaxBytes=*/40'000'000.0);
    }

    // Tiny out-of-cohort overload nicks must not restart the CUBIC epoch.
    Y_UNIT_TEST(TinyOutOfCohortCutDoesNotResetEpoch) {
        constexpr double wmax = 100.0;
        constexpr double kTarget = 2.0;
        constexpr double beta = 0.5;

        TFlowControlManagerServiceOperator::TDrainRateParams drain;
        drain.RMin = wmax;   // gating OVERLOADED must not change rate (Max(RMin, R*β)=RMin=RStart)
        drain.RMax = 0.0;
        drain.RStart = wmax;
        drain.RStartBytes = 100'000'000.0;
        drain.AimdBeta = beta;
        drain.CubicRecoveryTargetSec = kTarget;
        drain.CubicProbePercent = 0.0;
        TFlowControlManagerServiceOperator::SetDrainRateParams(drain);

        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        env.EnableSchedulesForAllActors();
        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), 1024);

        const ui64 tabletA = TTestTxConfig::TxTablet0;
        const ui32 nodeA = 42;
        env.SeedTabletLocation(tabletA, nodeA);
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(10));

        UNIT_ASSERT_VALUES_EQUAL((int)env.TryAdmit({ tabletA })->Get()->GetDecision(), (int)EAdmitDecision::Wait);
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(20));
        {
            const auto drained = runtime.GrabEdgeEventRethrow<TEvTryAdmitResult>(env.GetReplyTo());
            UNIT_ASSERT_VALUES_EQUAL((int)drained->Get()->GetDecision(), (int)EAdmitDecision::Allow);
        }
        // Drop RMin so the in-cohort cut can land below start (config picked up on outcome Sync).
        drain.RMin = 1.0;
        ApplyDrainParamsToFlowControl(runtime.GetAppData(0).ColumnShardConfig.MutableFlowControl(), drain);
        for (int i = 0; i < 100; ++i) {
            env.SendWriteOutcome(tabletA, nodeA, /*overloaded=*/true);
        }
        const i64 afterCut = env.ReadFcmValue("FlowControl/Drain/RefillRate");
        UNIT_ASSERT_VALUES_EQUAL(afterCut, 50);

        // Nick the rate slightly without resetting the epoch, then finish recovery at t=K.
        for (int i = 0; i < 5; ++i) {
            env.SendWriteOutcome(tabletA, nodeA, /*overloaded=*/true);
        }
        const i64 afterNicks = env.ReadFcmValue("FlowControl/Drain/RefillRate");
        UNIT_ASSERT_C(afterNicks >= 45 && afterNicks < afterCut, TStringBuilder() << "expected tiny nick, got " << afterNicks);

        runtime.AdvanceCurrentTime(TDuration::MilliSeconds(static_cast<ui64>(kTarget * 1000) + 100));
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(10));
        // Raise RMin so the empty→hot AimdBeta cut clamps to RStart instead of cutting further.
        drain.RMin = 100.0;
        ApplyDrainParamsToFlowControl(runtime.GetAppData(0).ColumnShardConfig.MutableFlowControl(), drain);
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL((int)env.TryAdmit({ tabletA })->Get()->GetDecision(), (int)EAdmitDecision::Wait);
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(20));
        {
            const auto drained = runtime.GrabEdgeEventRethrow<TEvTryAdmitResult>(env.GetReplyTo());
            UNIT_ASSERT_VALUES_EQUAL((int)drained->Get()->GetDecision(), (int)EAdmitDecision::Allow);
        }
        const ui64 growTarget = Max<ui64>(1, static_cast<ui64>(std::ceil(static_cast<double>(afterNicks))));
        for (ui64 i = 0; i < growTarget; ++i) {
            env.SendWriteOutcome(tabletA, nodeA, /*overloaded=*/false);
        }

        // Original epoch still ends at Wmax; a reset would leave us far below.
        const i64 recovered = env.ReadFcmValue("FlowControl/Drain/RefillRate");
        UNIT_ASSERT_C(recovered >= static_cast<i64>(wmax * 0.95), TStringBuilder() << "epoch reset? recovered only to " << recovered);

        runtime.GetAppData(0).ColumnShardConfig.ClearFlowControl();
    }

    // An overloaded outcome inside the cohort must abort growth and cut instead.
    Y_UNIT_TEST(CohortWithOverloadDoesNotGrow) {
        TFlowControlManagerServiceOperator::TDrainRateParams drain;
        // RMin strictly below RStart so the proportional cut can actually lower the rate
        // (otherwise it clamps to RMin and no cut is counted).
        drain.RMin = 0.1;
        drain.RMax = 100.0;
        drain.RStart = 1.0;
        drain.CubicProbePercent = 5.0;
        drain.AimdBeta = 0.5;
        TFlowControlManagerServiceOperator::SetDrainRateParams(drain);

        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        env.EnableSchedulesForAllActors();
        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), 1024);

        const ui64 tabletA = TTestTxConfig::TxTablet0;
        const ui32 nodeA = 42;
        env.SeedTabletLocation(tabletA, nodeA);
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);

        UNIT_ASSERT_VALUES_EQUAL((int)env.TryAdmit({ tabletA })->Get()->GetDecision(), (int)EAdmitDecision::Wait);

        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(20));
        {
            const auto drained = runtime.GrabEdgeEventRethrow<TEvTryAdmitResult>(env.GetReplyTo());
            UNIT_ASSERT_VALUES_EQUAL((int)drained->Get()->GetDecision(), (int)EAdmitDecision::Allow);
        }

        env.SendWriteOutcome(tabletA, nodeA, /*overloaded=*/true);

        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/Drain/Outcome/Overloaded/Count"), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/Drain/RateGrow/Count"), 0);
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/Drain/CohortAborted/Count"), 1);
        // empty→hot AimdBeta cut + dirty-cohort cut
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/Drain/RateCut/Count"), 2);
    }

    // A single overloaded outcome outside an open cohort must NOT apply the full AimdBeta.
    // Historically fraction=1.0 halved both rates per stray shard-writer outcome; with write
    // fan-out that cascaded RefillRate down by orders of magnitude and starved the queue.
    Y_UNIT_TEST(OutOfCohortOverloadDoesNotHalveRate) {
        TFlowControlManagerServiceOperator::TDrainRateParams drain;
        drain.RMin = 1.0;
        drain.RMax = 0.0;   // unset ceiling
        drain.RStart = 100.0;
        drain.AimdBeta = 0.5;
        drain.CubicProbePercent = 0.0;
        drain.RMinBytes = 1.0;
        drain.RMaxBytes = 0.0;
        drain.RStartBytes = 100'000'000.0;
        TFlowControlManagerServiceOperator::SetDrainRateParams(drain);

        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);

        const ui64 tabletA = TTestTxConfig::TxTablet0;
        const ui32 nodeA = 42;
        env.SeedTabletLocation(tabletA, nodeA);

        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmValue("FlowControl/Drain/RefillRate"), 100);
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmValue("FlowControl/Drain/RefillRateBytes"), 100'000'000);

        // No cohort open (empty queue): a burst of overloaded shard outcomes must not
        // collapse the rate the way repeated full-beta cuts would (100 → 50 → 25 → …).
        for (int i = 0; i < 10; ++i) {
            env.SendWriteOutcome(tabletA, nodeA, /*overloaded=*/true);
        }

        // fraction = 1/ceil(100) per outcome ⇒ ~0.5% cut each; after 10 cuts ≈ 95, not 0.1.
        const i64 rateAfter = env.ReadFcmValue("FlowControl/Drain/RefillRate");
        UNIT_ASSERT_C(rateAfter >= 90, TStringBuilder() << "rate collapsed to " << rateAfter);
        const i64 bytesAfter = env.ReadFcmValue("FlowControl/Drain/RefillRateBytes");
        UNIT_ASSERT_C(bytesAfter >= 90'000'000, TStringBuilder() << "bytes rate collapsed to " << bytesAfter);
    }

    // Bytes soft cap must raise to the FIFO head's BatchSize, otherwise a request larger
    // than one second of RefillRateBytesR can never collect enough tokens and the queue stalls.
    Y_UNIT_TEST(LargeBatchDrainsDespiteBytesSoftCap) {
        TFlowControlManagerServiceOperator::TDrainRateParams drain;
        drain.RMin = 1.0;
        drain.RMax = 1.0;
        drain.RStart = 1.0;
        drain.CubicProbePercent = 0.0;
        drain.RMinBytes = 1'000'000.0;   // 1 MB/s
        drain.RMaxBytes = 1'000'000.0;
        drain.RStartBytes = 1'000'000.0;
        TFlowControlManagerServiceOperator::SetDrainRateParams(drain);

        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        env.EnableSchedulesForAllActors();
        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), 1024);

        const ui64 tabletA = TTestTxConfig::TxTablet0;
        const ui32 nodeA = 42;
        env.SeedTabletLocation(tabletA, nodeA);
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);

        // Small head seeds TokensBytes to ~1 MB (the rate soft cap). Then a 5 MB waiter
        // sits behind it: after the small one drains, the soft cap must rise to 5 MB and
        // refill must be allowed to accumulate that much — otherwise the queue deadlocks.
        UNIT_ASSERT_VALUES_EQUAL(
            (int)env.TryAdmit({ tabletA }, TDuration::Seconds(60), /*batchSize=*/1)->Get()->GetDecision(), (int)EAdmitDecision::Wait);
        constexpr ui64 largeBatch = 5'000'000;
        UNIT_ASSERT_VALUES_EQUAL(
            (int)env.TryAdmit({ tabletA }, TDuration::Seconds(60), largeBatch)->Get()->GetDecision(), (int)EAdmitDecision::Wait);

        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(20));
        {
            // Small head drains immediately from the seeded cohort.
            const auto drained = runtime.GrabEdgeEventRethrow<TEvTryAdmitResult>(env.GetReplyTo());
            UNIT_ASSERT_VALUES_EQUAL((int)drained->Get()->GetDecision(), (int)EAdmitDecision::Allow);
        }

        // Without the soft-cap raise, TokensBytes would clamp at 1 MB forever and this
        // Allow would never arrive. Accrue > largeBatch / rate (+ one count token).
        runtime.AdvanceCurrentTime(TDuration::Seconds(6));
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));
        {
            const auto drained = runtime.GrabEdgeEventRethrow<TEvTryAdmitResult>(env.GetReplyTo());
            UNIT_ASSERT_VALUES_EQUAL((int)drained->Get()->GetDecision(), (int)EAdmitDecision::Allow);
        }
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/WaitQueue/Drained/Count"), 2);
    }

    // Retry-by-subscription may turn an overloaded write into a final STATUS_COMPLETED.
    // The outcome still reports Overloaded=true, so it must not be counted as clean.
    Y_UNIT_TEST(RetriedThenSucceededDoesNotGrowRate) {
        TFlowControlManagerServiceOperator::TDrainRateParams drain;
        drain.RMin = 1.0;
        drain.RMax = 100.0;
        drain.RStart = 1.0;
        drain.CubicProbePercent = 5.0;
        TFlowControlManagerServiceOperator::SetDrainRateParams(drain);

        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        env.EnableSchedulesForAllActors();
        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), 1024);

        const ui64 tabletA = TTestTxConfig::TxTablet0;
        const ui32 nodeA = 42;
        env.SeedTabletLocation(tabletA, nodeA);
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);

        UNIT_ASSERT_VALUES_EQUAL((int)env.TryAdmit({ tabletA })->Get()->GetDecision(), (int)EAdmitDecision::Wait);

        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(20));
        {
            const auto drained = runtime.GrabEdgeEventRethrow<TEvTryAdmitResult>(env.GetReplyTo());
            UNIT_ASSERT_VALUES_EQUAL((int)drained->Get()->GetDecision(), (int)EAdmitDecision::Allow);
        }

        // Write eventually succeeded, but only after being overloaded and retried twice.
        env.SendWriteOutcome(tabletA, nodeA, /*overloaded=*/true, /*retries=*/2);

        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/Drain/RateGrow/Count"), 0);
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/Drain/CohortAborted/Count"), 1);
    }

    // Outcomes must not grow the rate before a whole cohort has reported back.
    Y_UNIT_TEST(PartialCohortDoesNotGrowRate) {
        TFlowControlManagerServiceOperator::TDrainRateParams drain;
        drain.RMin = 3.0;
        drain.RMax = 100.0;
        drain.RStart = 3.0;   // cohort target = 3
        drain.CubicProbePercent = 5.0;
        TFlowControlManagerServiceOperator::SetDrainRateParams(drain);

        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        env.EnableSchedulesForAllActors();
        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), 1024);

        const ui64 tabletA = TTestTxConfig::TxTablet0;
        const ui32 nodeA = 42;
        env.SeedTabletLocation(tabletA, nodeA);
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);

        for (int i = 0; i < 3; ++i) {
            UNIT_ASSERT_VALUES_EQUAL((int)env.TryAdmit({ tabletA })->Get()->GetDecision(), (int)EAdmitDecision::Wait);
        }

        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));
        // Past the hot cooldown, so only cohort completeness decides growth here.
        runtime.AdvanceCurrentTime(TDuration::Seconds(3));

        // Only two of the three cohort members report back.
        env.SendWriteOutcome(tabletA, nodeA, /*overloaded=*/false);
        env.SendWriteOutcome(tabletA, nodeA, /*overloaded=*/false);

        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/Drain/Outcome/Ok/Count"), 2);
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/Drain/RateGrow/Count"), 0);

        // The third completes the cohort cleanly => exactly one growth step.
        env.SendWriteOutcome(tabletA, nodeA, /*overloaded=*/false);
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/Drain/RateGrow/Count"), 1);
    }

    Y_UNIT_TEST(AllowsWhenHotNodeBecomesReady) {
        TTestBasicRuntime runtime;
        const ui64 shardTabletId = TTestTxConfig::TxTablet0;
        const ui32 hotNodeId = 42;

        bool writeObserved = false;
        InstallFakeShardWriteResponder(runtime, shardTabletId, MakeFakeShardWriteCompleted, &writeObserved);

        TFlowControlManagerTestEnv env(runtime);
        env.SeedTabletLocation(shardTabletId, hotNodeId);
        env.SeedNodeOverloadStatus(hotNodeId, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);
        env.SeedNodeOverloadStatus(hotNodeId, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY);

        env.StartLongTxWrite(env.BuildLongTxWrite(MakeNavigateForSingleColumnShard(shardTabletId), MakeHappyPathBatch()));

        const auto completed = env.WaitCompleted();
        UNIT_ASSERT(writeObserved);
        UNIT_ASSERT_VALUES_EQUAL((Ydb::StatusIds::StatusCode)completed->Get()->Status, Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(WriterFeedsLocationThenGatesOnHotNode) {
        TTestBasicRuntime runtime;
        const ui64 shardTabletId = TTestTxConfig::TxTablet0;

        bool firstWriteObserved = false;
        InstallFakeShardWriteResponder(runtime, shardTabletId, MakeFakeShardWriteCompleted, &firstWriteObserved);

        TFlowControlManagerTestEnv env(runtime);
        runtime.GetAppData(0).FeatureFlags.SetEnableCsFlowControl(true);

        bool locationSeen = false;
        ui64 locationTabletId = 0;
        ui32 locationNodeId = 0;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvTabletLocationUpdated::EventType) {
                const auto* msg = event->Get<TEvTabletLocationUpdated>();
                locationSeen = true;
                locationTabletId = msg->GetTabletId();
                locationNodeId = msg->GetNodeId();
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        env.StartLongTxWrite(env.BuildLongTxWrite(MakeNavigateForSingleColumnShard(shardTabletId), MakeHappyPathBatch()));
        {
            const auto completed = env.WaitCompleted();
            UNIT_ASSERT(firstWriteObserved);
            UNIT_ASSERT_VALUES_EQUAL((Ydb::StatusIds::StatusCode)completed->Get()->Status, Ydb::StatusIds::SUCCESS);
        }

        UNIT_ASSERT(locationSeen);
        UNIT_ASSERT_VALUES_EQUAL(locationTabletId, shardTabletId);
        UNIT_ASSERT(locationNodeId != 0);

        env.SeedNodeOverloadStatus(locationNodeId, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);
        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), /*maxWaitQueueSize=*/0);

        bool secondWriteObserved = false;
        InstallFakeShardWriteResponder(runtime, shardTabletId, MakeFakeShardWriteCompleted, &secondWriteObserved);
        env.StartLongTxWrite(env.BuildLongTxWrite(MakeNavigateForSingleColumnShard(shardTabletId), MakeHappyPathBatch()));
        {
            const auto completed = env.WaitCompleted();
            UNIT_ASSERT(!secondWriteObserved);
            UNIT_ASSERT_VALUES_EQUAL((Ydb::StatusIds::StatusCode)completed->Get()->Status, Ydb::StatusIds::OVERLOADED);
        }
    }

    Y_UNIT_TEST(LocationRecheckUpdatesMapForNextAttempt) {
        TTestBasicRuntime runtime;
        const ui64 shardTabletId = TTestTxConfig::TxTablet0;
        const ui32 hotNodeId = 42;
        const ui32 coolNodeId = 43;

        bool writeObserved = false;
        InstallFakeShardWriteResponder(runtime, shardTabletId, MakeFakeShardWriteCompleted, &writeObserved);

        TFlowControlManagerTestEnv env(runtime);
        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), /*maxWaitQueueSize=*/0);
        env.SeedTabletLocation(shardTabletId, hotNodeId);
        env.SeedNodeOverloadStatus(hotNodeId, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);

        bool recheckSeen = false;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvTabletResolver::TEvForward::EventType) {
                const auto* forward = event->Get<TEvTabletResolver::TEvForward>();
                if (forward->TabletID == shardTabletId && !forward->Ev) {
                    recheckSeen = true;
                    runtime.Send(new IEventHandle(event->Sender, TActorId(coolNodeId, 0, shardTabletId, 0),
                        new TEvTabletResolver::TEvForwardResult(
                            shardTabletId, TActorId(coolNodeId, 0, shardTabletId, 0), TActorId(), /*cacheEpoch=*/1)));
                    return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        // Gated attempt still OVERLOADED, but kicks off A1 recheck.
        env.StartLongTxWrite(env.BuildLongTxWrite(MakeNavigateForSingleColumnShard(shardTabletId), MakeHappyPathBatch()));
        {
            const auto completed = env.WaitCompleted();
            UNIT_ASSERT(!writeObserved);
            UNIT_ASSERT_VALUES_EQUAL((Ydb::StatusIds::StatusCode)completed->Get()->Status, Ydb::StatusIds::OVERLOADED);
        }
        UNIT_ASSERT(recheckSeen);

        // After recheck maps tablet to cool node, next attempt is admitted (hotNodes still has hotNodeId).
        env.StartLongTxWrite(env.BuildLongTxWrite(MakeNavigateForSingleColumnShard(shardTabletId), MakeHappyPathBatch()));
        {
            const auto completed = env.WaitCompleted();
            UNIT_ASSERT(writeObserved);
            UNIT_ASSERT_VALUES_EQUAL((Ydb::StatusIds::StatusCode)completed->Get()->Status, Ydb::StatusIds::SUCCESS);
        }
    }

    Y_UNIT_TEST(OverloadManagerPushesStatusToFlowControl) {
        TTestBasicRuntime runtime;
        const ui64 shardTabletId = TTestTxConfig::TxTablet0;

        bool writeObserved = false;
        InstallFakeShardWriteResponder(runtime, shardTabletId, MakeFakeShardWriteCompleted, &writeObserved);

        TFlowControlManagerTestEnv env(runtime);
        runtime.GetAppData(0).FeatureFlags.SetEnableCsFlowControl(true);

        const ui32 localNodeId = runtime.GetNodeId(0);
        const TActorId seedSender = env.GetReplyTo();
        // Drop nameservice ListNodes/NodesInfo so a late Bootstrap refresh cannot Sync READY over OM push.
        runtime.SetEventFilter([seedSender](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvInterconnect::TEvListNodes::EventType) {
                return true;
            }
            if (ev->GetTypeRewrite() == TEvInterconnect::TEvNodesInfo::EventType && ev->Sender != seedSender) {
                return true;
            }
            return false;
        });
        SeedOverloadManagerNodes(runtime, seedSender, { localNodeId });

        bool statusSeen = false;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvNodeOverloadStatus::EventType) {
                const auto& record = event->Get<TEvNodeOverloadStatus>()->Record;
                if (event->Sender.NodeId() == localNodeId &&
                    record.GetStatus() == NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED)
                {
                    statusSeen = true;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        env.SeedTabletLocation(shardTabletId, localNodeId);
        runtime.Send(new IEventHandle(NOverload::TOverloadManagerServiceOperator::MakeServiceId(), seedSender,
                         new NOverload::TEvCompactionOverloadState(shardTabletId, true)), 0, true);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));

        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), /*maxWaitQueueSize=*/0);
        env.StartLongTxWrite(env.BuildLongTxWrite(MakeNavigateForSingleColumnShard(shardTabletId), MakeHappyPathBatch()));
        {
            const auto completed = env.WaitCompleted();
            UNIT_ASSERT(statusSeen);
            UNIT_ASSERT(!writeObserved);
            UNIT_ASSERT_VALUES_EQUAL((Ydb::StatusIds::StatusCode)completed->Get()->Status, Ydb::StatusIds::OVERLOADED);
        }

        // Clear compaction flag so process-global OM state does not leak into later UTs.
        runtime.Send(new IEventHandle(NOverload::TOverloadManagerServiceOperator::MakeServiceId(), seedSender,
                         new NOverload::TEvCompactionOverloadState(shardTabletId, false)), 0, true);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));
    }

    Y_UNIT_TEST(WriterReportsInvalidateOnDeliveryProblem) {
        TTestBasicRuntime runtime;
        const ui64 shardTabletId = TTestTxConfig::TxTablet0;

        bool writeObserved = false;
        InstallFakeShardDeliveryProblem(runtime, shardTabletId, &writeObserved);

        TFlowControlManagerTestEnv env(runtime);
        runtime.GetAppData(0).FeatureFlags.SetEnableCsFlowControl(true);
        DisableShardWriteRetries(runtime);

        bool invalidateSeen = false;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvTabletLocationInvalidated::EventType) {
                UNIT_ASSERT_VALUES_EQUAL(event->Get<TEvTabletLocationInvalidated>()->GetTabletId(), shardTabletId);
                invalidateSeen = true;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        env.StartLongTxWrite(env.BuildLongTxWrite(MakeNavigateForSingleColumnShard(shardTabletId), MakeHappyPathBatch()));
        const auto completed = env.WaitCompleted();
        UNIT_ASSERT(writeObserved);
        UNIT_ASSERT(invalidateSeen);
        UNIT_ASSERT_VALUES_EQUAL((Ydb::StatusIds::StatusCode)completed->Get()->Status, Ydb::StatusIds::UNAVAILABLE);
    }

    Y_UNIT_TEST(InvalidatedLocationFailOpensDespiteHotNode) {
        TTestBasicRuntime runtime;
        const ui64 shardTabletId = TTestTxConfig::TxTablet0;
        const ui32 hotNodeId = 42;

        bool writeObserved = false;
        InstallFakeShardWriteResponder(runtime, shardTabletId, MakeFakeShardWriteCompleted, &writeObserved);

        TFlowControlManagerTestEnv env(runtime);
        env.SeedTabletLocation(shardTabletId, hotNodeId);
        env.SeedNodeOverloadStatus(hotNodeId, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);
        env.SendToFlowControlManager(new TEvTabletLocationInvalidated(shardTabletId));

        env.StartLongTxWrite(env.BuildLongTxWrite(MakeNavigateForSingleColumnShard(shardTabletId), MakeHappyPathBatch()));
        const auto completed = env.WaitCompleted();
        UNIT_ASSERT(writeObserved);
        UNIT_ASSERT_VALUES_EQUAL((Ydb::StatusIds::StatusCode)completed->Get()->Status, Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(FeatureFlagOffDoesNotReportLocation) {
        TTestBasicRuntime runtime;
        const ui64 shardTabletId = TTestTxConfig::TxTablet0;

        bool writeObserved = false;
        InstallFakeShardWriteResponder(runtime, shardTabletId, MakeFakeShardWriteCompleted, &writeObserved);

        TFlowControlManagerTestEnv env(runtime);
        runtime.GetAppData(0).FeatureFlags.SetEnableCsFlowControl(false);

        bool locationSeen = false;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvTabletLocationUpdated::EventType) {
                locationSeen = true;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        env.StartLongTxWrite(env.BuildLongTxWrite(MakeNavigateForSingleColumnShard(shardTabletId), MakeHappyPathBatch()));
        const auto completed = env.WaitCompleted();
        UNIT_ASSERT(writeObserved);
        UNIT_ASSERT(!locationSeen);
        UNIT_ASSERT_VALUES_EQUAL((Ydb::StatusIds::StatusCode)completed->Get()->Status, Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(FeatureFlagOffDoesNotPublishFromOverloadManager) {
        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        runtime.GetAppData(0).FeatureFlags.SetEnableCsFlowControl(false);

        const ui32 localNodeId = runtime.GetNodeId(0);
        SeedOverloadManagerNodes(runtime, env.GetReplyTo(), { localNodeId });

        bool statusSeen = false;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvNodeOverloadStatus::EventType) {
                statusSeen = true;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        runtime.Send(new IEventHandle(NOverload::TOverloadManagerServiceOperator::MakeServiceId(), env.GetReplyTo(),
                         new NOverload::TEvPublishNodeOverloadStatus(NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED)), 0, true);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));
        UNIT_ASSERT(!statusSeen);
    }

    Y_UNIT_TEST(FeatureFlagOffLegacyPathIgnoresFcmHotMap) {
        TTestBasicRuntime runtime;
        const ui64 shardTabletId = TTestTxConfig::TxTablet0;
        const ui32 hotNodeId = 42;

        bool writeObserved = false;
        InstallFakeShardWriteResponder(runtime, shardTabletId, MakeFakeShardWriteCompleted, &writeObserved);

        TFlowControlManagerTestEnv env(runtime);
        runtime.GetAppData(0).FeatureFlags.SetEnableCsFlowControl(false);
        env.SeedTabletLocation(shardTabletId, hotNodeId);
        env.SeedNodeOverloadStatus(hotNodeId, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);

        // Legacy BulkUpsert path: DoLongTxWriteSameMailbox with FF off must not consult FCM admit.
        env.DoLongTxWriteSameMailbox(env.BuildLongTxWrite(MakeNavigateForSingleColumnShard(shardTabletId), MakeHappyPathBatch()));
        const auto completed = env.WaitCompleted();
        UNIT_ASSERT(writeObserved);
        UNIT_ASSERT_VALUES_EQUAL((Ydb::StatusIds::StatusCode)completed->Get()->Status, Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST(RequestResourcesPublishesOverloadAndReady) {
        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        runtime.GetAppData(0).FeatureFlags.SetEnableCsFlowControl(true);
        runtime.GetAppData(0).ColumnShardConfig.SetWritingInFlightRequestsCountLimit(1);

        const ui32 localNodeId = runtime.GetNodeId(0);
        SeedOverloadManagerNodes(runtime, env.GetReplyTo(), { localNodeId });

        bool overloadedSeen = false;
        bool readySeen = false;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvNodeOverloadStatus::EventType) {
                const auto status = event->Get<TEvNodeOverloadStatus>()->Record.GetStatus();
                if (status == NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED) {
                    overloadedSeen = true;
                } else if (status == NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY) {
                    readySeen = true;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        runtime.Register(new TRequestReleaseResourcesActor(/*writesCount=*/1, /*writesSize=*/0,
                             TRequestReleaseResourcesActor::EMode::RequestOnly), 0, runtime.GetAppData(0).UserPoolId);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));
        UNIT_ASSERT(overloadedSeen);

        runtime.Register(new TRequestReleaseResourcesActor(/*writesCount=*/1, /*writesSize=*/0,
                             TRequestReleaseResourcesActor::EMode::ReleaseOnly), 0, runtime.GetAppData(0).UserPoolId);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));
        UNIT_ASSERT(readySeen);
    }

    Y_UNIT_TEST(OverloadManagerRefreshesNodesListOnWakeup) {
        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);

        ui32 listNodesCount = 0;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvInterconnect::TEvListNodes::EventType) {
                ++listNodesCount;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        // Bootstrap already requested ListNodes; allow that to be observed.
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));
        const ui32 afterBootstrap = listNodesCount;
        UNIT_ASSERT_C(afterBootstrap >= 1, "expected ListNodes from OM Bootstrap");

        // Drive the refresh path directly: Schedule(60s) would need EnableScheduleForActor + a long
        // AdvanceCurrentTime, which makes this UT wall-clock heavy under the default dispatcher.
        runtime.Send(
            new IEventHandle(NOverload::TOverloadManagerServiceOperator::MakeServiceId(), env.GetReplyTo(), new TEvents::TEvWakeup()), 0, true);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));
        UNIT_ASSERT_C(listNodesCount > afterBootstrap, "expected another ListNodes on Wakeup");
    }

    Y_UNIT_TEST(CompactionOverloadPublishesOverloadAndReady) {
        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        runtime.GetAppData(0).FeatureFlags.SetEnableCsFlowControl(true);

        const ui32 localNodeId = runtime.GetNodeId(0);
        SeedOverloadManagerNodes(runtime, env.GetReplyTo(), { localNodeId });

        bool overloadedSeen = false;
        bool readySeen = false;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvNodeOverloadStatus::EventType) {
                const auto status = event->Get<TEvNodeOverloadStatus>()->Record.GetStatus();
                if (status == NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED) {
                    overloadedSeen = true;
                } else if (status == NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY) {
                    readySeen = true;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        constexpr ui64 tabletId = 42;
        runtime.Send(new IEventHandle(NOverload::TOverloadManagerServiceOperator::MakeServiceId(), env.GetReplyTo(),
                         new NOverload::TEvCompactionOverloadState(tabletId, true)), 0, true);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));
        UNIT_ASSERT(overloadedSeen);
        UNIT_ASSERT(NOverload::TOverloadManagerServiceOperator::IsCompactionOverloaded());

        // Writes still OK → clearing compaction should publish READY.
        readySeen = false;
        runtime.Send(new IEventHandle(NOverload::TOverloadManagerServiceOperator::MakeServiceId(), env.GetReplyTo(),
                         new NOverload::TEvCompactionOverloadState(tabletId, false)), 0, true);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));
        UNIT_ASSERT(readySeen);
        UNIT_ASSERT(!NOverload::TOverloadManagerServiceOperator::IsCompactionOverloaded());
    }

    Y_UNIT_TEST(CompactionReadySuppressedWhileWritesOverloaded) {
        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        runtime.GetAppData(0).FeatureFlags.SetEnableCsFlowControl(true);
        runtime.GetAppData(0).ColumnShardConfig.SetWritingInFlightRequestsCountLimit(1);

        const ui32 localNodeId = runtime.GetNodeId(0);
        SeedOverloadManagerNodes(runtime, env.GetReplyTo(), { localNodeId });

        bool readySeen = false;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvNodeOverloadStatus::EventType) {
                const auto status = event->Get<TEvNodeOverloadStatus>()->Record.GetStatus();
                if (status == NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY) {
                    readySeen = true;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        // Cross write limit first.
        runtime.Register(new TRequestReleaseResourcesActor(/*writesCount=*/1, /*writesSize=*/0,
                             TRequestReleaseResourcesActor::EMode::RequestOnly), 0, runtime.GetAppData(0).UserPoolId);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));

        constexpr ui64 tabletId = 42;
        runtime.Send(new IEventHandle(NOverload::TOverloadManagerServiceOperator::MakeServiceId(), env.GetReplyTo(),
                         new NOverload::TEvCompactionOverloadState(tabletId, true)), 0, true);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));

        readySeen = false;
        runtime.Send(new IEventHandle(NOverload::TOverloadManagerServiceOperator::MakeServiceId(), env.GetReplyTo(),
                         new NOverload::TEvCompactionOverloadState(tabletId, false)), 0, true);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));
        UNIT_ASSERT_C(!readySeen, "READY must not clear FCM while write resources are still overloaded");
        UNIT_ASSERT(!NOverload::TOverloadManagerServiceOperator::IsCompactionOverloaded());

        // Cleanup process-wide write resource counters for other UTs.
        runtime.Register(new TRequestReleaseResourcesActor(/*writesCount=*/1, /*writesSize=*/0,
                             TRequestReleaseResourcesActor::EMode::ReleaseOnly), 0, runtime.GetAppData(0).UserPoolId);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));
    }

    Y_UNIT_TEST(NodesInfoRefreshDoesNotStickOverloadAfterReady) {
        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        runtime.GetAppData(0).FeatureFlags.SetEnableCsFlowControl(true);

        const ui32 localNodeId = runtime.GetNodeId(0);
        SeedOverloadManagerNodes(runtime, env.GetReplyTo(), { localNodeId });

        ui32 overloadedCount = 0;
        ui32 readyCount = 0;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvNodeOverloadStatus::EventType) {
                const auto status = event->Get<TEvNodeOverloadStatus>()->Record.GetStatus();
                if (status == NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED) {
                    ++overloadedCount;
                } else if (status == NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY) {
                    ++readyCount;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        constexpr ui64 tabletId = 7;
        runtime.Send(new IEventHandle(NOverload::TOverloadManagerServiceOperator::MakeServiceId(), env.GetReplyTo(),
                         new NOverload::TEvCompactionOverloadState(tabletId, true)), 0, true);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));
        UNIT_ASSERT_C(overloadedCount >= 1, "expected OVERLOADED on compaction enter");

        runtime.Send(new IEventHandle(NOverload::TOverloadManagerServiceOperator::MakeServiceId(), env.GetReplyTo(),
                         new NOverload::TEvCompactionOverloadState(tabletId, false)), 0, true);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));
        UNIT_ASSERT_C(readyCount >= 1, "expected READY on compaction leave");

        const ui32 overloadedBeforeRefresh = overloadedCount;
        const ui32 readyBeforeRefresh = readyCount;

        // Periodic ListNodes refresh must re-publish READY (current truth): an FCM that saw
        // OVERLOADED and missed the cool edge has no other way to heal HotNodes.
        SeedOverloadManagerNodes(runtime, env.GetReplyTo(), { localNodeId });
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));

        UNIT_ASSERT_VALUES_EQUAL(overloadedCount, overloadedBeforeRefresh);
        UNIT_ASSERT_C(readyCount > readyBeforeRefresh, "refresh should re-push READY from current state");

        // While OVERLOADED the same refresh must re-assert for late FCMs.
        runtime.Send(new IEventHandle(NOverload::TOverloadManagerServiceOperator::MakeServiceId(), env.GetReplyTo(),
                         new NOverload::TEvCompactionOverloadState(tabletId, true)), 0, true);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));
        const ui32 overloadedBeforeHotRefresh = overloadedCount;
        UNIT_ASSERT_C(overloadedBeforeHotRefresh > overloadedBeforeRefresh, "expected OVERLOADED on re-enter");

        const ui32 readyBeforeHotRefresh = readyCount;
        SeedOverloadManagerNodes(runtime, env.GetReplyTo(), { localNodeId });
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));
        UNIT_ASSERT_C(overloadedCount > overloadedBeforeHotRefresh, "refresh must re-push OVERLOADED so late FCMs learn the hot node");
        UNIT_ASSERT_VALUES_EQUAL(readyCount, readyBeforeHotRefresh);
    }

    Y_UNIT_TEST(OverloadManagerPushesToAllCachedNodes) {
        TTestBasicRuntime runtime(/*nodeCount=*/2);
        TFlowControlManagerTestEnv env(runtime);
        runtime.GetAppData(0).FeatureFlags.SetEnableCsFlowControl(true);

        // Second node's FCM (env registers services on node 0 only).
        {
            auto actor = TFlowControlManagerServiceOperator::CreateService(runtime.GetDynamicCounters(1));
            const auto actorId = runtime.Register(actor.release(), 1, runtime.GetAppData(1).UserPoolId, TMailboxType::Revolving, 0);
            runtime.RegisterService(TFlowControlManagerServiceOperator::MakeServiceId(runtime.GetNodeId(1)), actorId, 1);
        }

        const TActorId seedSender = env.GetReplyTo();
        runtime.SetEventFilter([seedSender](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvInterconnect::TEvListNodes::EventType) {
                return true;
            }
            if (ev->GetTypeRewrite() == TEvInterconnect::TEvNodesInfo::EventType && ev->Sender != seedSender) {
                return true;
            }
            return false;
        });

        const ui32 node0 = runtime.GetNodeId(0);
        const ui32 node1 = runtime.GetNodeId(1);
        SeedOverloadManagerNodes(runtime, seedSender, { node0, node1 });
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));

        THashSet<ui32> statusRecipientNodeIds;
        ui32 statusCount = 0;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvNodeOverloadStatus::EventType) {
                ++statusCount;
                statusRecipientNodeIds.insert(event->Recipient.NodeId());
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        runtime.Send(new IEventHandle(NOverload::TOverloadManagerServiceOperator::MakeServiceId(), env.GetReplyTo(),
                         new NOverload::TEvPublishNodeOverloadStatus(NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED)), 0, true);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));

        UNIT_ASSERT_VALUES_EQUAL(statusCount, 2);
        UNIT_ASSERT(statusRecipientNodeIds.contains(node0));
        UNIT_ASSERT(statusRecipientNodeIds.contains(node1));
    }

    Y_UNIT_TEST(DelayedRejectWhenWaitQueueFull) {
        TTestBasicRuntime runtime;
        const ui64 shardTabletId = TTestTxConfig::TxTablet0;
        const ui32 hotNodeId = 42;

        bool writeObserved = false;
        InstallFakeShardWriteResponder(runtime, shardTabletId, MakeFakeShardWriteCompleted, &writeObserved);

        TFlowControlManagerTestEnv env(runtime);
        env.EnableSchedulesForAllActors();
        // Set wait queue size to 1, delayed-reject queue size to 2.
        // Delayed reject fires at DelayedRejectTimeoutPercent of the operation timeout; pin it to 10%
        // so the reject lands at 6s (10% of 60s), which the 7s advance below crosses.
        TFlowControlManagerServiceOperator::SetWaitQueueParams(
            TDuration::Zero(), TDuration::Zero(), /*maxWaitQueueSize=*/1, /*maxDelayedRejectQueueSize=*/2);
        TFlowControlManagerServiceOperator::SetDelayedRejectTimeoutPercent(10);
        env.SeedTabletLocation(shardTabletId, hotNodeId);
        env.SeedNodeOverloadStatus(hotNodeId, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);

        // First request occupies the only wait slot (stays queued).
        env.StartLongTxWrite(
            env.BuildLongTxWrite(MakeNavigateForSingleColumnShard(shardTabletId), MakeHappyPathBatch(), TDuration::Seconds(60)));
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(20));

        // Second request should go to delayed-reject queue (wait queue full).
        const auto replyTo2 = runtime.AllocateEdgeActor();
        {
            auto issues = std::make_shared<NYql::TIssues>();
            TLongTxId longTxId;
            Y_ABORT_UNLESS(longTxId.ParseString("ydb://long-tx/01ezvvxjdk2hd4vdgjs68knvp8?node_id=2"));
            const TInstant now = runtime.GetCurrentTime();
            auto tx = TLongTxWrite(replyTo2, longTxId, "2", "/Root", "/Root/table", MakeNavigateForSingleColumnShard(shardTabletId),
                MakeHappyPathBatch(), std::move(issues),
                NACLib::TUserContextBuilder().Build(), now + TDuration::Seconds(60), TDuration::Seconds(60));
            env.StartLongTxWrite(std::move(tx));
        }

        // Should not get immediate response (delayed reject)
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(20));
        UNIT_ASSERT(!writeObserved);

        // Advance time to trigger delayed reject (10% of 60s = 6s)
        runtime.AdvanceCurrentTime(TDuration::Seconds(7));
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));

        const auto completed2 = runtime.GrabEdgeEventRethrow<TEvents::TEvCompleted>(replyTo2);
        UNIT_ASSERT_VALUES_EQUAL((Ydb::StatusIds::StatusCode)completed2->Get()->Status, Ydb::StatusIds::OVERLOADED);
    }

    Y_UNIT_TEST(DelayedRejectQueueFullRejectsImmediately) {
        TTestBasicRuntime runtime;
        const ui64 shardTabletId = TTestTxConfig::TxTablet0;
        const ui32 hotNodeId = 42;

        bool writeObserved = false;
        InstallFakeShardWriteResponder(runtime, shardTabletId, MakeFakeShardWriteCompleted, &writeObserved);

        TFlowControlManagerTestEnv env(runtime);
        // Set wait queue size to 1, delayed-reject queue size to 1
        TFlowControlManagerServiceOperator::SetWaitQueueParams(
            TDuration::Zero(), TDuration::Zero(), /*maxWaitQueueSize=*/1, /*maxDelayedRejectQueueSize=*/1);
        env.SeedTabletLocation(shardTabletId, hotNodeId);
        env.SeedNodeOverloadStatus(hotNodeId, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);

        // First request occupies the only wait slot.
        env.StartLongTxWrite(
            env.BuildLongTxWrite(MakeNavigateForSingleColumnShard(shardTabletId), MakeHappyPathBatch(), TDuration::Seconds(60)));
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(20));

        // Second request goes to delayed-reject queue.
        const auto replyTo2 = runtime.AllocateEdgeActor();
        {
            auto issues = std::make_shared<NYql::TIssues>();
            TLongTxId longTxId;
            Y_ABORT_UNLESS(longTxId.ParseString("ydb://long-tx/01ezvvxjdk2hd4vdgjs68knvp8?node_id=2"));
            const TInstant now = runtime.GetCurrentTime();
            auto tx = TLongTxWrite(replyTo2, longTxId, "2", "/Root", "/Root/table", MakeNavigateForSingleColumnShard(shardTabletId),
                MakeHappyPathBatch(), std::move(issues),
                NACLib::TUserContextBuilder().Build(), now + TDuration::Seconds(60), TDuration::Seconds(60));
            env.StartLongTxWrite(std::move(tx));
        }
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(20));

        // Third request should be rejected immediately (both queues full).
        const auto replyTo3 = runtime.AllocateEdgeActor();
        {
            auto issues = std::make_shared<NYql::TIssues>();
            TLongTxId longTxId;
            Y_ABORT_UNLESS(longTxId.ParseString("ydb://long-tx/01ezvvxjdk2hd4vdgjs68knvp8?node_id=3"));
            const TInstant now = runtime.GetCurrentTime();
            auto tx = TLongTxWrite(replyTo3, longTxId, "3", "/Root", "/Root/table", MakeNavigateForSingleColumnShard(shardTabletId),
                MakeHappyPathBatch(), std::move(issues),
                NACLib::TUserContextBuilder().Build(), now + TDuration::Seconds(60), TDuration::Seconds(60));
            env.StartLongTxWrite(std::move(tx));
        }

        // Should get immediate reject
        const auto completed3 = runtime.GrabEdgeEventRethrow<TEvents::TEvCompleted>(replyTo3);
        UNIT_ASSERT(!writeObserved);
        UNIT_ASSERT_VALUES_EQUAL((Ydb::StatusIds::StatusCode)completed3->Get()->Status, Ydb::StatusIds::OVERLOADED);
    }

    Y_UNIT_TEST(DelayedRejectFiresAfterConfiguredDelay) {
        TTestBasicRuntime runtime;
        const ui64 shardTabletId = TTestTxConfig::TxTablet0;
        const ui32 hotNodeId = 42;

        bool writeObserved = false;
        InstallFakeShardWriteResponder(runtime, shardTabletId, MakeFakeShardWriteCompleted, &writeObserved);

        TFlowControlManagerTestEnv env(runtime);
        env.EnableSchedulesForAllActors();
        // Set delayed-reject percent to 10% (so delayed reject fires at 10% of operation timeout)
        TFlowControlManagerServiceOperator::SetWaitQueueParams(
            TDuration::Zero(), TDuration::Zero(), /*maxWaitQueueSize=*/1, /*maxDelayedRejectQueueSize=*/10);
        TFlowControlManagerServiceOperator::SetDelayedRejectTimeoutPercent(10);
        env.SeedTabletLocation(shardTabletId, hotNodeId);
        env.SeedNodeOverloadStatus(hotNodeId, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);

        // First request occupies the wait slot.
        env.StartLongTxWrite(
            env.BuildLongTxWrite(MakeNavigateForSingleColumnShard(shardTabletId), MakeHappyPathBatch(), TDuration::Seconds(100)));
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(20));

        // Second request goes to delayed-reject queue with 100s operation timeout.
        const auto replyTo2 = runtime.AllocateEdgeActor();
        const TInstant startTime = runtime.GetCurrentTime();
        {
            auto issues = std::make_shared<NYql::TIssues>();
            TLongTxId longTxId;
            Y_ABORT_UNLESS(longTxId.ParseString("ydb://long-tx/01ezvvxjdk2hd4vdgjs68knvp8?node_id=2"));
            auto tx = TLongTxWrite(replyTo2, longTxId, "2", "/Root", "/Root/table", MakeNavigateForSingleColumnShard(shardTabletId),
                MakeHappyPathBatch(), std::move(issues),
                NACLib::TUserContextBuilder().Build(), startTime + TDuration::Seconds(100), TDuration::Seconds(100));
            env.StartLongTxWrite(std::move(tx));
        }
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(20));

        // Should fire after 10% of 100s = 10s
        runtime.AdvanceCurrentTime(TDuration::Seconds(11));
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));

        const auto completed2 = runtime.GrabEdgeEventRethrow<TEvents::TEvCompleted>(replyTo2);
        UNIT_ASSERT_VALUES_EQUAL((Ydb::StatusIds::StatusCode)completed2->Get()->Status, Ydb::StatusIds::OVERLOADED);
    }

    Y_UNIT_TEST(DelayedRejectDropsArrowBatchImmediately) {
        TTestBasicRuntime runtime;
        const ui64 shardTabletId = TTestTxConfig::TxTablet0;
        const ui32 hotNodeId = 42;

        bool writeObserved = false;
        InstallFakeShardWriteResponder(runtime, shardTabletId, MakeFakeShardWriteCompleted, &writeObserved);

        TFlowControlManagerTestEnv env(runtime);
        env.EnableSchedulesForAllActors();
        // Pin the reject delay to 10% so it fires at 6s (10% of 60s), crossed by the 7s advance below.
        TFlowControlManagerServiceOperator::SetWaitQueueParams(
            TDuration::Zero(), TDuration::Zero(), /*maxWaitQueueSize=*/1, /*maxDelayedRejectQueueSize=*/10);
        TFlowControlManagerServiceOperator::SetDelayedRejectTimeoutPercent(10);
        env.SeedTabletLocation(shardTabletId, hotNodeId);
        env.SeedNodeOverloadStatus(hotNodeId, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);

        // First request occupies the wait slot.
        env.StartLongTxWrite(
            env.BuildLongTxWrite(MakeNavigateForSingleColumnShard(shardTabletId), MakeHappyPathBatch(), TDuration::Seconds(60)));
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(20));

        // Second request goes to delayed-reject queue.
        // The Arrow batch should be dropped immediately (not held in memory).
        const auto replyTo2 = runtime.AllocateEdgeActor();
        {
            auto issues = std::make_shared<NYql::TIssues>();
            TLongTxId longTxId;
            Y_ABORT_UNLESS(longTxId.ParseString("ydb://long-tx/01ezvvxjdk2hd4vdgjs68knvp8?node_id=2"));
            const TInstant now = runtime.GetCurrentTime();
            auto tx = TLongTxWrite(replyTo2, longTxId, "2", "/Root", "/Root/table", MakeNavigateForSingleColumnShard(shardTabletId),
                MakeHappyPathBatch(), std::move(issues),
                NACLib::TUserContextBuilder().Build(), now + TDuration::Seconds(60), TDuration::Seconds(60));
            env.StartLongTxWrite(std::move(tx));
        }
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(20));

        // The helper actor should have finished immediately (dropped the batch).
        // We verify this by checking that no write was attempted to the shard.
        UNIT_ASSERT(!writeObserved);

        // Advance time to trigger delayed reject
        runtime.AdvanceCurrentTime(TDuration::Seconds(7));
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));

        const auto completed2 = runtime.GrabEdgeEventRethrow<TEvents::TEvCompleted>(replyTo2);
        UNIT_ASSERT_VALUES_EQUAL((Ydb::StatusIds::StatusCode)completed2->Get()->Status, Ydb::StatusIds::OVERLOADED);
        UNIT_ASSERT(!writeObserved);   // Still no write attempted
    }

    Y_UNIT_TEST(MultipleDelayedRejectsFireIndependently) {
        TTestBasicRuntime runtime;
        const ui64 shardTabletId = TTestTxConfig::TxTablet0;
        const ui32 hotNodeId = 42;

        bool writeObserved = false;
        InstallFakeShardWriteResponder(runtime, shardTabletId, MakeFakeShardWriteCompleted, &writeObserved);

        TFlowControlManagerTestEnv env(runtime);
        env.EnableSchedulesForAllActors();
        TFlowControlManagerServiceOperator::SetWaitQueueParams(
            TDuration::Zero(), TDuration::Zero(), /*maxWaitQueueSize=*/1, /*maxDelayedRejectQueueSize=*/10);
        TFlowControlManagerServiceOperator::SetDelayedRejectTimeoutPercent(10);
        env.SeedTabletLocation(shardTabletId, hotNodeId);
        env.SeedNodeOverloadStatus(hotNodeId, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);

        // First request occupies the wait slot.
        env.StartLongTxWrite(
            env.BuildLongTxWrite(MakeNavigateForSingleColumnShard(shardTabletId), MakeHappyPathBatch(), TDuration::Seconds(100)));
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(20));

        // Second request: 100s timeout → fires at 10s
        const auto replyTo2 = runtime.AllocateEdgeActor();
        const TInstant startTime = runtime.GetCurrentTime();
        {
            auto issues = std::make_shared<NYql::TIssues>();
            TLongTxId longTxId;
            Y_ABORT_UNLESS(longTxId.ParseString("ydb://long-tx/01ezvvxjdk2hd4vdgjs68knvp8?node_id=2"));
            auto tx = TLongTxWrite(replyTo2, longTxId, "2", "/Root", "/Root/table", MakeNavigateForSingleColumnShard(shardTabletId),
                MakeHappyPathBatch(), std::move(issues),
                NACLib::TUserContextBuilder().Build(), startTime + TDuration::Seconds(100), TDuration::Seconds(100));
            env.StartLongTxWrite(std::move(tx));
        }
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(20));

        // Third request: 50s timeout → fires at 5s
        const auto replyTo3 = runtime.AllocateEdgeActor();
        {
            auto issues = std::make_shared<NYql::TIssues>();
            TLongTxId longTxId;
            Y_ABORT_UNLESS(longTxId.ParseString("ydb://long-tx/01ezvvxjdk2hd4vdgjs68knvp8?node_id=3"));
            const TInstant now = runtime.GetCurrentTime();
            auto tx = TLongTxWrite(replyTo3, longTxId, "3", "/Root", "/Root/table", MakeNavigateForSingleColumnShard(shardTabletId),
                MakeHappyPathBatch(), std::move(issues),
                NACLib::TUserContextBuilder().Build(), now + TDuration::Seconds(50), TDuration::Seconds(50));
            env.StartLongTxWrite(std::move(tx));
        }
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(20));

        // Advance to 6s: third request should fire
        runtime.AdvanceCurrentTime(TDuration::Seconds(6));
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));

        const auto completed3 = runtime.GrabEdgeEventRethrow<TEvents::TEvCompleted>(replyTo3);
        UNIT_ASSERT_VALUES_EQUAL((Ydb::StatusIds::StatusCode)completed3->Get()->Status, Ydb::StatusIds::OVERLOADED);

        // Advance to 11s: second request should fire (10% of 100s)
        runtime.AdvanceCurrentTime(TDuration::Seconds(5));
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));

        const auto completed2 = runtime.GrabEdgeEventRethrow<TEvents::TEvCompleted>(replyTo2);
        UNIT_ASSERT_VALUES_EQUAL((Ydb::StatusIds::StatusCode)completed2->Get()->Status, Ydb::StatusIds::OVERLOADED);
    }

}   // Y_UNIT_TEST_SUITE(TFlowControlManager)

}   // namespace
}   // namespace NKikimr
