#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/formats/arrow/arrow_batch_builder.h>
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
#include <ydb/core/tx/tx_proxy/upload_rows_common_impl.h>

#include <ydb/library/aclib/user_context.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/protos/interconnect.pb.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <library/cpp/testing/unittest/registar.h>

#include <functional>

namespace NKikimr {
namespace {

using namespace NActors;
using namespace NColumnShard;
using namespace NColumnShard::NFlowControl;
using namespace NLongTxService;
using namespace NTxUT;

class TStartLongTxWriteActor: public TActorBootstrapped<TStartLongTxWriteActor> {
public:
    explicit TStartLongTxWriteActor(TLongTxWrite longTxWrite)
        : LongTxWrite(std::move(longTxWrite))
    {
    }

    void Bootstrap(const TActorContext& ctx) {
        TFlowControlManagerServiceOperator::StartLongTxWrite(ctx, std::move(LongTxWrite));
        PassAway();
    }

private:
    TLongTxWrite LongTxWrite;
};

class TSendLongTxWriteEventActor: public TActorBootstrapped<TSendLongTxWriteEventActor> {
public:
    explicit TSendLongTxWriteEventActor(TLongTxWrite longTxWrite)
        : LongTxWrite(std::move(longTxWrite))
    {
    }

    void Bootstrap(const TActorContext& ctx) {
        ctx.Send(
            TFlowControlManagerServiceOperator::MakeServiceId(ctx.SelfID.NodeId()), std::make_unique<TEvLongTxWrite>(std::move(LongTxWrite)));
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
        // Pin the wait-timeout percent to the historical UT default (50%) so tests are deterministic
        // regardless of the process-wide default (which now matches production tuning at 10%).
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
        TFlowControlManagerServiceOperator::ResetDrainRateParamsToDefaults();
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

    void SendLongTxWriteEvent(TLongTxWrite longTxWrite) {
        Runtime.Register(new TSendLongTxWriteEventActor(std::move(longTxWrite)), 0, Runtime.GetAppData(0).UserPoolId);
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

    void SeedNodeOverloadStatus(ui32 nodeId, NKikimrTxColumnShard::TEvNodeOverloadStatus::EStatus status, ui64 generation = 1) {
        SendToFlowControlManager(new TEvNodeOverloadStatus(nodeId, status, generation));
    }

    // Emulate TShardWriter reporting a terminal per-request write outcome. This is the
    // only feedback that moves the drain rate, so tests drive it directly.
    void SendWriteOutcome(ui64 tabletId, ui32 nodeId, bool overloaded, ui32 retries = 0) {
        SendToFlowControlManager(new TEvWriteOutcome(tabletId, nodeId, overloaded, retries));
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

    Y_UNIT_TEST(ServiceReceivesLongTxWriteEvent) {
        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);

        env.SendLongTxWriteEvent(env.BuildLongTxWrite(MakeNavigateWithSchemeError()));

        const auto completed = env.WaitCompleted();
        UNIT_ASSERT_VALUES_EQUAL((Ydb::StatusIds::StatusCode)completed->Get()->Status, Ydb::StatusIds::SCHEME_ERROR);
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

        env.SeedNodeOverloadStatus(hotNodeId, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY, /*generation=*/1);

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

        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY, /*generation=*/1);
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
        drain.AimdAdd = 0.0;   // do not grow during the test
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

        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY, /*generation=*/1);
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
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY, /*generation=*/1);
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
        drain.AimdAdd = 0.0;
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

        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY, /*generation=*/1);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(20));
        {
            const auto drained = runtime.GrabEdgeEventRethrow<TEvTryAdmitResult>(env.GetReplyTo());
            UNIT_ASSERT_VALUES_EQUAL((int)drained->Get()->GetDecision(), (int)EAdmitDecision::Allow);
        }

        // OVERLOADED marks the node hot (gating only; the rate itself now moves on
        // per-request outcomes); further READY still drains the remaining waiters.
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED, /*generation=*/2);
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY, /*generation=*/2);
        runtime.AdvanceCurrentTime(TDuration::MilliSeconds(100));
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));
        {
            const auto drained = runtime.GrabEdgeEventRethrow<TEvTryAdmitResult>(env.GetReplyTo());
            UNIT_ASSERT_VALUES_EQUAL((int)drained->Get()->GetDecision(), (int)EAdmitDecision::Allow);
        }
    }

    Y_UNIT_TEST(DrainRateDoesNotGrowWithoutTraffic) {
        // AimdAdd > 0 but no traffic: growth is decided only by completed cohorts of
        // per-request write outcomes, so elapsed wall-clock time and repeated drain
        // scheduling must never raise the rate on their own.
        TFlowControlManagerServiceOperator::TDrainRateParams drain;
        drain.RMin = 10.0;
        drain.RMax = 500.0;
        drain.RStart = 10.0;
        drain.AimdAdd = 50.0;
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
            env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY, /*generation=*/1 + i);
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
        drain.AimdAdd = 0.0;
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
        fc->SetDrainAimdAdd(0);
        fc->SetDrainAimdBeta(0.5);

        // Drive a drain cycle (RefillTokens -> SyncDrainBounds) via STATUS_READY.
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY, /*generation=*/1);
        runtime.AdvanceCurrentTime(TDuration::MilliSeconds(50));
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(10));

        // RefillRate must be clamped from the seeded 200 down to the config RMax of 100.
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmValue("FlowControl/Drain/RefillRate"), 100);
    }

    // A partial FlowControl YAML (queue sizes only) must NOT apply protobuf DrainRateMax
    // defaults as a silent nail: unset HasDrainRateMax ⇒ RMax stays 0 (no ceiling).
    Y_UNIT_TEST(UnsetDrainRateMaxDoesNotCapFromProtoDefault) {
        TFlowControlManagerServiceOperator::TDrainRateParams drain;
        drain.RMin = 0.0;
        drain.RMax = 0.0;   // unset
        drain.RStart = 200.0;
        drain.AimdAdd = 0.0;
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
        UNIT_ASSERT(!fc->HasDrainRateMin());
        UNIT_ASSERT(!fc->HasDrainRateMaxBytes());
        UNIT_ASSERT(!fc->HasDrainRateMinBytes());

        const ui64 tabletA = TTestTxConfig::TxTablet0;
        const ui32 nodeA = 42;
        env.SeedTabletLocation(tabletA, nodeA);

        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY, /*generation=*/1);
        runtime.AdvanceCurrentTime(TDuration::MilliSeconds(50));
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(10));

        // Seeded 200 must remain; old bug clamped to proto default 500's absence as 500,
        // or would have left rate alone — assert we did NOT invent a 500 cap by reading
        // GetDrainRateMax() without Has*. Rate stays at construction seed 200.
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmValue("FlowControl/Drain/RefillRate"), 200);

        // And GetDrainRateParams reports unset max for both buckets.
        const auto params = TFlowControlManagerServiceOperator::GetDrainRateParams();
        UNIT_ASSERT_VALUES_EQUAL(params.RMax, 0.0);
        UNIT_ASSERT_VALUES_EQUAL(params.RMin, 0.0);
        UNIT_ASSERT_VALUES_EQUAL(params.RMaxBytes, 0.0);
        UNIT_ASSERT_VALUES_EQUAL(params.RMinBytes, 0.0);
    }

    // drain_aimd_add / drain_aimd_beta from FlowControl apply to BOTH count and bytes buckets.
    Y_UNIT_TEST(SharedConfigAimdAppliesToBothBuckets) {
        TFlowControlManagerServiceOperator::TDrainRateParams drain;
        drain.RMin = 0.0;
        drain.RMax = 0.0;
        drain.RStart = 100.0;
        drain.RStartBytes = 100'000'000.0;
        drain.AimdAdd = 1.0;   // will be overridden by config
        TFlowControlManagerServiceOperator::SetDrainRateParams(drain);

        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        env.EnableSchedulesForAllActors();
        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), 1024);

        auto* fc = runtime.GetAppData(0).ColumnShardConfig.MutableFlowControl();
        fc->SetDrainAimdAdd(10.0);   // +10% both buckets
        fc->SetDrainAimdBeta(0.5);

        const auto params = TFlowControlManagerServiceOperator::GetDrainRateParams();
        UNIT_ASSERT_DOUBLES_EQUAL(params.AimdAdd, 10.0, 1e-9);
        UNIT_ASSERT_DOUBLES_EQUAL(params.AimdAddBytes, 10.0, 1e-9);
        UNIT_ASSERT_DOUBLES_EQUAL(params.AimdBeta, 0.5, 1e-9);
        UNIT_ASSERT_DOUBLES_EQUAL(params.AimdBetaBytes, 0.5, 1e-9);

        const ui64 tabletA = TTestTxConfig::TxTablet0;
        const ui32 nodeA = 42;
        env.SeedTabletLocation(tabletA, nodeA);
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);
        UNIT_ASSERT_VALUES_EQUAL((int)env.TryAdmit({ tabletA })->Get()->GetDecision(), (int)EAdmitDecision::Wait);
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY, /*generation=*/1);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(20));
        {
            const auto drained = runtime.GrabEdgeEventRethrow<TEvTryAdmitResult>(env.GetReplyTo());
            UNIT_ASSERT_VALUES_EQUAL((int)drained->Get()->GetDecision(), (int)EAdmitDecision::Allow);
        }
        for (int i = 0; i < 100; ++i) {
            env.SendWriteOutcome(tabletA, nodeA, /*overloaded=*/false);
        }
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/Drain/RateGrow/Count"), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmValue("FlowControl/Drain/RefillRate"), 110);
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmValue("FlowControl/Drain/RefillRateBytes"), 110'000'000);
    }

    // Explicit bytes max from config clamps RefillRateBytes on the next SyncDrainBounds.
    Y_UNIT_TEST(DrainRateMaxBytesFromConfigAppliedLive) {
        TFlowControlManagerServiceOperator::TDrainRateParams drain;
        drain.RMin = 0.0;
        drain.RMax = 0.0;
        drain.RStart = 50.0;
        drain.RStartBytes = 200'000'000.0;
        drain.AimdAdd = 0.0;
        TFlowControlManagerServiceOperator::SetDrainRateParams(drain);

        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        env.EnableSchedulesForAllActors();

        auto* fc = runtime.GetAppData(0).ColumnShardConfig.MutableFlowControl();
        fc->SetDrainRateMaxBytes(100'000'000.0);

        const ui32 nodeA = 42;
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY, /*generation=*/1);
        runtime.AdvanceCurrentTime(TDuration::MilliSeconds(50));
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(10));

        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmValue("FlowControl/Drain/RefillRateBytes"), 100'000'000);
    }

    // Growth is decided purely by counting per-request outcomes, so a full clean cohort
    // must raise the rate without advancing simulated time at all.
    Y_UNIT_TEST(CohortAllOkGrowsRateWithoutTimeAdvance) {
        TFlowControlManagerServiceOperator::TDrainRateParams drain;
        drain.RMin = 1.0;
        drain.RMax = 100.0;
        drain.RStart = 1.0;   // cohort target = ceil(1.0) = 1
        drain.AimdAdd = 5.0;
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
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY, /*generation=*/1);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(20));
        {
            const auto drained = runtime.GrabEdgeEventRethrow<TEvTryAdmitResult>(env.GetReplyTo());
            UNIT_ASSERT_VALUES_EQUAL((int)drained->Get()->GetDecision(), (int)EAdmitDecision::Allow);
        }

        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/Drain/RateGrow/Count"), 0);

        // A clean outcome completes the cohort => percent increase, no time advanced.
        env.SendWriteOutcome(tabletA, nodeA, /*overloaded=*/false);

        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/Drain/Outcome/Ok/Count"), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/Drain/RateGrow/Count"), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/Drain/CohortAborted/Count"), 0);
        // AimdAdd=5% of RStart=1 ⇒ ~1.05 (gauge rounds to 1).
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmValue("FlowControl/Drain/RefillRate"), 1);
    }

    // Percent growth must scale with the current rate (not a fixed +N admits/s nail).
    Y_UNIT_TEST(PercentGrowthScalesWithCurrentRate) {
        TFlowControlManagerServiceOperator::TDrainRateParams drain;
        drain.RMin = 1.0;
        drain.RMax = 0.0;   // unset ceiling
        drain.RStart = 100.0;
        drain.AimdAdd = 5.0;   // +5%
        drain.RStartBytes = 100'000'000.0;
        drain.AimdAddBytes = 5.0;
        TFlowControlManagerServiceOperator::SetDrainRateParams(drain);

        TTestBasicRuntime runtime;
        TFlowControlManagerTestEnv env(runtime);
        env.EnableSchedulesForAllActors();
        TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration::Zero(), TDuration::Zero(), 1024);

        const ui64 tabletA = TTestTxConfig::TxTablet0;
        const ui32 nodeA = 42;
        env.SeedTabletLocation(tabletA, nodeA);
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);

        // Open a cohort of size ceil(100)=100 by draining 100 waiters is heavy; instead force
        // a cohort target of 1 via temporary low rate... Use RStart=100 and send 100 ok outcomes
        // after releasing one waiter that opens the cohort — NoteCohortRelease only on drain.
        // Simpler: RStart=1 for cohort size 1, but seed rate to 100 via... can't.
        // Drain one waiter at RStart=100 opens cohort with target 100; send 100 ok outcomes.
        for (int i = 0; i < 1; ++i) {
            UNIT_ASSERT_VALUES_EQUAL((int)env.TryAdmit({ tabletA })->Get()->GetDecision(), (int)EAdmitDecision::Wait);
        }
        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY, /*generation=*/1);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(20));
        {
            const auto drained = runtime.GrabEdgeEventRethrow<TEvTryAdmitResult>(env.GetReplyTo());
            UNIT_ASSERT_VALUES_EQUAL((int)drained->Get()->GetDecision(), (int)EAdmitDecision::Allow);
        }

        for (int i = 0; i < 100; ++i) {
            env.SendWriteOutcome(tabletA, nodeA, /*overloaded=*/false);
        }

        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/Drain/RateGrow/Count"), 1);
        // 100 * 1.05 = 105 (count); 1e8 * 1.05 = 1.05e8 (bytes).
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmValue("FlowControl/Drain/RefillRate"), 105);
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmValue("FlowControl/Drain/RefillRateBytes"), 105'000'000);
    }

    // An overloaded outcome inside the cohort must abort growth and cut instead.
    Y_UNIT_TEST(CohortWithOverloadDoesNotGrow) {
        TFlowControlManagerServiceOperator::TDrainRateParams drain;
        // RMin strictly below RStart so the proportional cut can actually lower the rate
        // (otherwise it clamps to RMin and no cut is counted).
        drain.RMin = 0.1;
        drain.RMax = 100.0;
        drain.RStart = 1.0;
        drain.AimdAdd = 5.0;
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

        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY, /*generation=*/1);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(20));
        {
            const auto drained = runtime.GrabEdgeEventRethrow<TEvTryAdmitResult>(env.GetReplyTo());
            UNIT_ASSERT_VALUES_EQUAL((int)drained->Get()->GetDecision(), (int)EAdmitDecision::Allow);
        }

        env.SendWriteOutcome(tabletA, nodeA, /*overloaded=*/true);

        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/Drain/Outcome/Overloaded/Count"), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/Drain/RateGrow/Count"), 0);
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/Drain/CohortAborted/Count"), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.ReadFcmDeriviative("FlowControl/Drain/RateCut/Count"), 1);
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
        drain.AimdAdd = 0.0;
        drain.RMinBytes = 1.0;
        drain.RMaxBytes = 0.0;
        drain.RStartBytes = 100'000'000.0;
        drain.AimdBetaBytes = 0.5;
        drain.AimdAddBytes = 0.0;
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
        drain.AimdAdd = 0.0;
        drain.RMinBytes = 1'000'000.0;   // 1 MB/s
        drain.RMaxBytes = 1'000'000.0;
        drain.RStartBytes = 1'000'000.0;
        drain.AimdAddBytes = 0.0;
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

        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY, /*generation=*/1);
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
        drain.AimdAdd = 5.0;
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

        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY, /*generation=*/1);
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
        drain.AimdAdd = 5.0;
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

        env.SeedNodeOverloadStatus(nodeA, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY, /*generation=*/1);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));

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
        env.SeedNodeOverloadStatus(hotNodeId, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_READY, /*generation=*/1);

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
                if (record.GetNodeId() == localNodeId && record.GetStatus() == NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED)
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

        // Simulate periodic ListNodes refresh: must re-publish READY (current truth), not stale OVERLOADED.
        SeedOverloadManagerNodes(runtime, env.GetReplyTo(), { localNodeId });
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));

        UNIT_ASSERT_VALUES_EQUAL(overloadedCount, overloadedBeforeRefresh);
        UNIT_ASSERT_C(readyCount > readyBeforeRefresh, "refresh should re-push READY from current state");
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
