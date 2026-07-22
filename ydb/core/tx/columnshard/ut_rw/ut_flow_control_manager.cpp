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
            tx.GetNavigateResult(), tx.GetBatch(), tx.GetIssues(), tx.GetUserCtx(), /*forceNoFlowControl=*/false);
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
        TTester::Setup(Runtime);
        RegisterServices();
        ReplyTo = Runtime.AllocateEdgeActor();
    }

    TActorId GetReplyTo() const {
        return ReplyTo;
    }

    TLongTxWrite BuildLongTxWrite(
        std::shared_ptr<const NSchemeCache::TSchemeCacheNavigate> navigateResult, std::shared_ptr<arrow::RecordBatch> batch = nullptr) const {
        auto issues = std::make_shared<NYql::TIssues>();
        if (!batch) {
            batch = MakeEmptyBatch();
        }
        TLongTxId longTxId;
        Y_ABORT_UNLESS(longTxId.ParseString("ydb://long-tx/01ezvvxjdk2hd4vdgjs68knvp8?node_id=1"));
        return TLongTxWrite(ReplyTo, longTxId, "0", "/Root", "/Root/table", std::move(navigateResult), std::move(batch), std::move(issues),
            NACLib::TUserContextBuilder().Build());
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

    void SeedTabletLocation(ui64 tabletId, ui32 nodeId) {
        SendToFlowControlManager(new TEvTabletLocationUpdated(tabletId, nodeId));
    }

    void SeedNodeOverloadStatus(ui32 nodeId, NKikimrTxColumnShard::TEvNodeOverloadStatus::EStatus status, ui64 generation = 1) {
        SendToFlowControlManager(new TEvNodeOverloadStatus(nodeId, status, generation));
    }

    TEvents::TEvCompleted::TPtr WaitCompleted() {
        return Runtime.GrabEdgeEventRethrow<TEvents::TEvCompleted>(ReplyTo);
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
            Runtime.RegisterService(NOverload::TOverloadManagerServiceOperator::MakeServiceId(), actorId, 0);
        }
        {
            auto actor = TFlowControlManagerServiceOperator::CreateService(counters);
            const auto actorId = Runtime.Register(actor.release(), 0, appData.UserPoolId, TMailboxType::Revolving, 0);
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
        env.SeedTabletLocation(shardTabletId, hotNodeId);
        env.SeedNodeOverloadStatus(hotNodeId, NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED);

        env.StartLongTxWrite(env.BuildLongTxWrite(MakeNavigateForSingleColumnShard(shardTabletId), MakeHappyPathBatch()));

        const auto completed = env.WaitCompleted();
        UNIT_ASSERT(!writeObserved);
        UNIT_ASSERT_VALUES_EQUAL((Ydb::StatusIds::StatusCode)completed->Get()->Status, Ydb::StatusIds::OVERLOADED);
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
        auto nodes = MakeIntrusive<TIntrusiveVector<TEvInterconnect::TNodeInfo>>();
        nodes->emplace_back(TEvInterconnect::TNodeInfo(localNodeId, "::", "localhost", "localhost", 1234, TNodeLocation()));
        runtime.Send(new IEventHandle(NOverload::TOverloadManagerServiceOperator::MakeServiceId(), env.GetReplyTo(),
                         new TEvInterconnect::TEvNodesInfo(nodes)), 0, true);

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
        runtime.Send(new IEventHandle(NOverload::TOverloadManagerServiceOperator::MakeServiceId(), env.GetReplyTo(),
                         new NOverload::TEvPublishNodeOverloadStatus(NKikimrTxColumnShard::TEvNodeOverloadStatus::STATUS_OVERLOADED)), 0, true);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(100));

        env.StartLongTxWrite(env.BuildLongTxWrite(MakeNavigateForSingleColumnShard(shardTabletId), MakeHappyPathBatch()));
        {
            const auto completed = env.WaitCompleted();
            UNIT_ASSERT(statusSeen);
            UNIT_ASSERT(!writeObserved);
            UNIT_ASSERT_VALUES_EQUAL((Ydb::StatusIds::StatusCode)completed->Get()->Status, Ydb::StatusIds::OVERLOADED);
        }
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

}   // Y_UNIT_TEST_SUITE(TFlowControlManager)

}   // namespace
}   // namespace NKikimr
