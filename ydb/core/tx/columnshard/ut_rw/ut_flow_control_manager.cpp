#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/columnshard/flow_control_manager/flow_control_manager_events.h>
#include <ydb/core/tx/columnshard/flow_control_manager/flow_control_manager_service.h>
#include <ydb/core/tx/columnshard/flow_control_manager/flow_control_manager_types.h>
#include <ydb/core/tx/columnshard/overload_manager/overload_manager_service.h>
#include <ydb/core/tx/columnshard/test_helper/columnshard_ut_common.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/long_tx_service/public/types.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx.h>

#include <ydb/library/aclib/user_context.h>
#include <ydb/library/actors/core/events.h>

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
        ctx.Send(TFlowControlManagerServiceOperator::MakeServiceId(), std::make_unique<TEvLongTxWrite>(std::move(LongTxWrite)));
        PassAway();
    }

private:
    TLongTxWrite LongTxWrite;
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
            Runtime.RegisterService(TFlowControlManagerServiceOperator::MakeServiceId(), actorId, 0);
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
                rt.Schedule(new IEventHandle(ev->Sender, TActorId(ev->Sender.NodeId(), shardTabletId), factory(shardTabletId).release()),
                    TDuration::Zero());
                return true;
            }
        }
        return false;
    });
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

}   // Y_UNIT_TEST_SUITE(TFlowControlManager)

}   // namespace
}   // namespace NKikimr
