#include "schema_operation.h"
#include "schema.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ::NSchema {

namespace {

constexpr ui64 SCHEME_SHARD_TABLET = 72075186233409545ull;
constexpr ui64 TX_ID = 42;

struct TPipeStats {
    size_t NotifyForwards = 0;
    size_t DeliveryProblems = 0;
};

class TFakePipeCacheActor: public NActors::TActorBootstrapped<TFakePipeCacheActor> {
public:
    enum class EMode { Complete, FailAlways, FailThenComplete };

    TFakePipeCacheActor(EMode mode, TPipeStats* stats)
        : Mode(mode)
        , Stats(stats)
    {
    }

    void Bootstrap() {
        Become(&TFakePipeCacheActor::StateWork);
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvPipeCache::TEvForward, Handle);
        IgnoreFunc(TEvPipeCache::TEvUnlink);
    )

private:
    void Handle(TEvPipeCache::TEvForward::TPtr& ev) {
        if (!ev->Get()->Ev) {
            return;
        }
        if (ev->Get()->Ev->Type() != NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion::EventType) {
            return;
        }

        ++Stats->NotifyForwards;
        const ui64 tabletId = ev->Get()->TabletId;
        const ui64 subscribeCookie = ev->Get()->Options.SubscribeCookie;

        if (Mode == EMode::FailAlways ||
            (Mode == EMode::FailThenComplete && Stats->NotifyForwards <= 2))
        {
            ++Stats->DeliveryProblems;
            Send(ev->Sender, new TEvPipeCache::TEvDeliveryProblem(tabletId, true), 0, subscribeCookie);
            return;
        }

        Send(ev->Sender, new NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult(TX_ID), 0, ev->Cookie);
    }

    EMode Mode;
    TPipeStats* Stats;
};

struct TDummyTxProxyActor: public NActors::TActorBootstrapped<TDummyTxProxyActor> {
    void Bootstrap() {
        Become(&TDummyTxProxyActor::StateWork);
    }

    STRICT_STFUNC(StateWork,
        IgnoreFunc(TEvTxUserProxy::TEvProposeTransaction);
    )
};

struct TSchemaOpEnv {
    NActors::TTestBasicRuntime Runtime;
    TPipeStats PipeStats;
    size_t ProposeCount = 0;
    TActorId Edge;
    TActorId ActorId;

    explicit TSchemaOpEnv(TFakePipeCacheActor::EMode pipeMode = TFakePipeCacheActor::EMode::Complete)
        : Runtime(1, false)
    {
        Runtime.Initialize(TAppPrepare().Unwrap());
        Runtime.UpdateCurrentTime(TInstant::Now());
        Runtime.SetLogPriority(NKikimrServices::PQ_SCHEMA, NActors::NLog::PRI_DEBUG);
        Runtime.SetScheduledLimit(10000);

        Runtime.RegisterService(MakeTxProxyID(), Runtime.Register(new TDummyTxProxyActor()));
        auto pipeCacheId = Runtime.Register(new TFakePipeCacheActor(pipeMode, &PipeStats));
        Runtime.RegisterService(MakePipePerNodeCacheID(false), pipeCacheId);

        Runtime.SetObserverFunc([this](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvTxUserProxy::TEvProposeTransaction::EventType) {
                ++ProposeCount;
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        Edge = Runtime.AllocateEdgeActor();
        auto operation = std::make_unique<TEvTxUserProxy::TEvProposeTransaction>();
        operation->Record.SetDatabaseName("/Root");
        ActorId = Runtime.Register(CreateSchemaOperation(
            Edge, "/Root/topic", std::move(operation), /*cookie=*/7));
        Runtime.EnableScheduleForActor(ActorId, true);

        NActors::TDispatchOptions options;
        options.CustomFinalCondition = [this] {
            return ProposeCount > 0;
        };
        Runtime.DispatchEvents(options, TDuration::Seconds(2));
        UNIT_ASSERT_GT_C(ProposeCount, 0u, "Bootstrap must send TEvProposeTransaction");
    }

    void SendStatus(
        TEvTxUserProxy::TEvProposeTransactionStatus::EStatus status,
        NKikimrScheme::EStatus ssStatus = NKikimrScheme::StatusSuccess)
    {
        auto ev = MakeHolder<TEvTxUserProxy::TEvProposeTransactionStatus>(status);
        ev->Record.SetSchemeShardTabletId(SCHEME_SHARD_TABLET);
        ev->Record.SetTxId(TX_ID);
        ev->Record.SetSchemeShardStatus(ssStatus);
        Runtime.Send(new IEventHandle(ActorId, Edge, ev.Release()));
    }

    THolder<TEvSchemaOperationResponse> GrabResponse(TDuration timeout = TDuration::Seconds(5)) {
        auto handle = Runtime.GrabEdgeEvent<TEvSchemaOperationResponse>(Edge, timeout);
        UNIT_ASSERT(handle);
        UNIT_ASSERT_VALUES_EQUAL(handle->Cookie, 7u);
        return THolder(handle->Release());
    }
};

} // namespace

Y_UNIT_TEST_SUITE(SchemaOperationActor) {

Y_UNIT_TEST(ExecCompleteSucceeds) {
    TSchemaOpEnv env;
    env.SendStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete);
    auto response = env.GrabResponse();
    UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT(response->ErrorMessage.empty());
}

Y_UNIT_TEST(ExecCompleteAlreadyExists) {
    TSchemaOpEnv env;
    env.SendStatus(
        TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete,
        NKikimrScheme::StatusAlreadyExists);
    auto response = env.GrabResponse();
    UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::ALREADY_EXISTS);
    UNIT_ASSERT_STRING_CONTAINS(response->ErrorMessage, "StatusAlreadyExists");
}

Y_UNIT_TEST(ExecAlreadyAlreadyExists) {
    TSchemaOpEnv env;
    env.SendStatus(
        TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecAlready,
        NKikimrScheme::StatusAlreadyExists);
    auto response = env.GrabResponse();
    UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::ALREADY_EXISTS);
}

Y_UNIT_TEST(ExecAlreadyOtherMapsViaYdbStatus) {
    TSchemaOpEnv env;
    env.SendStatus(
        TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecAlready,
        NKikimrScheme::StatusSchemeError);
    auto response = env.GrabResponse();
    UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::INTERNAL_ERROR);
    UNIT_ASSERT_STRING_CONTAINS(response->ErrorMessage, "StatusSchemeError");
}

Y_UNIT_TEST(ExecErrorMapsSchemeError) {
    TSchemaOpEnv env;
    env.SendStatus(
        TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError,
        NKikimrScheme::StatusPathDoesNotExist);
    auto response = env.GrabResponse();
    UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::SCHEME_ERROR);
    UNIT_ASSERT_STRING_CONTAINS(response->ErrorMessage, "StatusPathDoesNotExist");
}

Y_UNIT_TEST(ExecInProgressWaitsForNotify) {
    TSchemaOpEnv env;
    env.SendStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress);
    auto response = env.GrabResponse();
    UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(env.PipeStats.NotifyForwards, 1u);
}

Y_UNIT_TEST(ExecInProgressRetriesDeliveryProblemThenSucceeds) {
    TSchemaOpEnv env(TFakePipeCacheActor::EMode::FailThenComplete);
    env.SendStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress);
    auto response = env.GrabResponse();
    UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_GT(env.PipeStats.DeliveryProblems, 0u);
    UNIT_ASSERT_GT(env.PipeStats.NotifyForwards, 2u);
}

Y_UNIT_TEST(ExecInProgressDeliveryProblemExhaustsRetries) {
    TSchemaOpEnv env(TFakePipeCacheActor::EMode::FailAlways);
    env.SendStatus(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress);
    auto response = env.GrabResponse();
    UNIT_ASSERT_VALUES_EQUAL(response->Status, Ydb::StatusIds::UNAVAILABLE);
    UNIT_ASSERT_STRING_CONTAINS(response->ErrorMessage, "is unavailable");
    UNIT_ASSERT_GT(env.PipeStats.DeliveryProblems, 10u);
}

} // Y_UNIT_TEST_SUITE(SchemaOperationActor)

} // namespace NKikimr::NPQ::NSchema
