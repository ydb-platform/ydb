#include "mlp.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/ut/common/pq_ut_common.h>
#include <ydb/core/protos/msgbus.pb.h>
#include <ydb/core/protos/pqdata_mlp.pb.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/library/actors/core/actor.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ::NMLP {

namespace {

constexpr ui64 kParentTabletId = 100;
constexpr ui64 kChild1TabletId = 201;
constexpr ui64 kChild2TabletId = 202;
constexpr const char* kConsumer = "mlp-consumer";

NKikimrPQ::TPQTabletConfig MakeSplitTopicConfig() {
    NKikimrPQ::TPQTabletConfig config;
    config.SetTopicName("topic");
    config.SetTopicPath("/Root/topic");

    auto* parent = config.AddAllPartitions();
    parent->SetPartitionId(0);
    parent->SetTabletId(kParentTabletId);
    parent->SetStatus(NKikimrPQ::ETopicPartitionStatus::Inactive);
    parent->AddChildPartitionIds(1);
    parent->AddChildPartitionIds(2);

    auto* child1 = config.AddAllPartitions();
    child1->SetPartitionId(1);
    child1->SetTabletId(kChild1TabletId);
    child1->AddParentPartitionIds(0);

    auto* child2 = config.AddAllPartitions();
    child2->SetPartitionId(2);
    child2->SetTabletId(kChild2TabletId);
    child2->AddParentPartitionIds(0);

    auto* consumer = config.AddConsumers();
    consumer->SetName(kConsumer);
    consumer->SetType(NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP);
    consumer->SetKeepMessageOrder(true);
    consumer->SetGeneration(1);
    return config;
}

NKikimrPQ::TPQTabletConfig::TConsumer MakeConsumerConfig() {
    NKikimrPQ::TPQTabletConfig::TConsumer consumer;
    consumer.SetName(kConsumer);
    consumer.SetType(NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP);
    consumer.SetKeepMessageOrder(true);
    consumer.SetGeneration(1);
    return consumer;
}

THolder<TEvKeyValue::TEvResponse> MakeEmptySnapshotResponse(ui64 cookie) {
    auto response = MakeHolder<TEvKeyValue::TEvResponse>();
    response->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);
    response->Record.SetCookie(cookie);
    response->Record.AddReadResult()->SetStatus(NKikimrProto::NODATA);
    response->Record.AddReadRangeResult()->SetStatus(NKikimrProto::NODATA);
    return response;
}

THolder<TEvPQ::TEvMLPUpdateExternalLockedMessageGroupsId> MakeUpdateExternal(ui32 partitionId) {
    auto ev = MakeHolder<TEvPQ::TEvMLPUpdateExternalLockedMessageGroupsId>();
    ev->Record.SetConsumer(kConsumer);
    ev->Record.SetPartitionId(partitionId);
    auto* update = ev->Record.MutableUpdate();
    update->SetParentPartitionId(1);
    update->SetGeneration(1);
    update->SetConsumerGeneration(1);
    update->SetStep(1);
    update->SetMode(NKikimrPQ::READ_WITH_KEEP_ORDER_BLACKLIST);
    return ev;
}

struct TCapturedForward {
    ui64 TabletId = 0;
    ui64 Cookie = 0;
    ui32 ChildPartitionId = 0;
};

class TIgnorePipeCacheActor : public TActorBootstrapped<TIgnorePipeCacheActor> {
public:
    void Bootstrap() {
        Become(&TThis::StateWork);
    }

    STRICT_STFUNC(StateWork,
        IgnoreFunc(TEvPipeCache::TEvForward);
        IgnoreFunc(TEvPipeCache::TEvUnlink);
    )
};

struct TConsumerEnv {
    TTestBasicRuntime Runtime;
    TActorId Tablet;
    TActorId Partition;
    TActorId Consumer;
    NMonitoring::TDynamicCounterPtr Counters;
    TVector<TCapturedForward> Forwards;

    TConsumerEnv()
        : Runtime(1, false)
        , Counters(new NMonitoring::TDynamicCounters())
    {
        Runtime.Initialize(TAppPrepare().Unwrap());
        Runtime.SetScheduledLimit(10000);
        Runtime.SetLogPriority(NKikimrServices::PQ_MLP_CONSUMER, NLog::PRI_DEBUG);

        auto pipeCache = Runtime.Register(new TIgnorePipeCacheActor());
        Runtime.EnableScheduleForActor(pipeCache);
        Runtime.RegisterService(MakePipePerNodeCacheID(false), pipeCache);

        Tablet = Runtime.AllocateEdgeActor();
        Partition = Runtime.AllocateEdgeActor();

        Runtime.SetObserverFunc([this](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvPipeCache::TEvForward::EventType) {
                const auto* forward = ev->Get<TEvPipeCache::TEvForward>();
                if (forward->Ev && forward->Ev->Type() == TEvPQ::TEvMLPUpdateExternalLockedMessageGroupsId::EventType) {
                    const auto& update = *static_cast<const TEvPQ::TEvMLPUpdateExternalLockedMessageGroupsId*>(forward->Ev.Get());
                    Forwards.push_back(TCapturedForward{
                        .TabletId = forward->TabletId,
                        .Cookie = forward->Options.SubscribeCookie,
                        .ChildPartitionId = update.GetPartitionId(),
                    });
                }
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        const auto topicConfig = MakeSplitTopicConfig();
        Consumer = Runtime.Register(CreateConsumerActor(
            "/Root",
            kParentTabletId,
            Tablet,
            0,
            Partition,
            1,
            topicConfig,
            MakeConsumerConfig(),
            TDuration::Hours(1),
            0,
            Counters
        ));
        Runtime.EnableScheduleForActor(Consumer);

        auto kvReq = Runtime.GrabEdgeEvent<TEvKeyValue::TEvRequest>(TDuration::Seconds(5));
        UNIT_ASSERT(kvReq);
        Runtime.Send(new IEventHandle(Consumer, Tablet, MakeEmptySnapshotResponse(kvReq->Record.GetCookie()).Release()));
        Runtime.DispatchEvents();
        UNIT_ASSERT_VALUES_EQUAL(Forwards.size(), 2u);
    }

    const TCapturedForward* FindForward(ui32 childPartitionId) const {
        for (const auto& forward : Forwards) {
            if (forward.ChildPartitionId == childPartitionId) {
                return &forward;
            }
        }
        return nullptr;
    }
};

void PrepareMlpTablet(TTestContext& tc, ui32 partitions = 1) {
    TVector<TConsumerPreparationParameters> users = {{
        .Name = kConsumer,
        .Type = NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP,
        .KeepMessageOrder = true,
    }};
    PQTabletPrepare({.partitions = partitions, .AddDefaultConsumer = false}, users, *tc.Runtime, tc.TabletId, tc.Edge);
}

} // namespace

Y_UNIT_TEST_SUITE(TMLPConsumerChildSyncTests) {

Y_UNIT_TEST(ErrorResponseRetriesBlacklistByPartitionId) {
    TConsumerEnv env;
    const size_t before = env.Forwards.size();

    env.Runtime.Send(new IEventHandle(
        env.Consumer,
        TActorId(),
        new TEvPQ::TEvMLPErrorResponse(1, Ydb::StatusIds::SCHEME_ERROR, "Partition 1 not found")));
    env.Runtime.DispatchEvents();
    env.Runtime.AdvanceCurrentTime(TDuration::Seconds(2));
    env.Runtime.DispatchEvents();

    UNIT_ASSERT_GT(env.Forwards.size(), before);
    UNIT_ASSERT_VALUES_EQUAL(env.Forwards.back().ChildPartitionId, 1u);
    UNIT_ASSERT_VALUES_EQUAL(env.Forwards.back().TabletId, kChild1TabletId);
}

Y_UNIT_TEST(UnknownPartitionErrorDoesNotVerify) {
    TConsumerEnv env;
    const size_t before = env.Forwards.size();

    env.Runtime.Send(new IEventHandle(
        env.Consumer,
        TActorId(),
        new TEvPQ::TEvMLPErrorResponse(99, Ydb::StatusIds::SCHEME_ERROR, "Consumer 'mlp-consumer' does not exist")));
    env.Runtime.DispatchEvents();
    env.Runtime.AdvanceCurrentTime(TDuration::Seconds(2));
    env.Runtime.DispatchEvents();

    UNIT_ASSERT_VALUES_EQUAL(env.Forwards.size(), before);
}

Y_UNIT_TEST(DeliveryProblemRetriesBlacklistByCookie) {
    TConsumerEnv env;
    const auto* child2 = env.FindForward(2);
    UNIT_ASSERT(child2);

    const size_t before = env.Forwards.size();
    env.Runtime.Send(new IEventHandle(
        env.Consumer,
        TActorId(),
        new TEvPipeCache::TEvDeliveryProblem(child2->TabletId, true),
        0,
        child2->Cookie));
    env.Runtime.DispatchEvents();
    env.Runtime.AdvanceCurrentTime(TDuration::Seconds(2));
    env.Runtime.DispatchEvents();

    UNIT_ASSERT_GT(env.Forwards.size(), before);
    UNIT_ASSERT_VALUES_EQUAL(env.Forwards.back().ChildPartitionId, 2u);
    UNIT_ASSERT_VALUES_EQUAL(env.Forwards.back().TabletId, kChild2TabletId);
}

} // Y_UNIT_TEST_SUITE(TMLPConsumerChildSyncTests)

Y_UNIT_TEST_SUITE(TMLPTabletQueueTests) {

Y_UNIT_TEST(MlpRequestsWaitForConfigThenDeliver) {
    TTestContext tc;
    TFinalizer finalizer(tc);
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);
    PrepareMlpTablet(tc);

    bool holdKv = false;
    TVector<THolder<IEventHandle>> heldKv;
    tc.Runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
        if (holdKv && ev->GetTypeRewrite() == TEvKeyValue::TEvResponse::EventType) {
            heldKv.emplace_back(ev.Release());
            return TTestActorRuntime::EEventAction::DROP;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    });

    holdKv = true;
    RebootTablet(*tc.Runtime, tc.TabletId, tc.Edge);

    ForwardToTablet(*tc.Runtime, tc.TabletId, tc.Edge, new TEvPQ::TEvGetMLPConsumerStateRequest(
        "topic", kConsumer, 0));
    tc.Runtime->DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(50));

    auto earlyError = tc.Runtime->GrabEdgeEvent<TEvPQ::TEvMLPErrorResponse>(TDuration::MilliSeconds(200));
    UNIT_ASSERT_C(!earlyError, "MLP requests must wait for config instead of Partition not found");

    holdKv = false;
    for (auto& ev : heldKv) {
        tc.Runtime->Send(ev.Release());
    }

    auto response = tc.Runtime->GrabEdgeEvent<TEvPQ::TEvGetMLPConsumerStateResponse>(TDuration::Seconds(15));
    UNIT_ASSERT(response);
    UNIT_ASSERT_VALUES_EQUAL(response->Config.GetName(), kConsumer);

    ForwardToTablet(*tc.Runtime, tc.TabletId, tc.Edge, new TEvPQ::TEvGetMLPConsumerStateRequest(
        "topic", kConsumer, 99));
    auto unknown = tc.Runtime->GrabEdgeEvent<TEvPQ::TEvMLPErrorResponse>(TDuration::Seconds(10));
    UNIT_ASSERT(unknown);
    UNIT_ASSERT_VALUES_EQUAL(unknown->GetPartitionId(), 99u);
    UNIT_ASSERT(TString(unknown->Record.GetErrorMessage()).Contains("not found"));
}

} // Y_UNIT_TEST_SUITE(TMLPTabletQueueTests)

Y_UNIT_TEST_SUITE(TMLPPartitionQueueTests) {

Y_UNIT_TEST(UpdateExternalUnknownConsumerReturnsError) {
    TTestContext tc;
    TFinalizer finalizer(tc);
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    PQTabletPrepare({.partitions = 1, .AddDefaultConsumer = false}, TVector<TConsumerPreparationParameters>{}, *tc.Runtime, tc.TabletId, tc.Edge);

    tc.Runtime->SendToPipe(tc.TabletId, tc.Edge, MakeUpdateExternal(0).Release(), 0, GetPipeConfigWithRetries());
    auto error = tc.Runtime->GrabEdgeEvent<TEvPQ::TEvMLPErrorResponse>(TDuration::Seconds(10));
    UNIT_ASSERT(error);
    UNIT_ASSERT(TString(error->Record.GetErrorMessage()).Contains("does not exist"));
}

} // Y_UNIT_TEST_SUITE(TMLPPartitionQueueTests)

} // namespace NKikimr::NPQ::NMLP
