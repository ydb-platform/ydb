#include "pqrb_ut_common.h"

#include <library/cpp/threading/future/future.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/persqueue/common/proxy/actor_persqueue_client_iface.h>
#include <ydb/core/persqueue/pqrb/mirror_describer_factory.h>
#include <ydb/core/persqueue/pqrb/mirror_describer.h>
#include <ydb/core/persqueue/pqrb/partition_scale_request.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/control_plane.h>

namespace NKikimr::NPQ {

Y_UNIT_TEST_SUITE(TPqrbScaleRequest) {

Y_UNIT_TEST(PoisonPillStopsActorWithoutDone) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    tc.Runtime->SetObserverFunc([](TAutoPtr<IEventHandle>& ev) {
        if (ev->CastAsLocal<TEvTxUserProxy::TEvProposeTransaction>()) {
            return TTestActorRuntimeBase::EEventAction::DROP;
        }
        return TTestActorRuntimeBase::EEventAction::PROCESS;
    });

    auto actorId = tc.Runtime->Register(new TPartitionScaleRequest(
        "topic",
        "/Root/topic",
        "/Root",
        /*pathId=*/1,
        /*pathVersion=*/1,
        {},
        {},
        {},
        tc.Edge
    ));
    tc.Runtime->EnableScheduleForActor(actorId);
    DispatchFor(tc);

    tc.Runtime->Send(new IEventHandle(actorId, tc.Edge, new TEvents::TEvPoisonPill()));
    DispatchFor(tc);

    tc.Runtime->Send(new IEventHandle(
        actorId,
        tc.Edge,
        new TEvTabletPipe::TEvClientDestroyed(/*tabletId=*/1, TActorId(1, 1, 1, 1), TActorId(1, 1, 2, 1))
    ));

    auto done = tc.Runtime->GrabEdgeEvent<TPartitionScaleRequest::TEvPartitionScaleRequestDone>(
        TDuration::MilliSeconds(200)
    );
    UNIT_ASSERT_C(!done, "Poisoned scale request must not send TEvPartitionScaleRequestDone");
}

Y_UNIT_TEST(ForeignPipeDestroyedIsIgnored) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    tc.Runtime->SetObserverFunc([](TAutoPtr<IEventHandle>& ev) {
        if (ev->CastAsLocal<TEvTxUserProxy::TEvProposeTransaction>()) {
            return TTestActorRuntimeBase::EEventAction::DROP;
        }
        return TTestActorRuntimeBase::EEventAction::PROCESS;
    });

    auto actorId = tc.Runtime->Register(new TPartitionScaleRequest(
        "topic",
        "/Root/topic",
        "/Root",
        /*pathId=*/1,
        /*pathVersion=*/1,
        {},
        {},
        {},
        tc.Edge
    ));
    tc.Runtime->EnableScheduleForActor(actorId);
    DispatchFor(tc);

    tc.Runtime->Send(new IEventHandle(
        actorId,
        tc.Edge,
        new TEvTabletPipe::TEvClientDestroyed(/*tabletId=*/1, TActorId(1, 1, 1, 1), TActorId(1, 1, 2, 1))
    ));

    auto early = tc.Runtime->GrabEdgeEvent<TPartitionScaleRequest::TEvPartitionScaleRequestDone>(
        TDuration::MilliSeconds(200)
    );
    UNIT_ASSERT_C(!early, "ClientDestroyed for an unknown pipe must be ignored");

    auto status = MakeHolder<TEvTxUserProxy::TEvProposeTransactionStatus>(
        TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError
    );
    tc.Runtime->Send(new IEventHandle(actorId, tc.Edge, status.Release()));

    auto done = tc.Runtime->GrabEdgeEvent<TPartitionScaleRequest::TEvPartitionScaleRequestDone>(
        TDuration::Seconds(10)
    );
    UNIT_ASSERT(done);
}

Y_UNIT_TEST(ProposeStatusSendsDoneOnlyOnce) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    tc.Runtime->SetObserverFunc([](TAutoPtr<IEventHandle>& ev) {
        if (ev->CastAsLocal<TEvTxUserProxy::TEvProposeTransaction>()) {
            return TTestActorRuntimeBase::EEventAction::DROP;
        }
        return TTestActorRuntimeBase::EEventAction::PROCESS;
    });

    auto actorId = tc.Runtime->Register(new TPartitionScaleRequest(
        "topic",
        "/Root/topic",
        "/Root",
        /*pathId=*/1,
        /*pathVersion=*/1,
        {},
        {},
        {},
        tc.Edge
    ));
    tc.Runtime->EnableScheduleForActor(actorId);
    DispatchFor(tc);

    for (ui32 i = 0; i < 2; ++i) {
        auto status = MakeHolder<TEvTxUserProxy::TEvProposeTransactionStatus>(
            TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError
        );
        tc.Runtime->Send(new IEventHandle(actorId, tc.Edge, status.Release()));
    }

    auto done = tc.Runtime->GrabEdgeEvent<TPartitionScaleRequest::TEvPartitionScaleRequestDone>(
        TDuration::Seconds(10)
    );
    UNIT_ASSERT(done);

    auto extra = tc.Runtime->GrabEdgeEvent<TPartitionScaleRequest::TEvPartitionScaleRequestDone>(
        TDuration::MilliSeconds(200)
    );
    UNIT_ASSERT_C(!extra, "ReplyAndDie must send TEvPartitionScaleRequestDone only once");
}

Y_UNIT_TEST(ScaleRequestInflightIsClearedWhenSplitMergeDisabled) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    PQTabletPrepare({}, {}, tc);
    SendBalancerUpdate(tc, TBalancerUpdate{
        .Partitions = {{0, {tc.TabletId, 1}}},
        .Strategy = NKikimrPQ::TPQTabletConfig::CAN_SPLIT,
        .MaxPartitionCount = 10,
    });
    NotifyDatabasePath(tc);

    ui32 scaleRequestActors = 0;
    TTestActorRuntime::TRegistrationObserver prevRegistration;
    prevRegistration = tc.Runtime->SetRegistrationObserverFunc(
        [&](TTestActorRuntimeBase& runtime, const TActorId& parentId, const TActorId& actorId) {
            if (prevRegistration) {
                prevRegistration(runtime, parentId, actorId);
            }
            ++scaleRequestActors;
        }
    );

    auto needSplit = MakeHolder<TEvPQ::TEvPartitionScaleStatusChanged>(0, NKikimrPQ::EScaleStatus::NEED_SPLIT);
    needSplit->Record.SetSplitBoundary("m");
    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        needSplit.Release(),
        0,
        GetPipeConfigWithRetries()
    );
    DispatchFor(tc, TDuration::MilliSeconds(200));
    UNIT_ASSERT_GT(scaleRequestActors, 0u);

    SendBalancerUpdate(tc, TBalancerUpdate{
        .Partitions = {{0, {tc.TabletId, 1}}},
        .Strategy = NKikimrPQ::TPQTabletConfig::DISABLED,
    });

    ForwardToTablet(
        *tc.Runtime,
        tc.BalancerTabletId,
        tc.Edge,
        new TPartitionScaleRequest::TEvPartitionScaleRequestDone(
            TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete
        )
    );
    DispatchFor(tc);

    SendBalancerUpdate(tc, TBalancerUpdate{
        .Partitions = {{0, {tc.TabletId, 1}}},
        .Strategy = NKikimrPQ::TPQTabletConfig::CAN_SPLIT,
        .MaxPartitionCount = 10,
    });
    NotifyDatabasePath(tc);
    const ui32 afterReenable = scaleRequestActors;

    auto needSplitAgain = MakeHolder<TEvPQ::TEvPartitionScaleStatusChanged>(0, NKikimrPQ::EScaleStatus::NEED_SPLIT);
    needSplitAgain->Record.SetSplitBoundary("m");
    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        needSplitAgain.Release(),
        0,
        GetPipeConfigWithRetries()
    );
    DispatchFor(tc, TDuration::MilliSeconds(200));
    UNIT_ASSERT_GT_C(scaleRequestActors, afterReenable, "A new scale request must be sent after split/merge is re-enabled");
}

} // Y_UNIT_TEST_SUITE(TPqrbScaleRequest)

class TControllableMirrorFactory : public IPersQueueMirrorReaderFactory {
public:
    mutable ui32 DescribeCalls = 0;
    mutable std::vector<NThreading::TPromise<NYdb::NTopic::TDescribeTopicResult>> DescribePromises;

    NThreading::TFuture<NYdb::TCredentialsProviderFactoryPtr> GetCredentialsProviderImpl(
        const NKikimrPQ::TMirrorPartitionConfig::TCredentials&
    ) const override {
        return NThreading::MakeFuture(NYdb::CreateInsecureCredentialsProviderFactory());
    }

    std::shared_ptr<NYdb::NTopic::IReadSession> GetReadSession(
        const NKikimrPQ::TMirrorPartitionConfig&,
        ui32,
        std::shared_ptr<NYdb::ICredentialsProviderFactory>,
        ui64,
        TMaybe<TLog>
    ) const override {
        return nullptr;
    }

    NThreading::TFuture<NYdb::NTopic::TDescribeTopicResult> GetTopicDescription(
        const NKikimrPQ::TMirrorPartitionConfig&,
        std::shared_ptr<NYdb::ICredentialsProviderFactory>
    ) const override {
        ++DescribeCalls;
        auto promise = NThreading::NewPromise<NYdb::NTopic::TDescribeTopicResult>();
        DescribePromises.push_back(promise);
        return promise.GetFuture();
    }

    NThreading::TFuture<NYdb::TStatus> CommitOffset(
        const NKikimrPQ::TMirrorPartitionConfig&,
        std::shared_ptr<NYdb::ICredentialsProviderFactory>,
        ui32,
        ui64
    ) const override {
        return NThreading::MakeFuture(NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues()));
    }
};

Y_UNIT_TEST_SUITE(TPqrbMirrorDescriber) {

Y_UNIT_TEST(ConfigChangeDuringInflightDescribeDoesNotStuck) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    auto factory = std::make_shared<TControllableMirrorFactory>();
    tc.Runtime->GetAppData(0).PersQueueMirrorReaderFactory = factory.get();

    NKikimrPQ::TMirrorPartitionConfig mirrorConfig;
    mirrorConfig.SetEndpoint("src");
    mirrorConfig.SetEndpointPort(2135);
    mirrorConfig.SetTopic("src-topic");

    auto describer = tc.Runtime->Register(CreateMirrorDescriber(
        /*tabletId=*/1,
        tc.Edge,
        "topic",
        mirrorConfig
    ));
    tc.Runtime->EnableScheduleForActor(describer);
    DispatchFor(tc);

    tc.Runtime->ResetScheduledCount();
    tc.Runtime->AdvanceCurrentTime(TDuration::Seconds(1));
    DispatchFor(tc);
    UNIT_ASSERT_VALUES_EQUAL(factory->DescribeCalls, 1u);
    UNIT_ASSERT_VALUES_EQUAL(factory->DescribePromises.size(), 1u);

    NKikimrPQ::TPQTabletConfig newConfig;
    newConfig.MutablePartitionConfig()->MutableMirrorFrom()->SetEndpoint("src-2");
    newConfig.MutablePartitionConfig()->MutableMirrorFrom()->SetEndpointPort(2135);
    newConfig.MutablePartitionConfig()->MutableMirrorFrom()->SetTopic("src-topic-2");
    tc.Runtime->Send(new IEventHandle(
        describer,
        tc.Edge,
        new TEvPQ::TEvChangePartitionConfig(nullptr, newConfig)
    ));
    DispatchFor(tc);

    factory->DescribePromises[0].SetValue(NYdb::NTopic::TDescribeTopicResult(
        NYdb::TStatus(NYdb::EStatus::UNAVAILABLE, NYdb::NIssue::TIssues()),
        Ydb::Topic::DescribeTopicResult{}
    ));
    DispatchFor(tc);

    tc.Runtime->ResetScheduledCount();
    tc.Runtime->AdvanceCurrentTime(TDuration::Seconds(1));
    DispatchFor(tc);
    UNIT_ASSERT_GT_C(factory->DescribeCalls, 1u, "Mirror describer must start a new describe after config change");
}

} // Y_UNIT_TEST_SUITE(TPqrbMirrorDescriber)

} // namespace NKikimr::NPQ
