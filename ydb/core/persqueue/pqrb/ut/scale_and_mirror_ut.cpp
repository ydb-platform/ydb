#include "pqrb_ut_common.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/persqueue/pqrb/partition_scale_request.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/actors/core/events.h>

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

} // namespace NKikimr::NPQ
