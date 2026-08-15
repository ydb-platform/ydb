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

} // Y_UNIT_TEST_SUITE(TPqrbScaleRequest)

} // namespace NKikimr::NPQ
