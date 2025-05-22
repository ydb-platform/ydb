#include "txallocator_ut_helpers.h"

namespace NTxAllocatorUT_Private {

const TDuration TTestEnv::SimTimeOut = TDuration::Seconds(1u);

void CheckExpectedStatus(const TVector<TResultStatus> &expected, TResultStatus result) {
    bool isExpectedStatus = false;
    for (auto exp : expected) {
        Cerr << "expected " << exp <<'\n';
        if (result == exp)
            isExpectedStatus = true;
    }
    UNIT_ASSERT_C(isExpectedStatus, "Unexpected status: " << result);
}

void CheckExpectedStatus(TResultStatus expected, TResultStatus result) {
    const TVector<TResultStatus> temp = {expected};
    CheckExpectedStatus(temp, result);
}

ui64 SomeCockie(ui64 size) {
    return size / 2;
}

void CheckExpectedCookie(NKikimrTx::TEvTxAllocateResult result, ui64 cookie) {
    const ui64  rangeSize = result.GetRangeEnd() - result.GetRangeBegin();
    if (rangeSize > 0) // rangeSize == 0 when status is error
        UNIT_ASSERT_EQUAL(cookie, SomeCockie(rangeSize));
}



TAnswerWithCookie GrabAnswer(NActors::TTestActorRuntime &runtime) {
    TAutoPtr<IEventHandle> handle;
    TEvTxAllocator::TEvAllocateResult *event = runtime.GrabEdgeEvent<TEvTxAllocator::TEvAllocateResult>(handle);
    UNIT_ASSERT(event);
    return TAnswerWithCookie(event->Record, handle->Cookie);
}

void AsyncAllocate(NActors::TTestActorRuntime &runtime, ui64 size) {
    TActorId sender = runtime.AllocateEdgeActor();
    TEvTxAllocator::TEvAllocate *ev = new TEvTxAllocator::TEvAllocate(size);
    runtime.SendToPipe(TTestEnv::TxAllocatorTablet, sender, ev, 0, NKikimr::NTabletPipe::TClientConfig(), TActorId(), SomeCockie(size));
}

void AllocateAndCheck(NActors::TTestActorRuntime &runtime, ui64 size, const TVector<TResultStatus> &expected) {
    AsyncAllocate(runtime, size);
    TAnswerWithCookie result = GrabAnswer(runtime);
    NKikimrTx::TEvTxAllocateResult &event = result.first;
    CheckExpectedStatus(expected, event.GetStatus());
    ui64 cookie = result.second;
    CheckExpectedCookie(event, cookie);
}

void AllocateAndCheck(NActors::TTestActorRuntime &runtime, ui64 size, TResultStatus expected) {
    const TVector<TResultStatus> temp = {expected};
    AllocateAndCheck(runtime, size, temp);
}

void TTestEnv::Boot(TTestActorRuntime &runtime) {
    CreateTestBootstrapper(runtime, CreateTestTabletInfo(TxAllocatorTablet, TTabletTypes::TxAllocator), &CreateTxAllocator);

    TDispatchOptions options;
    options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
    runtime.DispatchEvents(options, SimTimeOut);
}

void TTestEnv::SetupLogging(TTestActorRuntime &runtime) {
    runtime.SetLogPriority(NKikimrServices::TX_ALLOCATOR, NActors::NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::TABLET_MAIN, NActors::NLog::PRI_DEBUG);
}

void TTestEnv::Setup(TTestActorRuntime &runtime) {
    static constexpr ui32 domainId = 0;
    SetupLogging(runtime);
    TAppPrepare app;
    auto domain = TDomainsInfo::TDomain::ConstructDomainWithExplicitTabletIds("dc-1", domainId, 0,
                                                                              100500,
                                                                              TVector<ui64>{},
                                                                              TVector<ui64>{},
                                                                              TVector<ui64>{},
                                                                              DefaultPoolKinds(2));
    app.AddDomain(domain.Release());
    //app.AddHive(0, 0);
    SetupChannelProfiles(app);
    SetupTabletServices(runtime, &app, true);
}

void TTestEnv::Reboot(TTestActorRuntime &runtime) {
    TActorId sender = runtime.AllocateEdgeActor();
    RebootTablet(runtime, TxAllocatorTablet, sender);
}

void TIntersectionChecker::AssertIntersection(bool continuous) {
    Sort(Responses);
    for (ui32 idx = 1; idx < Responses.size(); ++idx) {
        auto &prev = Responses[idx - 1];
        auto &cur = Responses[idx];
        if (continuous) {
            UNIT_ASSERT_C(prev.second == cur.first, "ranges aren't intersect and don't have gaps");
        } else {
            UNIT_ASSERT_C(prev.second <= cur.first, "ranges aren't intersect");
        }
    }
}

}
