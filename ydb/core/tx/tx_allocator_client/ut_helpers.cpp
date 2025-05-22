#include "ut_helpers.h"

#include <ydb/core/tx/tx_allocator/txallocator.h>

namespace NTxAllocatorUT_Private {

const TDuration TTestEnv::SimTimeOut = TDuration::Seconds(1u);

ui64 SomeCockie(ui64 size) {
    return size / 2;
}

void CheckExpectedCookie(const TVector<ui64>& txIds, ui64 cookie) {
    const ui64  rangeSize = txIds.size();
    if (rangeSize > 0) // rangeSize == 0 when status is error
        UNIT_ASSERT_EQUAL(cookie, SomeCockie(rangeSize));
}

TAnswerWithCookie GrabAnswer(NActors::TTestActorRuntime &runtime) {
    TAutoPtr<IEventHandle> handle;
    TEvTxAllocatorClient::TEvAllocateResult *event = runtime.GrabEdgeEvent<TEvTxAllocatorClient::TEvAllocateResult>(handle);
    UNIT_ASSERT(event);
    return TAnswerWithCookie(std::move(event->TxIds), handle->Cookie);
}

void TTestEnv::AllocateAndCheck(ui64 size) {
    AsyncAllocate(size);
    TAnswerWithCookie result = GrabAnswer(Runtime);

    auto& txIds = result.first;
    UNIT_ASSERT_EQUAL(txIds.size(), size);

    ui64 cookie = result.second;
    CheckExpectedCookie(txIds, cookie);
}

void TTestEnv::AsyncAllocate(ui64 size) {
    Y_ABORT_UNLESS(TxAllocatorClient);
    TActorId sender = Runtime.AllocateEdgeActor();
    TEvTxAllocatorClient::TEvAllocate *ev = new TEvTxAllocatorClient::TEvAllocate(size);
    Runtime.Send(new IEventHandle(TxAllocatorClient, sender, ev, 0, SomeCockie(size)), 0, true);
}

void TTestEnv::Boot() {
    CreateTestBootstrapper(Runtime, CreateTestTabletInfo(TxAllocatorTablet, TTabletTypes::TxAllocator), &CreateTxAllocator);

    TDispatchOptions options;
    options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
    Runtime.DispatchEvents(options, SimTimeOut);
}

void TTestEnv::SetupLogging() {
    Runtime.SetLogPriority(NKikimrServices::TX_ALLOCATOR, NActors::NLog::PRI_DEBUG);
    Runtime.SetLogPriority(NKikimrServices::TABLET_MAIN, NActors::NLog::PRI_DEBUG);
}

void TTestEnv::Setup() {
    static constexpr ui32 domainId = 0;
    SetupLogging();
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
    SetupTabletServices(Runtime, &app, true);
}

void TTestEnv::SetupClient() {
    TxAllocatorClient = Runtime.Register(CreateTxAllocatorClient({TTestEnv::TxAllocatorTablet}));
}

void TTestEnv::Reboot() {
    TActorId sender = Runtime.AllocateEdgeActor();
    RebootTablet(Runtime, TxAllocatorTablet, sender);
}

TMsgCounter::TMsgCounter(TTestActorRuntime &runtime, ui32 msgType)
    : Runtime(runtime)
    , Counter(0)
{
    PrevObserver = Runtime.SetObserverFunc([this, msgType](TAutoPtr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == msgType) {
            this->Counter += 1;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    });
}

void TMsgCounter::Wait(ui32 count) {
    if (Counter < count) {
        TDispatchOptions opts;
        opts.FinalEvents.emplace_back(TDispatchOptions::TFinalEventCondition([this, count](IEventHandle&) -> bool {
                                          return Counter >= count;
                                      }));
        Runtime.DispatchEvents(opts);
    }
}

TMsgCounter::~TMsgCounter() {
    Runtime.SetObserverFunc(PrevObserver);
}

}
