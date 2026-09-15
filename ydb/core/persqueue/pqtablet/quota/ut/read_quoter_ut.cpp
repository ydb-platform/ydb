#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/pqtablet/quota/quota.h>

#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {

namespace {

const TDuration NegativeTimeout = TDuration::MilliSeconds(200);

class TEvReadTestEventHandle: public NActors::IEventHandle {
public:
    TEvReadTestEventHandle(THolder<TEvPQ::TEvRead>&& event, const TActorId& sender)
        : NActors::IEventHandle(TActorId{}, sender, event.Release())
    {}
};

void InitRuntime(NActors::TTestBasicRuntime& runtime) {
    TAppPrepare app;
    runtime.Initialize(app.Unwrap());
}

TActorId RegisterQuoter(NActors::TTestBasicRuntime& runtime, const TActorId& edgeActor) {
    NKikimrPQ::TPQConfig pqConfig;
    pqConfig.SetMaxInflightReadRequestsPerPartition(16);

    NKikimrPQ::TPQTabletConfig config;
    auto* part = config.MutablePartitionConfig();
    part->SetWriteSpeedInBytesPerSecond(1_MB);
    part->SetBurstSize(1_MB);
    part->SetWriteSpeedInMessagesPerSecond(1_MB);
    part->SetBurstSizeInMessages(1_MB);
    part->SetReadSpeedInBytesPerSecond(1_MB);
    part->SetReadBurstBytes(1_MB);
    part->SetReadSpeedInMessagesPerSecond(1_MB);
    part->SetReadBurstMessages(1_MB);

    std::shared_ptr<TTabletCountersBase> counters = std::make_shared<TTabletCountersBase>();
    auto quoterId = runtime.Register(CreateReadQuoter(
        pqConfig,
        /*topicConverter=*/nullptr,
        config,
        TPartitionId{},
        TActorId{},
        edgeActor,
        1234567890,
        counters
    ));
    runtime.EnableScheduleForActor(quoterId);
    return quoterId;
}

void SendAcquireReadQuota(
    NActors::TTestBasicRuntime& runtime,
    const TActorId& quoter,
    const TActorId& sender,
    ui64 cookie,
    const TString& consumer = "client"
) {
    auto request = MakeHolder<TEvPQ::TEvRead>(
        cookie, 0, 99999, 0, 9999, "", consumer, 999, 99999, true, 99999, 0, "", false, TActorId{}
    );
    auto handle = new TEvReadTestEventHandle(std::move(request), sender);
    runtime.Send(new IEventHandle(quoter, sender, new TEvPQ::TEvRequestQuota(cookie, handle)), 0, true);
}

void SendReadQuotaConsumed(
    NActors::TTestBasicRuntime& runtime,
    const TActorId& quoter,
    const TActorId& sender,
    ui64 cookie,
    const TString& consumer = "client"
) {
    runtime.Send(new IEventHandle(quoter, sender, new TEvPQ::TEvConsumed(1024, 0, cookie, consumer)), 0, true);
}

void WaitReadQuotaAcquired(NActors::TTestBasicRuntime& runtime, const TActorId& edge) {
    auto event = runtime.GrabEdgeEvent<TEvPQ::TEvApproveReadQuota>(edge, TDuration::Seconds(1));
    UNIT_ASSERT(event);
}

void ExpectNoReadQuotaAcquired(NActors::TTestBasicRuntime& runtime, const TActorId& edge) {
    auto event = runtime.GrabEdgeEvent<TEvPQ::TEvApproveReadQuota>(edge, NegativeTimeout);
    UNIT_ASSERT(event == nullptr);
}

void ExpectNoExclusiveLockAcquired(NActors::TTestBasicRuntime& runtime, const TActorId& edge) {
    auto event = runtime.GrabEdgeEvent<TEvPQ::TEvExclusiveLockAcquired>(edge, NegativeTimeout);
    UNIT_ASSERT(event == nullptr);
}

void WaitExclusiveLockAcquired(NActors::TTestBasicRuntime& runtime, const TActorId& edge) {
    auto event = runtime.GrabEdgeEvent<TEvPQ::TEvExclusiveLockAcquired>(edge, TDuration::Seconds(1));
    UNIT_ASSERT(event);
}

} // namespace

Y_UNIT_TEST_SUITE(TReadQuoterTests) {

Y_UNIT_TEST(ExclusiveLock) {
    NActors::TTestBasicRuntime runtime(1, false);
    InitRuntime(runtime);
    auto edge = runtime.AllocateEdgeActor();
    auto quoter = RegisterQuoter(runtime, edge);

    SendAcquireReadQuota(runtime, quoter, edge, 1);
    WaitReadQuotaAcquired(runtime, edge);

    runtime.Send(new IEventHandle(quoter, edge, new TEvPQ::TEvAcquireExclusiveLock()), 0, true);
    ExpectNoExclusiveLockAcquired(runtime, edge);

    SendReadQuotaConsumed(runtime, quoter, edge, 1);
    WaitExclusiveLockAcquired(runtime, edge);

    SendAcquireReadQuota(runtime, quoter, edge, 2);
    ExpectNoReadQuotaAcquired(runtime, edge);

    runtime.Send(new IEventHandle(quoter, edge, new TEvPQ::TEvReleaseExclusiveLock()), 0, true);
    WaitReadQuotaAcquired(runtime, edge);
}

Y_UNIT_TEST(ConsumerRemovedReleasesQueuedReads) {
    NActors::TTestBasicRuntime runtime(1, false);
    InitRuntime(runtime);
    auto edge = runtime.AllocateEdgeActor();
    auto quoter = RegisterQuoter(runtime, edge);

    SendAcquireReadQuota(runtime, quoter, edge, 1);
    WaitReadQuotaAcquired(runtime, edge);

    runtime.Send(new IEventHandle(quoter, edge, new TEvPQ::TEvAcquireExclusiveLock()), 0, true);
    ExpectNoExclusiveLockAcquired(runtime, edge);

    SendAcquireReadQuota(runtime, quoter, edge, 2);
    ExpectNoReadQuotaAcquired(runtime, edge);

    runtime.Send(new IEventHandle(quoter, edge, new TEvPQ::TEvConsumerRemoved("client")), 0, true);
    WaitReadQuotaAcquired(runtime, edge);

    SendReadQuotaConsumed(runtime, quoter, edge, 1);
    SendReadQuotaConsumed(runtime, quoter, edge, 2);
    WaitExclusiveLockAcquired(runtime, edge);
}

Y_UNIT_TEST(DuplicateExclusiveLockDoesNotKill) {
    NActors::TTestBasicRuntime runtime(1, false);
    InitRuntime(runtime);
    auto edge = runtime.AllocateEdgeActor();
    auto quoter = RegisterQuoter(runtime, edge);

    SendAcquireReadQuota(runtime, quoter, edge, 1);
    WaitReadQuotaAcquired(runtime, edge);

    runtime.Send(new IEventHandle(quoter, edge, new TEvPQ::TEvAcquireExclusiveLock()), 0, true);
    ExpectNoExclusiveLockAcquired(runtime, edge);

    SendReadQuotaConsumed(runtime, quoter, edge, 1);
    WaitExclusiveLockAcquired(runtime, edge);

    runtime.Send(new IEventHandle(quoter, edge, new TEvPQ::TEvAcquireExclusiveLock()), 0, true);

    runtime.Send(new IEventHandle(quoter, edge, new TEvPQ::TEvReleaseExclusiveLock()), 0, true);
    SendAcquireReadQuota(runtime, quoter, edge, 2);
    WaitReadQuotaAcquired(runtime, edge);
}

}

} // namespace NKikimr::NPQ
