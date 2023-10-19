#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/actorid.h>
#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/event.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/executor_pool_basic.h>
#include <library/cpp/actors/core/log.h>
#include <library/cpp/actors/core/scheduler_basic.h>
#include <library/cpp/actors/dnsresolver/dnsresolver.h>
#include <library/cpp/actors/interconnect/interconnect_tcp_proxy.h>
#include <library/cpp/actors/interconnect/interconnect_tcp_server.h>
#include <library/cpp/actors/util/should_continue.h>

#include <util/datetime/base.h>
#include <util/generic/xrange.h>
#include <util/generic/yexception.h>
#include <util/system/sigset.h>
#include <util/system/types.h>

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/protos/blobstorage.pb.h>

namespace {

using namespace NActors;

static TProgramShouldContinue ShouldContinue;

void OnTerminate(int) {
    ShouldContinue.ShouldStop();
}

struct TVdiskBenchEvents {
    enum {
        Begin = EventSpaceBegin(TEvents::ES_USERSPACE),
        Ack,
        End,
    };

    static_assert(End < EventSpaceEnd(TEvents::ES_USERSPACE), "expect End < EventSpaceEnd(ES_HELLOWORLD)");
};

struct TEvAck : public TEventBase<TEvAck, TVdiskBenchEvents::Ack> {
    DEFINE_SIMPLE_NONLOCAL_EVENT(TEvAck, "Ack");
};

class TSenderActor : public NActors::TActorBootstrapped<TSenderActor> {
    const TActorId Target;
    ui64 SendEvents;
    ui64 AckEvents;
    TInstant PeriodStart;

    void ScheduleStats() {
        HandledEvents = 0;
        PeriodStart = TInstant::Now();
        Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup());
    }

    void SendToTarget() {
        SendEvents++;
        auto* ptr = new NKikimr::TEvBlobStorage::TEvVPut;
        Send(Target, ptr);
    }

    void Ack() {
        AckEvents++;
        SendToTarget();
    }

    void PrintStats() {
        const i64 ms = (TInstant::Now() - PeriodStart).MilliSeconds();
        Cout << "Send " << SendEvents << " Ack " << AckEvents << " over " << ms << "ms" << Endl;
        SendEvents = AckEvents = 0;
        ScheduleStats();
    }

public:
    TSenderActor(TActorId target)
        : Target(target)
        , SendEvents(0)
        , AckEvents(0)
        , PeriodStart(TInstant::Now())
    {}

    void Bootstrap() {
        if (Target) {
            Become(&TThis::Main);
            SendToTarget();
            ScheduleStats();
        } else {
            throw yexception();
        }
    }

    STFUNC(Main) {
        STRICT_STFUNC_BODY(
            sFunc(TEvAck, Ack);
            sFunc(TEvents::TEvWakeup, PrintStats);
        )
    }
};

class TReceiverActor : public TActor<TReceiverActor> {
    void Handle(NKikimr::TEvBlobStorage::TEvVPut::TPtr& ev) {
        Y_UNUSED(ev->Get());
        Send(ev->Sender, new TEvAck());
    }

public:
    TReceiverActor()
        : TActor<TReceiverActor>(&TThis::Main)
    {}

    STFUNC(Main) {
        STRICT_STFUNC_BODY(
            hFunc(NKikimr::TEvBlobStorage::TEvVPut, Handle);
        )
    }
};

namespace NLocalBench {

THolder<TActorSystemSetup> BuildActorSystemSetup(ui32 pools) {
    constexpr static auto threads = 1;
    Y_ABORT_UNLESS(threads > 0 && threads < 100);
    Y_ABORT_UNLESS(pools > 0 && pools < 10);

    auto setup = MakeHolder<TActorSystemSetup>();

    setup->NodeId = 1;

    setup->ExecutorsCount = pools;
    setup->Executors.Reset(new TAutoPtr<IExecutorPool>[pools]);
    for (ui32 idx : xrange(pools)) {
        setup->Executors[idx] = new TBasicExecutorPool(idx, threads, 50);
    }

    setup->Scheduler = new TBasicSchedulerThread(TSchedulerConfig(512, 0));

    return setup;
}

int test() {
#ifdef _unix_
    signal(SIGPIPE, SIG_IGN);
#endif
    signal(SIGINT, &OnTerminate);
    signal(SIGTERM, &OnTerminate);

    constexpr static auto pools = 2;

    THolder<TActorSystemSetup> actorSystemSetup = BuildActorSystemSetup(pools);
    TActorSystem actorSystem(actorSystemSetup);

    actorSystem.Start();

    TActorId receiver = actorSystem.Register(new TReceiverActor(), TMailboxType::HTSwap, std::min(pools - 1, 0));
    TActorId sender = actorSystem.Register(new TSenderActor(receiver), TMailboxType::HTSwap, std::min(pools - 1, 1));
    Y_UNUSED(sender);

    while (ShouldContinue.PollState() == TProgramShouldContinue::Continue) {
        Sleep(TDuration::MilliSeconds(200));
    }

    actorSystem.Stop();
    actorSystem.Cleanup();

    return ShouldContinue.GetReturnCode();
}

} // namespace NLocalBench

namespace NRemoteBench {

THolder<TActorSystemSetup> BuildActorSystemSetup(ui32 nodeId, ui32 totalNodes, ui32 basePort, NMonitoring::TDynamicCounters& counters) {
    constexpr static auto pools = 1;
    constexpr static auto threads = 1;

    auto setup = MakeHolder<TActorSystemSetup>();

    setup->NodeId = nodeId;

    setup->ExecutorsCount = 1;
    setup->Executors.Reset(new TAutoPtr<IExecutorPool>[pools]);
    for (ui32 idx : xrange(pools)) {
        setup->Executors[idx] = new TBasicExecutorPool(idx, threads, 50);
    }

    setup->Scheduler = new TBasicSchedulerThread(TSchedulerConfig(512, 0));

    setup->LocalServices.emplace_back(MakePollerActorId(), TActorSetupCmd(CreatePollerActor(), TMailboxType::ReadAsFilled, 0));

    TIntrusivePtr<TTableNameserverSetup> nameserverTable = new TTableNameserverSetup();
    for (ui32 xnode : xrange<ui32>(1, totalNodes + 1)) {
        nameserverTable->StaticNodeTable[xnode] = std::make_pair("127.0.0.1", basePort + xnode);
    }

    setup->LocalServices.emplace_back(
        NDnsResolver::MakeDnsResolverActorId(),
        TActorSetupCmd(NDnsResolver::CreateOnDemandDnsResolver(), TMailboxType::ReadAsFilled, 0)
    );

    setup->LocalServices.emplace_back(
        GetNameserviceActorId(),
        TActorSetupCmd(CreateNameserverTable(nameserverTable), TMailboxType::ReadAsFilled, 0)
    );

    TIntrusivePtr<TInterconnectProxyCommon> icCommon = new TInterconnectProxyCommon();
    icCommon->TechnicalSelfHostName = "127.0.0.1";
    icCommon->MonCounters = counters.GetSubgroup("counters", "interconnect");
    icCommon->NameserviceId = GetNameserviceActorId();

    setup->Interconnect.ProxyActors.resize(totalNodes + 1);
    for (ui32 xnode : xrange<ui32>(1, totalNodes + 1)) {
        if (xnode != nodeId) {
            IActor *actor = new TInterconnectProxyTCP(xnode, icCommon);
            setup->Interconnect.ProxyActors[xnode] = TActorSetupCmd(actor, TMailboxType::ReadAsFilled, 0);
        } else {
            IActor *listener = new TInterconnectListenerTCP("127.0.0.1", basePort + xnode, icCommon);
            setup->LocalServices.emplace_back(
                MakeInterconnectListenerActorId(false),
                TActorSetupCmd(listener, TMailboxType::ReadAsFilled, 0)
            );
        }
    }

    return setup;
}

int test() {
#ifdef _unix_
    signal(SIGPIPE, SIG_IGN);
#endif
    signal(SIGINT, &OnTerminate);
    signal(SIGTERM, &OnTerminate);

    constexpr static auto totalNodes = 2;
    constexpr static auto basePort = 9876;

    TVector<TIntrusivePtr<NMonitoring::TDynamicCounters>> countersHolder;
    countersHolder.emplace_back(new NMonitoring::TDynamicCounters());
    countersHolder.emplace_back(new NMonitoring::TDynamicCounters());

    auto node1 = BuildActorSystemSetup(1, totalNodes, basePort, *countersHolder[0]);
    auto node2 = BuildActorSystemSetup(2, totalNodes, basePort, *countersHolder[1]);

    TActorSystem sys1(node1);
    TActorSystem sys2(node2);

    sys1.Start();
    sys2.Start();

    TActorId receiver = sys1.Register(new TReceiverActor());
    Cerr << "Receiver: " << receiver << Endl;
    TActorId sender = sys2.Register(new TSenderActor(receiver));
    Cerr << "Sender: " << sender << Endl;

    while (ShouldContinue.PollState() == TProgramShouldContinue::Continue) {
        Sleep(TDuration::MilliSeconds(200));
    }

    sys1.Stop();
    sys2.Stop();
    sys1.Cleanup();
    sys2.Cleanup();

    return ShouldContinue.GetReturnCode();
}

} // namespace NRemoteBench

} // namespace

int main() {
    return NRemoteBench::test();
}
