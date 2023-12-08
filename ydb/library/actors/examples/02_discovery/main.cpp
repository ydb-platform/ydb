#include "services.h"

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/dnsresolver/dnsresolver.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect_common.h>
#include <ydb/library/actors/interconnect/interconnect_tcp_proxy.h>
#include <ydb/library/actors/interconnect/interconnect_tcp_server.h>
#include <ydb/library/actors/interconnect/poller_actor.h>
#include <ydb/library/actors/interconnect/poller_tcp.h>
#include <ydb/library/actors/util/should_continue.h>

#include <util/system/sigset.h>
#include <util/generic/xrange.h>

using namespace NActors;
using namespace NActors::NDnsResolver;

static const ui32 CfgTotalReplicaNodes = 5;
static const ui16 CfgBasePort = 13300;
static const ui16 CfgHttpPort = 8881;
static const TString  PublishKey = "endpoint";

static TProgramShouldContinue ShouldContinue;

void OnTerminate(int) {
    ShouldContinue.ShouldStop();
}

THolder<TActorSystemSetup> BuildActorSystemSetup(ui32 nodeId, ui32 threads, NMonitoring::TDynamicCounters &counters) {
    Y_ABORT_UNLESS(threads > 0 && threads < 100);

    auto setup = MakeHolder<TActorSystemSetup>();

    setup->NodeId = nodeId;

    setup->ExecutorsCount = 1;
    setup->Executors.Reset(new TAutoPtr<IExecutorPool>[1]);
    setup->Executors[0] = new TBasicExecutorPool(0, threads, 50);
    setup->Scheduler = new TBasicSchedulerThread(TSchedulerConfig(512, 0));

    setup->LocalServices.emplace_back(MakePollerActorId(), TActorSetupCmd(CreatePollerActor(), TMailboxType::ReadAsFilled, 0));

    TIntrusivePtr<TTableNameserverSetup> nameserverTable = new TTableNameserverSetup();
    for (ui32 xnode : xrange<ui32>(1, CfgTotalReplicaNodes + 1)) {
        nameserverTable->StaticNodeTable[xnode] = std::make_pair("127.0.0.1", CfgBasePort + xnode);
    }

    setup->LocalServices.emplace_back(
        MakeDnsResolverActorId(),
        TActorSetupCmd(CreateOnDemandDnsResolver(), TMailboxType::ReadAsFilled, 0)
    );

    setup->LocalServices.emplace_back(
        GetNameserviceActorId(),
        TActorSetupCmd(CreateNameserverTable(nameserverTable), TMailboxType::ReadAsFilled, 0)
    );

    TIntrusivePtr<TInterconnectProxyCommon> icCommon = new TInterconnectProxyCommon();
    icCommon->NameserviceId = GetNameserviceActorId();
    icCommon->MonCounters = counters.GetSubgroup("counters", "interconnect");
    icCommon->TechnicalSelfHostName = "127.0.0.1";

    setup->Interconnect.ProxyActors.resize(CfgTotalReplicaNodes + 1);
    for (ui32 xnode : xrange<ui32>(1, CfgTotalReplicaNodes + 1)) {
        if (xnode != nodeId) {
            IActor *actor = new TInterconnectProxyTCP(xnode, icCommon);
            setup->Interconnect.ProxyActors[xnode] = TActorSetupCmd(actor, TMailboxType::ReadAsFilled, 0);
        }
        else {
            IActor *listener = new TInterconnectListenerTCP("127.0.0.1", CfgBasePort + xnode, icCommon);
            setup->LocalServices.emplace_back(
                MakeInterconnectListenerActorId(false),
                TActorSetupCmd(listener, TMailboxType::ReadAsFilled, 0)
            );
        }
    }

    return setup;
}

int main(int argc, char **argv) {
    Y_UNUSED(argc);
    Y_UNUSED(argv);

#ifdef _unix_
    signal(SIGPIPE, SIG_IGN);
#endif
    signal(SIGINT, &OnTerminate);
    signal(SIGTERM, &OnTerminate);

    TIntrusivePtr<TExampleStorageConfig> config = new TExampleStorageConfig();
    for (ui32 nodeid : xrange<ui32>(1, CfgTotalReplicaNodes + 1)) {
        config->Replicas.push_back(MakeReplicaId(nodeid));
    }

    TVector<THolder<TActorSystem>> actorSystemHolder;
    TVector<TIntrusivePtr<NMonitoring::TDynamicCounters>> countersHolder;
    for (ui32 nodeid : xrange<ui32>(1, CfgTotalReplicaNodes + 1)) {
        countersHolder.emplace_back(new NMonitoring::TDynamicCounters());
        THolder<TActorSystemSetup> actorSystemSetup = BuildActorSystemSetup(nodeid, 2, *countersHolder.back());
        actorSystemSetup->LocalServices.emplace_back(
            TActorId(),
            TActorSetupCmd(CreateEndpointActor(config.Get(), PublishKey, CfgHttpPort + nodeid), TMailboxType::HTSwap, 0)
        );

        actorSystemSetup->LocalServices.emplace_back(
            MakeReplicaId(nodeid),
            TActorSetupCmd(CreateReplica(), TMailboxType::ReadAsFilled, 0)
        );

        actorSystemHolder.emplace_back(new TActorSystem(actorSystemSetup));
    }

    for (auto &xh : actorSystemHolder)
        xh->Start();

    while (ShouldContinue.PollState() == TProgramShouldContinue::Continue) {
        Sleep(TDuration::MilliSeconds(200));
    }

    // stop actorsystem to not generate new reqeusts for external services
    // no events would be processed anymore
    for (auto &xh : actorSystemHolder)
        xh->Stop();

    // and then cleanup actorsystem
    // from this moment working with actorsystem prohibited
    for (auto &xh : actorSystemHolder)
        xh->Cleanup();

    return ShouldContinue.GetReturnCode();
}
