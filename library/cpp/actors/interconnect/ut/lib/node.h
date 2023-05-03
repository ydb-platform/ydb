#pragma once

#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/executor_pool_basic.h>
#include <library/cpp/actors/core/executor_pool_io.h>
#include <library/cpp/actors/core/scheduler_basic.h>
#include <library/cpp/actors/core/mailbox.h>
#include <library/cpp/actors/dnsresolver/dnsresolver.h>

#include <library/cpp/actors/interconnect/handshake_broker.h>
#include <library/cpp/actors/interconnect/interconnect_tcp_server.h>
#include <library/cpp/actors/interconnect/interconnect_tcp_proxy.h>
#include <library/cpp/actors/interconnect/interconnect_proxy_wrapper.h>

using namespace NActors;

class TNode {
    THolder<TActorSystem> ActorSystem;

public:
    TNode(ui32 nodeId, ui32 numNodes, const THashMap<ui32, ui16>& nodeToPort, const TString& address,
          NMonitoring::TDynamicCounterPtr counters, TDuration deadPeerTimeout,
          TChannelsConfig channelsSettings = TChannelsConfig(),
          ui32 numDynamicNodes = 0, ui32 numThreads = 1,
          TIntrusivePtr<NLog::TSettings> loggerSettings = nullptr) {
        TActorSystemSetup setup;
        setup.NodeId = nodeId;
        setup.ExecutorsCount = 2;
        setup.Executors.Reset(new TAutoPtr<IExecutorPool>[setup.ExecutorsCount]);
        setup.Executors[0].Reset(new TBasicExecutorPool(0, numThreads, 20 /* magic number */));
        setup.Executors[1].Reset(new TIOExecutorPool(1, 1));
        setup.Scheduler.Reset(new TBasicSchedulerThread());
        const ui32 interconnectPoolId = 0;

        auto common = MakeIntrusive<TInterconnectProxyCommon>();
        common->NameserviceId = GetNameserviceActorId();
        common->MonCounters = counters->GetSubgroup("nodeId", ToString(nodeId));
        common->ChannelsConfig = channelsSettings;
        common->ClusterUUID = "cluster";
        common->AcceptUUID = {common->ClusterUUID};
        common->TechnicalSelfHostName = address;
        common->Settings.Handshake = TDuration::Seconds(1);
        common->Settings.DeadPeer = deadPeerTimeout;
        common->Settings.CloseOnIdle = TDuration::Minutes(1);
        common->Settings.SendBufferDieLimitInMB = 512;
        common->Settings.TotalInflightAmountOfData = 512 * 1024;
        common->Settings.TCPSocketBufferSize = 2048 * 1024;
        common->OutgoingHandshakeInflightLimit = 3;

        setup.Interconnect.ProxyActors.resize(numNodes + 1 - numDynamicNodes);
        setup.Interconnect.ProxyWrapperFactory = CreateProxyWrapperFactory(common, interconnectPoolId);

        for (ui32 i = 1; i <= numNodes; ++i) {
            if (i == nodeId) {
                // create listener actor for local node "nodeId"
                setup.LocalServices.emplace_back(TActorId(), TActorSetupCmd(new TInterconnectListenerTCP(address,
                    nodeToPort.at(nodeId), common), TMailboxType::ReadAsFilled, interconnectPoolId));
            } else if (i <= numNodes - numDynamicNodes) {
                // create proxy actor to reach node "i"
                setup.Interconnect.ProxyActors[i] = {new TInterconnectProxyTCP(i, common),
                    TMailboxType::ReadAsFilled, interconnectPoolId};
            }
        }

        setup.LocalServices.emplace_back(MakePollerActorId(), TActorSetupCmd(CreatePollerActor(),
            TMailboxType::ReadAsFilled, 0));

        const TActorId loggerActorId = loggerSettings ? loggerSettings->LoggerActorId : TActorId(0, "logger");

        if (!loggerSettings) {
            constexpr ui32 LoggerComponentId = 410; // NKikimrServices::LOGGER
            loggerSettings = MakeIntrusive<NLog::TSettings>(
                loggerActorId,
                (NLog::EComponent)LoggerComponentId,
                NLog::PRI_INFO,
                NLog::PRI_DEBUG,
                0U);

            loggerSettings->Append(
                NActorsServices::EServiceCommon_MIN,
                NActorsServices::EServiceCommon_MAX,
                NActorsServices::EServiceCommon_Name
            );

            constexpr ui32 WilsonComponentId = 430; // NKikimrServices::WILSON
            static const TString WilsonComponentName = "WILSON";

            loggerSettings->Append(
                (NLog::EComponent)WilsonComponentId,
                (NLog::EComponent)WilsonComponentId + 1,
                [](NLog::EComponent) -> const TString & { return WilsonComponentName; });
        }

        // register nameserver table
        auto names = MakeIntrusive<TTableNameserverSetup>();
        for (ui32 i = 1; i <= numNodes; ++i) {
            names->StaticNodeTable[i] = TTableNameserverSetup::TNodeInfo(address, address, nodeToPort.at(i));
        }
        setup.LocalServices.emplace_back(
            NDnsResolver::MakeDnsResolverActorId(),
            TActorSetupCmd(
                NDnsResolver::CreateOnDemandDnsResolver(),
                TMailboxType::ReadAsFilled, interconnectPoolId));
        setup.LocalServices.emplace_back(GetNameserviceActorId(), TActorSetupCmd(
            CreateNameserverTable(names, interconnectPoolId), TMailboxType::ReadAsFilled,
            interconnectPoolId));

        // register logger
        setup.LocalServices.emplace_back(loggerActorId, TActorSetupCmd(new TLoggerActor(loggerSettings,
            CreateStderrBackend(), counters->GetSubgroup("subsystem", "logger")),
            TMailboxType::ReadAsFilled, 1));

        if (common->OutgoingHandshakeInflightLimit) {
            // create handshake broker actor
            setup.LocalServices.emplace_back(MakeHandshakeBrokerOutId(), TActorSetupCmd(
                    CreateHandshakeBroker(*common->OutgoingHandshakeInflightLimit),
                    TMailboxType::ReadAsFilled, interconnectPoolId));
        }

        auto sp = MakeHolder<TActorSystemSetup>(std::move(setup));
        ActorSystem.Reset(new TActorSystem(sp, nullptr, loggerSettings));
        ActorSystem->Start();
    }

    ~TNode() {
        ActorSystem->Stop();
    }

    bool Send(const TActorId& recipient, IEventBase* ev) {
        return ActorSystem->Send(recipient, ev);
    }

    TActorId RegisterActor(IActor* actor) {
        return ActorSystem->Register(actor);
    }

    TActorId InterconnectProxy(ui32 peerNodeId) {
        return ActorSystem->InterconnectProxy(peerNodeId);
    }

    void RegisterServiceActor(const TActorId& serviceId, IActor* actor) {
        const TActorId actorId = ActorSystem->Register(actor);
        ActorSystem->RegisterLocalService(serviceId, actorId);
    }

    TActorSystem *GetActorSystem() const {
        return ActorSystem.Get();
    }
};
