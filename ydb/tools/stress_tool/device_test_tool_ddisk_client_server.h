#pragma once

#include "defs.h"
#include "device_test_tool_ddisk_test.h"

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/dnsresolver/dnsresolver.h>
#include <ydb/library/actors/interconnect/interconnect_tcp_proxy.h>
#include <ydb/library/actors/interconnect/interconnect_tcp_server.h>
#include <ydb/library/actors/interconnect/interconnect_proxy_wrapper.h>
#include <ydb/library/actors/interconnect/handshake_broker.h>
#include <ydb/library/actors/interconnect/poller/poller_actor.h>

#include <util/system/event.h>

#include <algorithm>

namespace NKikimr {

static constexpr ui32 InterconnectPoolId = 0;

struct TInterconnectPeer {
    ui32 NodeId;
    TString Address;
    ui16 Port;
};

static TInterconnectProxyCommon::TPtr MakeInterconnectCommon(
        const TIntrusivePtr<NMonitoring::TDynamicCounters>& counters, ui32 selfNodeId) {
    auto common = MakeIntrusive<TInterconnectProxyCommon>();
    common->NameserviceId = GetNameserviceActorId();
    common->MonCounters = counters->GetSubgroup("nodeId", ToString(selfNodeId));
    common->ClusterUUID = "stress-tool-cluster";
    common->AcceptUUID = {common->ClusterUUID};
    common->TechnicalSelfHostName = "localhost";
    common->Settings.Handshake = TDuration::Seconds(5);
    common->Settings.DeadPeer = TDuration::Seconds(10);
    common->Settings.CloseOnIdle = TDuration::Minutes(10);
    common->Settings.SendBufferDieLimitInMB = 2048;
    common->Settings.TotalInflightAmountOfData = 256ull * 1024 * 1024;
    common->Settings.TCPSocketBufferSize = 8 * 1024 * 1024;
    common->Settings.EnableExternalDataChannel = true;
    common->OutgoingHandshakeInflightLimit = 3;
    return common;
}

static void SetupInterconnectServices(TActorSystemSetup* setup,
        TInterconnectProxyCommon::TPtr common,
        ui32 selfNodeId, const TString& selfAddress, ui16 selfPort,
        const TVector<TInterconnectPeer>& peers,
        bool listen) {
    auto names = MakeIntrusive<TTableNameserverSetup>();
    // Host field must match TechnicalSelfHostName for handshake verification.
    // ResolveHost is the actual address used for DNS resolution and TCP connection.
    const TString& hostName = common->TechnicalSelfHostName;
    names->StaticNodeTable[selfNodeId] = TTableNameserverSetup::TNodeInfo(
        selfAddress, hostName, selfAddress, selfPort, TNodeLocation{});
    for (const auto& peer : peers) {
        names->StaticNodeTable[peer.NodeId] = TTableNameserverSetup::TNodeInfo(
            peer.Address, hostName, peer.Address, peer.Port, TNodeLocation{});
    }

    setup->LocalServices.emplace_back(
        NDnsResolver::MakeDnsResolverActorId(),
        TActorSetupCmd(NDnsResolver::CreateOnDemandDnsResolver(), TMailboxType::ReadAsFilled, InterconnectPoolId));

    setup->LocalServices.emplace_back(
        GetNameserviceActorId(),
        TActorSetupCmd(CreateNameserverTable(names, InterconnectPoolId), TMailboxType::ReadAsFilled, InterconnectPoolId));

    setup->LocalServices.emplace_back(
        MakePollerActorId(),
        TActorSetupCmd(CreatePollerActor(), TMailboxType::ReadAsFilled, InterconnectPoolId));

    setup->LocalServices.emplace_back(
        MakeHandshakeBrokerOutId(),
        TActorSetupCmd(CreateHandshakeBroker(*common->OutgoingHandshakeInflightLimit),
            TMailboxType::ReadAsFilled, InterconnectPoolId));

    ui32 maxNodeId = selfNodeId;
    for (const auto& peer : peers) {
        maxNodeId = std::max(maxNodeId, peer.NodeId);
    }
    setup->Interconnect.ProxyActors.resize(maxNodeId + 1);
    setup->Interconnect.ProxyWrapperFactory = CreateProxyWrapperFactory(common, InterconnectPoolId);
    for (const auto& peer : peers) {
        setup->Interconnect.ProxyActors[peer.NodeId] = {
            new TInterconnectProxyTCP(peer.NodeId, common), TMailboxType::ReadAsFilled, InterconnectPoolId};
    }

    if (listen) {
        setup->LocalServices.emplace_back(TActorId(),
            TActorSetupCmd(new TInterconnectListenerTCP(selfAddress, selfPort, common),
                TMailboxType::ReadAsFilled, InterconnectPoolId));
    }
}

////////////////////////////////////////////////////////////////////////////////

static TSystemEvent DDiskServerStopEvent(TSystemEvent::rAuto);

template<ui32 ChunkSize = 128 << 20>
struct TDDiskServer : public TPDiskTest<ChunkSize> {
    using TBase = TPDiskTest<ChunkSize>;
    NDevicePerfTest::TDDiskTest DDiskTestProto;
    ui32 ServerNodeId;
    ui32 ClientNodeId;
    ui16 Port;
    static constexpr ui32 DDiskSlotId = 1;

    TDDiskServer(const TPerfTestConfig& cfg, const NDevicePerfTest::TDDiskTest& testProto,
                 ui32 serverNodeId, ui32 clientNodeId, ui16 port)
        : TBase(cfg, DefaultPDiskTestProto())
        , DDiskTestProto(testProto)
        , ServerNodeId(serverNodeId)
        , ClientNodeId(clientNodeId)
        , Port(port)
    {
        // Re-key PDisk actor IDs and the logger to ServerNodeId before Init() runs;
        // otherwise local sends from DDisk -> PDisk/logger get routed through
        // interconnect to NodeId=1 and silently drop.
        TBase::SetSelfNodeId(serverNodeId);
    }

    static const NDevicePerfTest::TPDiskTest& DefaultPDiskTestProto() {
        static const NDevicePerfTest::TPDiskTest proto;
        return proto;
    }

    NDDisk::TDDiskConfig ExtractDDiskConfig() const {
        NDDisk::TDDiskConfig config;
        bool initialized = false;
        for (ui32 i = 0; i < DDiskTestProto.DDiskTestListSize(); ++i) {
            const auto& record = DDiskTestProto.GetDDiskTestList(i);
            if (record.Command_case() != NKikimr::TEvLoadTestRequest::CommandCase::kDDiskLoad) {
                continue;
            }
            const auto& load = record.GetDDiskLoad();
            const bool useSQPoll = load.GetSQPoll();
            const bool useIOPoll = load.GetIOPoll();
            if (!initialized) {
                config.UseSQPoll = useSQPoll;
                config.UseIOPoll = useIOPoll;
                initialized = true;
                continue;
            }
            if (config.UseSQPoll != useSQPoll || config.UseIOPoll != useIOPoll) {
                ythrow TWithBackTrace<yexception>()
                    << "Invalid configuration: all DDiskLoad entries must use identical SQPoll/IOPoll values";
            }
        }
        return config;
    }

    void Init() override {
        try {
            TBase::DoBasicSetup();

            // Remove the empty nameservice registered by DoBasicSetup() since
            // SetupInterconnectServices will register a proper one with node table.
            const TActorId nameserviceId = GetNameserviceActorId();
            auto& services = TBase::Setup->LocalServices;
            services.erase(
                std::remove_if(services.begin(), services.end(),
                    [&](const auto& p) { return p.first == nameserviceId; }),
                services.end());

            auto groupInfo = MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType::ErasureNone);
            const NDDisk::TDDiskConfig ddiskConfig = ExtractDDiskConfig();

            for (ui32 i = 0; i < TBase::Cfg.NumDevices(); ++i) {
                const TActorId ddiskId = MakeBlobStorageDDiskId(ServerNodeId, i + 1, DDiskSlotId);
                const TVDiskID vdiskId(0, 1, 0, 0, i);
                TVDiskConfig::TBaseInfo baseInfo(
                    TVDiskIdShort(vdiskId),
                    TBase::PDiskActorIds[i],
                    TBase::PDiskGuids[i],
                    1,
                    TBase::Cfg.DeviceType,
                    DDiskSlotId,
                    NKikimrBlobStorage::TVDiskKind::Default,
                    1000,
                    "ddisk_pool");
                NDDisk::TPersistentBufferFormat pbFormat{512, 512, 128_MB, 8, 5000, 4096_MB * 8, 64, 1024};
                TActorSetupCmd ddiskSetup(NDDisk::CreateDDiskActor(std::move(baseInfo), groupInfo, std::move(pbFormat),
                    NDDisk::TDDiskConfig(ddiskConfig), TBase::Counters),
                    TMailboxType::Revolving, 1);
                TBase::Setup->LocalServices.push_back(
                    std::pair<TActorId, TActorSetupCmd>(ddiskId, std::move(ddiskSetup)));
            }

            // Server only accepts incoming connections, but the interconnect handshake still
            // resolves the peer NodeId via the local nameserver, so we must register the
            // client with a placeholder address (port 0; server never initiates connection).
            auto common = MakeInterconnectCommon(TBase::Counters, ServerNodeId);

            TVector<TInterconnectPeer> peers = {{ClientNodeId, "::", 0}};
            SetupInterconnectServices(TBase::Setup.Get(), common, ServerNodeId,
                "::", Port, peers, /*listen=*/true);

            TBase::StartActorSystem();
        } catch (yexception& ex) {
            TBase::IsLastExceptionSet = true;
            VERBOSE_COUT("Error on server init, what# " << ex.what());
        }
    }

    void Run() override {
        if (TBase::IsLastExceptionSet) {
            return;
        }

        Cerr << "DDisk server (node " << ServerNodeId << ", expecting client node " << ClientNodeId << ")"
             << " is running on port " << Port
             << " with " << TBase::Cfg.NumDevices() << " device(s). Press Ctrl+C to stop." << Endl;

        DDiskServerStopEvent.Wait();

        if (TBase::ActorSystem.Get()) {
            TBase::ActorSystem->Stop();
            TBase::ActorSystem.Destroy();
        }
    }

    void Finish() override {
    }

    ~TDDiskServer() override {
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TDDiskClient : public TPerfTest {
    THolder<TActorSystemSetup> Setup;
    TIntrusivePtr<NActors::NLog::TSettings> LogSettings;
    THolder<TActorSystem> ActorSystem;
    TAppData AppData;
    TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
    yexception LastException;
    volatile bool IsLastExceptionSet = false;
    TActorId TestId;
    NDevicePerfTest::TDDiskTest TestProto;
    ui32 ClientNodeId;
    TVector<TInterconnectPeer> ServerPeers;
    ui32 NumDevicesPerServer;

    TDDiskClient(const TPerfTestConfig& cfg, const NDevicePerfTest::TDDiskTest& testProto,
                 ui32 clientNodeId, const TVector<TInterconnectPeer>& serverPeers,
                 ui32 numDevicesPerServer)
        : TPerfTest(cfg)
        , Setup(new TActorSystemSetup())
        , LogSettings(new NActors::NLog::TSettings(NActors::TActorId(clientNodeId, "logger"),
                                                   NActorsServices::LOGGER,
                                                   NActors::NLog::PRI_ERROR,
                                                   NActors::NLog::PRI_ERROR,
                                                   0))
        , AppData(0, 1, 3, 2, TMap<TString, ui32>(), nullptr, nullptr, nullptr, nullptr)
        , TestProto(testProto)
        , ClientNodeId(clientNodeId)
        , ServerPeers(serverPeers)
        , NumDevicesPerServer(numDevicesPerServer)
    {
    }

    void Init() override {
        try {
            Counters = MakeIntrusive<NMonitoring::TDynamicCounters>();

            const ui32 totalDevices = ServerPeers.size() * NumDevicesPerServer;
            Setup->NodeId = ClientNodeId;
            Setup->ExecutorsCount = 4;
            Setup->Executors.Reset(new TAutoPtr<IExecutorPool>[4]);
            Setup->Executors[0].Reset(new TBasicExecutorPool(0, 4, 20, "interconnect"));
            Setup->Executors[1].Reset(new TBasicExecutorPool(1, std::max(1u, totalDevices), 20, "load_actors"));
            Setup->Executors[2].Reset(new TBasicExecutorPool(2, std::max(4u, totalDevices + 1), 20, "perf_actors"));
            Setup->Executors[3].Reset(new TIOExecutorPool(3, 1, "IO"));
            Setup->Scheduler.Reset(new TBasicSchedulerThread(TSchedulerConfig(64, 20)));

            // Set up interconnect with all server peers
            auto common = MakeInterconnectCommon(Counters, ClientNodeId);

            SetupInterconnectServices(Setup.Get(), common, ClientNodeId,
                "::", 0, ServerPeers, /*listen=*/false);

            // Logger
            LogSettings->Append(
                NActorsServices::EServiceCommon_MIN,
                NActorsServices::EServiceCommon_MAX,
                NActorsServices::EServiceCommon_Name
            );
            LogSettings->Append(
                NKikimrServices::EServiceKikimr_MIN,
                NKikimrServices::EServiceKikimr_MAX,
                NKikimrServices::EServiceKikimr_Name
            );

            TString explanation;
            // INTERCONNECT is too chatty at DEBUG/TRACE; floor it at INFO regardless of --log-level.
            const NLog::EPriority icLevel = std::min<NLog::EPriority>(Cfg.LogLevel, NLog::PRI_INFO);
            LogSettings->SetLevel(Cfg.LogLevel, NKikimrServices::BS_LOAD_TEST, explanation);
            LogSettings->SetLevel(Cfg.LogLevel, NKikimrServices::BS_DDISK, explanation);
            LogSettings->SetLevel(icLevel, NActorsServices::INTERCONNECT, explanation);

            NActors::TLoggerActor *loggerActor = new NActors::TLoggerActor(LogSettings,
                NActors::CreateStderrBackend(),
                GetServiceCounters(Counters, "utils"));
            NActors::TActorSetupCmd loggerActorCmd(loggerActor, NActors::TMailboxType::Simple, 3);
            Setup->LocalServices.emplace_back(
                NActors::TActorId(ClientNodeId, "logger"), std::move(loggerActorCmd));

            // Build flat device list as cartesian product of servers and per-server devices.
            // Server with NodeId=N exposes PDisks numbered 1..NumDevicesPerServer.
            static constexpr ui32 DDiskSlotId = 1;
            TVector<TDDiskDeviceInfo> ddiskDevices;
            for (const auto& server : ServerPeers) {
                for (ui32 j = 0; j < NumDevicesPerServer; ++j) {
                    ddiskDevices.push_back({server.NodeId, j + 1, DDiskSlotId});
                }
            }

            TestId = MakeBlobStorageProxyID(1);
            TActorSetupCmd testSetup(
                new TDDiskPerfTestActor(ddiskDevices, Cfg, TestProto, Printer, Counters),
                TMailboxType::Revolving, 2);
            Setup->LocalServices.push_back(
                std::pair<TActorId, TActorSetupCmd>(TestId, std::move(testSetup)));

            ActorSystem.Reset(new TActorSystem(Setup, &AppData, LogSettings));
            ActorSystem->Start();

            // Give interconnect some time to establish connections to all servers
            Sleep(TDuration::Seconds(3));
        } catch (yexception& ex) {
            IsLastExceptionSet = true;
            VERBOSE_COUT("Error on client init, what# " << ex.what());
        }
    }

    void Run() override {
        if (IsLastExceptionSet) {
            return;
        }

        try {
            ActorSystem->Send(TestId, new TEvTablet::TEvBoot(MakeTabletID(0, 0, 1), 0, nullptr, TActorId(), nullptr));

            const ui32 totalDevices = ServerPeers.size() * NumDevicesPerServer;
            const ui32 eventsPerTest = totalDevices == 1 ? 1 : totalDevices + 1;
            for (ui32 i = 0; i < TestProto.DDiskTestListSize(); ++i) {
                for (ui32 j = 0; j < eventsPerTest; ++j) {
                    DDiskDoneEvent.Wait();
                    Printer->PrintResults();
                    DDiskResultsPrintedEvent.Signal();
                }
            }
        } catch (yexception ex) {
            LastException = ex;
            IsLastExceptionSet = true;
            VERBOSE_COUT(ex.what());
        }

        if (ActorSystem.Get()) {
            ActorSystem->Stop();
            ActorSystem.Destroy();
        }
        DDiskDoneEvent.Reset();
        if (IsLastExceptionSet) {
            IsLastExceptionSet = false;
            ythrow LastException;
        }
    }

    void Finish() override {
    }

    ~TDDiskClient() override {
    }
};

} // namespace NKikimr
