#include "interconnect_helpers.h"
#include "service_node.h"

#include "grpc_service.h"

#include <ydb/library/actors/helpers/selfping_actor.h>

#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/backtrace/backtrace.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>

#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/core/scheduler_actor.h>
#include <ydb/library/actors/dnsresolver/dnsresolver.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect_common.h>
#include <ydb/library/actors/interconnect/interconnect_tcp_proxy.h>
#include <ydb/library/actors/interconnect/interconnect_tcp_server.h>
#include <ydb/library/actors/interconnect/poller_actor.h>
#include <library/cpp/yson/node/node_io.h>

#include <util/stream/file.h>
#include <util/system/env.h>
#include <util/system/fs.h>

namespace NYql::NDqs {
    using namespace NActors;
    using namespace NActors::NDnsResolver;
    using namespace NYdbGrpc;

    class TYqlLogBackend: public TLogBackend {
        void WriteData(const TLogRecord& rec) override {
            TString message(rec.Data, rec.Len);
            if (message.find("ICP01 ready to work") != TString::npos) {
                return;
            }
            YQL_CLOG(DEBUG, ProviderDq) << message;
        }

        void ReopenLog() override { }
    };

    static void InitSelfPingActor(NActors::TActorSystemSetup* setup, NMonitoring::TDynamicCounterPtr rootCounters)
    {
        const TDuration selfPingInterval = TDuration::MilliSeconds(10);

        const auto counters = rootCounters->GetSubgroup("counters", "utils");

        for (size_t poolId = 0; poolId < setup->GetExecutorsCount(); ++poolId) {
            const auto& poolName = setup->GetPoolName(poolId);
            auto poolGroup = counters->GetSubgroup("execpool", poolName);
            auto selfPinfMaxCounter = poolGroup->GetCounter("SelfPingMaxUs", false);
            auto selfPinfAvgCounter = poolGroup->GetCounter("SelfPingAvgUs", false);
            auto selfPinfAvgCounterIn1s = poolGroup->GetCounter("SelfPingAvgUsIn1s", false);
            auto cpuTimeCounter = poolGroup->GetCounter("CpuMatBenchNs", false);
            IActor* selfPingActor = CreateSelfPingActor(selfPingInterval, selfPinfMaxCounter,
                selfPinfAvgCounter, selfPinfAvgCounterIn1s, cpuTimeCounter);
            setup->LocalServices.push_back(
                std::make_pair(TActorId(),
                               TActorSetupCmd(selfPingActor,
                                              TMailboxType::HTSwap,
                                              poolId)));
        }
    }

    std::tuple<THolder<NActors::TActorSystemSetup>, TIntrusivePtr<NActors::NLog::TSettings>> BuildActorSetup(
        ui32 nodeId,
        TString interconnectAddress,
        ui16 port,
        SOCKET socket,
        TVector<ui32> threads,
        NMonitoring::TDynamicCounterPtr counters,
        const TNameserverFactory& nameserverFactory,
        TMaybe<ui32> maxNodeId,
        const NYql::NProto::TDqConfig::TICSettings& icSettings)
    {
        auto setup = MakeHolder<TActorSystemSetup>();

        setup->NodeId = nodeId;

        if (threads.empty()) {
            threads = {icSettings.GetThreads()};
        }

        setup->ExecutorsCount = threads.size();
        setup->Executors.Reset(new TAutoPtr<IExecutorPool>[setup->ExecutorsCount]);
        for (ui32 i = 0; i < setup->ExecutorsCount; ++i) {
            setup->Executors[i] = new TBasicExecutorPool(
                i,
                threads[i],
                50,
                "pool-"+ToString(i)// poolName
            );
        }
        auto schedulerConfig = TSchedulerConfig();
        schedulerConfig.MonCounters = counters;

#define SET_VALUE(name) \
        if (icSettings.Has ## name()) { \
            schedulerConfig.name = icSettings.Get ## name (); \
            YQL_CLOG(DEBUG, ProviderDq) << "Scheduler IC " << #name << " set to " << schedulerConfig.name; \
        }

        SET_VALUE(ResolutionMicroseconds);
        SET_VALUE(SpinThreshold);
        SET_VALUE(ProgressThreshold);
        SET_VALUE(UseSchedulerActor);
        SET_VALUE(RelaxedSendPaceEventsPerSecond);
        SET_VALUE(RelaxedSendPaceEventsPerCycle);
        SET_VALUE(RelaxedSendThresholdEventsPerSecond);
        SET_VALUE(RelaxedSendThresholdEventsPerCycle);

#undef SET_VALUE

        setup->Scheduler = CreateSchedulerThread(schedulerConfig);

        YQL_CLOG(DEBUG, ProviderDq) << "Initializing local services";
        setup->LocalServices.emplace_back(MakePollerActorId(), TActorSetupCmd(CreatePollerActor(), TMailboxType::ReadAsFilled, 0));
        if (IActor* schedulerActor = CreateSchedulerActor(schedulerConfig)) {
            TActorId schedulerActorId = MakeSchedulerActorId();
            setup->LocalServices.emplace_back(schedulerActorId, TActorSetupCmd(schedulerActor, TMailboxType::ReadAsFilled, 0));
        }

        NActors::TActorId loggerActorId(nodeId, "logger");
        auto logSettings = MakeIntrusive<NActors::NLog::TSettings>(loggerActorId,
            0, NActors::NLog::PRI_INFO);
        static TString defaultComponent = "ActorLib";
        logSettings->Append(0, 1024, [&](NActors::NLog::EComponent) -> const TString & { return defaultComponent; });
        TString explanation = "";
        if (YQL_CVLOG_ACTIVE(NLog::ELevel::TRACE, NLog::EComponent::CoreDq)) {
            logSettings->SetLevel(NActors::NLog::PRI_TRACE, 535 /*NKikimrServices::KQP_COMPUTE*/, explanation);
            logSettings->SetLevel(NActors::NLog::PRI_TRACE, 713 /*NKikimrServices::YQL_PROXY*/, explanation);
            logSettings->SetLevel(NActors::NLog::PRI_TRACE, 1165 /*NKikimrServices::DQ_TASK_RUNNER*/, explanation);
        }
        NActors::TLoggerActor *loggerActor = new NActors::TLoggerActor(
            logSettings,
            new TYqlLogBackend,
            counters->GetSubgroup("logger", "counters"));
        setup->LocalServices.emplace_back(logSettings->LoggerActorId, TActorSetupCmd(loggerActor, TMailboxType::Simple, 0));

        TIntrusivePtr<TTableNameserverSetup> nameserverTable = new TTableNameserverSetup();
        THashSet<ui32> staticNodeId;

        YQL_CLOG(DEBUG, ProviderDq) << "Initializing node table";
        nameserverTable->StaticNodeTable[nodeId] = std::make_pair(interconnectAddress, port);

        setup->LocalServices.emplace_back(
            MakeDnsResolverActorId(), TActorSetupCmd(CreateOnDemandDnsResolver(), TMailboxType::ReadAsFilled, 0));

        setup->LocalServices.emplace_back(
            GetNameserviceActorId(), TActorSetupCmd(nameserverFactory(nameserverTable), TMailboxType::ReadAsFilled, 0));


        InitSelfPingActor(setup.Get(), counters);

        TIntrusivePtr<TInterconnectProxyCommon> icCommon = new TInterconnectProxyCommon();
        icCommon->NameserviceId = GetNameserviceActorId();
        Y_UNUSED(counters);
        //icCommon->MonCounters = counters->GetSubgroup("counters", "interconnect");
        icCommon->MonCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();

#define SET_DURATION(name) \
        { \
            icCommon->Settings.name = TDuration::MilliSeconds(icSettings.Get ## name ## Ms()); \
            YQL_CLOG(DEBUG, ProviderDq) << "IC " << #name << " set to " << icCommon->Settings.name; \
        }

#define SET_VALUE(name) \
        { \
            icCommon->Settings.name = icSettings.Get ## name(); \
            YQL_CLOG(DEBUG, ProviderDq) << "IC " << #name << " set to " << icCommon->Settings.name; \
        }

        SET_DURATION(Handshake);
        SET_DURATION(DeadPeer);
        SET_DURATION(CloseOnIdle);

        SET_VALUE(SendBufferDieLimitInMB);
        SET_VALUE(TotalInflightAmountOfData);
        SET_VALUE(MergePerPeerCounters);
        SET_VALUE(MergePerDataCenterCounters);
        SET_VALUE(TCPSocketBufferSize);

        SET_DURATION(PingPeriod);
        SET_DURATION(ForceConfirmPeriod);
        SET_DURATION(LostConnection);
        SET_DURATION(BatchPeriod);

        SET_DURATION(MessagePendingTimeout);

        SET_VALUE(MessagePendingSize);
        SET_VALUE(MaxSerializedEventSize);
        SET_VALUE(EnableExternalDataChannel);

#undef SET_DURATION
#undef SET_VALUE

        YQL_CLOG(DEBUG, ProviderDq) << "Initializing proxy actors";
        auto effectiveMaxNodeId = maxNodeId.GetOrElse(static_cast<ui32>(ENodeIdLimits::MaxWorkerNodeId));
        setup->Interconnect.ProxyActors.resize(effectiveMaxNodeId + 1);
        for (ui32 id = 1; id <= effectiveMaxNodeId; ++id) {
            if (nodeId != id) {
                IActor* actor = new TInterconnectProxyTCP(id, icCommon);
                setup->Interconnect.ProxyActors[id] = TActorSetupCmd(actor, TMailboxType::ReadAsFilled, 0);
            }
        }

        // start listener
        YQL_CLOG(DEBUG, ProviderDq) << "Start listener";
        {
            icCommon->TechnicalSelfHostName = interconnectAddress;
            YQL_CLOG(INFO, ProviderDq) << "Start listener " << interconnectAddress << ":" << port << " socket: " << socket;
            IActor* listener;
            TMaybe<SOCKET> maybeSocket = socket < 0
                ? Nothing()
                : TMaybe<SOCKET>(socket);

            listener = new NActors::TInterconnectListenerTCP(interconnectAddress, port, icCommon, maybeSocket);

            setup->LocalServices.emplace_back(
                MakeInterconnectListenerActorId(false),
                TActorSetupCmd(listener, TMailboxType::ReadAsFilled, 0));
        }

        YQL_CLOG(DEBUG, ProviderDq) << "Actor initialization complete";

#ifdef _unix_
        signal(SIGPIPE, SIG_IGN);
#endif

        return std::make_tuple(std::move(setup), logSettings);
    }

    std::tuple<TString, TString> GetLocalAddress(const TString* overrideHostname, int family) {
        constexpr auto MaxLocalHostNameLength = 4096;
        std::array<char, MaxLocalHostNameLength> buffer;
        buffer.fill(0);
        TString hostName;
        TString localAddress;

        int result = gethostname(buffer.data(), buffer.size() - 1);
        if (result != 0) {
            Cerr << "gethostname failed for " << std::string_view{buffer.data(), buffer.size()} << " error " << strerror(errno) << Endl;
            return std::make_tuple(hostName, localAddress);
        }

        if (overrideHostname) {
            memcpy(&buffer[0], overrideHostname->c_str(), Min<int>(
                overrideHostname->size()+1, buffer.size()-1
            ));
        }

        hostName = &buffer[0];

        addrinfo request;
        memset(&request, 0, sizeof(request));
        request.ai_family = family;
        request.ai_socktype = SOCK_STREAM;

        addrinfo* response = nullptr;
        result = getaddrinfo(buffer.data(), nullptr, &request, &response);
        if (result != 0) {
            Cerr << "getaddrinfo failed for " << std::string_view{buffer.data(), buffer.size()} << " error " << gai_strerror(result) << Endl;
            return std::make_tuple(hostName, localAddress);
        }

        std::unique_ptr<addrinfo, void (*)(addrinfo*)> holder(response, &freeaddrinfo);

        if (!response->ai_addr) {
            Cerr << "getaddrinfo failed: no ai_addr" << Endl;
            return std::make_tuple(hostName, localAddress);
        }

        auto* sa = response->ai_addr;
        Y_ABORT_UNLESS(sa->sa_family == family);
        switch (family) {
        case AF_INET6: 
            inet_ntop(AF_INET6, &(((struct sockaddr_in6*)sa)->sin6_addr),
                    &buffer[0], buffer.size() - 1);
            break;
        case AF_INET: 
            inet_ntop(AF_INET, &(((struct sockaddr_in*)sa)->sin_addr),
                    &buffer[0], buffer.size() - 1);
            break;
        default:
            Y_ABORT_UNLESS(false);
            break;
        }

        localAddress = &buffer[0];

        return std::make_tuple(hostName, localAddress);
    }

    std::tuple<TString, TString> GetUserToken(const TMaybe<TString>& maybeUser, const TMaybe<TString>& maybeTokenFile)
    {
        auto home = GetEnv("HOME");
        auto systemUser = GetEnv("USER");

        TString userName = maybeUser
            ? *maybeUser
            : systemUser;

        TString tokenFile = maybeTokenFile
            ? *maybeTokenFile
            : home + "/.yt/token";

        TString token = NFs::Exists(tokenFile) 
            ? TFileInput(tokenFile).ReadLine()
            : "";

        return std::make_tuple(userName, token);
    }
}
