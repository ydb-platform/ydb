#include <ydb/library/services/services.pb.h>
#include <util/system/mlock.h>
#include <library/cpp/getopt/last_getopt.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/core/process_stats.h>
#include <ydb/library/actors/protos/services_common.pb.h>
#include <ydb/library/actors/interconnect/poller_actor.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_discovery/discovery.h>
#include <ydb/public/sdk/cpp/client/ydb_query/client.h>
#include <ydb/apps/etcd_proxy/service/grpc_service.h>
#include <ydb/core/grpc_services/base/base.h>

#include "proxy.h"
#include "blog.h"

namespace NKikimrConfig {
    class TAppConfig {};
}

namespace NEtcd {

std::atomic<bool> TProxy::Quit;

void TProxy::OnTerminate(int) {
    Quit = true;
}

int TProxy::Init() {
    ActorSystem->Start();
    ActorSystem->Register(NActors::CreateProcStatCollector(TDuration::Seconds(7), AppData.MetricRegistry));

    Poller = ActorSystem->Register(NActors::CreatePollerActor());
//    const NKikimrConfig::TAppConfig config;
//    const auto grpc = NKikimr::NGRpcService::CreateGRpcRequestProxySimple(config);
//    GrpcProxy = ActorSystem->Register(NKikimr::NGRpcService::CreateGRpcRequestProxySimple(config));
  //  const auto id = NKikimr::NGRpcService::CreateGRpcRequestProxyId(0);

/*
    auto grpcReqProxy = Config.HasGRpcConfig() && Config.GetGRpcConfig().GetSkipSchemeCheck()
                ? NGRpcService::CreateGRpcRequestProxySimple(Config)
                : NGRpcService::CreateGRpcRequestProxy(Config);
            setup->LocalServices.push_back(std::pair<TActorId,
                                           TActorSetupCmd>(NGRpcService::CreateGRpcRequestProxyId(i),
                                                           TActorSetupCmd(grpcReqProxy, TMailboxType::ReadAsFilled,
                                                                          appData->UserPoolId))); */
    if (const auto res = Discovery()) {
        return res;
    }

    return 0;
}

int TProxy::Discovery() {
    NYdb::TDriverConfig config;
    config.SetEndpoint(Endpoint);
    config.SetDatabase(Database);

    const auto driver = NYdb::TDriver(config);
    auto client = NYdb::NDiscovery::TDiscoveryClient(driver);
    const auto res = client.ListEndpoints().GetValueSync();
    if (res.IsSuccess()) {
        TStringBuilder str;
        str << res.GetEndpointsInfo().front().Address << ':' << res.GetEndpointsInfo().front().Port;
        Endpoint = str;
        Cerr << __func__ << ' ' << Endpoint << Endl;
        return 0;
    } else {
        Cerr << res.GetIssues().ToString() << Endl;
        return 1;
    }
}

int TProxy::StartGrpc() {
    NYdbGrpc::TServerOptions opts;
    opts.SetPort(ListeningPort);

    GRpcServer = std::make_unique<NYdbGrpc::TGRpcServer>(opts, Counters);
    GRpcServer->AddService(new NKikimr::NGRpcService::TEtcdGRpcService(ActorSystem.get(), Counters));
    GRpcServer->Start();

    return 0;
}

int TProxy::Run() try {
    if (const auto res = Init()) {
        return res;
    }
    if (Initialize_) {
        if (const auto res = InitDatabase()) {
            return res;
        }
    } else {
        if (const auto res = StartGrpc()) {
            return res;
        }
#ifndef NDEBUG
        Cout << "Started" << Endl;
#endif
        while (!Quit) {
            Sleep(TDuration::MilliSeconds(101));
        }
#ifndef NDEBUG
        Cout << Endl << "Finished" << Endl;
#endif
    }
    return Shutdown();
}
catch (const yexception& e) {
    Cerr << e.what() << Endl;
    return 1;
}

int TProxy::InitDatabase() {
    NYdb::TDriverConfig config;
    config.SetEndpoint(Endpoint);
    config.SetDatabase(Database);

    const auto driver = NYdb::TDriver(config);
    auto client = NYdb::NQuery::TQueryClient(driver);

    const auto query = NResource::Find("create.sql");
    const auto res = client.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();

    if (res.IsSuccess()) {
        return 0;
    } else {
        Cerr << res.GetIssues().ToString() << Endl;
        return 1;
    }
}

int TProxy::Shutdown() {
    if (GRpcServer)
        GRpcServer->Stop();
    ActorSystem->Stop();
    return 0;
}

TProxy::TProxy(int argc, char** argv)
    : Counters(MakeIntrusive<::NMonitoring::TDynamicCounters>()) {
    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
    bool useStdErr = false;
    bool mlock = false;
    TString sslCertificateFile;
    opts.AddLongOption("database", "YDB etcd databse").Required().RequiredArgument("DATABASE").StoreResult(&Database);
    opts.AddLongOption("endpoint", "YDB endpoint to connect").Required().RequiredArgument("ENDPOINT").StoreResult(&Endpoint);
    opts.AddLongOption("port", "Listening port").Optional().DefaultValue("2379").RequiredArgument("PORT").StoreResult(&ListeningPort);
    opts.AddLongOption("init", "Initialize etcd databse").NoArgument().SetFlag(&Initialize_);
    opts.AddLongOption("stderr", "Redirect log to stderr").NoArgument().SetFlag(&useStdErr);
    opts.AddLongOption("mlock", "Lock resident memory").NoArgument().SetFlag(&mlock);

    NLastGetopt::TOptsParseResult res(&opts, argc, argv);

    if (mlock) {
        LockAllMemory(LockCurrentMemory);
    }

    THolder<NActors::TActorSystemSetup> actorSystemSetup = BuildActorSystemSetup();

    TIntrusivePtr<NActors::NLog::TSettings> loggerSettings = BuildLoggerSettings();
    NActors::TLoggerActor* loggerActor = new NActors::TLoggerActor(
        loggerSettings,
        useStdErr ? NActors::CreateStderrBackend() : NActors::CreateSysLogBackend("etcd", false, true),
        NMonitoring::TMetricRegistry::SharedInstance());
    actorSystemSetup->LocalServices.emplace_back(loggerSettings->LoggerActorId, NActors::TActorSetupCmd(loggerActor, NActors::TMailboxType::HTSwap, 0));

    ActorSystem = std::make_unique<NActors::TActorSystem>(actorSystemSetup, &AppData, loggerSettings);
    AppData.MetricRegistry = NMonitoring::TMetricRegistry::SharedInstance();
}

TIntrusivePtr<NActors::NLog::TSettings> TProxy::BuildLoggerSettings() {
    const NActors::TActorId loggerActorId = NActors::TActorId(1, "logger");
    TIntrusivePtr<NActors::NLog::TSettings> loggerSettings = new NActors::NLog::TSettings(loggerActorId, NActorsServices::LOGGER, NActors::NLog::PRI_WARN);
    loggerSettings->Append(
        NActorsServices::EServiceCommon_MIN,
        NActorsServices::EServiceCommon_MAX,
        NActorsServices::EServiceCommon_Name
    );
    loggerSettings->Append(
        NKikimrServices::EServiceKikimr_MIN,
        NKikimrServices::EServiceKikimr_MAX,
        NKikimrServices::EServiceKikimr_Name
    );
    TString explanation;
    loggerSettings->SetLevel(NActors::NLog::PRI_DEBUG, NKikimrServices::PGWIRE, explanation);
    loggerSettings->SetLevel(NActors::NLog::PRI_DEBUG, NKikimrServices::PGYDB, explanation);
    return loggerSettings;
}

THolder<NActors::TActorSystemSetup> TProxy::BuildActorSystemSetup() {
    THolder<NActors::TActorSystemSetup> setup = MakeHolder<NActors::TActorSystemSetup>();
    setup->NodeId = 1;
    setup->Executors.Reset(new TAutoPtr<NActors::IExecutorPool>[1]);
    setup->ExecutorsCount = 1;
    setup->Executors[0] = new NActors::TBasicExecutorPool(0, 4, 10);
    setup->Scheduler = new NActors::TBasicSchedulerThread(NActors::TSchedulerConfig(512, 100));

    return setup;
}

}
