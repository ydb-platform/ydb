#include <ydb/library/services/services.pb.h>
#include <util/stream/file.h>
#include <util/system/mlock.h>
#include <library/cpp/getopt/last_getopt.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/core/process_stats.h>
#include <ydb/library/actors/protos/services_common.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/discovery/discovery.h>
#include <ydb/apps/etcd_proxy/service/etcd_base_init.h>
#include <ydb/apps/etcd_proxy/service/etcd_watch.h>
#include <ydb/apps/etcd_proxy/service/etcd_grpc.h>
#include <ydb/core/grpc_services/base/base.h>

#include "proxy.h"

namespace NEtcd {

std::atomic<bool> TProxy::Quit;

void TProxy::OnTerminate(int) {
    Quit.store(true);
}

int TProxy::Init() {
    ActorSystem->Start();
    ActorSystem->Register(NActors::CreateProcStatCollector(TDuration::Seconds(7), MetricRegistry));
    return Discovery();
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

        NYdb::TDriverConfig config;
        config.SetEndpoint(Endpoint);
        config.SetDatabase(Database);
        const auto driver = NYdb::TDriver(config);
        Stuff->Client = std::make_unique<NYdb::NQuery::TQueryClient>(driver);
        return 0;
    } else {
        std::cout << res.GetIssues().ToString() << std::endl;
        return 1;
    }
}

int TProxy::StartServer() {
    if (const auto res = Stuff->Client->ExecuteQuery(NEtcd::GetLastRevisionSQL(), NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync(); res.IsSuccess()) {
        if (auto result = res.GetResultSetParser(0); result.TryNextRow()) {
            const auto revision = NYdb::TValueParser(result.GetValue(0)).GetInt64();
            const auto lease = NYdb::TValueParser(result.GetValue(1)).GetInt64();
            std::cout << "The last revision is " << revision << ", the last lease is " << lease << '.' << std::endl;
            Stuff->Revision.store(revision);
            Stuff->Lease.store(lease);
        } else {
            std::cout << "Unexpected result of get last revision and lease." << std::endl;
            return 1;
        }
    } else {
        std::cout << res.GetIssues().ToString() << std::endl;
        return 1;
    }

    NYdbGrpc::TServerOptions opts;
    opts.SetPort(ListeningPort);

    if (!Root.empty() || !Cert.empty() || !Key.empty()) {
        NYdbGrpc::TSslData sslData {
            .Cert = TFileInput(Cert).ReadAll(),
            .Key = TFileInput(Key).ReadAll(),
            .Root = TFileInput(Root).ReadAll(),
            .DoRequestClientCertificate = true
        };
        opts.SetSslData(std::move(sslData));
    }

    const auto watchtower = ActorSystem->Register(NEtcd::BuildWatchtower(Counters, Stuff));

    GRpcServer = std::make_unique<NYdbGrpc::TGRpcServer>(opts, Counters);
    GRpcServer->AddService(new NEtcd::TEtcdKVService(ActorSystem.get(), Counters, watchtower, Stuff));
    GRpcServer->AddService(new NEtcd::TEtcdWatchService(ActorSystem.get(), Counters, watchtower, Stuff));
    GRpcServer->AddService(new NEtcd::TEtcdLeaseService(ActorSystem.get(), Counters, watchtower, Stuff));
    GRpcServer->Start();
    std::cout << "Etcd service over " << Database << " on " << Endpoint << " was started." << std::endl;
    return 0;
}

int TProxy::Run() {
    if (const auto res = Init()) {
        return res;
    }
    if (Initialize_) {
        if (const auto res = InitDatabase()) {
            return res;
        }
    } else {
        if (const auto res = StartServer()) {
            return res;
        }
        do
            Sleep(TDuration::MilliSeconds(97));
        while (!Quit);
    }
    return Shutdown();
}

int TProxy::InitDatabase() {
    if (const auto res = Stuff->Client->ExecuteQuery(NEtcd::GetCreateTablesSQL(), NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync(); res.IsSuccess()) {
        std::cout << "Database " << Database << " on " << Endpoint << " was initialized." << std::endl;
        return 0;
    } else {
        std::cout << res.GetIssues().ToString() << std::endl;
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
    : Stuff(std::make_shared<NEtcd::TSharedStuff>()), MetricRegistry(NMonitoring::TMetricRegistry::SharedInstance()), Counters(MakeIntrusive<::NMonitoring::TDynamicCounters>())
{
    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
    bool useStdErr = false;
    bool mlock = false;

    opts.AddLongOption("database", "YDB etcd databse").Required().RequiredArgument("DATABASE").StoreResult(&Database);
    opts.AddLongOption("endpoint", "YDB endpoint to connect").Required().RequiredArgument("ENDPOINT").StoreResult(&Endpoint);
    opts.AddLongOption("port", "Listening port").Optional().DefaultValue("2379").RequiredArgument("PORT").StoreResult(&ListeningPort);
    opts.AddLongOption("init", "Initialize etcd databse").NoArgument().SetFlag(&Initialize_);
    opts.AddLongOption("stderr", "Redirect log to stderr").NoArgument().SetFlag(&useStdErr);
    opts.AddLongOption("mlock", "Lock resident memory").NoArgument().SetFlag(&mlock);

    opts.AddLongOption("ca", "SSL CA certificate file").Optional().RequiredArgument("CA").StoreResult(&Root);
    opts.AddLongOption("cert", "SSL certificate file").Optional().RequiredArgument("CERT").StoreResult(&Cert);
    opts.AddLongOption("key", "SSL key file").Optional().RequiredArgument("KEY").StoreResult(&Key);

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

    ActorSystem = std::make_unique<NActors::TActorSystem>(actorSystemSetup, nullptr, loggerSettings);
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
