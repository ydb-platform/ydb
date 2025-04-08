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
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
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
    if (!Token.empty())
        config.SetAuthToken(Token);
    if (!CA.empty())
        config.UseSecureConnection(TFileInput(CA).ReadAll());

    const auto driver = NYdb::TDriver(config);
    auto client = NYdb::NDiscovery::TDiscoveryClient(driver);
    const auto res = client.ListEndpoints().GetValueSync();
    if (res.IsSuccess()) {
        std::ostringstream str;
        str << res.GetEndpointsInfo().front().Address << ':' << res.GetEndpointsInfo().front().Port;
        Endpoint = str.str();

        config.SetEndpoint(Endpoint);

        const auto driver = NYdb::TDriver(config);
        Stuff->Client = std::make_unique<NYdb::NQuery::TQueryClient>(driver);
        return 0;
    } else {
        std::cout << res.GetIssues().ToString() << std::endl;
        return 1;
    }
}

int TProxy::StartServer() {
    if (const auto res = Stuff->Client->ExecuteQuery(NEtcd::GetLastRevisionSQL(Stuff->TablePrefix), NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync(); res.IsSuccess()) {
        if (auto result = res.GetResultSetParser(0); result.TryNextRow()) {
            Stuff->Revision.store(NYdb::TValueParser(result.GetValue(0)).GetInt64());
        } else {
            std::cout << "Unexpected result of get last revision." << std::endl;
            return 1;
        }
        if (auto result = res.GetResultSetParser(1); result.TryNextRow()) {
            Stuff->Lease.store(NYdb::TValueParser(result.GetValue(0)).GetInt64());
        } else {
            std::cout << "Unexpected result of get last lease." << std::endl;
            return 1;
        }
        std::cout << "The last revision is " << Stuff->Revision.load() << ", the last lease is " << Stuff->Lease.load() << '.' << std::endl;
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
    }
    if (!ImportPrefix_.empty()) {
        if (const auto res = ImportDatabase()) {
            return res;
        }
    }
    if (!Initialize_ && ImportPrefix_.empty()) {
        if (const auto res = StartServer()) {
            return res;
        }
        do Sleep(TDuration::MilliSeconds(97));
        while (!Quit);
    }
    return Shutdown();
}

int TProxy::InitDatabase() {
    if (const auto res = Stuff->Client->ExecuteQuery(NEtcd::GetCreateTablesSQL(Stuff->TablePrefix), NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync(); res.IsSuccess()) {
        std::cout << "Database " << Database << " on " << Endpoint << " was initialized." << std::endl;
        return 0;
    } else {
        std::cout << res.GetIssues().ToString() << std::endl;
        return 1;
    }
}

int TProxy::ImportDatabase() {
    auto credentials = grpc::InsecureChannelCredentials();
    if (!Root.empty() || !Cert.empty() || !Key.empty()) {
        const grpc::SslCredentialsOptions opts {
            .pem_root_certs = TFileInput(Root).ReadAll(),
            .pem_private_key = TFileInput(Key).ReadAll(),
            .pem_cert_chain = TFileInput(Cert).ReadAll()
        };
        credentials = grpc::SslCredentials(opts);
    }

    const auto channel = grpc::CreateChannel(TString(ImportFrom_), credentials);
    const std::unique_ptr<etcdserverpb::KV::Stub> kv = etcdserverpb::KV::NewStub(channel);

    grpc::ClientContext readRangeCtx;
    etcdserverpb::RangeRequest rangeRequest;
    rangeRequest.set_key(ImportPrefix_);
    rangeRequest.set_range_end(NEtcd::IncrementKey(ImportPrefix_));

    etcdserverpb::RangeResponse rangeResponse;
    if (const auto& status = kv->Range(&readRangeCtx, rangeRequest, &rangeResponse); !status.ok()) {
        std::cout << status.error_message() << std::endl;
        return 1;
    }

    std::cout << rangeResponse.count() << " keys received." << std::endl;

    if (!rangeResponse.count())
        return 0;

    const auto& type = NYdb::TTypeBuilder()
        .BeginList()
            .BeginStruct()
                .AddMember("key").Primitive(NYdb::EPrimitiveType::String)
                .AddMember("created").Primitive(NYdb::EPrimitiveType::Int64)
                .AddMember("modified").Primitive(NYdb::EPrimitiveType::Int64)
                .AddMember("version").Primitive(NYdb::EPrimitiveType::Int64)
                .AddMember("value").Primitive(NYdb::EPrimitiveType::String)
                .AddMember("lease").Primitive(NYdb::EPrimitiveType::Int64)
            .EndStruct()
        .EndList()
    .Build();

    NYdb::TValueBuilder valueBuilder(type);
    valueBuilder.BeginList();
    auto count = 0U;
    for (const auto& kv : rangeResponse.kvs()) if (!kv.lease()) {
        valueBuilder.AddListItem()
            .BeginStruct()
                .AddMember("key").String(kv.key())
                .AddMember("created").Int64(kv.create_revision())
                .AddMember("modified").Int64(kv.mod_revision())
                .AddMember("version").Int64(kv.version())
                .AddMember("value").String(kv.value())
                .AddMember("lease").Int64(kv.lease())
            .EndStruct();
        ++count;
    }

    auto value = valueBuilder.EndList().Build();

    NYdb::TDriverConfig config;
    config.SetEndpoint(Endpoint);
    config.SetDatabase(Database);
    if (!Token.empty())
        config.SetAuthToken(Token);
    if (!CA.empty())
        config.UseSecureConnection(TFileInput(CA).ReadAll());

    const auto driver = NYdb::TDriver(config);
    auto client = NYdb::NTable::TTableClient(driver);

    if (const auto res = client.BulkUpsert(Database + Folder + "/content", std::move(value)).ExtractValueSync(); !res.IsSuccess()) {
        std::cout << res.GetIssues().ToString() << std::endl;
        return 1;
    }

    std::cout << count << " of " << rangeResponse.count() << " keys imported successfully." << std::endl;
    return 0;
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
    opts.AddLongOption("folder", "YDB etcd root folder").Required().RequiredArgument("FOLDER").StoreResult(&Folder);
    opts.AddLongOption("token", "YDB token for connection").Optional().RequiredArgument("TOKEN").StoreResult(&Token);
    opts.AddLongOption("ydbca", "YDB CA for connection").Optional().RequiredArgument("CA").StoreResult(&CA);

    opts.AddLongOption("port", "Listening port").Optional().DefaultValue("2379").RequiredArgument("PORT").StoreResult(&ListeningPort);
    opts.AddLongOption("init", "Initialize etcd database").NoArgument().SetFlag(&Initialize_);
    opts.AddLongOption("stderr", "Redirect log to stderr").NoArgument().SetFlag(&useStdErr);
    opts.AddLongOption("mlock", "Lock resident memory").NoArgument().SetFlag(&mlock);

    opts.AddLongOption("import-from", "Import existing data from etcd base").RequiredArgument("ENDPOINT").DefaultValue("localhost:2379").StoreResult(&ImportFrom_);
    opts.AddLongOption("import-prefix", "Prefix of data to import").RequiredArgument("PREFIX").StoreResult(&ImportPrefix_);

    opts.AddLongOption("ca", "SSL CA certificate file").Optional().RequiredArgument("CA").StoreResult(&Root);
    opts.AddLongOption("cert", "SSL certificate file").Optional().RequiredArgument("CERT").StoreResult(&Cert);
    opts.AddLongOption("key", "SSL key file").Optional().RequiredArgument("KEY").StoreResult(&Key);

    NLastGetopt::TOptsParseResult res(&opts, argc, argv);

    if (mlock) {
        LockAllMemory(LockCurrentMemory);
    }

    if (!Folder.empty()) {
        std::ostringstream prefix;
        prefix << "pragma TablePathPrefix = '" << Database << Folder << "';" << std::endl;
        Stuff->TablePrefix = prefix.str();
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
