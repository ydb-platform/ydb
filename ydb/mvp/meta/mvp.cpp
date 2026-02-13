#include <util/datetime/base.h>
#include <util/generic/yexception.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/stream/file.h>
#include <util/system/hostname.h>
#include <util/system/mlock.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/interconnect/poller/poller_actor.h>
#include <ydb/library/actors/protos/services_common.pb.h>
#include <google/protobuf/text_format.h>
#include <ydb/mvp/core/protos/mvp.pb.h>
#include <ydb/library/actors/core/process_stats.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/actors/http/http_cache.h>
#include "mvp.h"
#include <ydb/mvp/core/core_ydb.h>
#include <ydb/mvp/core/core_ydbc.h>

using namespace NMVP;

NMVP::TMVP* NMVP::InstanceMVP;

const TString& NMVP::GetEServiceName(NActors::NLog::EComponent component) {
    static const TString loggerName("LOGGER");
    static const TString mvpName("MVP");
    static const TString grpcName("GRPC");
    static const TString queryName("QUERY");
    static const TString unknownName("UNKNOW");
    switch (component) {
    case EService::Logger:
        return loggerName;
    case EService::MVP:
        return mvpName;
    case EService::GRPC:
        return grpcName;
    case EService::QUERY:
        return queryName;
    default:
        return unknownName;
    }
}

void TMVP::OnTerminate(int) {
    AtomicSet(Quit, true);
}

int TMVP::Init() {
    ActorSystem.Start();

    ActorSystem.Register(NActors::CreateProcStatCollector(TDuration::Seconds(5), AppData.MetricRegistry = std::make_shared<NMonitoring::TMetricRegistry>()));

    HttpProxyId = ActorSystem.Register(NHttp::CreateHttpProxy(AppData.MetricRegistry));
    ActorSystem.Register(AppData.Tokenator = TMvpTokenator::CreateTokenator(TokensConfig, HttpProxyId));

    if (StartupOptions.HttpPort) {
        auto ev = new NHttp::TEvHttpProxy::TEvAddListeningPort(StartupOptions.HttpPort, TStringBuilder() << FQDNHostName() << ':' << StartupOptions.HttpPort);
        ev->CompressContentTypes = {
            "text/plain",
            "text/html",
            "text/css",
            "text/javascript",
            "application/json",
        };
        ActorSystem.Send(HttpProxyId, ev);
    }
    if (StartupOptions.HttpsPort) {
        auto ev = new NHttp::TEvHttpProxy::TEvAddListeningPort(StartupOptions.HttpsPort, TStringBuilder() << FQDNHostName() << ':' << StartupOptions.HttpsPort);
        ev->Secure = true;
        ev->SslCertificatePem = TYdbLocation::SslCertificate;
        ev->CompressContentTypes = {
            "text/plain",
            "text/html",
            "text/css",
            "text/javascript",
            "application/json",
        };
        ActorSystem.Send(HttpProxyId, ev);
    }

    InitMeta();

    return 0;
}

int TMVP::Run() {
    try {
        int res = Init();
        if (res != 0) {
            return res;
        }
#ifndef NDEBUG
        Cout << "Started" << Endl;
#endif
        while (!AtomicGet(Quit)) {
            Sleep(TDuration::MilliSeconds(100));
        }
#ifndef NDEBUG
        Cout << Endl << "Finished" << Endl;
#endif
        Shutdown();
    }
    catch (const yexception& e) {
        Cerr << e.what() << Endl;
        return 1;
    }
    return 0;
}

int TMVP::Shutdown() {
    ActorSystem.Stop();
    AppData.GRpcClientLow->Stop(true);
    ActorSystem.Cleanup();
    return 0;
}

TString TMVP::GetAppropriateEndpoint(const NHttp::THttpIncomingRequestPtr& req) {
    const bool secure = req->Endpoint->Secure;
    const ui16 port = secure ? StartupOptions.HttpsPort : StartupOptions.HttpPort;

    TStringBuilder b;
    b << (secure ? "https://[::1]" : "http://[::1]");
    if (port) {
        b << ":" << port;
    }
    return b;
}

NMvp::TTokensConfig TMVP::TokensConfig;
TString TMVP::MetaDatabaseTokenName;

bool TMVP::DbUserTokenSource = false;

TMVP::TMVP(int argc, const char* argv[])
    : StartupOptions(TMvpStartupOptions::Build(argc, argv))
    , LoggerSettings(BuildLoggerSettings())
    , ActorSystemSetup(BuildActorSystemSetup())
    , ActorSystem(ActorSystemSetup, &AppData, LoggerSettings)
{
    InstanceMVP = this;
}

TIntrusivePtr<NActors::NLog::TSettings> TMVP::BuildLoggerSettings() {
    const NActors::TActorId loggerActorId = NActors::TActorId(1, "logger");
    TIntrusivePtr<NActors::NLog::TSettings> loggerSettings = new NActors::NLog::TSettings(loggerActorId, EService::Logger, NActors::NLog::PRI_WARN);
    loggerSettings->Append(
        NActorsServices::EServiceCommon_MIN,
        NActorsServices::EServiceCommon_MAX,
        NActorsServices::EServiceCommon_Name
    );
    loggerSettings->Append(
        EService::MIN,
        EService::MAX,
        GetEServiceName
    );
    TString explanation;
    loggerSettings->SetLevel(NActors::NLog::PRI_DEBUG, NActorsServices::HTTP, explanation);
    loggerSettings->SetLevel(NActors::NLog::PRI_DEBUG, EService::MVP, explanation);
    loggerSettings->SetLevel(NActors::NLog::PRI_DEBUG, EService::GRPC, explanation);
    loggerSettings->SetLevel(NActors::NLog::PRI_INFO, EService::QUERY, explanation);
    return loggerSettings;
}

void TMVP::TryGetMetaOptionsFromConfig(const YAML::Node& config) {
    if (!config["meta"]) {
        return;
    }
    auto meta = config["meta"];

    MetaApiEndpoint = meta["meta_api_endpoint"].as<std::string>("");
    MetaDatabase = meta["meta_database"].as<std::string>("");
    MetaCache = meta["meta_cache"].as<bool>(false);
    MetaDatabaseTokenName = meta["meta_database_token_name"].as<std::string>("");
    DbUserTokenSource = meta["db_user_token_access"].as<bool>(false);
}


THolder<NActors::TActorSystemSetup> TMVP::BuildActorSystemSetup() {
    TString defaultMetaDatabase = "/Root";
    TString defaultMetaApiEndpoint = "grpc://meta.ydb.yandex.net:2135";

    if (StartupOptions.Config) {
        try {
            TryGetMetaOptionsFromConfig(StartupOptions.Config);
        } catch (const YAML::Exception& e) {
            std::cerr << "Error parsing YAML configuration file: " << e.what() << std::endl;
            std::exit(EXIT_FAILURE);
        }
    }

    if (MetaApiEndpoint.empty()) {
        MetaApiEndpoint = defaultMetaApiEndpoint;
    }

    if (MetaDatabase.empty()) {
        MetaDatabase = defaultMetaDatabase;
    }

    if (StartupOptions.Mlock) {
        LockAllMemory(LockCurrentMemory);
    }

    TYdbLocation::UserToken = StartupOptions.UserToken;
    TYdbLocation::CaCertificate = StartupOptions.CaCertificate;
    TYdbLocation::SslCertificate = StartupOptions.SslCertificate;

    TokensConfig = StartupOptions.Tokens;

    NActors::TLoggerActor* loggerActor = new NActors::TLoggerActor(
                LoggerSettings,
                StartupOptions.LogToStderr ? NActors::CreateStderrBackend() : NActors::CreateSysLogBackend("mvp", false, true),
                new NMonitoring::TDynamicCounters());
    THolder<NActors::TActorSystemSetup> setup = MakeHolder<NActors::TActorSystemSetup>();
    setup->NodeId = 1;
    setup->Executors.Reset(new TAutoPtr<NActors::IExecutorPool>[3]);
    setup->ExecutorsCount = 3;
    setup->Executors[0] = new NActors::TBasicExecutorPool(0, 4, 10);
    // For UI v2 Logbroker RPCs. We use separate thread pools for RPCs so CPU heavy
    // and slow requests don't interfere with requests that communicate with
    // Logbroker Configuration manager only and suppose to work fast
    setup->Executors[1] = new NActors::TBasicExecutorPool(1, 4, 10);
    // For JSON Merger used by  UI v2 dynamic Logbroker RPCs
    setup->Executors[2] = new NActors::TBasicExecutorPool(2, 4, 10);

    setup->Scheduler = new NActors::TBasicSchedulerThread(NActors::TSchedulerConfig(512, 100));
    setup->LocalServices.emplace_back(LoggerSettings->LoggerActorId, NActors::TActorSetupCmd(loggerActor, NActors::TMailboxType::HTSwap, 0));
    setup->LocalServices.emplace_back(NActors::MakePollerActorId(), NActors::TActorSetupCmd(NActors::CreatePollerActor(), NActors::TMailboxType::HTSwap, 0));
    return setup;
}

TAtomic TMVP::Quit = false;
