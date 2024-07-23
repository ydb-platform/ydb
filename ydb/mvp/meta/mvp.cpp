#include <util/datetime/base.h>
#include <util/generic/algorithm.h>
#include <util/generic/yexception.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/stream/file.h>
#include <util/system/hostname.h>
#include <util/system/mlock.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/core/log.h>
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

namespace {

TString AddSchemeToUserToken(const TString& token, const TString& scheme) {
    if (token.find(' ') != TString::npos) {
        return token;
    }
    return scheme + " " + token;
}

}

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
    ActorSystem.Register(AppData.Tokenator = TMvpTokenator::CreateTokenator(TokensConfig, HttpProxyId, AccessServiceType));

    if (Http) {
        auto ev = new NHttp::TEvHttpProxy::TEvAddListeningPort(HttpPort, TStringBuilder() << FQDNHostName() << ':' << HttpPort);
        ev->CompressContentTypes = {
            "text/plain",
            "text/html",
            "text/css",
            "text/javascript",
            "application/json",
        };
        ActorSystem.Send(HttpProxyId, ev);
    }
    if (Https) {
        auto ev = new NHttp::TEvHttpProxy::TEvAddListeningPort(HttpsPort, TStringBuilder() << FQDNHostName() << ':' << HttpsPort);
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
    ActorSystemStoppingLock.AcquireWrite();
    AtomicSet(ActorSystemStopping, true);
    ActorSystemStoppingLock.ReleaseWrite();
    ActorSystem.Stop();
    return 0;
}

TString TMVP::GetAppropriateEndpoint(const NHttp::THttpIncomingRequestPtr& req) {
    static TString httpEndpoint = "http://[::1]:" + ToString(HttpPort);
    static TString httpsEndpoint = "https://[::1]:" + ToString(HttpsPort);
    return req->Endpoint->Secure ? httpsEndpoint : httpEndpoint;
}

NMvp::TTokensConfig TMVP::TokensConfig;
<<<<<<< HEAD
TString TMVP::MetaDatabaseTokenName;
=======
>>>>>>> 8e0d57db1b (rewrite GetTableClient)

TMVP::TMVP(int argc, char** argv)
    : ActorSystemStoppingLock()
    , ActorSystemStopping(false)
    , LoggerSettings(BuildLoggerSettings())
    , ActorSystemSetup(BuildActorSystemSetup(argc, argv))
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
}

void TMVP::TryGetGenericOptionsFromConfig(
    const YAML::Node& config,
    const NLastGetopt::TOptsParseResult& opts,
    TString& ydbTokenFile,
    TString& caCertificateFile,
    TString& sslCertificateFile,
    bool& useStderr,
    bool& mlock,
    NMvp::EAccessServiceType& /*accessServiceType*/
) {
    if (!config["generic"]) {
        return;
    }
    auto generic = config["generic"];

    if (generic["logging"] && generic["logging"]["stderr"]) {
        if (opts.FindLongOptParseResult("stderr") == nullptr) {
            useStderr = generic["logging"]["stderr"].as<bool>(false);
        }
    }

    if (generic["mlock"]) {
        if (opts.FindLongOptParseResult("mlock") == nullptr) {
            mlock = generic["mlock"].as<bool>(false);
        }
    }

    if (generic["auth"]) {
        auto auth = generic["auth"];
        ydbTokenFile = auth["token_file"].as<std::string>("");
    }

    if (generic["server"]) {
        auto server = generic["server"];
        caCertificateFile = server["ca_cert_file"].as<std::string>("");
        sslCertificateFile = server["ssl_cert_file"].as<std::string>("");

        if (opts.FindLongOptParseResult("http-port") == nullptr) {
            HttpPort = server["http_port"].as<ui16>(0);
        }

        if (opts.FindLongOptParseResult("https-port") == nullptr) {
            HttpsPort = server["https_port"].as<ui16>(0);
        }
    }

    if (generic["access_service_type"]) {
        auto name = to_lower(ToString(generic["access_service_type"].as<std::string>("yandex_v2")));
        auto it = AccessServiceTypeByName.find(name);
        if (it != AccessServiceTypeByName.end()) {
            AccessServiceType = it->second;
        } else {
            ythrow yexception() << "Unknown auth profile: " << name;
        }
    }
}

THolder<NActors::TActorSystemSetup> TMVP::BuildActorSystemSetup(int argc, char** argv) {
    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
    bool useStderr = false;
    bool mlock = false;
    TString yamlConfigPath;

    TString ydbTokenFile;
    TString caCertificateFile;
    TString sslCertificateFile;

    TString defaultMetaDatabase = "/Root";
    TString defaultMetaApiEndpoint = "grpc://meta.ydb.yandex.net:2135";

    NMvp::EAccessServiceType accessServiceType = NMvp::EAccessServiceType::yandex_v2;

    opts.AddLongOption("stderr", "Redirect log to stderr").NoArgument().SetFlag(&useStderr);
    opts.AddLongOption("mlock", "Lock resident memory").NoArgument().SetFlag(&mlock);

    opts.AddLongOption("config", "Path to configuration YAML file").RequiredArgument("PATH").StoreResult(&yamlConfigPath);

    opts.AddLongOption("http-port", "HTTP port. Default 8788").StoreResult(&HttpPort);
    opts.AddLongOption("https-port", "HTTPS port. Default 8789").StoreResult(&HttpsPort);

    NLastGetopt::TOptsParseResult res(&opts, argc, argv);

    if (!yamlConfigPath.empty()) {
        try {
            YAML::Node config = YAML::LoadFile(yamlConfigPath);

            TryGetMetaOptionsFromConfig(config);
            TryGetGenericOptionsFromConfig(
                config,
                res,
                ydbTokenFile,
                caCertificateFile,
                sslCertificateFile,
                useStderr,
                mlock,
                accessServiceType
            );
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

    if (mlock) {
        LockAllMemory(LockCurrentMemory);
    }
    if (HttpPort > 0) {
        Http = true;
    }
    if (HttpsPort > 0 || !sslCertificateFile.empty()) {
        Https = true;
    }
    if (!Http && !Https) {
        Http = true;
    }

    if (HttpPort == 0) {
        HttpPort = 8788;
    }
    if (HttpsPort == 0) {
        HttpsPort = 8789;
    }

    if (!ydbTokenFile.empty()) {
        NMvp::TTokensConfig tokens;
        if (google::protobuf::TextFormat::ParseFromString(TUnbufferedFileInput(ydbTokenFile).ReadAll(), &tokens)) {
            if (tokens.HasStaffApiUserTokenInfo()) {
                TYdbLocation::UserToken = tokens.GetStaffApiUserTokenInfo().GetToken();
            } else if (tokens.HasStaffApiUserToken()) {
                TYdbLocation::UserToken = tokens.GetStaffApiUserToken();
            }
            TokensConfig = tokens;
        } else {
            ythrow yexception() << "Invalid ydb token file format";
        }
    }

    if (TYdbLocation::UserToken) {
        TYdbLocation::UserToken = AddSchemeToUserToken(TYdbLocation::UserToken, "OAuth");
    }

    if (!caCertificateFile.empty()) {
        TString caCertificate = TUnbufferedFileInput(caCertificateFile).ReadAll();
        if (!caCertificate.empty()) {
            TYdbLocation::CaCertificate = caCertificate;
        } else {
            ythrow yexception() << "Invalid CA certificate file";
        }
    }
    if (!sslCertificateFile.empty()) {
        TString sslCertificate = TUnbufferedFileInput(sslCertificateFile).ReadAll();
        if (!sslCertificate.empty()) {
            TYdbLocation::SslCertificate = sslCertificate;
        } else {
            ythrow yexception() << "Invalid SSL certificate file";
        }
    }

    NActors::TLoggerActor* loggerActor = new NActors::TLoggerActor(
                LoggerSettings,
                useStderr ? NActors::CreateStderrBackend() : NActors::CreateSysLogBackend("mvp", false, true),
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
    return setup;
}

TAtomic TMVP::Quit = false;
