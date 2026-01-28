#include "mvp.h"
#include "oidc_client.h"

#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/interconnect/poller/poller_actor.h>
#include <ydb/library/actors/protos/services_common.pb.h>
#include <ydb/library/actors/core/process_stats.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/actors/http/http_cache.h>
#include <ydb/library/actors/http/http_static.h>

#include <ydb/mvp/core/protos/mvp.pb.h>
#include <ydb/mvp/core/core_ydb.h>
#include <ydb/mvp/core/mvp_tokens.h>
#include <ydb/mvp/core/mvp_swagger.h>
#include <ydb/mvp/core/http_check.h>
#include <ydb/mvp/core/http_sensors.h>
#include <ydb/mvp/core/cache_policy.h>

#include <library/cpp/deprecated/atomic/atomic.h>
#include <library/cpp/getopt/last_getopt.h>
#include <google/protobuf/text_format.h>

#include <util/datetime/base.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/string/strip.h>
#include <util/system/hostname.h>
#include <util/system/mlock.h>

NActors::IActor* CreateMemProfiler();

namespace NMVP::NOIDC {

namespace {

TString AddSchemeToUserToken(const TString& token, const TString& scheme) {
    if (token.find(' ') != TString::npos) {
        return token;
    }
    return scheme + " " + token;
}

}

const ui16 TMVP::DefaultHttpPort = 8788;
const ui16 TMVP::DefaultHttpsPort = 8789;

const TString& GetEServiceName(NActors::NLog::EComponent component) {
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

    BaseHttpProxyId = ActorSystem.Register(NHttp::CreateHttpProxy(AppData.MetricRegistry));
    ActorSystem.Register(AppData.Tokenator = TMvpTokenator::CreateTokenator(TokensConfig, BaseHttpProxyId));

    HttpProxyId = ActorSystem.Register(NHttp::CreateHttpCache(BaseHttpProxyId, GetCachePolicy));

    if (Http) {
        auto ev = new NHttp::TEvHttpProxy::TEvAddListeningPort(HttpPort, FQDNHostName());
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
        auto ev = new NHttp::TEvHttpProxy::TEvAddListeningPort(HttpsPort, FQDNHostName());
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

    InitOIDC(ActorSystem, BaseHttpProxyId, OpenIdConnectSettings);

    ActorSystem.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/ping",
                         ActorSystem.Register(new THandlerActorHttpCheck())
                         )
                     );

    ActorSystem.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/mem_profiler",
                         ActorSystem.Register(CreateMemProfiler())
                         )
                     );

    ActorSystem.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/mvp/sensors.json",
                         ActorSystem.Register(new THandlerActorHttpSensors())
                         )
                     );

    ActorSystem.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/api/mvp.json",
                         ActorSystem.Register(new THandlerActorMvpSwagger())
                         )
                     );

    ActorSystem.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvRegisterHandler(
                         "/api/",
                         ActorSystem.Register(NHttp::CreateHttpStaticContentHandler(
                                                  "/api/", // url
                                                  "./content/api/", // file path
                                                  "/mvp/content/api/", // resource path
                                                  "index.html" // index namt
                                                  )
                                              )
                         )
                     );

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

ui16 TMVP::HttpPort;
ui16 TMVP::HttpsPort;
bool TMVP::Http;
bool TMVP::Https;
TString TMVP::GetAppropriateEndpoint(const NHttp::THttpIncomingRequestPtr& req) {
    static TString httpEndpoint = "http://[::1]:" + ToString(HttpPort);
    static TString httpsEndpoint = "https://[::1]:" + ToString(HttpsPort);
    return req->Endpoint->Secure ? httpsEndpoint : httpEndpoint;
}

NMvp::TTokensConfig TMVP::TokensConfig;
TOpenIdConnectSettings TMVP::OpenIdConnectSettings;

TMVP::TMVP(int argc, char** argv)
    : LoggerSettings(BuildLoggerSettings())
    , ActorSystemSetup(BuildActorSystemSetup(argc, argv))
    , ActorSystem(ActorSystemSetup, &AppData, LoggerSettings)
{}

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

void TMVP::TryGetOidcOptionsFromConfig(const YAML::Node& config) {
    auto oidc = config["oidc"];
    if (!oidc) {
        ythrow yexception() << "Check that `oidc` section exists and is on the same indentation as `generic` section";
    }
    OpenIdConnectSettings.SecretName = oidc["secret_name"].as<std::string>(""); // вернуть в OpenIdConnectSettings
    OpenIdConnectSettings.ClientId = oidc["client_id"].as<std::string>(OpenIdConnectSettings.DEFAULT_CLIENT_ID);
    OpenIdConnectSettings.SessionServiceEndpoint = oidc["session_service_endpoint"].as<std::string>("");
    OpenIdConnectSettings.SessionServiceTokenName = oidc["session_service_token_name"].as<std::string>("");
    OpenIdConnectSettings.AuthorizationServerAddress = oidc["authorization_server_address"].as<std::string>("");
    OpenIdConnectSettings.AuthUrlPath = oidc["auth_url_path"].as<std::string>(OpenIdConnectSettings.DEFAULT_AUTH_URL_PATH);
    OpenIdConnectSettings.TokenUrlPath = oidc["token_url_path"].as<std::string>(OpenIdConnectSettings.DEFAULT_TOKEN_URL_PATH);
    OpenIdConnectSettings.ExchangeUrlPath = oidc["exchange_url_path"].as<std::string>(OpenIdConnectSettings.DEFAULT_EXCHANGE_URL_PATH);
    OpenIdConnectSettings.ImpersonateUrlPath = oidc["impersonate_url_path"].as<std::string>(OpenIdConnectSettings.DEFAULT_IMPERSONATE_URL_PATH);
    OpenIdConnectSettings.WhoamiExtendedInfoEndpoint = oidc["whoami_extended_info_endpoint"].as<std::string>("");
    Cout << "Started processing allowed_proxy_hosts..." << Endl;
    for (const std::string& host : oidc["allowed_proxy_hosts"].as<std::vector<std::string>>()) {
        Cout << host << " added to allowed_proxy_hosts" << Endl;
        OpenIdConnectSettings.AllowedProxyHosts.push_back(TString(host));
    }
    Cout << "Finished processing allowed_proxy_hosts." << Endl;
}

void TMVP::TryGetGenericOptionsFromConfig(
    const YAML::Node& config,
    const NLastGetopt::TOptsParseResult& parseRes,
    TGenericOptions& opts
) {
    if (!config["generic"]) {
        return;
    }
    auto generic = config["generic"];

    if (generic["logging"] && generic["logging"]["stderr"]) {
        if (parseRes.FindLongOptParseResult("stderr") == nullptr) {
            opts.UseStderr = generic["logging"]["stderr"].as<bool>(false);
        }
    }

    if (generic["mlock"]) {
        if (parseRes.FindLongOptParseResult("mlock") == nullptr) {
            opts.Mlock = generic["mlock"].as<bool>(false);
        }
    }

    if (generic["auth"]) {
        auto auth = generic["auth"];
        bool hasFederatedCreds = auth["federated_creds"].IsDefined();
        bool hasTokenFile = auth["token_file"].IsDefined();
        if (hasFederatedCreds && hasTokenFile) {
            ythrow yexception() << "Configuration error: Both 'federated_creds' and 'token_file' are set in 'auth'. Only one must be specified.";
        }
        opts.YdbTokenFile = auth["token_file"].as<std::string>("");

        if (hasFederatedCreds) {
            auto jwt = auth["federated_creds"];
            auto tokenPath = jwt["k8s_token_path"] ? jwt["k8s_token_path"].as<std::string>("") : "";
            opts.JwtTokenEndpoint = jwt["token_service_endpoint"] ? jwt["token_service_endpoint"].as<std::string>("") : "";
            opts.JwtSaId = jwt["service_account_id"] ? jwt["service_account_id"].as<std::string>("") : "";

            if (tokenPath.empty()) {
                ythrow yexception() << "Configuration error: 'k8s_token_path' must be specified in 'federated_creds'.";
            }
            if (opts.JwtSaId.empty()) {
                ythrow yexception() << "Configuration error: 'service_account_id' must be specified in 'federated_creds'.";
            }
            if (opts.JwtTokenEndpoint.empty()) {
                ythrow yexception() << "Configuration error: 'token_service_endpoint' must be specified in 'federated_creds'.";
            }
            opts.JwtToken = Strip(TUnbufferedFileInput(tokenPath).ReadAll());
        }
    }

    if (generic["server"]) {
        auto server = generic["server"];
        opts.CaCertificateFile = server["ca_cert_file"].as<std::string>("");
        opts.SslCertificateFile = server["ssl_cert_file"].as<std::string>("");
        if (parseRes.FindLongOptParseResult("http-port") == nullptr) {
            opts.HttpPort = server["http_port"].as<ui16>(0);
        }
        if (parseRes.FindLongOptParseResult("https-port") == nullptr) {
            opts.HttpsPort = server["https_port"].as<ui16>(0);
        }
    }

    if (generic["access_service_type"]) {
        auto accessServiceTypeStr = TString(generic["access_service_type"].as<std::string>(""));
        if (!NMvp::EAccessServiceType_Parse(to_lower(accessServiceTypeStr), &OpenIdConnectSettings.AccessServiceType)) {
            ythrow yexception() << "Unknown access_service_type value: " << accessServiceTypeStr;
        }
    }
    OpenIdConnectSettings.InitRequestTimeoutsByPath();
}

THolder<NActors::TActorSystemSetup> TMVP::BuildActorSystemSetup(int argc, char** argv) {
    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
    TGenericOptions genericOpts;
    TString yamlConfigPath;


    opts.AddLongOption("stderr", "Redirect log to stderr").NoArgument().SetFlag(&genericOpts.UseStderr);
    opts.AddLongOption("mlock", "Lock resident memory").NoArgument().SetFlag(&genericOpts.Mlock);
    opts.AddLongOption("config", "Path to configuration YAML file").RequiredArgument("PATH").StoreResult(&yamlConfigPath);
    opts.AddLongOption("http-port", "HTTP port. Default " + ToString(DefaultHttpPort)).StoreResult(&genericOpts.HttpPort);
    opts.AddLongOption("https-port", "HTTPS port. Default " + ToString(DefaultHttpsPort)).StoreResult(&genericOpts.HttpsPort);

    NLastGetopt::TOptsParseResult res(&opts, argc, argv);

    if (!yamlConfigPath.empty()) {
        try {
            YAML::Node config = YAML::LoadFile(yamlConfigPath);
            TryGetOidcOptionsFromConfig(config);
            TryGetGenericOptionsFromConfig(config, res, genericOpts);
        } catch (const YAML::Exception& e) {
            std::cerr << "Error parsing YAML configuration file: " << e.what() << std::endl;
            std::exit(EXIT_FAILURE);
        }
    }

    if (genericOpts.Mlock) {
        LockAllMemory(LockCurrentMemory);
    }
    if (genericOpts.HttpPort > 0) {
        Http = true;
    }
    if (genericOpts.HttpsPort > 0 || !genericOpts.SslCertificateFile.empty()) {
        Https = true;
    }
    if (!Http && !Https) {
        Http = true;
    }
    if (genericOpts.HttpPort == 0) {
        HttpPort = DefaultHttpPort;
    } else {
        HttpPort = genericOpts.HttpPort;
    }
    if (genericOpts.HttpsPort == 0) {
        HttpsPort = DefaultHttpsPort;
    } else {
        HttpsPort = genericOpts.HttpsPort;
    }

    NMvp::TTokensConfig tokens;
    if (!genericOpts.YdbTokenFile.empty()) {
        if (google::protobuf::TextFormat::ParseFromString(TUnbufferedFileInput(genericOpts.YdbTokenFile).ReadAll(), &tokens)) {
            if (tokens.HasStaffApiUserTokenInfo()) {
                TYdbLocation::UserToken = tokens.GetStaffApiUserTokenInfo().GetToken();
            } else if (tokens.HasStaffApiUserToken()) {
                TYdbLocation::UserToken = tokens.GetStaffApiUserToken();
            }
            if (!tokens.HasAccessServiceType()) {
                tokens.SetAccessServiceType(OpenIdConnectSettings.AccessServiceType);
            }
        } else {
            ythrow yexception() << "Invalid ydb token file format";
        }
    }
    if (!genericOpts.JwtToken.empty()) {
        auto* jwtInfo = tokens.AddJwtInfo();
        jwtInfo->SetAuthMethod(NMvp::TJwtInfo::federated_creds);
        jwtInfo->SetAccountId(genericOpts.JwtSaId);
        jwtInfo->SetToken(genericOpts.JwtToken);
        jwtInfo->SetEndpoint(genericOpts.JwtTokenEndpoint);
        jwtInfo->SetName(OpenIdConnectSettings.SecretName); // the only name used
        TYdbLocation::UserToken = genericOpts.JwtToken;
    }
    TokensConfig = tokens;

    if (TYdbLocation::UserToken) {
        TYdbLocation::UserToken = AddSchemeToUserToken(TYdbLocation::UserToken, "OAuth");
    }

    for (auto secret : TokensConfig.GetSecretInfo()) {
        if (OpenIdConnectSettings.SecretName == secret.GetName()) {
            OpenIdConnectSettings.ClientSecret = secret.GetSecret();
        }
    }

    if (!genericOpts.CaCertificateFile.empty()) {
        TString caCertificate = TUnbufferedFileInput(genericOpts.CaCertificateFile).ReadAll();
        if (!caCertificate.empty()) {
            TYdbLocation::CaCertificate = caCertificate;
        } else {
            ythrow yexception() << "Invalid CA certificate file";
        }
    }
    if (!genericOpts.SslCertificateFile.empty()) {
        TString sslCertificate = TUnbufferedFileInput(genericOpts.SslCertificateFile).ReadAll();
        if (!sslCertificate.empty()) {
            TYdbLocation::SslCertificate = sslCertificate;
        } else {
            ythrow yexception() << "Invalid SSL certificate file";
        }
    }

    NActors::TLoggerActor* loggerActor = new NActors::TLoggerActor(
                LoggerSettings,
                genericOpts.UseStderr ? NActors::CreateStderrBackend() : NActors::CreateSysLogBackend("mvp", false, true),
                new NMonitoring::TDynamicCounters());
    THolder<NActors::TActorSystemSetup> setup = MakeHolder<NActors::TActorSystemSetup>();
    setup->NodeId = 1;
    setup->Executors.Reset(new TAutoPtr<NActors::IExecutorPool>[3]);
    setup->ExecutorsCount = 3;
    setup->Executors[0] = new NActors::TBasicExecutorPool(0, 4, 10);
    setup->Executors[1] = new NActors::TBasicExecutorPool(1, 4, 10);
    setup->Executors[2] = new NActors::TBasicExecutorPool(2, 4, 10);

    setup->Scheduler = new NActors::TBasicSchedulerThread(NActors::TSchedulerConfig(512, 100));
    setup->LocalServices.emplace_back(LoggerSettings->LoggerActorId, NActors::TActorSetupCmd(loggerActor, NActors::TMailboxType::HTSwap, 0));
    setup->LocalServices.emplace_back(NActors::MakePollerActorId(), NActors::TActorSetupCmd(NActors::CreatePollerActor(), NActors::TMailboxType::HTSwap, 0));
    return setup;
}

TAtomic TMVP::Quit = false;

} // NMVP::NOIDC
