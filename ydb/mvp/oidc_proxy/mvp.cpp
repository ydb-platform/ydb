#include "mvp.h"
#include "oidc_client.h"
#include "openid_connect.h"

#include <ydb/mvp/core/cache_policy.h>
#include <ydb/mvp/core/core_ydb.h>
#include <ydb/mvp/core/http_check.h>
#include <ydb/mvp/core/http_sensors.h>
#include <ydb/mvp/core/mvp_swagger.h>
#include <ydb/mvp/core/mvp_tokens.h>
#include <ydb/mvp/core/protos/mvp.pb.h>
#include <ydb/mvp/core/utils.h>

#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/process_stats.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/http/http_cache.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/actors/http/http_static.h>
#include <ydb/library/actors/interconnect/poller/poller_actor.h>
#include <ydb/library/actors/protos/services_common.pb.h>

#include <library/cpp/deprecated/atomic/atomic.h>
#include <yaml-cpp/yaml.h>

#include <util/datetime/base.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/string/strip.h>
#include <util/system/hostname.h>
#include <util/system/mlock.h>

NActors::IActor* CreateMemProfiler();

namespace NMVP::NOIDC {

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

    if (StartupOptions.HttpPort) {
        auto ev = new NHttp::TEvHttpProxy::TEvAddListeningPort(StartupOptions.HttpPort, FQDNHostName());
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
        auto ev = new NHttp::TEvHttpProxy::TEvAddListeningPort(StartupOptions.HttpsPort, FQDNHostName());
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

NMvp::TTokensConfig TMVP::TokensConfig;
TOpenIdConnectSettings TMVP::OpenIdConnectSettings;

TMVP::TMVP(int argc, const char* argv[])
    : StartupOptions(TMvpStartupOptions::Build(argc, argv))
    , LoggerSettings(BuildLoggerSettings())
    , ActorSystemSetup(BuildActorSystemSetup())
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

void TMVP::TryGetOidcOptionsFromConfig(const NMvp::NOidcProxy::TOidcProxyConfig& config) {
    OpenIdConnectSettings.SecretName = config.GetSecretName();
    OpenIdConnectSettings.ClientId = config.HasClientId() ? config.GetClientId() : OpenIdConnectSettings.DEFAULT_CLIENT_ID;
    OpenIdConnectSettings.SessionServiceEndpoint = config.GetSessionServiceEndpoint();
    OpenIdConnectSettings.SessionServiceTokenName = config.GetSessionServiceTokenName();
    OpenIdConnectSettings.AuthorizationServerAddress = config.GetAuthorizationServerAddress();
    OpenIdConnectSettings.AuthUrlPath = config.HasAuthUrlPath() ? config.GetAuthUrlPath() : OpenIdConnectSettings.DEFAULT_AUTH_URL_PATH;
    OpenIdConnectSettings.TokenUrlPath = config.HasTokenUrlPath() ? config.GetTokenUrlPath() : OpenIdConnectSettings.DEFAULT_TOKEN_URL_PATH;
    OpenIdConnectSettings.ExchangeUrlPath = config.HasExchangeUrlPath() ? config.GetExchangeUrlPath() : OpenIdConnectSettings.DEFAULT_EXCHANGE_URL_PATH;
    OpenIdConnectSettings.ImpersonateUrlPath = config.HasImpersonateUrlPath() ? config.GetImpersonateUrlPath() : OpenIdConnectSettings.DEFAULT_IMPERSONATE_URL_PATH;
    OpenIdConnectSettings.WhoamiExtendedInfoEndpoint = config.GetWhoamiExtendedInfoEndpoint();

    Cout << "Started processing allowed_proxy_hosts..." << Endl;
    OpenIdConnectSettings.AllowedProxyHosts.clear();
    for (const auto& host : config.GetAllowedProxyHosts()) {
        Cout << host << " added to allowed_proxy_hosts" << Endl;
        OpenIdConnectSettings.AllowedProxyHosts.push_back(host);
    }
    Cout << "Finished processing allowed_proxy_hosts." << Endl;
}

void TMVP::TryGetOidcOptionsFromConfig() {
    if (StartupOptions.GetYamlConfigPath().empty()) {
        return;
    }
    try {
        YAML::Node config = YAML::LoadFile(StartupOptions.GetYamlConfigPath());
        NMvp::NOidcProxy::TOidcProxyAppConfig appConfig;
        MergeYamlNodeToProto(config, appConfig);
        if (!appConfig.HasOidc()) {
            ythrow yexception() << "Check that `oidc` section exists and is on the same indentation as `generic` section";
        }
        TryGetOidcOptionsFromConfig(appConfig.GetOidc());
    } catch (const YAML::Exception& e) {
        std::cerr << "Error parsing YAML configuration file: " << e.what() << std::endl;
        std::exit(EXIT_FAILURE);
    }
}

THolder<NActors::TActorSystemSetup> TMVP::BuildActorSystemSetup() {
    TryGetOidcOptionsFromConfig();

    OpenIdConnectSettings.AccessServiceType = StartupOptions.AccessServiceType;
    if (OpenIdConnectSettings.SessionServiceTokenName.empty()) {
        ythrow yexception() << NMVP::CONFIG_ERROR_PREFIX
                            << "SessionServiceTokenName must be specified in oidc config.";
    }
    if (OpenIdConnectSettings.SecretName.empty()) {
        ythrow yexception() << NMVP::CONFIG_ERROR_PREFIX
                            << "SecretName must be specified in oidc config.";
    }
    OpenIdConnectSettings.InitRequestTimeoutsByPath();

    if (StartupOptions.Mlock) {
        LockAllMemory(LockCurrentMemory);
    }

    TYdbLocation::UserToken = StartupOptions.UserToken;
    TYdbLocation::CaCertificate = StartupOptions.CaCertificate;
    TYdbLocation::SslCertificate = StartupOptions.SslCertificate;

    TokensConfig = StartupOptions.Tokens;

    bool clientSecretFound = false;
    for (const auto& secret : TokensConfig.GetSecretInfo()) {
        if (OpenIdConnectSettings.SecretName == secret.GetName()) {
            clientSecretFound = true;
            if (secret.GetSecret().empty()) {
                ythrow yexception() << NMVP::CONFIG_ERROR_PREFIX
                                    << "oidc.secret_name '" << OpenIdConnectSettings.SecretName
                                    << "' has empty value in auth token config secret_info.";
            }
            OpenIdConnectSettings.ClientSecret = secret.GetSecret();
            break;
        }
    }
    if (!clientSecretFound) {
        ythrow yexception() << NMVP::CONFIG_ERROR_PREFIX
                            << "oidc.secret_name '" << OpenIdConnectSettings.SecretName
                            << "' was not found in auth token config secret_info.";
    }

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

} // NMVP::NOIDC
