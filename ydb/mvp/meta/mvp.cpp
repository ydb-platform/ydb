#include "mvp.h"

#include <ydb/mvp/core/core_ydb.h>
#include <ydb/mvp/core/core_ydbc.h>
#include <ydb/mvp/core/protos/mvp.pb.h>
#include <ydb/mvp/core/utils.h>
#include <ydb/mvp/meta/support_links/events.h>
#include <ydb/mvp/meta/support_links/grafana_dashboard_source.h>
#include <ydb/mvp/meta/support_links/source.h>

#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/process_stats.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/http/http_cache.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/actors/interconnect/poller/poller_actor.h>
#include <ydb/library/actors/protos/services_common.pb.h>

#include <google/protobuf/text_format.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/string_utils/quote/quote.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <yaml-cpp/yaml.h>

#include <util/datetime/base.h>
#include <util/string/cast.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/system/hostname.h>
#include <util/system/mlock.h>

using namespace NMVP;

NMVP::TMVP* NMVP::InstanceMVP;

namespace {

class TGrafanaDashboardSearchResolveActor final : public NActors::TActorBootstrapped<TGrafanaDashboardSearchResolveActor> {
public:
    TGrafanaDashboardSearchResolveActor(
        NMVP::TSupportLinkEntryConfig config,
        TString grafanaEndpoint,
        TString grafanaTokenName,
        NActors::TActorId owner,
        NActors::TActorId httpProxyId,
        size_t place,
        THashMap<TString, TString> clusterColumns,
        NHttp::TUrlParameters urlParameters)
        : Config(std::move(config))
        , GrafanaEndpoint(std::move(grafanaEndpoint))
        , GrafanaTokenName(std::move(grafanaTokenName))
        , Owner(owner)
        , HttpProxyId(httpProxyId)
        , Place(place)
        , ClusterColumns(std::move(clusterColumns))
        , UrlParameters(std::move(urlParameters))
    {}

    void Bootstrap() {
        const TString authHeaderValue = FindAuthorizationHeaderValue();
        if (authHeaderValue.empty()) {
            ReplyAndDie();
            return;
        }

        NHttp::THttpOutgoingRequestPtr request = NHttp::THttpOutgoingRequest::CreateRequestGet(BuildSearchUrl());
        request->Set("Authorization", authHeaderValue);

        auto event = MakeHolder<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(request);
        event->Timeout = TDuration::Seconds(30);
        Send(HttpProxyId, event.Release());

        Become(&TGrafanaDashboardSearchResolveActor::StateWork, TDuration::Seconds(30), new NActors::TEvents::TEvWakeup());
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            cFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

private:
    static bool IsAbsoluteUrl(const TString& url) {
        return url.StartsWith("http://") || url.StartsWith("https://");
    }

    static TString JoinUrl(const TString& endpoint, const TString& path) {
        const bool endpointHasSlash = endpoint.EndsWith('/');
        const bool pathHasSlash = path.StartsWith('/');
        if (endpointHasSlash && pathHasSlash) {
            return endpoint.substr(0, endpoint.size() - 1) + path;
        }
        if (!endpointHasSlash && !pathHasSlash) {
            return endpoint + "/" + path;
        }
        return endpoint + path;
    }

    static TString AppendQueryParam(const TString& url, TStringBuf key, TStringBuf value) {
        TStringBuilder result;
        result << url << (url.Contains('?') ? '&' : '?') << key << "=" << CGIEscapeRet(value);
        return result;
    }

    TString ResolveGrafanaUrl(const TString& configuredUrl) const {
        if (IsAbsoluteUrl(configuredUrl)) {
            return configuredUrl;
        }
        return JoinUrl(GrafanaEndpoint, configuredUrl);
    }

    TString BuildSearchUrl() const {
        TString url = Config.GetUrl().empty() ? TString("/api/search") : Config.GetUrl();
        url = ResolveGrafanaUrl(url);
        if (Config.HasTag() && Config.GetTag()) {
            url = AppendQueryParam(url, "tag", Config.GetTag());
        }
        if (Config.HasFolder() && Config.GetFolder()) {
            url = AppendQueryParam(url, "folderUIDs", Config.GetFolder());
        }
        return url;
    }

    TString FindAuthorizationHeaderValue() {
        if (GrafanaTokenName.empty()) {
            Errors.emplace_back(NMVP::NSupportLinks::TSupportError{
                .Source = Config.GetSource(),
                .Message = TStringBuilder() << "meta.meta_database_token_name is required for source=" << Config.GetSource(),
            });
            return {};
        }

        auto* appData = NMVP::MVPAppData();
        if (!appData || !appData->Tokenator) {
            Errors.emplace_back(NMVP::NSupportLinks::TSupportError{
                .Source = Config.GetSource(),
                .Message = "Tokenator is unavailable",
            });
            return {};
        }

        const TString authHeaderValue = appData->Tokenator->GetToken(GrafanaTokenName);
        if (!authHeaderValue.empty()) {
            return authHeaderValue;
        }

        Errors.emplace_back(NMVP::NSupportLinks::TSupportError{
            .Source = Config.GetSource(),
            .Message = TStringBuilder() << "IAM token '" << GrafanaTokenName << "' is not available from tokenator",
        });
        return {};
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event) {
        if (!event->Get()->Error.empty() || !event->Get()->Response || event->Get()->Response->Status != "200") {
            NMVP::NSupportLinks::TSupportError error;
            error.Source = Config.GetSource();
            if (!event->Get()->Error.empty()) {
                error.Message = event->Get()->Error;
            } else if (event->Get()->Response) {
                ui32 status = 0;
                if (TryFromString<ui32>(event->Get()->Response->Status, status)) {
                    error.Status = status;
                }
                error.Reason = TString(event->Get()->Response->Message);
                error.Message = TStringBuilder() << event->Get()->Response->Status << " " << event->Get()->Response->Message;
            } else {
                error.Message = "Unknown Grafana request error";
            }
            Errors.push_back(std::move(error));
            ReplyAndDie();
            return;
        }

        NJson::TJsonValue dashboardsJson;
        NJson::TJsonReaderConfig jsonReaderConfig;
        if (!NJson::ReadJsonTree(event->Get()->Response->Body, &jsonReaderConfig, &dashboardsJson)
            || dashboardsJson.GetType() != NJson::JSON_ARRAY)
        {
            Errors.emplace_back(NMVP::NSupportLinks::TSupportError{
                .Source = Config.GetSource(),
                .Message = "Invalid JSON from Grafana Search API",
            });
            ReplyAndDie();
            return;
        }

        for (const auto& item : dashboardsJson.GetArray()) {
            if (item.GetType() != NJson::JSON_MAP) {
                continue;
            }

            TString title;
            TString dashboardPath;
            if (item.Has("title") && item["title"].GetType() == NJson::JSON_STRING) {
                title = item["title"].GetStringRobust();
            }
            if (item.Has("url") && item["url"].GetType() == NJson::JSON_STRING) {
                dashboardPath = item["url"].GetStringRobust();
            } else if (item.Has("uri") && item["uri"].GetType() == NJson::JSON_STRING) {
                dashboardPath = item["uri"].GetStringRobust();
            }

            if (dashboardPath.empty()) {
                continue;
            }

            Links.emplace_back(NMVP::NSupportLinks::TResolvedLink{
                .Title = std::move(title),
                .Url = ResolveDashboardUrl(dashboardPath),
            });
        }

        ReplyAndDie();
    }

    TString ResolveDashboardUrl(const TString& dashboardPath) {
        TString url = ResolveGrafanaUrl(dashboardPath);

        static constexpr TStringBuf WorkspaceColumn = "k8s_namespace";
        static constexpr TStringBuf DatasourceColumn = "datasource";

        const auto workspaceIt = ClusterColumns.find(WorkspaceColumn);
        if (workspaceIt == ClusterColumns.end() || workspaceIt->second.empty()) {
            Errors.emplace_back(NMVP::NSupportLinks::TSupportError{
                .Source = "meta",
                .Message = TStringBuilder() << "Cluster metadata column '" << WorkspaceColumn << "' is missing or empty",
            });
        } else {
            url = AppendQueryParam(url, "var-workspace", workspaceIt->second);
        }

        const auto datasourceIt = ClusterColumns.find(DatasourceColumn);
        if (datasourceIt == ClusterColumns.end() || datasourceIt->second.empty()) {
            Errors.emplace_back(NMVP::NSupportLinks::TSupportError{
                .Source = "meta",
                .Message = TStringBuilder() << "Cluster metadata column '" << DatasourceColumn << "' is missing or empty",
            });
        } else {
            url = AppendQueryParam(url, "var-ds", datasourceIt->second);
        }

        for (const auto& [name, value] : UrlParameters.Parameters) {
            Y_UNUSED(value);
            url = AppendQueryParam(url, TStringBuilder() << "var-" << name, UrlParameters[name]);
        }
        return url;
    }

    void HandleTimeout() {
        Errors.emplace_back(NMVP::NSupportLinks::TSupportError{
            .Source = Config.GetSource(),
            .Message = "Timeout while resolving support links source",
        });
        ReplyAndDie();
    }

    void ReplyAndDie() {
        Send(Owner, new NMVP::NSupportLinks::TEvPrivate::TEvSourceResponse(Place, std::move(Links), std::move(Errors)));
        PassAway();
    }

private:
    NMVP::TSupportLinkEntryConfig Config;
    TString GrafanaEndpoint;
    TString GrafanaTokenName;
    NActors::TActorId Owner;
    NActors::TActorId HttpProxyId;
    size_t Place = 0;
    THashMap<TString, TString> ClusterColumns;
    NHttp::TUrlParameters UrlParameters;
    TVector<NMVP::NSupportLinks::TResolvedLink> Links;
    TVector<NMVP::NSupportLinks::TSupportError> Errors;
};

} // namespace

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

void TMVP::TryGetMetaOptionsFromConfig(const NMvp::NMeta::TMetaAppConfig& appConfig) {
    if (!appConfig.HasMeta()) {
        ythrow yexception() << "Check that `meta` section exists and is on the same indentation as `generic` section";
    }

    const auto& config = appConfig.GetMeta();

    MetaCache = config.GetMetaCache();
    MetaDatabaseTokenName = config.GetMetaDatabaseTokenName();
    DbUserTokenSource = config.GetDbUserTokenAccess();
    MetaSettings = BuildMetaSettings(config, StartupOptions.AccessServiceType);
    MetaApiEndpoint = MetaSettings.MetaApiEndpoint;
    MetaDatabase = MetaSettings.MetaDatabase;
}

void TMVP::TryGetMetaOptionsFromConfig() {
    if (StartupOptions.GetYamlConfigPath().empty()) {
        return;
    }
    try {
        YAML::Node config = YAML::LoadFile(StartupOptions.GetYamlConfigPath());
        NMvp::NMeta::TMetaAppConfig appConfig;
        MergeYamlNodeToProto(config, appConfig);
        TryGetMetaOptionsFromConfig(appConfig);
    } catch (const YAML::Exception& e) {
        ythrow yexception() << "Error parsing YAML configuration file: " << e.what();
    }
}

THolder<NActors::TActorSystemSetup> TMVP::BuildActorSystemSetup() {
    TString defaultMetaDatabase = "/Root";
    TString defaultMetaApiEndpoint = "grpc://meta.ydb.yandex.net:2135";

    TryGetMetaOptionsFromConfig();

    if (MetaApiEndpoint.empty()) {
        MetaApiEndpoint = defaultMetaApiEndpoint;
    }

    if (MetaDatabase.empty()) {
        MetaDatabase = defaultMetaDatabase;
    }

    MetaSettings.AccessServiceType = StartupOptions.AccessServiceType;

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
