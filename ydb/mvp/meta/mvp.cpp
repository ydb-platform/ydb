#include "mvp.h"

#include <ydb/mvp/core/core_ydb.h>
#include <ydb/mvp/core/core_ydbc.h>
#include <ydb/mvp/core/protos/mvp.pb.h>
#include <ydb/mvp/core/utils.h>
#include <ydb/mvp/meta/support_links/events.h>
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

#include <mutex>

using namespace NMVP;

NMVP::TMVP* NMVP::InstanceMVP;

namespace {

const TString* CurrentGrafanaEndpointForConfigParsing = nullptr;

class TGrafanaEndpointConfigGuard {
public:
    explicit TGrafanaEndpointConfigGuard(const TString& grafanaEndpoint) {
        CurrentGrafanaEndpointForConfigParsing = &grafanaEndpoint;
    }

    ~TGrafanaEndpointConfigGuard() {
        CurrentGrafanaEndpointForConfigParsing = nullptr;
    }
};

class TGrafanaDashboardSource final : public NMVP::ILinkSource {
public:
    TGrafanaDashboardSource(NMVP::TSupportLinkEntryConfig config, TString grafanaEndpoint)
        : Config_(std::move(config))
        , GrafanaEndpoint_(std::move(grafanaEndpoint))
    {}

    const NMVP::TSupportLinkEntryConfig& Config() const override {
        return Config_;
    }

    NMVP::TResolveOutput Resolve(const TResolveInput& input) const override {
        NMVP::TResolveOutput output{
            .Name = Config_.GetSource(),
            .Ready = true,
        };
        TString url = ResolveUrl(Config_.GetUrl(), GrafanaEndpoint_);

        static constexpr TStringBuf WorkspaceColumn = "k8s_namespace";
        static constexpr TStringBuf DatasourceColumn = "datasource";

        const auto workspaceIt = input.ClusterColumns.find(WorkspaceColumn);
        if (workspaceIt == input.ClusterColumns.end() || workspaceIt->second.empty()) {
            output.Errors.emplace_back(NMVP::NSupportLinks::TSupportError{
                .Source = "meta",
                .Message = TStringBuilder() << "Cluster metadata column '" << WorkspaceColumn << "' is missing or empty",
            });
        } else {
            url = AppendQueryParam(url, "var-workspace", workspaceIt->second);
        }

        const auto datasourceIt = input.ClusterColumns.find(DatasourceColumn);
        if (datasourceIt == input.ClusterColumns.end() || datasourceIt->second.empty()) {
            output.Errors.emplace_back(NMVP::NSupportLinks::TSupportError{
                .Source = "meta",
                .Message = TStringBuilder() << "Cluster metadata column '" << DatasourceColumn << "' is missing or empty",
            });
        } else {
            url = AppendQueryParam(url, "var-ds", datasourceIt->second);
        }

        for (const auto& [name, _] : input.UrlParameters.Parameters) {
            url = AppendQueryParam(url, TStringBuilder() << "var-" << name, input.UrlParameters[name]);
        }

        output.Links.emplace_back(NMVP::NSupportLinks::TResolvedLink{
            .Title = Config_.GetTitle(),
            .Url = std::move(url),
        });
        return output;
    }

private:
    static bool IsAbsoluteUrl(const TString& url) {
        return url.StartsWith("http://") || url.StartsWith("https://");
    }

    static TString ResolveUrl(const TString& configuredUrl, const TString& grafanaEndpoint) {
        if (IsAbsoluteUrl(configuredUrl)) {
            return configuredUrl;
        }
        if (grafanaEndpoint.empty()) {
            return configuredUrl;
        }

        const bool endpointHasSlash = grafanaEndpoint.EndsWith('/');
        const bool urlHasSlash = configuredUrl.StartsWith('/');
        if (endpointHasSlash && urlHasSlash) {
            return grafanaEndpoint.substr(0, grafanaEndpoint.size() - 1) + configuredUrl;
        }
        if (!endpointHasSlash && !urlHasSlash) {
            return grafanaEndpoint + "/" + configuredUrl;
        }
        return grafanaEndpoint + configuredUrl;
    }

    static TString AppendQueryParam(const TString& url, TStringBuf key, TStringBuf value) {
        TStringBuilder result;
        result << url;
        result << (url.Contains('?') ? '&' : '?');
        result << key << "=" << CGIEscapeRet(value);
        return result;
    }

private:
    NMVP::TSupportLinkEntryConfig Config_;
    TString GrafanaEndpoint_;
};

class TGrafanaDashboardSearchResolveActor final : public NActors::TActorBootstrapped<TGrafanaDashboardSearchResolveActor> {
public:
    TGrafanaDashboardSearchResolveActor(
        NMVP::TSupportLinkEntryConfig config,
        TString grafanaEndpoint,
        TString grafanaTokenName,
        NActors::TActorId parent,
        NActors::TActorId httpProxyId,
        size_t place,
        THashMap<TString, TString> clusterColumns,
        NHttp::TUrlParameters urlParameters)
        : Config_(std::move(config))
        , GrafanaEndpoint_(std::move(grafanaEndpoint))
        , GrafanaTokenName_(std::move(grafanaTokenName))
        , Parent_(parent)
        , HttpProxyId_(httpProxyId)
        , Place_(place)
        , ClusterColumns_(std::move(clusterColumns))
        , UrlParameters_(std::move(urlParameters))
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
        Send(HttpProxyId_, event.Release());

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
        return JoinUrl(GrafanaEndpoint_, configuredUrl);
    }

    TString BuildSearchUrl() const {
        TString url = Config_.GetUrl().empty() ? TString("/api/search") : Config_.GetUrl();
        url = ResolveGrafanaUrl(url);
        if (Config_.HasTag() && Config_.GetTag()) {
            url = AppendQueryParam(url, "tag", Config_.GetTag());
        }
        if (Config_.HasFolder() && Config_.GetFolder()) {
            url = AppendQueryParam(url, "folderUIDs", Config_.GetFolder());
        }
        return url;
    }

    TString FindAuthorizationHeaderValue() {
        if (GrafanaTokenName_.empty()) {
            Errors_.emplace_back(NMVP::NSupportLinks::TSupportError{
                .Source = Config_.GetSource(),
                .Message = TStringBuilder() << "meta.meta_database_token_name is required for source=" << Config_.GetSource(),
            });
            return {};
        }

        auto* appData = NMVP::MVPAppData();
        if (!appData || !appData->Tokenator) {
            Errors_.emplace_back(NMVP::NSupportLinks::TSupportError{
                .Source = Config_.GetSource(),
                .Message = "Tokenator is unavailable",
            });
            return {};
        }

        const TString authHeaderValue = appData->Tokenator->GetToken(GrafanaTokenName_);
        if (!authHeaderValue.empty()) {
            return authHeaderValue;
        }

        Errors_.emplace_back(NMVP::NSupportLinks::TSupportError{
            .Source = Config_.GetSource(),
            .Message = TStringBuilder() << "IAM token '" << GrafanaTokenName_ << "' is not available from tokenator",
        });
        return {};
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event) {
        if (!event->Get()->Error.empty() || !event->Get()->Response || event->Get()->Response->Status != "200") {
            NMVP::NSupportLinks::TSupportError error;
            error.Source = Config_.GetSource();
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
            Errors_.push_back(std::move(error));
            ReplyAndDie();
            return;
        }

        NJson::TJsonValue dashboardsJson;
        NJson::TJsonReaderConfig jsonReaderConfig;
        if (!NJson::ReadJsonTree(event->Get()->Response->Body, &jsonReaderConfig, &dashboardsJson)
            || dashboardsJson.GetType() != NJson::JSON_ARRAY)
        {
            Errors_.emplace_back(NMVP::NSupportLinks::TSupportError{
                .Source = Config_.GetSource(),
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

            Links_.emplace_back(NMVP::NSupportLinks::TResolvedLink{
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

        const auto workspaceIt = ClusterColumns_.find(WorkspaceColumn);
        if (workspaceIt == ClusterColumns_.end() || workspaceIt->second.empty()) {
            Errors_.emplace_back(NMVP::NSupportLinks::TSupportError{
                .Source = "meta",
                .Message = TStringBuilder() << "Cluster metadata column '" << WorkspaceColumn << "' is missing or empty",
            });
        } else {
            url = AppendQueryParam(url, "var-workspace", workspaceIt->second);
        }

        const auto datasourceIt = ClusterColumns_.find(DatasourceColumn);
        if (datasourceIt == ClusterColumns_.end() || datasourceIt->second.empty()) {
            Errors_.emplace_back(NMVP::NSupportLinks::TSupportError{
                .Source = "meta",
                .Message = TStringBuilder() << "Cluster metadata column '" << DatasourceColumn << "' is missing or empty",
            });
        } else {
            url = AppendQueryParam(url, "var-ds", datasourceIt->second);
        }

        for (const auto& [name, _] : UrlParameters_.Parameters) {
            url = AppendQueryParam(url, TStringBuilder() << "var-" << name, UrlParameters_[name]);
        }
        return url;
    }

    void HandleTimeout() {
        Errors_.emplace_back(NMVP::NSupportLinks::TSupportError{
            .Source = Config_.GetSource(),
            .Message = "Timeout while resolving support links source",
        });
        ReplyAndDie();
    }

    void ReplyAndDie() {
        Send(Parent_, new NMVP::NSupportLinks::TEvPrivate::TEvSourceResponse(Place_, std::move(Links_), std::move(Errors_)));
        PassAway();
    }

private:
    NMVP::TSupportLinkEntryConfig Config_;
    TString GrafanaEndpoint_;
    TString GrafanaTokenName_;
    NActors::TActorId Parent_;
    NActors::TActorId HttpProxyId_;
    size_t Place_ = 0;
    THashMap<TString, TString> ClusterColumns_;
    NHttp::TUrlParameters UrlParameters_;
    TVector<NMVP::NSupportLinks::TResolvedLink> Links_;
    TVector<NMVP::NSupportLinks::TSupportError> Errors_;
};

class TGrafanaDashboardSearchSource final : public NMVP::ILinkSource {
public:
    TGrafanaDashboardSearchSource(
        NMVP::TSupportLinkEntryConfig config,
        TString grafanaEndpoint,
        TString grafanaTokenName)
        : Config_(std::move(config))
        , GrafanaEndpoint_(std::move(grafanaEndpoint))
        , GrafanaTokenName_(std::move(grafanaTokenName))
    {}

    const NMVP::TSupportLinkEntryConfig& Config() const override {
        return Config_;
    }

    NMVP::TResolveOutput Resolve(const TResolveInput& input) const override {
        NMVP::TResolveOutput out;
        out.Name = Config_.GetSource();
        out.Ready = false;
        auto actorId = NActors::TActivationContext::Register(
            new TGrafanaDashboardSearchResolveActor(
                Config_,
                GrafanaEndpoint_,
                GrafanaTokenName_,
                input.Parent,
                input.HttpProxyId,
                input.Place,
                input.ClusterColumns,
                input.UrlParameters),
            input.Parent);
        out.Actor = actorId;
        return out;
    }

private:
    NMVP::TSupportLinkEntryConfig Config_;
    TString GrafanaEndpoint_;
    TString GrafanaTokenName_;
};

std::shared_ptr<NMVP::ILinkSource> MakeGrafanaDashboardSource(NMVP::TSupportLinkEntryConfig config) {
    if (config.GetUrl().empty()) {
        ythrow yexception() << "url is required for source=" << config.GetSource();
    }

    const TString grafanaEndpoint = NMVP::InstanceMVP
        ? NMVP::InstanceMVP->MetaSettings.GrafanaEndpoint
        : (CurrentGrafanaEndpointForConfigParsing ? *CurrentGrafanaEndpointForConfigParsing : TString());
    if (!config.GetUrl().StartsWith("http://")
        && !config.GetUrl().StartsWith("https://")
        && grafanaEndpoint.empty())
    {
        ythrow yexception() << "grafana.endpoint is required for relative url";
    }

    return std::make_shared<TGrafanaDashboardSource>(std::move(config), grafanaEndpoint);
}

std::shared_ptr<NMVP::ILinkSource> MakeGrafanaDashboardSearchSource(NMVP::TSupportLinkEntryConfig config) {
    const TString grafanaEndpoint = NMVP::InstanceMVP
        ? NMVP::InstanceMVP->MetaSettings.GrafanaEndpoint
        : (CurrentGrafanaEndpointForConfigParsing ? *CurrentGrafanaEndpointForConfigParsing : TString());
    const TString& grafanaTokenName = NMVP::TMVP::MetaDatabaseTokenName;

    const TString effectiveUrl = config.GetUrl().empty() ? TString("/api/search") : config.GetUrl();
    if (!effectiveUrl.StartsWith("http://")
        && !effectiveUrl.StartsWith("https://")
        && grafanaEndpoint.empty())
    {
        ythrow yexception() << "grafana.endpoint is required for relative url";
    }
    if (grafanaTokenName.empty()) {
        ythrow yexception() << "meta.meta_database_token_name is required for source=" << config.GetSource();
    }

    return std::make_shared<TGrafanaDashboardSearchSource>(std::move(config), grafanaEndpoint, grafanaTokenName);
}

void RegisterSupportLinkSourcesOnce() {
    static std::once_flag once;
    std::call_once(once, []() {
        NMVP::RegisterLinkSource("grafana/dashboard", &MakeGrafanaDashboardSource);
        NMVP::RegisterLinkSource("grafana/dashboard/search", &MakeGrafanaDashboardSearchSource);
    });
}

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

    MetaApiEndpoint = config.GetMetaApiEndpoint();
    MetaDatabase = config.GetMetaDatabase();
    MetaCache = config.GetMetaCache();
    MetaDatabaseTokenName = config.GetMetaDatabaseTokenName();
    DbUserTokenSource = config.GetDbUserTokenAccess();
    MetaSettings.MetaApiEndpoint = MetaApiEndpoint;
    MetaSettings.MetaDatabase = MetaDatabase;
    if (MetaSettings.MetaApiEndpoint.empty()) {
        ythrow yexception() << NMVP::CONFIG_ERROR_PREFIX << "meta.meta_api_endpoint must be specified";
    }
    if (MetaSettings.MetaDatabase.empty()) {
        ythrow yexception() << NMVP::CONFIG_ERROR_PREFIX << "meta.meta_database must be specified";
    }

    if (config.HasGrafana()) {
        MetaSettings.GrafanaEndpoint = config.GetGrafana().GetEndpoint();
    } else {
        MetaSettings.GrafanaEndpoint.clear();
    }

    if (config.HasSupportLinks()) {
        RegisterSupportLinkSourcesOnce();

        const auto& supportLinks = config.GetSupportLinks();
        TGrafanaEndpointConfigGuard grafanaEndpointGuard(MetaSettings.GrafanaEndpoint);
        MetaSettings.ClusterLinkSources.clear();
        MetaSettings.ClusterLinkSources.reserve(supportLinks.GetCluster().size());
        for (int i = 0; i < supportLinks.GetCluster().size(); ++i) {
            MetaSettings.ClusterLinkSources.push_back(MakeLinkSource(supportLinks.GetCluster(i)));
        }
        MetaSettings.DatabaseLinkSources.clear();
        MetaSettings.DatabaseLinkSources.reserve(supportLinks.GetDatabase().size());
        for (int i = 0; i < supportLinks.GetDatabase().size(); ++i) {
            MetaSettings.DatabaseLinkSources.push_back(MakeLinkSource(supportLinks.GetDatabase(i)));
        }
    }
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

    if (StartupOptions.AccessServiceType == NMvp::nebius_v1) {
        MetaSettings.AccessServiceType = StartupOptions.AccessServiceType;
    }

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
