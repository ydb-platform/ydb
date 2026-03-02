#pragma once
#include <ydb/mvp/core/appdata.h>
#include <ydb/mvp/core/mvp_log.h>
#include <ydb/mvp/core/mvp_startup_options.h>
#include <ydb/mvp/core/mvp_tokens.h>
#include <ydb/mvp/core/signals.h>
#include <ydb/mvp/meta/meta_settings.h>
#include <ydb/mvp/meta/protos/config.pb.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/http/http.h>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/generic/vector.h>

namespace YAML {
class Node;
}

namespace NMVP {

const TString& GetEServiceName(NActors::NLog::EComponent component);
void ValidateMetaBaseConfig(TStringBuf metaApiEndpoint, TStringBuf metaDatabase, bool hasMetaConfigBlock, bool isNebius);

struct TGrafanaSupportConfig {
    TString Endpoint;
    TString SecretName;
    TString WorkspaceColumn = "workspace";
    TString DatasourceColumn = "grafana_ds";
};

struct TSupportLinkEntryConfig {
    TString Source;
    TString Title;
    TString Url;
    TString Tag;
    TString Folder;
};

struct TSupportLinksConfig {
    TVector<TSupportLinkEntryConfig> Cluster;
    TVector<TSupportLinkEntryConfig> Database;
};

class TMVP {
protected:
    static TAtomic Quit;
    static void OnTerminate(int);

    NSignals::TSignalHandler<SIGINT, &TMVP::OnTerminate> SignalSIGINT;
    NSignals::TSignalHandler<SIGTERM, &TMVP::OnTerminate> SignalSIGTERM;
    NSignals::TSignalIgnore<SIGPIPE> SignalSIGPIPE;

public:
    TString GetAppropriateEndpoint(const NHttp::THttpIncomingRequestPtr&);

    TString MetaApiEndpoint = "";
    TString MetaDatabase = "";
    bool MetaCache = false;
    TMetaSettings MetaSettings;
    static TString MetaDatabaseTokenName;
    static bool DbUserTokenSource;

    TMVP(int argc, const char* argv[]);
    int Init();
    int Run();
    int Shutdown();

    THolder<NActors::TActorSystemSetup> BuildActorSystemSetup();
    TIntrusivePtr<NActors::NLog::TSettings> BuildLoggerSettings();
    void InitMeta();

    TString static GetMetaDatabaseAuthToken(const TRequest& request);
    NYdb::NTable::TClientSettings static GetMetaDatabaseClientSettings(const TRequest& request, const TYdbLocation& location);

    void TryGetMetaOptionsFromConfig();
    void TryGetMetaOptionsFromConfig(const NMvp::NMeta::TMetaConfig& config);
    void TryGetMetaOptionsFromConfig(const YAML::Node& config);
    void ValidateSupportLinksConfig() const;

    TMVPAppData AppData;
    const TMvpStartupOptions StartupOptions;
    TIntrusivePtr<NActors::NLog::TSettings> LoggerSettings;
    THolder<NActors::TActorSystemSetup> ActorSystemSetup;
    NActors::TActorSystem ActorSystem;
    NActors::TActorId HttpProxyId;
    NActors::TActorId HandlerId;

    TGrafanaSupportConfig GrafanaSupportConfig;
    TSupportLinksConfig SupportLinksConfig;

    static NMvp::TTokensConfig TokensConfig;
};

extern TMVP* InstanceMVP;

} // namespace NMVP
