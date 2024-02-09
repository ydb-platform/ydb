#pragma once
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/http/http.h>
#include <library/cpp/getopt/last_getopt.h>
#include <ydb/mvp/core/mvp_log.h>
#include <ydb/mvp/core/signals.h>
#include <ydb/mvp/core/appdata.h>
#include <ydb/mvp/core/mvp_tokens.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/rwlock.h>
#include <contrib/libs/yaml-cpp/include/yaml-cpp/yaml.h>
#include "openid_connect.h"

namespace NMVP {

const TString& GetEServiceName(NActors::NLog::EComponent component);

class TMVP {
private:
    TRWMutex ActorSystemStoppingLock;
    TAtomic ActorSystemStopping; // Used by async gRPC callbacks to determine whether they are still allowed to send messages to the actor system.
    TString SecretName;

    const static ui16 DefaultHttpPort;
    const static ui16 DefaultHttpsPort;

protected:
    static TAtomic Quit;
    static void OnTerminate(int);

    NSignals::TSignalHandler<SIGINT, &TMVP::OnTerminate> SignalSIGINT;
    NSignals::TSignalHandler<SIGTERM, &TMVP::OnTerminate> SignalSIGTERM;
    NSignals::TSignalIgnore<SIGPIPE> SignalSIGPIPE;

    THolder<NActors::TActorSystemSetup> BuildActorSystemSetup(int argc, char** argv);
    TIntrusivePtr<NActors::NLog::TSettings> BuildLoggerSettings();

    void TryGetOidcOptionsFromConfig(const YAML::Node& config);
    void TryGetGenericOptionsFromConfig(
        const YAML::Node& config,
        const NLastGetopt::TOptsParseResult& opts,
        TString& ydbTokenFile,
        TString& caCertificateFile,
        TString& sslCertificateFile,
        bool& useStderr,
        bool& mlock);

    TMVPAppData AppData;
    TIntrusivePtr<NActors::NLog::TSettings> LoggerSettings;
    THolder<NActors::TActorSystemSetup> ActorSystemSetup;
    NActors::TActorSystem ActorSystem;
    NActors::TActorId BaseHttpProxyId;
    NActors::TActorId HttpProxyId;
    NActors::TActorId HandlerId;

    TString YdbUserToken;
    static NMvp::TTokensConfig TokensConfig;
    static TOpenIdConnectSettings OpenIdConnectSettings;

public:
    static ui16 HttpPort;
    static ui16 HttpsPort;
    static bool Http;
    static bool Https;
    static TString GetAppropriateEndpoint(const NHttp::THttpIncomingRequestPtr&);

    TMVP(int argc, char** argv);
    int Init();
    int Run();
    int Shutdown();
};

} // namespace NMVP
