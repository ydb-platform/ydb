#pragma once
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/http/http.h>
#include <library/cpp/getopt/last_getopt.h>
#include <ydb/mvp/core/mvp_log.h>
#include <ydb/mvp/core/signals.h>
#include <ydb/mvp/core/appdata.h>
#include <ydb/mvp/core/mvp_startup_options.h>
#include <ydb/mvp/core/mvp_tokens.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <contrib/libs/yaml-cpp/include/yaml-cpp/yaml.h>
#include "oidc_settings.h"

namespace NMVP::NOIDC {

const TString& GetEServiceName(NActors::NLog::EComponent component);

class TMVP {
private:
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
    void TryGetStartupOptionsFromConfig(const YAML::Node& config, const NLastGetopt::TOptsParseResult& parsedArgs);

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
    TMvpStartupOptions startupOptions;
    TString GetAppropriateEndpoint(const NHttp::THttpIncomingRequestPtr&);

    TMVP(int argc, char** argv);
    int Init();
    int Run();
    int Shutdown();
};

} // NMVP::NOIDC
