#pragma once
#include <atomic>
#include <ydb/library/actors/core/actorsystem.h>
#include "signals.h"
#include "appdata.h"

namespace NPGW {

class TPgWire {
public:
    static std::atomic<bool> Quit;
    static void OnTerminate(int);
    NSignals::TSignalHandler<SIGINT, &TPgWire::OnTerminate> SignalSIGINT;
    NSignals::TSignalHandler<SIGTERM, &TPgWire::OnTerminate> SignalSIGTERM;
    NSignals::TSignalIgnore<SIGPIPE> SignalSIGPIPE;

    TPgWire(int argc, char** argv);
    int Init();
    int Run();
    int Shutdown();
    static THolder<NActors::TActorSystemSetup> BuildActorSystemSetup();
    static TIntrusivePtr<NActors::NLog::TSettings> BuildLoggerSettings();
    static const TString& GetEServiceName(NActors::NLog::EComponent component);

    TAppData AppData;
    std::unique_ptr<NActors::TActorSystem> ActorSystem;
    NActors::TActorId Poller;
    NActors::TActorId Listener;
    NActors::TActorId DatabaseProxy;
    // arguments
    TString Endpoint;
    uint16_t ListeningPort = 5432;
    TString SslCertificate;
};

}
