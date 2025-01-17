#pragma once

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/grpc/server/grpc_server.h>

#include "signals.h"
#include "service/appdata.h"

#include <atomic>

namespace NEtcd {

class TProxy {
public:
    TProxy(int argc, char** argv);
    int Run();

private:
    static std::atomic<bool> Quit;
    static void OnTerminate(int);
    NSignals::TSignalHandler<SIGINT, &TProxy::OnTerminate> SignalSIGINT;
    NSignals::TSignalHandler<SIGTERM, &TProxy::OnTerminate> SignalSIGTERM;
    NSignals::TSignalIgnore<SIGPIPE> SignalSIGPIPE;

    int Init();
    int Shutdown();

    int Discovery();
    int InitDatabase();
    int StartServer();

    static THolder<NActors::TActorSystemSetup> BuildActorSystemSetup();
    static TIntrusivePtr<NActors::NLog::TSettings> BuildLoggerSettings();
    static const TString& GetEServiceName(NActors::NLog::EComponent component);

    TAppData AppData;
    std::unique_ptr<NActors::TActorSystem> ActorSystem;
    std::unique_ptr<NYdbGrpc::TGRpcServer> GRpcServer;
    const ::NMonitoring::TDynamicCounterPtr Counters;

    NActors::TActorId Poller;
    NActors::TActorId Listener;
    NActors::TActorId DatabaseProxy;
    // arguments
    bool Initialize_ = false;
    TString Database, Endpoint;
    uint16_t ListeningPort = 2379;
    TString SslCertificate;

};

}
