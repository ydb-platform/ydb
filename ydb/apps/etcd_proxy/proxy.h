#pragma once

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/grpc/server/grpc_server.h>

#include <ydb/apps/etcd_proxy/service/etcd_shared.h>

#include <library/cpp/monlib/metrics/metric_registry.h>

#include "signals.h"

#include <atomic>

namespace NEtcd {

class TProxy {
public:
    TProxy(int argc, char** argv);
    int Run();

private:
    const NEtcd::TSharedStuff::TPtr Stuff;

    static std::atomic_bool Quit;
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

    const std::shared_ptr<NMonitoring::TMetricRegistry> MetricRegistry;
    const NMonitoring::TDynamicCounterPtr Counters;

    std::unique_ptr<NActors::TActorSystem> ActorSystem;
    std::unique_ptr<NYdbGrpc::TGRpcServer> GRpcServer;

    // arguments
    bool Initialize_ = false;
    TString Database, Endpoint;
    uint16_t ListeningPort = 2379;
    TString SslCertificate;
};

}
