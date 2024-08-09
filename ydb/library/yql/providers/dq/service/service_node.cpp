#include "service_node.h"

#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/providers/common/metrics/metrics_registry.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <ydb/library/yql/providers/dq/actors/execution_helpers.h>

#include <ydb/library/grpc/server/actors/logger.h>

#include <utility>

namespace NYql {
    using namespace NActors;
    using namespace NYdbGrpc;
    using namespace NYql::NDqs;

    class TGrpcExternalListener: public IExternalListener {
    public:
        TGrpcExternalListener(SOCKET socket)
            : Socket(socket)
            , Listener(MakeIntrusive<NInterconnect::TStreamSocket>(Socket))
        {
            SetNonBlock(socket, true);
        }

        void Init(std::unique_ptr<grpc::experimental::ExternalConnectionAcceptor> acceptor) override {
            Acceptor = std::move(acceptor);
        }

    private:
        void Start() override {
            YQL_CLOG(DEBUG, ProviderDq) << "Start GRPC Listener";
            Poller.Start();
            StartRead();
        }

        void Stop() override {
            Poller.Stop();
        }

        void StartRead() {
            YQL_CLOG(TRACE, ProviderDq) << "Read next GRPC event";
            Poller.StartRead(Listener, [&](const TIntrusivePtr<TSharedDescriptor>& ss) {
                return Accept(ss);
            });
        }

        TDelegate Accept(const TIntrusivePtr<TSharedDescriptor>& ss)
        {
            NInterconnect::TStreamSocket* socket = (NInterconnect::TStreamSocket*)ss.Get();
            int r = 0;
            while (r >= 0) {
                NInterconnect::TAddress address;
                r = socket->Accept(address);
                if (r >= 0) {
                    YQL_CLOG(TRACE, ProviderDq) << "New GRPC connection";
                    grpc::experimental::ExternalConnectionAcceptor::NewConnectionParameters params;
                    SetNonBlock(r, true);
                    params.listener_fd = -1; // static_cast<int>(Socket);
                    params.fd = r;
                    Acceptor->HandleNewConnection(&params);
                } else if (-r != EAGAIN && -r != EWOULDBLOCK) {
                    YQL_CLOG(DEBUG, ProviderDq) << "Unknown error code " + ToString(r);
                }
            }

            return [this] {
                StartRead();
            };
        }

        SOCKET Socket;
        TIntrusivePtr<NInterconnect::TStreamSocket> Listener;
        NInterconnect::TPollerThreads Poller;
        std::unique_ptr<grpc::experimental::ExternalConnectionAcceptor> Acceptor;
    };

    TServiceNode::TServiceNode(
        const TServiceNodeConfig& config,
        ui32 threads,
        IMetricsRegistryPtr metricsRegistry)
        : Config(config)
        , Threads(threads)
        , MetricsRegistry(std::move(metricsRegistry))
    {
        std::tie(Setup, LogSettings) = BuildActorSetup(
            Config.NodeId,
            Config.InterconnectAddress,
            Config.Port,
            Config.Socket,
            {Threads, 8},
            MetricsRegistry->GetSensors(),
            Config.NameserverFactory,
            Config.MaxNodeId,
            Config.ICSettings);
    }

    void TServiceNode::AddLocalService(TActorId actorId, TActorSetupCmd service) {
        YQL_ENSURE(!ActorSystem);
        Setup->LocalServices.emplace_back(actorId, std::move(service));
    }

    NActors::TActorSystem* TServiceNode::StartActorSystem(void* appData) {
        Y_ABORT_UNLESS(!ActorSystem);

        ActorSystem = MakeHolder<NActors::TActorSystem>(Setup, appData, LogSettings);
        ActorSystem->Start();

        return ActorSystem.Get();
    }

    void TServiceNode::StartService(const TDqTaskPreprocessorFactoryCollection& dqTaskPreprocessorFactories) {
        class TCustomOption : public grpc::ServerBuilderOption {
        public:
            TCustomOption() { }

            void UpdateArguments(grpc::ChannelArguments *args) override {
                args->SetInt(GRPC_ARG_ALLOW_REUSEPORT, 1);
            }

            void UpdatePlugins(std::vector<std::unique_ptr<grpc::ServerBuilderPlugin>>* /*plugins*/) override
            { }
        };

        YQL_CLOG(INFO, ProviderDq) << "Starting GRPC on " << Config.GrpcPort;

        IExternalListener::TPtr listener = nullptr;
        if (Config.GrpcSocket >= 0) {
            listener = MakeIntrusive<TGrpcExternalListener>(Config.GrpcSocket);
        }

        auto options = TServerOptions()
                           // .SetHost(CurrentNode->Address)
                        //    .SetHost("[::]")
                           .SetHost("0.0.0.0")
                           .SetPort(Config.GrpcPort)
                           .SetExternalListener(listener)
                           .SetWorkerThreads(2)
                           .SetGRpcMemoryQuotaBytes(1024 * 1024 * 1024)
                           .SetMaxMessageSize(1024 * 1024 * 256)
                           .SetMaxGlobalRequestInFlight(50000)
                           .SetUseAuth(false)
                           .SetKeepAliveEnable(true)
                           .SetKeepAliveIdleTimeoutTriggerSec(360)
                           .SetKeepAliveMaxProbeCount(3)
                           .SetKeepAliveProbeIntervalSec(1)
                           .SetServerBuilderMutator([](grpc::ServerBuilder& builder) {
                               builder.SetOption(std::make_unique<TCustomOption>());
                           })
                           .SetLogger(CreateActorSystemLogger(*ActorSystem, 413)); // 413 - NKikimrServices::GRPC_SERVER

        Server = MakeHolder<TGRpcServer>(options);
        Service = TIntrusivePtr<IGRpcService>(new TDqsGrpcService(*ActorSystem, MetricsRegistry->GetSensors(), dqTaskPreprocessorFactories));
        Server->AddService(Service);

        Cout << "CRAB: TServiceNode::StartService Before " << Config.GrpcPort << Endl;

        Server->Start();

        Cout << "CRAB: TServiceNode::StartService After " << Config.GrpcPort << Endl;
    }

    void TServiceNode::Stop(TDuration timeout) {
        (static_cast<TDqsGrpcService*>(Service.Get()))->Stop().Wait(timeout);

        Server->Stop();
        for (auto id : ActorIds) {
            ActorSystem->Send(id, new NActors::TEvents::TEvPoison);
        }
        ActorSystem->Stop();
    }
} // namespace NYql
