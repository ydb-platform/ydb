#include "yql_dq_gateway_local.h"

#include <ydb/library/yql/providers/dq/provider/yql_dq_gateway.h>
#include <ydb/library/yql/providers/dq/task_runner/tasks_runner_local.h>

#include <ydb/library/yql/providers/dq/service/interconnect_helpers.h>
#include <ydb/library/yql/providers/dq/service/service_node.h>

#include <ydb/library/yql/providers/dq/worker_manager/local_worker_manager.h>

#include <ydb/library/yql/utils/range_walker.h>
#include <ydb/library/yql/utils/bind_in_range.h>

#include <library/cpp/messagebus/network.h>

namespace NYql {

using namespace NActors;
using NDqs::MakeWorkerManagerActorID;

class TLocalServiceHolder {
public:
    TLocalServiceHolder(const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry, NKikimr::NMiniKQL::TComputationNodeFactory compFactory,
        TTaskTransformFactory taskTransformFactory, const TDqTaskPreprocessorFactoryCollection& dqTaskPreprocessorFactories, NBus::TBindResult interconnectPort, NBus::TBindResult grpcPort,
        NDq::IDqSourceFactory::TPtr sourceFactory, NDq::IDqSinkFactory::TPtr sinkFactory, NDq::IDqOutputTransformFactory::TPtr transformFactory)
    {
        ui32 nodeId = 1;

        TString hostName;
        TString localAddress;
        std::tie(hostName, localAddress) = NDqs::GetLocalAddress();

        NDqs::TServiceNodeConfig config = {
            nodeId,
            localAddress,
            hostName,
            static_cast<ui16>(interconnectPort.Addr.GetPort()),
            static_cast<ui16>(grpcPort.Addr.GetPort()),
            0, // mbus
            interconnectPort.Socket.Get()->Release(),
            grpcPort.Socket.Get()->Release(),
        };

        ServiceNode = MakeHolder<TServiceNode>(
            config,
            1,
            CreateMetricsRegistry(GetSensorsGroupFor(NSensorComponent::kDq)));

        NDqs::TLocalWorkerManagerOptions lwmOptions;
        lwmOptions.Factory = NTaskRunnerProxy::CreateFactory(functionRegistry, compFactory, taskTransformFactory, true);
        lwmOptions.SourceFactory = std::move(sourceFactory);
        lwmOptions.SinkFactory = std::move(sinkFactory);
        lwmOptions.TransformFactory = std::move(transformFactory);
        lwmOptions.FunctionRegistry = functionRegistry;
        lwmOptions.TaskRunnerInvokerFactory = new NDqs::TTaskRunnerInvokerFactory();
        lwmOptions.TaskRunnerActorFactory = NDq::NTaskRunnerActor::CreateLocalTaskRunnerActorFactory(
            [=](const NDqProto::TDqTask& task, const NDq::TLogFunc& )
                {
                    return lwmOptions.Factory->Get(task);
                });
        auto resman = NDqs::CreateLocalWorkerManager(lwmOptions);

        ServiceNode->AddLocalService(
            MakeWorkerManagerActorID(nodeId),
            TActorSetupCmd(resman, TMailboxType::Simple, 0));

        ServiceNode->StartActorSystem();

        ServiceNode->StartService(dqTaskPreprocessorFactories);
    }

    ~TLocalServiceHolder()
    {
        ServiceNode->Stop();
    }

private:
    THolder<TServiceNode> ServiceNode;
};

class TDqGatewayLocal: public IDqGateway
{
public:
    TDqGatewayLocal(THolder<TLocalServiceHolder>&& localService, const IDqGateway::TPtr& gateway)
        : LocalService(std::move(localService))
        , Gateway(gateway)
    { }

    NThreading::TFuture<void> OpenSession(const TString& sessionId, const TString& username) override {
        return Gateway->OpenSession(sessionId, username);
    }

    void CloseSession(const TString& sessionId) override {
        return Gateway->CloseSession(sessionId);
    }

    NThreading::TFuture<TResult>
    ExecutePlan(const TString& sessionId, NDqs::IDqsExecutionPlanner& plan, const TVector<TString>& columns,
                const THashMap<TString, TString>& secureParams, const THashMap<TString, TString>& graphParams,
                const TDqSettings::TPtr& settings,
                const TDqProgressWriter& progressWriter, const THashMap<TString, TString>& modulesMapping,
                bool discard) override
    {
        return Gateway->ExecutePlan(sessionId, plan, columns, secureParams, graphParams, settings, progressWriter, modulesMapping, discard);
    }

private:
    THolder<TLocalServiceHolder> LocalService;
    IDqGateway::TPtr Gateway;
};

THolder<TLocalServiceHolder> CreateLocalServiceHolder(const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    NKikimr::NMiniKQL::TComputationNodeFactory compFactory,
    TTaskTransformFactory taskTransformFactory, const TDqTaskPreprocessorFactoryCollection& dqTaskPreprocessorFactories,
    NBus::TBindResult interconnectPort, NBus::TBindResult grpcPort,
    NDq::IDqSourceFactory::TPtr sourceFactory, NDq::IDqSinkFactory::TPtr sinkFactory, NDq::IDqOutputTransformFactory::TPtr transformFactory)
{
    return MakeHolder<TLocalServiceHolder>(functionRegistry, compFactory, taskTransformFactory, dqTaskPreprocessorFactories, interconnectPort, grpcPort, std::move(sourceFactory), std::move(sinkFactory), std::move(transformFactory));
}

TIntrusivePtr<IDqGateway> CreateLocalDqGateway(const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    NKikimr::NMiniKQL::TComputationNodeFactory compFactory,
    TTaskTransformFactory taskTransformFactory, const TDqTaskPreprocessorFactoryCollection& dqTaskPreprocessorFactories,
    NDq::IDqSourceFactory::TPtr sourceFactory, NDq::IDqSinkFactory::TPtr sinkFactory, NDq::IDqOutputTransformFactory::TPtr transformFactory)
{
    int startPort = 31337;
    TRangeWalker<int> portWalker(startPort, startPort+100);
    auto interconnectPort = BindInRange(portWalker)[1];
    auto grpcPort = BindInRange(portWalker)[1];

    return new TDqGatewayLocal(
        CreateLocalServiceHolder(functionRegistry, compFactory, taskTransformFactory, dqTaskPreprocessorFactories, interconnectPort, grpcPort, std::move(sourceFactory), std::move(sinkFactory), std::move(transformFactory)),
        CreateDqGateway(std::get<0>(NDqs::GetLocalAddress()), grpcPort.Addr.GetPort(), 8));
}

} // namespace NYql
