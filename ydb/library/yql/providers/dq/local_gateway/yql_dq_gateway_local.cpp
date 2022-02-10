#include "yql_dq_gateway_local.h"

#include <ydb/library/yql/providers/dq/provider/yql_dq_gateway.h> 
#include <ydb/library/yql/providers/dq/task_runner/tasks_runner_local.h> 

#include <ydb/library/yql/providers/dq/service/interconnect_helpers.h>
#include <ydb/library/yql/providers/dq/service/service_node.h>

#include <ydb/library/yql/providers/dq/worker_manager/local_worker_manager.h> 
#include <ydb/library/yql/providers/pq/async_io/dq_pq_read_actor.h> 
#include <ydb/library/yql/providers/pq/async_io/dq_pq_write_actor.h> 
#include <ydb/library/yql/providers/clickhouse/actors/yql_ch_source_factory.h> 
#include <ydb/library/yql/providers/s3/actors/yql_s3_source_factory.h> 
#include <ydb/library/yql/providers/ydb/actors/yql_ydb_source_factory.h> 

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_io_actors_factory.h> 
#include <ydb/library/yql/utils/range_walker.h> 
#include <ydb/library/yql/utils/bind_in_range.h> 

#include <library/cpp/messagebus/network.h>

namespace NYql {

using namespace NActors;
using NDqs::MakeWorkerManagerActorID;

namespace {
    // TODO: Use the only driver for both sources.
    NDq::IDqSourceActorFactory::TPtr CreateSourceActorFactory(const NYdb::TDriver& driver, IHTTPGateway::TPtr httpGateway) {
        auto factory = MakeIntrusive<NYql::NDq::TDqSourceFactory>();
        RegisterDqPqReadActorFactory(*factory, driver, nullptr);
        RegisterYdbReadActorFactory(*factory, driver, nullptr);
        RegisterS3ReadActorFactory(*factory, nullptr, httpGateway);
        RegisterClickHouseReadActorFactory(*factory, nullptr, httpGateway);
        return factory;
    }

    NDq::IDqSinkActorFactory::TPtr CreateSinkActorFactory(const NYdb::TDriver& driver) {
        auto factory = MakeIntrusive<NYql::NDq::TDqSinkFactory>();
        RegisterDqPqWriteActorFactory(*factory, driver, nullptr);
        return factory;
    }
}

class TLocalServiceHolder {
public:
    TLocalServiceHolder(NYdb::TDriver driver, IHTTPGateway::TPtr httpGateway, const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry, NKikimr::NMiniKQL::TComputationNodeFactory compFactory,
        TTaskTransformFactory taskTransformFactory, const TDqTaskPreprocessorFactoryCollection& dqTaskPreprocessorFactories, NBus::TBindResult interconnectPort, NBus::TBindResult grpcPort)
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
        lwmOptions.SourceActorFactory = CreateSourceActorFactory(driver, std::move(httpGateway));
        lwmOptions.SinkActorFactory = CreateSinkActorFactory(driver);
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

THolder<TLocalServiceHolder> CreateLocalServiceHolder(NYdb::TDriver driver, const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    NKikimr::NMiniKQL::TComputationNodeFactory compFactory,
    TTaskTransformFactory taskTransformFactory, const TDqTaskPreprocessorFactoryCollection& dqTaskPreprocessorFactories, IHTTPGateway::TPtr gateway,
    NBus::TBindResult interconnectPort, NBus::TBindResult grpcPort)
{
    return MakeHolder<TLocalServiceHolder>(driver, std::move(gateway), functionRegistry, compFactory, taskTransformFactory, dqTaskPreprocessorFactories, interconnectPort, grpcPort);
}

TIntrusivePtr<IDqGateway> CreateLocalDqGateway(NYdb::TDriver driver, const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    NKikimr::NMiniKQL::TComputationNodeFactory compFactory,
    TTaskTransformFactory taskTransformFactory, const TDqTaskPreprocessorFactoryCollection& dqTaskPreprocessorFactories,
    IHTTPGateway::TPtr gateway)
{
    int startPort = 31337;
    TRangeWalker<int> portWalker(startPort, startPort+100);
    auto interconnectPort = BindInRange(portWalker)[1];
    auto grpcPort = BindInRange(portWalker)[1];

    return new TDqGatewayLocal(
        CreateLocalServiceHolder(driver, functionRegistry, compFactory, taskTransformFactory, dqTaskPreprocessorFactories, std::move(gateway), interconnectPort, grpcPort),
        CreateDqGateway(std::get<0>(NDqs::GetLocalAddress()), grpcPort.Addr.GetPort(), 8));
}

} // namespace NYql
