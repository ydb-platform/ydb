#pragma once

#include <ydb/core/fq/libs/config/protos/common.pb.h>
#include <ydb/core/fq/libs/config/protos/fq_config.pb.h>
#include <ydb/core/fq/libs/config/protos/pinger.pb.h>
#include <ydb/core/fq/libs/config/protos/storage.pb.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>

#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/yql/providers/generic/connector/libcpp/client.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_gateway.h>
#include <ydb/library/yql/providers/dq/worker_manager/interface/counters.h>
#include <ydb/library/yql/providers/pq/cm_client/client.h>
#include <ydb/library/yql/providers/solomon/provider/yql_solomon_gateway.h>
#include <ydb/library/yql/providers/s3/actors_factory/yql_s3_actors_factory.h>

#include <ydb/public/lib/fq/scope.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>

namespace NFq {

struct TRunActorParams { // TODO2 : Change name
    TRunActorParams(
        TYqSharedResources::TPtr yqSharedResources,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
        NYql::IHTTPGateway::TPtr s3Gateway,
        NYql::NConnector::IClient::TPtr connectorClient,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        TIntrusivePtr<IRandomProvider> randomProvider,
        NYql::IModuleResolver::TPtr& moduleResolver,
        ui64 nextUniqueId,
        NKikimr::NMiniKQL::TComputationNodeFactory dqCompFactory,
        ::NPq::NConfigurationManager::IConnections::TPtr pqCmConnections,
        const ::NFq::NConfig::TConfig& config,
        const TString& sql,
        const NYdb::NFq::TScope& scope,
        const TString& authToken,
        const NActors::TActorId& databaseResolver,
        const TString& queryId,
        const TString& userId,
        const TString& owner,
        const int64_t previousQueryRevision,
        TVector<FederatedQuery::Connection> connections,
        TVector<FederatedQuery::Binding> bindings,
        NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
        THashMap<TString, TString> accountIdSignatures,
        FederatedQuery::QueryContent::QueryType queryType,
        FederatedQuery::QueryContent::QuerySyntax querySyntax,
        FederatedQuery::ExecuteMode executeMode,
        const TString& resultId,
        const FederatedQuery::StateLoadMode stateLoadMode,
        const FederatedQuery::StreamingDisposition& streamingDisposition,
        FederatedQuery::QueryMeta::ComputeStatus status,
        const TString& cloudId,
        TVector<FederatedQuery::ResultSetMeta> resultSetMetas,
        TVector<TString> dqGraphs,
        int32_t dqGraphIndex,
        bool automatic,
        const TString& queryName,
        const TInstant& deadline,
        const ::NMonitoring::TDynamicCounterPtr& clientCounters,
        TInstant createdAt,
        const TString& tenantName,
        uint64_t resultBytesLimit,
        TDuration executionTtl,
        TInstant requestStartedAt,
        ui32 restartCount,
        const TString& jobId,
        const Fq::Private::TaskResources& resources,
        const TString& executionId,
        const TString& operationId,
        const ::NFq::NConfig::TYdbStorageConfig& computeConnection,
        TDuration resultTtl,
        std::map<TString, Ydb::TypedValue>&& queryParameters,
        std::shared_ptr<NYql::NDq::IS3ActorsFactory> s3ActorsFactory,
        const ::NFq::NConfig::TWorkloadManagerConfig& workloadManager
    );

    TRunActorParams(const TRunActorParams& params) = default;
    TRunActorParams(TRunActorParams&& params) = default;

    friend IOutputStream& operator<<(IOutputStream& out, const TRunActorParams& params);

    TYqSharedResources::TPtr YqSharedResources;
    NKikimr::TYdbCredentialsProviderFactory CredentialsProviderFactory;
    NYql::IHTTPGateway::TPtr S3Gateway;
    NYql::NConnector::IClient::TPtr ConnectorClient;
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry;
    TIntrusivePtr<IRandomProvider> RandomProvider;
    NYql::IModuleResolver::TPtr ModuleResolver;
    ui64 NextUniqueId;
    NKikimr::NMiniKQL::TComputationNodeFactory DqCompFactory;

    ::NPq::NConfigurationManager::IConnections::TPtr PqCmConnections;
    const ::NFq::NConfig::TConfig Config;
    const TString Sql;
    const NYdb::NFq::TScope Scope;
    const TString AuthToken;
    const NActors::TActorId DatabaseResolver;
    const NYql::IMdbEndpointGenerator::TPtr MdbEndpointGenerator;
    const TString QueryId;
    const TString UserId;
    const TString Owner;
    const int64_t PreviousQueryRevision;
    const TVector<FederatedQuery::Connection> Connections;
    const TVector<FederatedQuery::Binding> Bindings;
    const NYql::ISecuredServiceAccountCredentialsFactory::TPtr CredentialsFactory;
    const THashMap<TString, TString> AccountIdSignatures;
    const FederatedQuery::QueryContent::QueryType QueryType;
    const FederatedQuery::QueryContent::QuerySyntax QuerySyntax;
    const FederatedQuery::ExecuteMode ExecuteMode;
    const TString ResultId;
    const FederatedQuery::StateLoadMode StateLoadMode;
    const FederatedQuery::StreamingDisposition StreamingDisposition;
    FederatedQuery::QueryMeta::ComputeStatus Status;
    const TString CloudId;
    const TVector<FederatedQuery::ResultSetMeta> ResultSetMetas;
    const TVector<TString> DqGraphs;
    const int32_t DqGraphIndex;

    const bool Automatic = false;
    const TString QueryName;
    const TInstant Deadline;

    const ::NMonitoring::TDynamicCounterPtr ClientCounters;
    const TInstant CreatedAt;
    const TString TenantName;
    const uint64_t ResultBytesLimit;
    const TDuration ExecutionTtl;
    TInstant RequestStartedAt;
    const ui32 RestartCount;
    const TString JobId;
    Fq::Private::TaskResources Resources;
    TString ExecutionId;
    NYdb::TOperation::TOperationId OperationId;
    ::NFq::NConfig::TYdbStorageConfig ComputeConnection;
    TDuration ResultTtl;
    std::map<TString, Ydb::TypedValue> QueryParameters;
    std::shared_ptr<NYql::NDq::IS3ActorsFactory> S3ActorsFactory;
    ::NFq::NConfig::TWorkloadManagerConfig WorkloadManager;
};

} /* NFq */
