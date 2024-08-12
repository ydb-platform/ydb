#include "run_actor_params.h"

#include <ydb/core/fq/libs/db_id_async_resolver_impl/mdb_endpoint_generator.h>

namespace NFq {

using namespace NActors;

TRunActorParams::TRunActorParams(
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
    const TActorId& databaseResolver,
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
    const NFq::NConfig::TYdbStorageConfig& computeConnection,
    TDuration resultTtl,
    std::map<TString, Ydb::TypedValue>&& queryParameters,
    std::shared_ptr<NYql::NDq::IS3ActorsFactory> s3ActorsFactory,
    const ::NFq::NConfig::TWorkloadManagerConfig& workloadManager
    )
    : YqSharedResources(yqSharedResources)
    , CredentialsProviderFactory(credentialsProviderFactory)
    , S3Gateway(s3Gateway)
    , ConnectorClient(connectorClient)
    , FunctionRegistry(functionRegistry)
    , RandomProvider(randomProvider)
    , ModuleResolver(moduleResolver)
    , NextUniqueId(nextUniqueId)
    , DqCompFactory(dqCompFactory)
    , PqCmConnections(std::move(pqCmConnections))
    , Config(config)
    , Sql(sql)
    , Scope(scope)
    , AuthToken(authToken)
    , DatabaseResolver(databaseResolver)
    , MdbEndpointGenerator(NFq::MakeMdbEndpointGeneratorGeneric(config.GetCommon().GetMdbTransformHost()))
    , QueryId(queryId)
    , UserId(userId)
    , Owner(owner)
    , PreviousQueryRevision(previousQueryRevision)
    , Connections(std::move(connections))
    , Bindings(std::move(bindings))
    , CredentialsFactory(std::move(credentialsFactory))
    , AccountIdSignatures(std::move(accountIdSignatures))
    , QueryType(queryType)
    , QuerySyntax(querySyntax)
    , ExecuteMode(executeMode)
    , ResultId(resultId)
    , StateLoadMode(stateLoadMode)
    , StreamingDisposition(streamingDisposition)
    , Status(status)
    , CloudId(cloudId)
    , ResultSetMetas(std::move(resultSetMetas))
    , DqGraphs(std::move(dqGraphs))
    , DqGraphIndex(dqGraphIndex)
    , Automatic(automatic)
    , QueryName(queryName)
    , Deadline(deadline)
    , ClientCounters(clientCounters)
    , CreatedAt(createdAt)
    , TenantName(tenantName)
    , ResultBytesLimit(resultBytesLimit)
    , ExecutionTtl(executionTtl)
    , RequestStartedAt(requestStartedAt)
    , RestartCount(restartCount)
    , JobId(jobId)
    , Resources(resources)
    , ExecutionId(executionId)
    , OperationId(operationId, true)
    , ComputeConnection(computeConnection)
    , ResultTtl(resultTtl)
    , QueryParameters(std::move(queryParameters))
    , S3ActorsFactory(std::move(s3ActorsFactory))
    , WorkloadManager(workloadManager)
    {
    }

IOutputStream& operator<<(IOutputStream& out, const TRunActorParams& params) {
    return out  << "Run actors params: { QueryId: " << params.QueryId
                << " CloudId: " << params.CloudId
                << " UserId: " << params.UserId
                << " Owner: " << params.Owner
                << " PreviousQueryRevision: " << params.PreviousQueryRevision
                << " Connections: " << params.Connections.size()
                << " Bindings: " << params.Bindings.size()
                << " AccountIdSignatures: " << params.AccountIdSignatures.size()
                << " QueryType: " << FederatedQuery::QueryContent::QueryType_Name(params.QueryType)
                << " ExecuteMode: " << FederatedQuery::ExecuteMode_Name(params.ExecuteMode)
                << " ResultId: " << params.ResultId
                << " StateLoadMode: " << FederatedQuery::StateLoadMode_Name(params.StateLoadMode)
                << " StreamingDisposition: " << params.StreamingDisposition
                << " Status: " << FederatedQuery::QueryMeta::ComputeStatus_Name(params.Status)
                << " DqGraphs: " << params.DqGraphs.size()
                << " DqGraphIndex: " << params.DqGraphIndex
                << " Resource.TopicConsumers: " << params.Resources.topic_consumers().size()
                << " ExecutionId: " << params.ExecutionId
                << " OperationId: " << (params.OperationId.GetKind() != Ydb::TOperationId::UNUSED ? ProtoToString(params.OperationId) : "<empty>")
                << " ComputeConnection: " << params.ComputeConnection.ShortDebugString()
                << " ResultTtl: " << params.ResultTtl
                << " QueryParameters: " << params.QueryParameters.size()
                << " WorkloadManager: " << params.WorkloadManager.ShortDebugString()
                << "}";
}

} /* NFq */
