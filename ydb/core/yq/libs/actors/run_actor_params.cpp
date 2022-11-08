#include "run_actor_params.h"

namespace NYq {

using namespace NActors;

TRunActorParams::TRunActorParams(
    TYqSharedResources::TPtr yqSharedResources,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    NYql::IHTTPGateway::TPtr s3Gateway,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    TIntrusivePtr<IRandomProvider> randomProvider,
    NYql::IModuleResolver::TPtr& moduleResolver,
    ui64 nextUniqueId,
    NKikimr::NMiniKQL::TComputationNodeFactory dqCompFactory,
    ::NPq::NConfigurationManager::IConnections::TPtr pqCmConnections,
    const ::NYq::NConfig::TCommonConfig& commonConfig,
    const ::NYq::NConfig::TCheckpointCoordinatorConfig& checkpointCoordinatorConfig,
    const ::NYq::NConfig::TPrivateApiConfig& privateApiConfig,
    const ::NYq::NConfig::TGatewaysConfig& gatewaysConfig,
    const ::NYq::NConfig::TPingerConfig& pingerConfig,
    const ::NYq::NConfig::TRateLimiterConfig& rateLimiterConfig,
    const TString& sql,
    const TScope& scope,
    const TString& authToken,
    const TActorId& databaseResolver,
    const TString& queryId,
    const TString& userId,
    const TString& owner,
    const int64_t previousQueryRevision,
    TVector<YandexQuery::Connection> connections,
    TVector<YandexQuery::Binding> bindings,
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    THashMap<TString, TString> accountIdSignatures,
    YandexQuery::QueryContent::QueryType queryType,
    YandexQuery::QueryContent::QuerySyntax querySyntax,
    YandexQuery::ExecuteMode executeMode,
    const TString& resultId,
    const YandexQuery::StateLoadMode stateLoadMode,
    const YandexQuery::StreamingDisposition& streamingDisposition,
    YandexQuery::QueryMeta::ComputeStatus status,
    const TString& cloudId,
    TVector<YandexQuery::ResultSetMeta> resultSetMetas,
    TVector<TString> dqGraphs,
    int32_t dqGraphIndex,
    TVector<Fq::Private::TopicConsumer> createdTopicConsumers,
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
    const Fq::Private::TaskResources& resources
    )
    : YqSharedResources(yqSharedResources)
    , CredentialsProviderFactory(credentialsProviderFactory)
    , S3Gateway(s3Gateway)
    , FunctionRegistry(functionRegistry)
    , RandomProvider(randomProvider)
    , ModuleResolver(moduleResolver)
    , NextUniqueId(nextUniqueId)
    , DqCompFactory(dqCompFactory)
    , PqCmConnections(std::move(pqCmConnections))
    , CommonConfig(commonConfig)
    , CheckpointCoordinatorConfig(checkpointCoordinatorConfig)
    , PrivateApiConfig(privateApiConfig)
    , GatewaysConfig(gatewaysConfig)
    , PingerConfig(pingerConfig)
    , RateLimiterConfig(rateLimiterConfig)
    , Sql(sql)
    , Scope(scope)
    , AuthToken(authToken)
    , DatabaseResolver(databaseResolver)
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
    , CreatedTopicConsumers(std::move(createdTopicConsumers))
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
    {
    }

} /* NYq */
