#pragma once
#include <ydb/core/yq/libs/config/protos/common.pb.h>
#include <ydb/core/yq/libs/config/protos/pinger.pb.h>
#include <ydb/core/yq/libs/config/protos/yq_config.pb.h>
#include <ydb/core/yq/libs/events/events.h>
#include <ydb/core/yq/libs/shared_resources/shared_resources.h>

#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_gateway.h>
#include <ydb/library/yql/providers/dq/worker_manager/interface/counters.h>
#include <ydb/library/yql/providers/solomon/provider/yql_solomon_gateway.h>
#include <ydb/library/yql/providers/pq/cm_client/client.h>

#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/time_provider/time_provider.h>
#include <library/cpp/random_provider/random_provider.h>

namespace NYq {

struct TRunActorParams { // TODO2 : Change name
    TRunActorParams(
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
        const NActors::TActorId& databaseResolver,
        const TString& queryId,
        const TString& userId,
        const TString& owner,
        const int64_t previousQueryRevision,
        TVector<YandexQuery::Connection> connections,
        TVector<YandexQuery::Binding> bindings,
        NYql::ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
        THashMap<TString, TString> accountIdSignatures,
        YandexQuery::QueryContent::QueryType queryType,
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
        TInstant requestStartedAt
    );

    TRunActorParams(const TRunActorParams& params) = default;
    TRunActorParams(TRunActorParams&& params) = default;

    TYqSharedResources::TPtr YqSharedResources;
    NKikimr::TYdbCredentialsProviderFactory CredentialsProviderFactory;
    NYql::IHTTPGateway::TPtr S3Gateway;
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry;
    TIntrusivePtr<IRandomProvider> RandomProvider;
    NYql::IModuleResolver::TPtr ModuleResolver;
    ui64 NextUniqueId;
    NKikimr::NMiniKQL::TComputationNodeFactory DqCompFactory;

    ::NPq::NConfigurationManager::IConnections::TPtr PqCmConnections;
    const ::NYq::NConfig::TCommonConfig CommonConfig;
    const ::NYq::NConfig::TCheckpointCoordinatorConfig CheckpointCoordinatorConfig;
    const ::NYq::NConfig::TPrivateApiConfig PrivateApiConfig;
    const ::NYq::NConfig::TGatewaysConfig GatewaysConfig;
    const ::NYq::NConfig::TPingerConfig PingerConfig;
    const ::NYq::NConfig::TRateLimiterConfig RateLimiterConfig;
    const TString Sql;
    const TScope Scope;
    const TString AuthToken;
    const NActors::TActorId DatabaseResolver;
    const TString QueryId;
    const TString UserId;
    const TString Owner;
    const int64_t PreviousQueryRevision;
    const TVector<YandexQuery::Connection> Connections;
    const TVector<YandexQuery::Binding> Bindings;
    const NYql::ISecuredServiceAccountCredentialsFactory::TPtr CredentialsFactory;
    const THashMap<TString, TString> AccountIdSignatures;
    const YandexQuery::QueryContent::QueryType QueryType;
    const YandexQuery::ExecuteMode ExecuteMode;
    const TString ResultId;
    const YandexQuery::StateLoadMode StateLoadMode;
    const YandexQuery::StreamingDisposition StreamingDisposition;
    YandexQuery::QueryMeta::ComputeStatus Status;
    const TString CloudId;
    const TVector<YandexQuery::ResultSetMeta> ResultSetMetas;
    const TVector<TString> DqGraphs;
    const int32_t DqGraphIndex;
    TVector<Fq::Private::TopicConsumer> CreatedTopicConsumers;

    bool Automatic = false;
    TString QueryName;
    TInstant Deadline;

    const ::NMonitoring::TDynamicCounterPtr ClientCounters;
    const TInstant CreatedAt;
    const TString TenantName;
    uint64_t ResultBytesLimit;
    TDuration ExecutionTtl;
    TInstant RequestStartedAt;
};

} /* NYq */
