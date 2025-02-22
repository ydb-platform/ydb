#pragma once

#include "yql_pq_topic_client.h"

#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <yql/essentials/providers/common/metrics/metrics_registry.h>
#include <ydb/library/yql/providers/pq/cm_client/client.h>
#include <ydb-cpp-sdk/client/datastreams/datastreams.h>

#include <ydb/library/yql/providers/pq/provider/yql_pq_gateway.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>

#include <ydb-cpp-sdk/client/driver/driver.h>
#include <library/cpp/threading/future/core/future.h>

#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>

namespace NKikimr {
namespace NMiniKQL {
class IFunctionRegistry;
} // namespace NMiniKQL
}

namespace NYql {

class TPqGatewayConfig;
using TPqGatewayConfigPtr = std::shared_ptr<TPqGatewayConfig>;

struct TPqGatewayServices {
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry = nullptr;
    TPqGatewayConfigPtr Config;
    IMetricsRegistryPtr Metrics;
    ISecuredServiceAccountCredentialsFactory::TPtr CredentialsFactory;
    ::NPq::NConfigurationManager::IConnections::TPtr CmConnections;
    NYdb::TDriver YdbDriver;
    TMaybe<NYdb::NTopic::TTopicClientSettings> CommonTopicClientSettings;

    TPqGatewayServices(
        NYdb::TDriver driver,
        ::NPq::NConfigurationManager::IConnections::TPtr cmConnections,
        ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
        TPqGatewayConfigPtr config,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        IMetricsRegistryPtr metrics = nullptr,
        TMaybe<NYdb::NTopic::TTopicClientSettings> commonTopicClientSettings = Nothing())
        : FunctionRegistry(functionRegistry)
        , Config(std::move(config))
        , Metrics(std::move(metrics))
        , CredentialsFactory(std::move(credentialsFactory))
        , CmConnections(std::move(cmConnections))
        , YdbDriver(std::move(driver))
        , CommonTopicClientSettings(commonTopicClientSettings)
    {
    }
};

struct IPqGateway : public TThrRefBase {
    using TPtr = TIntrusivePtr<IPqGateway>;

    struct TListStreams {
        TVector<TString> Names;
    };

    virtual NThreading::TFuture<void> OpenSession(const TString& sessionId, const TString& username) = 0;
    virtual NThreading::TFuture<void> CloseSession(const TString& sessionId) = 0;

    // CM API.
    virtual ::NPq::NConfigurationManager::TAsyncDescribePathResult DescribePath(const TString& sessionId, const TString& cluster, const TString& database, const TString& path, const TString& token) = 0;

    // DS API.
    virtual NThreading::TFuture<TListStreams> ListStreams(const TString& sessionId, const TString& cluster, const TString& database, const TString& token, ui32 limit, const TString& exclusiveStartStreamName = {}) = 0;
    
    virtual ITopicClient::TPtr GetTopicClient(const NYdb::TDriver& driver, const NYdb::NTopic::TTopicClientSettings& settings) = 0;

    virtual void UpdateClusterConfigs(
        const TString& clusterName,
        const TString& endpoint,
        const TString& database,
        bool secure) = 0;

    virtual NYdb::NTopic::TTopicClientSettings GetTopicClientSettings() const = 0;
};

struct IPqGatewayFactory : public TThrRefBase {
    using TPtr = TIntrusivePtr<IPqGatewayFactory>;

    virtual ~IPqGatewayFactory() = default;
    virtual IPqGateway::TPtr CreatePqGateway(const NYql::TPqGatewayServices& services) = 0;
};

} // namespace NYql
