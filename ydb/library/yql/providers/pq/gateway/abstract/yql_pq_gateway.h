#pragma once

#include "yql_pq_federated_topic_client.h"
#include "yql_pq_topic_client.h"

#include <ydb/library/yql/providers/pq/cm_client/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/federated_topic/federated_topic.h>

#include <util/generic/ptr.h>
#include <util/generic/vector.h>

namespace NKikimr::NMiniKQL {

class IFunctionRegistry;

} // namespace NKikimr::NMiniKQL

namespace NYql {

class TPqClusterConfig;
class TPqGatewayConfig;
using TPqGatewayConfigPtr = std::shared_ptr<TPqGatewayConfig>;

class IPqGateway : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IPqGateway>;

    struct TListStreams {
        TVector<TString> Names;
    };

    struct TClusterInfo {
        NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo Info;
        ui32 PartitionsCount = 0;
    };
    using TDescribeFederatedTopicResult = std::vector<TClusterInfo>;
    using TAsyncDescribeFederatedTopicResult = NThreading::TFuture<IPqGateway::TDescribeFederatedTopicResult>;

    virtual NThreading::TFuture<void> OpenSession(const TString& sessionId, const TString& username) = 0;

    virtual NThreading::TFuture<void> CloseSession(const TString& sessionId) = 0;

    // CM API.
    virtual ::NPq::NConfigurationManager::TAsyncDescribePathResult DescribePath(const TString& sessionId, const TString& cluster, const TString& database, const TString& path, const TString& token) = 0;

    // DS API.
    virtual NThreading::TFuture<TListStreams> ListStreams(const TString& sessionId, const TString& cluster, const TString& database, const TString& token, ui32 limit, const TString& exclusiveStartStreamName = {}) = 0;

    virtual TAsyncDescribeFederatedTopicResult DescribeFederatedTopic(const TString& sessionId, const TString& cluster, const TString& database, const TString& path, const TString& token) = 0;

    virtual ITopicClient::TPtr GetTopicClient(const NYdb::TDriver& driver, const NYdb::NTopic::TTopicClientSettings& settings) = 0;

    virtual IFederatedTopicClient::TPtr GetFederatedTopicClient(const NYdb::TDriver& driver, const NYdb::NFederatedTopic::TFederatedTopicClientSettings& settings) = 0;

    virtual void UpdateClusterConfigs(
        const TString& clusterName,
        const TString& endpoint,
        const TString& database,
        bool secure) = 0;

    virtual void UpdateClusterConfigs(const TPqGatewayConfigPtr& config) = 0;

    virtual void AddCluster(const NYql::TPqClusterConfig& cluster) = 0;

    virtual NYdb::NTopic::TTopicClientSettings GetTopicClientSettings() const = 0;

    virtual NYdb::NFederatedTopic::TFederatedTopicClientSettings GetFederatedTopicClientSettings() const = 0;
};

class IPqGatewayFactory : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IPqGatewayFactory>;

    virtual IPqGateway::TPtr CreatePqGateway() = 0;
};

} // namespace NYql
