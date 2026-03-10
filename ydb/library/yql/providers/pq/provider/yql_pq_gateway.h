#pragma once

#include "yql_pq_topic_client.h"

#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/pq/cm_client/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/datastreams/datastreams.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <library/cpp/threading/future/core/future.h>

#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>

namespace NKikimr {
namespace NMiniKQL {
class IFunctionRegistry;
} // namespace NMiniKQL
}

namespace NYql {

<<<<<<< HEAD:ydb/library/yql/providers/pq/provider/yql_pq_gateway.h
class TPqGatewayConfig;
using TPqGatewayConfigPtr = std::shared_ptr<TPqGatewayConfig>;

struct IPqGateway : public TThrRefBase {
=======
class IPqStaticGateway : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IPqStaticGateway>;

    virtual ITopicClient::TPtr GetTopicClient(const NYdb::TDriver& driver, const NYdb::NTopic::TTopicClientSettings& settings) = 0;

    virtual IFederatedTopicClient::TPtr GetFederatedTopicClient(const NYdb::TDriver& driver, const NYdb::NFederatedTopic::TFederatedTopicClientSettings& settings) = 0;

    virtual NYdb::NTopic::TTopicClientSettings GetTopicClientSettings() const = 0;

    virtual NYdb::NFederatedTopic::TFederatedTopicClientSettings GetFederatedTopicClientSettings() const = 0;
};

class TPqClusterConfig;
class TPqGatewayConfig;
using TPqGatewayConfigPtr = std::shared_ptr<TPqGatewayConfig>;

// May be dynamically changed by methods:
// - OpenSession (thread safe)
// - CloseSession (thread safe)
// - UpdateClusterConfigs (not thread safe)
// - AddCluster (not thread safe)
class IPqGateway : public IPqStaticGateway {
public:
>>>>>>> 4f3f67de666 (YQ-5161 fixed race with PQ / Solomon gateway (#35636)):ydb/library/yql/providers/pq/gateway/abstract/yql_pq_gateway.h
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

<<<<<<< HEAD:ydb/library/yql/providers/pq/provider/yql_pq_gateway.h
    virtual ITopicClient::TPtr GetTopicClient(const NYdb::TDriver& driver, const NYdb::NTopic::TTopicClientSettings& settings) = 0;
    virtual IFederatedTopicClient::TPtr GetFederatedTopicClient(const NYdb::TDriver& driver, const NYdb::NFederatedTopic::TFederatedTopicClientSettings& settings) = 0;

=======
>>>>>>> 4f3f67de666 (YQ-5161 fixed race with PQ / Solomon gateway (#35636)):ydb/library/yql/providers/pq/gateway/abstract/yql_pq_gateway.h
    virtual void UpdateClusterConfigs(
        const TString& clusterName,
        const TString& endpoint,
        const TString& database,
        bool secure) = 0;

    // Not thread safe
    virtual void UpdateClusterConfigs(const TPqGatewayConfigPtr& config) = 0;

    // Not thread safe
    virtual void AddCluster(const NYql::TPqClusterConfig& cluster) = 0;
};

struct IPqGatewayFactory : public TThrRefBase {
    using TPtr = TIntrusivePtr<IPqGatewayFactory>;

    virtual ~IPqGatewayFactory() = default;
    virtual IPqGateway::TPtr CreatePqGateway() = 0;
};

} // namespace NYql
