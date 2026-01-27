#pragma once

#include <ydb/library/yql/providers/pq/gateway/abstract/yql_pq_gateway.h>
#include <ydb/library/yql/providers/pq/gateway/clients/file/yql_pq_file_topic_defs.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/system/mutex.h>

namespace NYql {

// Dummy PQ gateway for tests.
class TDummyPqGateway final : public IPqGateway {
public:
    using TPtr = TIntrusivePtr<TDummyPqGateway>;

    explicit TDummyPqGateway(bool skipDatabasePrefix = false);

    TDummyPqGateway& AddDummyTopic(const TDummyTopic& topic);

    NThreading::TFuture<void> OpenSession(const TString& sessionId, const TString& username) final;

    NThreading::TFuture<void> CloseSession(const TString& sessionId) final;

    ::NPq::NConfigurationManager::TAsyncDescribePathResult DescribePath(
        const TString& sessionId,
        const TString& cluster,
        const TString& database,
        const TString& path,
        const TString& token) final;

    IPqGateway::TAsyncDescribeFederatedTopicResult DescribeFederatedTopic(
        const TString& sessionId,
        const TString& cluster,
        const TString& database,
        const TString& path,
        const TString& token) final;

    NThreading::TFuture<TListStreams> ListStreams(
        const TString& sessionId,
        const TString& cluster,
        const TString& database,
        const TString& token,
        ui32 limit,
        const TString& exclusiveStartStreamName) final;

    void UpdateClusterConfigs(
        const TString& clusterName,
        const TString& endpoint,
        const TString& database,
        bool secure) final;

    void UpdateClusterConfigs(const TPqGatewayConfigPtr& config) final;

    void AddCluster(const NYql::TPqClusterConfig& cluster) final;

    ITopicClient::TPtr GetTopicClient(const NYdb::TDriver& driver, const NYdb::NTopic::TTopicClientSettings& settings) final;

    IFederatedTopicClient::TPtr GetFederatedTopicClient(const NYdb::TDriver& driver, const NYdb::NFederatedTopic::TFederatedTopicClientSettings& settings) final;

    NYdb::NTopic::TTopicClientSettings GetTopicClientSettings() const final;

    NYdb::NFederatedTopic::TFederatedTopicClientSettings GetFederatedTopicClientSettings() const final;

private:
    TString SkipDatabasePrefix(const TString& path, const TString& database) const;

    mutable TMutex Mutex;
    const bool AllowSkipDatabasePrefix = false;
    THashMap<TClusterNPath, TDummyTopic> Topics;
    THashSet<TString> OpenedSessions;
};

TDummyPqGateway::TPtr CreatePqFileGateway(bool skipDatabasePrefix = false);

} // namespace NYql
