#pragma once
#include <ydb/library/yql/providers/pq/provider/yql_pq_gateway.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/system/mutex.h>

namespace NYql {

struct TDummyTopic {
    TDummyTopic(const TString& cluster, const TString& topicName, const TMaybe<TString>& path = {}, size_t partitionCount = 1)
        : Cluster(cluster)
        , TopicName(topicName)
        , Path(path)
        , PartitionsCount(partitionCount)
    {
    }

    TDummyTopic& SetPartitionsCount(size_t count) {
        PartitionsCount = count;
        return *this;
    }

    TString Cluster;
    TString TopicName;
    TMaybe<TString> Path;
    size_t PartitionsCount;
    bool CancelOnFileFinish = false;
};

// Dummy Pq gateway for tests.
class TDummyPqGateway : public IPqGateway {
public:
    TDummyPqGateway& AddDummyTopic(const TDummyTopic& topic);
    ~TDummyPqGateway() {}

    NThreading::TFuture<void> OpenSession(const TString& sessionId, const TString& username) override;
    NThreading::TFuture<void> CloseSession(const TString& sessionId) override;

    ::NPq::NConfigurationManager::TAsyncDescribePathResult DescribePath(
        const TString& sessionId,
        const TString& cluster,
        const TString& database,
        const TString& path,
        const TString& token) override;

    IPqGateway::TAsyncDescribeFederatedTopicResult DescribeFederatedTopic(
        const TString& sessionId,
        const TString& cluster,
        const TString& database,
        const TString& path,
        const TString& token) override;

    NThreading::TFuture<TListStreams> ListStreams(
        const TString& sessionId,
        const TString& cluster,
        const TString& database,
        const TString& token,
        ui32 limit,
        const TString& exclusiveStartStreamName = {}) override;

    void UpdateClusterConfigs(
        const TString& clusterName,
        const TString& endpoint,
        const TString& database,
        bool secure) override;

    void UpdateClusterConfigs(const TPqGatewayConfigPtr& config) override;

    void AddCluster(const NYql::TPqClusterConfig& /*cluster*/) override {}

    ITopicClient::TPtr GetTopicClient(const NYdb::TDriver& driver, const NYdb::NTopic::TTopicClientSettings& settings) override;
    IFederatedTopicClient::TPtr GetFederatedTopicClient(const NYdb::TDriver& driver, const NYdb::NFederatedTopic::TFederatedTopicClientSettings& settings) override;
    NYdb::NTopic::TTopicClientSettings GetTopicClientSettings() const override;
    NYdb::NFederatedTopic::TFederatedTopicClientSettings GetFederatedTopicClientSettings() const override;

    using TClusterNPath = std::pair<TString, TString>;
private:
    mutable TMutex Mutex;
    THashMap<TClusterNPath, TDummyTopic> Topics;

    THashSet<TString> OpenedSessions;
};

IPqGateway::TPtr CreatePqFileGateway();

IPqGatewayFactory::TPtr CreatePqFileGatewayFactory(const TDummyPqGateway::TPtr pqFileGateway);

} // namespace NYql
