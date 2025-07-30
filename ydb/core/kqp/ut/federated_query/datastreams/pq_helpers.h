#pragma once

#include <util/system/mutex.h>

#include <ydb/library/yql/providers/pq/provider/yql_pq_gateway.h>

namespace NTestUtils {

struct TMockPqSession {
    NYdb::NTopic::TPartitionSession::TPtr Session;
};

struct TMockPqGatewaySettings {
    struct TTopic {
        std::function<NYdb::NTopic::TReadSessionEvent::TEvent(TMockPqSession)> EventGen;
    };

    struct TCluster {
        TString Database;
        ui64 ExpectedQueriesToCluster = 0;
    };

    std::unordered_map<TString, TCluster> Clusters;  // cluster name -> settings
    std::unordered_map<TString, std::unordered_map<TString, TTopic>> Topics;  // database name -> path -> settings
};

class TMockPqGateway : public NYql::IPqGateway {
public:
    explicit TMockPqGateway(const TMockPqGatewaySettings& settings);

    ~TMockPqGateway();

    NThreading::TFuture<void> OpenSession(const TString& sessionId, const TString& username) override;

    NThreading::TFuture<void> CloseSession(const TString& sessionId) override;

    NPq::NConfigurationManager::TAsyncDescribePathResult DescribePath(const TString& sessionId, const TString& cluster, const TString& database, const TString& path, const TString& token) override;

    NThreading::TFuture<TListStreams> ListStreams(const TString& sessionId, const TString& cluster, const TString& database, const TString& token, ui32 limit, const TString& exclusiveStartStreamName = {}) override;

    TAsyncDescribeFederatedTopicResult DescribeFederatedTopic(const TString& sessionId, const TString& cluster, const TString& database, const TString& path, const TString& token) override;

    NYql::ITopicClient::TPtr GetTopicClient(const NYdb::TDriver& driver, const NYdb::NTopic::TTopicClientSettings& settings) override;

    NYql::IFederatedTopicClient::TPtr GetFederatedTopicClient(const NYdb::TDriver& driver, const NYdb::NFederatedTopic::TFederatedTopicClientSettings& settings) override;

    void UpdateClusterConfigs(const TString& clusterName, const TString& endpoint, const TString& database, bool secure) override;

    void UpdateClusterConfigs(const NYql::TPqGatewayConfigPtr& config) override;

    void AddCluster(const NYql::TPqClusterConfig& cluster) override;

    NYdb::NTopic::TTopicClientSettings GetTopicClientSettings() const override;

    NYdb::NFederatedTopic::TFederatedTopicClientSettings GetFederatedTopicClientSettings() const override;

private:
    void CheckTopicPath(const TString& cluster, const TString& path) const;

    std::unordered_map<TString, TMockPqGatewaySettings::TTopic> GetDatabaseTopics(const TString& database) const;

private:
    TMutex Mutex;
    std::unordered_set<TString> Sessions;
    TMockPqGatewaySettings Settings;
};

NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent MakePqMessage(ui64 offset, const TString& data, const TMockPqSession& meta);

} // namespace NTestUtils
