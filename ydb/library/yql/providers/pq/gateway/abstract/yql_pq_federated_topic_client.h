#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/federated_topic/federated_topic.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/ptr.h>

namespace NYql {

class IFederatedTopicClient : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IFederatedTopicClient>;

    virtual NThreading::TFuture<std::vector<NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo>> GetAllTopicClusters() = 0;

    virtual std::shared_ptr<NYdb::NTopic::IWriteSession> CreateWriteSession(const NYdb::NFederatedTopic::TFederatedWriteSessionSettings& settings) = 0;
};

} // namespace NYql 
