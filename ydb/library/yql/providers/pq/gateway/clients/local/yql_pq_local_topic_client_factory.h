#pragma once

#include <ydb/library/yql/providers/pq/gateway/abstract/yql_pq_federated_topic_client.h>
#include <ydb/library/yql/providers/pq/gateway/abstract/yql_pq_topic_client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <util/generic/ptr.h>

namespace NYql {

class IPqLocalClientFactory : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IPqLocalClientFactory>;

    virtual ITopicClient::TPtr CreateTopicClient(const NYdb::NTopic::TTopicClientSettings& clientSettings) = 0;

    virtual IFederatedTopicClient::TPtr CreateFederatedTopicClient(const NYdb::NFederatedTopic::TFederatedTopicClientSettings& settings) = 0;
};

} // namespace NYql
