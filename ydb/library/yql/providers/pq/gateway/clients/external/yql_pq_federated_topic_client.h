#pragma once

#include <ydb/library/yql/providers/pq/gateway/abstract/yql_pq_federated_topic_client.h>

namespace NYql {

IFederatedTopicClient::TPtr CreateExternalFederatedTopicClient(const NYdb::TDriver& driver, const NYdb::NTopic::TFederatedTopicClientSettings& settings);

} // namespace NYql
