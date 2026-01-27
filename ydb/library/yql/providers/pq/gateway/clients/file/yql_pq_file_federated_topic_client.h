#pragma once

#include "yql_pq_file_topic_defs.h"

#include <ydb/library/yql/providers/pq/gateway/abstract/yql_pq_federated_topic_client.h>

namespace NYql {

IFederatedTopicClient::TPtr CreateFileFederatedTopicClient(const THashMap<TClusterNPath, TDummyTopic>& topics, const NYdb::NTopic::TFederatedTopicClientSettings& settings, const TFileTopicClientSettings& fileClientSettings);

} // namespace NYql
