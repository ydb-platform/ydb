#pragma once

#include "local_topic_client_settings.h"

#include <ydb/library/yql/providers/pq/gateway/abstract/yql_pq_federated_topic_client.h>

namespace NKikimr::NKqp {

NYql::IFederatedTopicClient::TPtr CreateLocalFederatedTopicClient(const TLocalTopicClientSettings& localSettings, const NYdb::NFederatedTopic::TFederatedTopicClientSettings& clientSettings);

} // namespace NKikimr::NKqp
