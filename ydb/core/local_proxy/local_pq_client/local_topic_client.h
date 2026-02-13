#pragma once

#include "local_topic_client_settings.h"

#include <ydb/library/yql/providers/pq/gateway/abstract/yql_pq_topic_client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

namespace NKikimr::NKqp {

NYql::ITopicClient::TPtr CreateLocalTopicClient(const TLocalTopicClientSettings& localSettings, const NYdb::NTopic::TTopicClientSettings& clientSettings);

} // namespace NKikimr::NKqp
