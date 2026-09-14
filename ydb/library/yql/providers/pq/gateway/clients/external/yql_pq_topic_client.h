#pragma once

#include <ydb/library/yql/providers/pq/gateway/abstract/yql_pq_topic_client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

namespace NYql {

ITopicClient::TPtr CreateExternalTopicClient(const NYdb::TDriver& driver, const NYdb::NTopic::TTopicClientSettings& settings);

} // namespace NYql
