#pragma once

#include "yql_pq_file_topic_defs.h"

#include <ydb/library/yql/providers/pq/gateway/abstract/yql_pq_topic_client.h>

namespace NYql {

ITopicClient::TPtr CreateFileTopicClient(const THashMap<TClusterNPath, TDummyTopic>& topics, const TFileTopicClientSettings& settings);

} // namespace NYql
