#pragma once

#include "local_topic_client_settings.h"

#include <ydb/library/yql/providers/pq/gateway/clients/local/yql_pq_local_topic_client_factory.h>

namespace NKikimr::NKqp {

NYql::IPqLocalClientFactory::TPtr CreateLocalTopicClientFactory(const TLocalTopicClientSettings& settings);

} // namespace NKikimr::NKqp

