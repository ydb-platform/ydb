#pragma once

#include "local_topic_client_settings.h"

#include <ydb/library/yql/providers/pq/gateway/abstract/yql_pq_deferred_publish_client.h>

namespace NKikimr::NKqp {

// Note: for local client where is no automaticall ack waiting on publish
NYql::IDeferredPublishClient::TPtr CreateLocalDeferredPublishClient(const TLocalTopicClientSettings& localSettings, const NYdb::TCommonClientSettings& clientSettings);

} // namespace NKikimr::NKqp
