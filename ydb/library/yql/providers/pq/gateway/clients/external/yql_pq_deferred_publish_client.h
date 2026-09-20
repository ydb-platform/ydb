#pragma once

#include <ydb/library/yql/providers/pq/gateway/abstract/yql_pq_deferred_publish_client.h>

namespace NYql {

IDeferredPublishClient::TPtr CreateExternalDeferredPublishClient(const NYdb::TDriver& driver, const NYdb::TCommonClientSettings& settings);

} // namespace NYql
