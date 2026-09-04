#pragma once

#include <ydb/core/fq/libs/ydb/table_client.h>

namespace NFq {

IYdbTableClient::TPtr CreateLocalTableClient(ui64 maxActiveSessions);

} // namespace NFq
