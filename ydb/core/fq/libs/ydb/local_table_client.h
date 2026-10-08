#pragma once

#include <ydb/core/fq/libs/ydb/table_client.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NFq {

IYdbTableClient::TPtr CreateLocalTableClient(ui64 maxActiveSessions, const NMonitoring::TDynamicCounterPtr& counters = {});

} // namespace NFq
