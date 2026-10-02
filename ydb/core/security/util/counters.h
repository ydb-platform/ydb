#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NSecurity {

NMonitoring::TDynamicCounterPtr GetCountersForTicketParser(NMonitoring::TDynamicCounterPtr root);
NMonitoring::TDynamicCounterPtr GetCountersForExternalIdpProvider(NMonitoring::TDynamicCounterPtr root);

} // namespace NKikimr::NSecurity
