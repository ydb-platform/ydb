#include "counters.h"

#include <ydb/core/base/counters.h>

namespace NKikimr::NSecurity {

NMonitoring::TDynamicCounterPtr GetCountersForTicketParser(NMonitoring::TDynamicCounterPtr root) {
    TIntrusivePtr<NMonitoring::TDynamicCounters> authCounters = GetServiceCounters(root, "auth");
    return authCounters->GetSubgroup("subsystem", "TicketParser");
}

NMonitoring::TDynamicCounterPtr GetCountersForExternalIdpProvider(NMonitoring::TDynamicCounterPtr root) {
    auto ticketParser = GetCountersForTicketParser(root);
    return ticketParser->GetSubgroup("component", "ExternalIdpProvider");
}

} // namespace NKikimr::NSecurity
