#pragma once
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr::NColumnShard {

// Group counters=tablets/type=<type> of a node service that only ColumnShards use.
// It is not exported until the first ColumnShard on the node calls ExportNodeServiceCounters.
TIntrusivePtr<::NMonitoring::TDynamicCounters> GetNodeServiceCounters(
    const TIntrusivePtr<::NMonitoring::TDynamicCounters>& root, const TString& type);

void ExportNodeServiceCounters(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& root);

}   // namespace NKikimr::NColumnShard
