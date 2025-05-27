#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>


namespace NYql {
namespace NSensorComponent {
    static const TString kExecutor = "executor";
    static const TString kWorkerServer = "worker_server";
    static const TString kDataServer = "data_server";
    static const TString kInspectorClient = "inspector_client";
    static const TString kDq = "dq";
} // namspace NSensorComponent


using TSensorsGroup = ::NMonitoring::TDynamicCounters;
using TSensorsGroupPtr = TIntrusivePtr<TSensorsGroup>;

using TSensorCounter = NMonitoring::TCounterForPtr;
using TSensorCounterPtr = TIntrusivePtr<TSensorCounter>;


TSensorsGroupPtr GetSensorsRootGroup();

inline TSensorsGroupPtr GetSensorsGroupFor(const TString& compName) {
    static TString compLabel("component");
    return GetSensorsRootGroup()->GetSubgroup(compLabel, compName);
}

} // namspace NYql
