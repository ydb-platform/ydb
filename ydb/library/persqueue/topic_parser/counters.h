#pragma once
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <ydb/core/base/counters.h>
#include "type_definitions.h"
#include "topic_parser.h"

namespace NPersQueue {

TVector<NPersQueue::TPQLabelsInfo> GetLabels(const TTopicCounterNames& topic);
TVector<NPersQueue::TPQLabelsInfo> GetLabelsForCustomCluster(const TTopicCounterNames& topic, TString cluster);
TVector<std::pair<TString, TString>> GetSubgroupsForTopic(const TTopicCounterNames& topic, const TString& cloudId,
                                                      const TString& dbId, const TString& dbPath,
                                                      const TString& folderId);
::NMonitoring::TDynamicCounterPtr GetCounters(::NMonitoring::TDynamicCounterPtr counters,
                                            const TString& subsystem,
                                            const TTopicCounterNames& topic);
::NMonitoring::TDynamicCounterPtr GetCountersForTopic(::NMonitoring::TDynamicCounterPtr counters, bool isServerless);

} // namespace NPersQueue
