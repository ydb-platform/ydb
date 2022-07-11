#pragma once
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <ydb/core/base/counters.h>
#include "type_definitions.h"
#include "topic_parser.h"

namespace NPersQueue {

TVector<NPersQueue::TPQLabelsInfo> GetLabels(const TTopicConverterPtr& topic);
//TVector<NPersQueue::TPQLabelsInfo> GetLabelsForLegacyName(const TString& topic);
TVector<NPersQueue::TPQLabelsInfo> GetLabelsForCustomCluster(const TTopicConverterPtr& topic, TString cluster);
TVector<NPersQueue::TPQLabelsInfo> GetLabelsForStream(const TTopicConverterPtr& topic, const TString& cloudId,
                                                      const TString& dbId, const TString& folderId);

::NMonitoring::TDynamicCounterPtr GetCounters(::NMonitoring::TDynamicCounterPtr counters,
                                            const TString& subsystem,
                                            const TTopicConverterPtr& topic);
::NMonitoring::TDynamicCounterPtr GetCountersForStream(::NMonitoring::TDynamicCounterPtr counters);
} // namespace NPersQueue
