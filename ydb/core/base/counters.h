#pragma once
#include "defs.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/hash_set.h>

namespace NKikimr {
    constexpr char DATABASE_LABEL[] = "database";
    constexpr char SLOT_LABEL[] = "slot";
    constexpr char HOST_LABEL[] = "host";

    // Get counters group for specified service. Skip tenant and slot labels.
    TIntrusivePtr<::NMonitoring::TDynamicCounters> GetServiceCounters(TIntrusivePtr<::NMonitoring::TDynamicCounters> root,
                                                                    const TString &service, bool skipAddedLabels = true);
    // Get parent node for subsvc/svc if any. root->svc->subvc => svc, root->svc => root.
    TIntrusivePtr<::NMonitoring::TDynamicCounters> GetServiceCountersRoot(TIntrusivePtr<::NMonitoring::TDynamicCounters> root,
                                                                    const TString &service);
    // Extract subservice name if any. aba|caba => aba, caba.
    std::pair<TString, TString> ExtractSubServiceName(const TString &service);
    // Get list of services which use top-level database labels for own sensors.
    const THashSet<TString> &GetDatabaseSensorServices();
    // Get list of services which use top-level database attribute labels for own sensors.
    const THashSet<TString> &GetDatabaseAttributeSensorServices();
    const THashSet<TString> &GetDatabaseAttributeLabels();
    // Drop all extra labels.
    void ReplaceSubgroup(TIntrusivePtr<::NMonitoring::TDynamicCounters> root, const TString &service);
} // namespace NKikimr
