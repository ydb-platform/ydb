#pragma once

#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/histogram_counter_options.h>
#include <ydb/core/nbs/cloud/storage/core/libs/user_stats/counter/user_counter.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/metrics/metric_registry.h>

#include <util/generic/hash_multi_map.h>

namespace NYdb::NBS::NBlockStore::NUserCounter {

using IUserCounterSupplier =
    NYdb::NBS::NStorage::NUserStats::IUserCounterSupplier;

////////////////////////////////////////////////////////////////////////////////

void RegisterServiceVolume(
    IUserCounterSupplier& dsc,
    const TString& cloudId,
    const TString& folderId,
    const TString& diskId,
    EHistogramCounterOptions histogramCounterOptions,
    NMonitoring::TDynamicCounterPtr src);

void UnregisterServiceVolume(
    IUserCounterSupplier& dsc,
    const TString& cloudId,
    const TString& folderId,
    const TString& diskId);

void RegisterServerVolumeInstance(
    IUserCounterSupplier& dsc,
    const TString& cloudId,
    const TString& folderId,
    const TString& diskId,
    const TString& instanceId,
    const bool reportZeroBlocksMetrics,
    EHistogramCounterOptions histogramCounterOptions,
    NMonitoring::TDynamicCounterPtr src);

void UnregisterServerVolumeInstance(
    IUserCounterSupplier& dsc,
    const TString& cloudId,
    const TString& folderId,
    const TString& diskId,
    const TString& instanceId);

}   // namespace NYdb::NBS::NBlockStore::NUserCounter
