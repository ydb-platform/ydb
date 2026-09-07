#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/public.h>

#include <util/datetime/base.h>

#include <memory>

namespace NMonitoring {

////////////////////////////////////////////////////////////////////////////////

class IMetricSupplier;

}   // namespace NMonitoring

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

using IUserMetricsSupplierPtr = std::shared_ptr<NMonitoring::IMetricSupplier>;

}   // namespace NCloud::NStorage

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration RequestTimeWarnThreshold = TDuration::Seconds(10);
constexpr TDuration UpdateLeakyBucketCountersInterval = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

class TDiagnosticsConfig;
using TDiagnosticsConfigPtr = std::shared_ptr<TDiagnosticsConfig>;

struct IVolumeInfo;
using IVolumeInfoPtr = std::shared_ptr<IVolumeInfo>;

struct IVolumeStats;
using IVolumeStatsPtr = std::shared_ptr<IVolumeStats>;

struct IRequestStats;
using IRequestStatsPtr = std::shared_ptr<IRequestStats>;

using NYdb::NBS::NBlockStore::IDumpable;
using NYdb::NBS::NBlockStore::IDumpablePtr;

struct IServerStats;
using IServerStatsPtr = std::shared_ptr<IServerStats>;

struct IStatsAggregator;
using IStatsAggregatorPtr = std::shared_ptr<IStatsAggregator>;

struct IClientPercentileCalculator;
using IClientPercentileCalculatorPtr =
    std::shared_ptr<IClientPercentileCalculator>;

using IMetricConsumerPtr = std::shared_ptr<NMonitoring::IMetricConsumer>;

struct IIncompleteRequestProvider;
using IIncompleteRequestProviderPtr =
    std::shared_ptr<IIncompleteRequestProvider>;

struct IBlockDigestGenerator;
using IBlockDigestGeneratorPtr = std::shared_ptr<IBlockDigestGenerator>;

struct IVolumeBalancerSwitch;
using IVolumeBalancerSwitchPtr = std::shared_ptr<IVolumeBalancerSwitch>;

}   // namespace NCloud::NBlockStore
