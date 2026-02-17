#pragma once

#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/public.h>

#include <util/datetime/base.h>

#include <memory>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration RequestTimeWarnThreshold = TDuration::Seconds(10);
constexpr TDuration UpdateLeakyBucketCountersInterval = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

struct IVHostStats;
using IVHostStatsPtr = std::shared_ptr<IVHostStats>;

class TVHostStatsSimple;
using TVHostStatsSimplePtr = std::shared_ptr<TVHostStatsSimple>;

class TDiagnosticsConfig;
using TDiagnosticsConfigPtr = std::shared_ptr<TDiagnosticsConfig>;

struct IVolumeInfo;
using IVolumeInfoPtr = std::shared_ptr<IVolumeInfo>;

struct IVolumeStats;
using IVolumeStatsPtr = std::shared_ptr<IVolumeStats>;

struct IRequestStats;
using IRequestStatsPtr = std::shared_ptr<IRequestStats>;

struct IDumpable;
using IDumpablePtr = std::shared_ptr<IDumpable>;

struct IServerStats;
using IServerStatsPtr = std::shared_ptr<IServerStats>;

struct IStatsAggregator;
using IStatsAggregatorPtr = std::shared_ptr<IStatsAggregator>;

struct IClientPercentileCalculator;
using IClientPercentileCalculatorPtr = std::shared_ptr<IClientPercentileCalculator>;

struct IIncompleteRequestProvider;
using IIncompleteRequestProviderPtr = std::shared_ptr<IIncompleteRequestProvider>;

struct IProfileLog;
using IProfileLogPtr = std::shared_ptr<IProfileLog>;

struct IBlockDigestGenerator;
using IBlockDigestGeneratorPtr = std::shared_ptr<IBlockDigestGenerator>;

struct IVolumeBalancerSwitch;
using IVolumeBalancerSwitchPtr = std::shared_ptr<IVolumeBalancerSwitch>;

}   // namespace NYdb::NBS::NBlockStore
