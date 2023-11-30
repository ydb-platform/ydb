#pragma once

#include <ydb/library/yql/providers/dq/worker_manager/interface/events.h>
#include <ydb/library/yql/providers/dq/worker_manager/interface/worker_info.h>
#include <ydb/library/yql/providers/dq/actors/events.h>
#include "coordination_helper.h"
#include <ydb/library/yql/providers/dq/actors/yt/resource_manager.h>
#include <ydb/library/yql/providers/common/metrics/metrics_registry.h>

#include <ydb/library/actors/core/events.h>

namespace NYql {

NActors::IActor* CreateGlobalWorkerManager(
        const ICoordinationHelper::TPtr& coordinator,
        const TVector<TResourceManagerOptions>& resourceUploaderOptions,
        IMetricsRegistryPtr metricsRegistry,
        const NProto::TDqConfig::TScheduler& schedulerConfig);

} // namespace NYql
