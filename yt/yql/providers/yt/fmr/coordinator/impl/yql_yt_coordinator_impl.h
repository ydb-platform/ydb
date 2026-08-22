#pragma once

#include <yt/yql/providers/yt/fmr/coordinator/interface/yql_yt_coordinator.h>
#include <yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/interface/yql_yt_coordinator_service_interface.h>
#include <yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/impl/yql_yt_coordinator_service_impl.h>
#include <yt/yql/providers/yt/fmr/gc_service/impl/yql_yt_gc_service_impl.h>

#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>
#include <library/cpp/yson/node/node_io.h>

#include <util/generic/maybe.h>
#include <util/system/mutex.h>
#include <util/system/guard.h>
#include <util/generic/queue.h>

namespace NYql::NFmr {

struct TFmrCoordinatorSettings {
    NYT::TNode DefaultFmrOperationSpec;
    ui32 WorkersNum;
    TIntrusivePtr<IRandomProvider> RandomProvider;
    TIntrusivePtr<ITimeProvider> TimeProvider;

    TDuration IdempotencyKeyStoreTime = TDuration::Seconds(10);
    TDuration TimeToSleepBetweenClearKeyRequests = TDuration::Seconds(1);
    TDuration WorkerDeadlineLease = TDuration::Seconds(5);
    TDuration TimeToSleepBetweenCheckWorkerStatusRequests = TDuration::Seconds(1);
    TDuration SessionInactivityTimeout = TDuration::Minutes(30);
    TDuration HealthCheckInterval = TDuration::Seconds(1);
    bool RequireFmrJob = false;

    TFmrCoordinatorSettings();
};

TFmrCoordinatorSettings GetDefaultCoordinatorSettings(
    const TMaybe<NYT::TNode>& configOverride = Nothing(),
    const TMaybe<NYT::TNode>& operationSpecOverride = Nothing()
);

struct TPartIdInfo {
    TString TableId;
    TString PartId;
};

IFmrCoordinator::TPtr MakeFmrCoordinator(
    const TFmrCoordinatorSettings& settings = TFmrCoordinatorSettings(),
    IYtCoordinatorService::TPtr ytCoordinatorService = MakeYtCoordinatorService(),
    IFmrGcService::TPtr gcService = MakeGcService(nullptr)
);

} // namespace NYql::NFmr
