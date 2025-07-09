#pragma once

#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/yson/node/node_io.h>
#include <util/system/mutex.h>
#include <util/system/guard.h>
#include <util/generic/queue.h>
#include <yt/yql/providers/yt/fmr/coordinator/interface/yql_yt_coordinator.h>
#include <yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/interface/yql_yt_coordinator_service_interface.h>
#include <yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/impl/yql_yt_coordinator_service_impl.h>
#include <yt/yql/providers/yt/fmr/gc_service/impl/yql_yt_gc_service_impl.h>

namespace NYql::NFmr {

struct TFmrCoordinatorSettings {
    NYT::TNode DefaultFmrOperationSpec;
    ui32 WorkersNum;
    TIntrusivePtr<IRandomProvider> RandomProvider;
    TDuration IdempotencyKeyStoreTime = TDuration::Seconds(10);
    TDuration TimeToSleepBetweenClearKeyRequests = TDuration::Seconds(1);
    TDuration WorkerDeadlineLease = TDuration::Seconds(5); // Number of seconds to wait for worker ping,
    TDuration TimeToSleepBetweenCheckWorkerStatusRequests = TDuration::Seconds(1);

    TFmrCoordinatorSettings();
};

IFmrCoordinator::TPtr MakeFmrCoordinator(
    const TFmrCoordinatorSettings& settings = TFmrCoordinatorSettings(),
    IYtCoordinatorService::TPtr ytCoordinatorService = MakeYtCoordinatorService(),
    IFmrGcService::TPtr gcService = MakeGcService(nullptr)
);

} // namespace NYql::NFmr
