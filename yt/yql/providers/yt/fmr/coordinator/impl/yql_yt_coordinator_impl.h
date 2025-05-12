#pragma once

#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/yson/node/node_io.h>
#include <util/system/mutex.h>
#include <util/system/guard.h>
#include <util/generic/queue.h>
#include <yt/yql/providers/yt/fmr/coordinator/interface/yql_yt_coordinator.h>

namespace NYql::NFmr {

struct TFmrCoordinatorSettings {
    NYT::TNode DefaultFmrOperationSpec;
    ui32 WorkersNum;
    TIntrusivePtr<IRandomProvider> RandomProvider;
    TDuration IdempotencyKeyStoreTime = TDuration::Seconds(10);
    TDuration TimeToSleepBetweenClearKeyRequests = TDuration::Seconds(1);

    TFmrCoordinatorSettings();
};

IFmrCoordinator::TPtr MakeFmrCoordinator(const TFmrCoordinatorSettings& settings = TFmrCoordinatorSettings());

} // namespace NYql::NFmr
