#include <library/cpp/random_provider/random_provider.h>
#include <util/system/mutex.h>
#include <util/system/guard.h>
#include <util/generic/queue.h>
#include <yt/yql/providers/yt/fmr/coordinator/interface/yql_yt_coordinator.h>

namespace NYql::NFmr {

struct TFmrCoordinatorSettings {
    ui32 WorkersNum; // Not supported yet
    TIntrusivePtr<IRandomProvider> RandomProvider;
    TDuration IdempotencyKeyStoreTime = TDuration::Seconds(10);
    TDuration TimeToSleepBetweenClearKeyRequests = TDuration::Seconds(1);
};

IFmrCoordinator::TPtr MakeFmrCoordinator(const TFmrCoordinatorSettings& settings = {.WorkersNum = 1, .RandomProvider = CreateDeterministicRandomProvider(2)});

} // namespace NYql::NFmr
