#include <library/cpp/random_provider/random_provider.h>
#include <yt/yql/providers/yt/fmr/coordinator/interface/yql_yt_coordinator.h>
#include <yt/yql/providers/yt/fmr/job_factory/interface/yql_yt_job_factory.h>
#include <yql/essentials/utils/runnable.h>

namespace NYql::NFmr {

struct TFmrWorkerSettings {
    ui32 WorkerId;
    TIntrusivePtr<IRandomProvider> RandomProvider = CreateDefaultRandomProvider();
    TDuration TimeToSleepBetweenRequests = TDuration::Seconds(1);
};

using IFmrWorker = IRunnable;

IFmrWorker::TPtr MakeFmrWorker(IFmrCoordinator::TPtr coordinator, IFmrJobFactory::TPtr jobFactory, const TFmrWorkerSettings& settings);

} // namespace NYql::NFmr
