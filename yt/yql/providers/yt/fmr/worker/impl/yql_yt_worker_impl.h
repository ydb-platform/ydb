#pragma once

#include <yt/yql/providers/yt/fmr/coordinator/interface/yql_yt_coordinator.h>
#include <yt/yql/providers/yt/fmr/job_factory/impl/yql_yt_job_factory_impl.h>
#include <yt/yql/providers/yt/fmr/job_preparer/interface/yql_yt_job_preparer_interface.h>
#include <yt/yql/providers/yt/fmr/worker/interface/yql_yt_fmr_worker.h>

#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/yson/node/node.h>

#include <util/generic/maybe.h>

namespace NYql::NFmr {

enum EFmrResourceType {
    DistributedCacheFile, // udfs and local files uploaded to distributed cache.
    YtResource, // cypress table or file.
    FmrResource // fmr table which we need to download to file.
};

struct TFmrWorkerSettings {
    ui32 WorkerId;
    TIntrusivePtr<IRandomProvider> RandomProvider = CreateDefaultRandomProvider();
    TDuration TimeToSleepBetweenRequests = TDuration::MilliSeconds(100);
    ui64 MemoryLimitBytes = 0;
    TFmrJobFactorySettings JobFactorySettings;
};

TFmrWorkerSettings GetDefaultWorkerSettings(const TMaybe<NYT::TNode>& configOverride = Nothing());

IFmrWorker::TPtr MakeFmrWorker(
    IFmrCoordinator::TPtr coordinator,
    IFmrJobFactory::TPtr jobFactory,
    IFmrJobPreparer::TPtr jobPreparer,
    const TFmrWorkerSettings& settings
);

} // namespace NYql::NFmr
