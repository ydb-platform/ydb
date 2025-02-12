#pragma once

#include <library/cpp/threading/future/async.h>
#include <util/thread/pool.h>
#include <yt/yql/providers/yt/fmr/job_factory/interface/yql_yt_job_factory.h>

namespace NYql {

struct TFmrJobFactorySettings {
    ui32 NumThreads = 3;
    std::function<ETaskStatus(TTask::TPtr, std::shared_ptr<std::atomic<bool>>)> Function;
};

IFmrJobFactory::TPtr MakeFmrJobFactory(const TFmrJobFactorySettings& settings);

} // namepspace NYql
