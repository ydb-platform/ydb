#pragma once

#include <library/cpp/threading/future/core/future.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>

namespace NYql {

class IFmrJobFactory: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IFmrJobFactory>;

    virtual ~IFmrJobFactory() = default;

    virtual NThreading::TFuture<TTaskResult::TPtr> StartJob(TTask::TPtr task, std::shared_ptr<std::atomic<bool>> cancelFlag) = 0;
};

} // namspace NYql
