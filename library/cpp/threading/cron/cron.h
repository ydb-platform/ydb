#pragma once

#include <util/generic/ptr.h>
#include <util/generic/function.h>
#include <util/datetime/base.h>

namespace NCron {
    struct IHandle {
        virtual ~IHandle();
    };

    using TJob = std::function<void()>;
    using IHandlePtr = TAutoPtr<IHandle>;

    IHandlePtr StartPeriodicJob(TJob job);
    IHandlePtr StartPeriodicJob(TJob job, TDuration interval);
    IHandlePtr StartPeriodicJob(TJob job, TDuration interval, const TString& threadName);
}
