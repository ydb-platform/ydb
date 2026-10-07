#pragma once

#include <yql/essentials/core/yql_execution.h>

#include <yt/cpp/mapreduce/interface/fwd.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/system/thread.h>

#include <functional>

namespace NYql {

class IOperationTracker: public TThrRefBase {
public:
    using TPtr = ::TIntrusivePtr<IOperationTracker>;

    virtual void Stop() = 0;

    virtual NThreading::TFuture<void> MakeOperationWaiter(const NYT::IOperationPtr& operation, TMaybe<ui32> publicId,
        const TString& ytServer, const TString& ytClusterName, const TOperationProgressWriter& progressWriter,
        const TStatWriter& statWriter, std::function<void(NYT::TOperationId)> onOperationStarted, bool isExternalProgress) = 0;
};

} // NYql
