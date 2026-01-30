#pragma once

#include "app_context.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/algorithm.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/string/printf.h>

#include <atomic>

namespace NCloud::NBlockStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

inline TString MakeLoggingTag(const TString& testName)
{
    return Sprintf("[%s] ", testName.c_str());
}

////////////////////////////////////////////////////////////////////////////////

inline void StopTest(TTestContext& testContext)
{
    testContext.ShouldStop.store(true, std::memory_order_release);
    testContext.WaitCondVar.Signal();
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
T WaitForCompletion(
    const TString& request,
    const NThreading::TFuture<T>& future,
    const TVector<ui32>& successOnError)
{
    const auto& response = future.GetValue(TDuration::Max());
    if (HasError(response)) {
        const auto& error = response.GetError();
        if (Find(successOnError, error.GetCode()) == successOnError.end()) {
            throw yexception()
                << "Failed to execute " << request << " request: "
                << FormatError(error);
        }
    }
    return response;
}

}   // namespace NCloud::NBlockStore::NLoadTest
