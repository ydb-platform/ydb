#pragma once

#include <library/cpp/threading/future/fwd.h>
#include <library/cpp/time_provider/monotonic.h>

namespace NYql::NUdf {

// Internal hook for statically linked UDFs that need to park a query evaluation.
// The runtime binds it only while the computation is executing.
class IAsyncUdfWaiter {
public:
    virtual bool Wait(const NThreading::TFuture<void>& ready, NMonotonic::TMonotonic deadline) = 0;

    static inline thread_local IAsyncUdfWaiter* Current = nullptr;

protected:
    ~IAsyncUdfWaiter() = default;
};

} // namespace NYql::NUdf
