#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////
// Current invoker API
//
// Note that this invoker is passed by raw pointer to avoid contention at its
// ref counter. The caller of #SetCurrentInvoker must ensure that the invoker
// remains alive.

IInvoker* GetCurrentInvoker();
void SetCurrentInvoker(IInvoker* invoker);

//! Swaps the current active invoker with a provided one.
class TCurrentInvokerGuard
    : public NConcurrency::TContextSwitchGuard
{
public:
    explicit TCurrentInvokerGuard(IInvoker* invoker);
    ~TCurrentInvokerGuard();

private:
    void Restore();

    bool Active_;
    IInvoker* SavedInvoker_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
