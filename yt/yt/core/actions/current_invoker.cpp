#include "current_invoker.h"

#include "invoker_util.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

thread_local IInvoker* CurrentInvoker;

IInvoker* GetCurrentInvoker()
{
    if (CurrentInvoker) {
        return CurrentInvoker;
    }
    return GetSyncInvoker().Get();
}

void SetCurrentInvoker(IInvoker* invoker)
{
    CurrentInvoker = invoker;
}

TCurrentInvokerGuard::TCurrentInvokerGuard(IInvoker* invoker)
    : NConcurrency::TContextSwitchGuard(
        [this] () noexcept {
            Restore();
        },
        nullptr)
    , Active_(true)
    , SavedInvoker_(std::move(invoker))
{
    std::swap(CurrentInvoker, SavedInvoker_);
}

void TCurrentInvokerGuard::Restore()
{
    if (!Active_) {
        return;
    }
    Active_ = false;
    CurrentInvoker = std::move(SavedInvoker_);
}

TCurrentInvokerGuard::~TCurrentInvokerGuard()
{
    Restore();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
