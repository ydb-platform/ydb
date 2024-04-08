#include "coroutine.h"

namespace NYT::NConcurrency::NDetail {

////////////////////////////////////////////////////////////////////////////////

TCoroutineBase::~TCoroutineBase()
{
    std::destroy_at(&CoroutineContext);
}

void TCoroutineBase::Suspend() noexcept
{
    CoroutineContext.SwitchTo(&CallerContext_);
}

void TCoroutineBase::Resume()
{
    CallerContext_.SwitchTo(&CoroutineContext);

    if (CoroutineException_) {
        std::exception_ptr exception;
        std::swap(exception, CoroutineException_);
        std::rethrow_exception(std::move(exception));
    }
}

bool TCoroutineBase::IsCompleted() const noexcept
{
    return Completed_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency::NDetail
