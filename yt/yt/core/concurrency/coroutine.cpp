#include "coroutine.h"

namespace NYT::NConcurrency::NDetail {

////////////////////////////////////////////////////////////////////////////////

TCoroutineBase::TCoroutineBase(const EExecutionStackKind stackKind)
    : CoroutineStack_(CreateExecutionStack(stackKind))
    , CoroutineContext_({
        this,
        TArrayRef(static_cast<char*>(CoroutineStack_->GetStack()), CoroutineStack_->GetSize())})
{ }

void TCoroutineBase::DoRun()
{
    try {
        Invoke();
    } catch (...) {
        CoroutineException_ = std::current_exception();
    }

    Completed_ = true;
    JumpToCaller();

    YT_ABORT();
}

void TCoroutineBase::JumpToCaller()
{
    CoroutineContext_.SwitchTo(&CallerContext_);
}

void TCoroutineBase::JumpToCoroutine()
{
    CallerContext_.SwitchTo(&CoroutineContext_);

    if (CoroutineException_) {
        std::exception_ptr exception;
        std::swap(exception, CoroutineException_);
        std::rethrow_exception(std::move(exception));
    }
}

bool TCoroutineBase::IsCompleted() const
{
    return Completed_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency::NDetail
