#include "coroutine.h"

namespace NYT::NConcurrency::NDetail {

////////////////////////////////////////////////////////////////////////////////

TCoroutineBase::~TCoroutineBase()
{
    if (State_ == ECoroState::Running) {
        State_ = ECoroState::Abandoned;
        Resume();
    }

    std::destroy_at(std::launder(&CoroutineContext));
}

void TCoroutineBase::Suspend()
{
    std::launder(&CoroutineContext)->SwitchTo(&CallerContext_);

    if (State_ == ECoroState::Abandoned) {
        throw TCoroutineAbandonedException{};
    }
}

void TCoroutineBase::Resume()
{
    CallerContext_.SwitchTo(std::launder(&CoroutineContext));

    if (CoroutineException_) {
        std::exception_ptr exception;
        std::swap(exception, CoroutineException_);
        std::rethrow_exception(std::move(exception));
    }
}

bool TCoroutineBase::IsCompleted() const noexcept
{
    return State_ == ECoroState::Completed;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency::NDetail
