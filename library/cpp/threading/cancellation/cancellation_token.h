#pragma once

#include "operation_cancelled_exception.h"

#include <library/cpp/threading/future/future.h>

#include <util/generic/ptr.h>
#include <util/generic/singleton.h>

namespace NThreading {

class TCancellationTokenSource;

//! A cancellation token could be passed to an async or long running operation to perform a cooperative operation cancel
class TCancellationToken {
private:
    TFuture<void> Future_;
    TInstant Deadline_ = TInstant::Max();

public:
    TCancellationToken() = delete;
    TCancellationToken(const TCancellationToken&) noexcept = default;
    TCancellationToken(TCancellationToken&&) noexcept = default;
    TCancellationToken& operator = (const TCancellationToken&) noexcept = default;
    TCancellationToken& operator = (TCancellationToken&&) noexcept = default;

    //! Shows whether a cancellation has been requested
    bool IsCancellationRequested() const {
        return Future_.HasValue();
    }

    //! Shows whether a cancellation has been requested
    bool IsDeadlineReached() const {
        return TInstant::Now() > Deadline_;
    }

    //! Throws the TOperationCancelledException if a cancellation has been requested
    void ThrowIfCancellationRequested() const {
        if (IsCancellationRequested()) {
            ythrow TOperationCancelledException();
        }
    }

    //! Throws the TOperationCancelledException if the Deadline_ has been reached
    void ThrowIfDeadlineReached() const {
        if (IsDeadlineReached()) {
            ythrow TOperationCancelledException();
        }
    }

    //! Throws the TOperationCancelledException if a cancellation has been requested
    // or reached the Deadline_
    void ThrowIfTokenCancelled() const {
        ThrowIfCancellationRequested();
        ThrowIfDeadlineReached();
    }

    //! Waits for a cancellation
    bool Wait(TDuration duration) const {
        return Future_.Wait(duration);
    }

    bool Wait(TInstant deadline) const {
        return Future_.Wait(deadline);
    }

    void Wait() const {
        return Future_.Wait();
    }

    //! Returns a future that could be used for waiting for a cancellation
    TFuture<void> const& Future() const noexcept {
        return Future_;
    }

    //! The default cancellation token that cannot be cancelled
    static TCancellationToken const& Default() {
        return *SingletonWithPriority<TCancellationToken, 0>(NewPromise());
    }

    void SetDeadline(TInstant deadline) {
        Deadline_ = deadline;
    }

    TInstant GetDeadline() const {
        return Deadline_;
    }

private:
    TCancellationToken(TFuture<void> future)
        : Future_(std::move(future))
    {
    }

private:
    friend class TCancellationTokenSource;

    Y_DECLARE_SINGLETON_FRIEND();
};

//! A cancellation token source produces cancellation tokens to be passed to cancellable operations
class TCancellationTokenSource {
private:
    TPromise<void> Promise;

public:
    TCancellationTokenSource()
        : Promise(NewPromise())
    {
    }

    TCancellationTokenSource(TCancellationTokenSource const&) = delete;
    TCancellationTokenSource(TCancellationTokenSource&&) = delete;
    TCancellationTokenSource& operator=(TCancellationTokenSource const&) = delete;
    TCancellationTokenSource& operator=(TCancellationTokenSource&&) = delete;

    //! Shows whether a cancellation has been requested
    bool IsCancellationRequested() const noexcept {
        return Promise.HasValue();
    }

    //! Produces a cancellation token
    TCancellationToken Token() const {
        return TCancellationToken(Promise.GetFuture());
    }

    //! Propagates a cancel request to all produced tokens
    void Cancel() noexcept {
        Promise.TrySetValue();
    }
};

}
