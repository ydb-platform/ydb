#ifndef CALLBACK_LIST_INL_H_
#error "Direct inclusion of this file is not allowed, include callback_list.h"
// For the sake of sane code completion.
#include "callback_list.h"
#endif
#undef CALLBACK_LIST_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TResult, class... TArgs>
void TCallbackList<TResult(TArgs...)>::Subscribe(const TCallback& callback)
{
    auto guard = WriterGuard(SpinLock_);
    Callbacks_.push_back(callback);
    Empty_.store(false);
}

template <class TResult, class... TArgs>
void TCallbackList<TResult(TArgs...)>::Unsubscribe(const TCallback& callback)
{
    auto guard = WriterGuard(SpinLock_);
    for (auto it = Callbacks_.begin(); it != Callbacks_.end(); ++it) {
        if (*it == callback) {
            Callbacks_.erase(it);
            break;
        }
    }
    Empty_.store(Callbacks_.empty(), std::memory_order::release);
}

template <class TResult, class... TArgs>
std::vector<TCallback<TResult(TArgs...)>> TCallbackList<TResult(TArgs...)>::ToVector() const
{
    if (IsEmpty()) [[likely]] {
        return {};
    }
    auto guard = ReaderGuard(SpinLock_);
    return std::vector<TCallback>(Callbacks_.begin(), Callbacks_.end());
}

template <class TResult, class... TArgs>
int TCallbackList<TResult(TArgs...)>::Size() const
{
    if (IsEmpty()) [[likely]] {
        return 0;
    }
    auto guard = ReaderGuard(SpinLock_);
    return Callbacks_.size();
}

template <class TResult, class... TArgs>
bool TCallbackList<TResult(TArgs...)>::IsEmpty() const
{
    return Empty_.load(std::memory_order::acquire);
}

template <class TResult, class... TArgs>
void TCallbackList<TResult(TArgs...)>::Clear()
{
    auto guard = WriterGuard(SpinLock_);
    Callbacks_.clear();
    Empty_.store(true, std::memory_order::release);
}

template <class TResult, class... TArgs>
template <class... TCallArgs>
void TCallbackList<TResult(TArgs...)>::Fire(TCallArgs&&... args) const
{
    if (IsEmpty()) [[likely]] {
        return;
    }

    TCallbackVector callbacks;
    {
        auto guard = ReaderGuard(SpinLock_);
        callbacks = Callbacks_;
    }

    for (const auto& callback : callbacks) {
        callback.Run(std::forward<TCallArgs>(args)...);
    }
}

template <class TResult, class... TArgs>
template <class... TCallArgs>
void TCallbackList<TResult(TArgs...)>::FireAndClear(TCallArgs&&... args)
{
    if (IsEmpty()) [[likely]] {
        return;
    }

    TCallbackVector callbacks;
    {
        auto guard = WriterGuard(SpinLock_);
        callbacks.swap(Callbacks_);
        Empty_.store(true, std::memory_order::release);
    }

    for (const auto& callback : callbacks) {
        callback(std::forward<TCallArgs>(args)...);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TResult, class... TArgs>
void TSimpleCallbackList<TResult(TArgs...)>::Subscribe(const TCallback& callback)
{
    Callbacks_.push_back(callback);
}

template <class TResult, class... TArgs>
void TSimpleCallbackList<TResult(TArgs...)>::Unsubscribe(const TCallback& callback)
{
    Callbacks_.erase(std::find(Callbacks_.begin(), Callbacks_.end(), callback));
}

template <class TResult, class... TArgs>
template <class... TCallArgs>
void TSimpleCallbackList<TResult(TArgs...)>::Fire(TCallArgs&&... args) const
{
    for (const auto& callback : Callbacks_) {
        callback(std::forward<TCallArgs>(args)...);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TResult, class... TArgs>
void TSingleShotCallbackList<TResult(TArgs...)>::Subscribe(const TCallback& callback)
{
    auto guard = WriterGuard(SpinLock_);
    if (Fired_.load(std::memory_order::acquire)) {
        guard.Release();
        std::apply(callback, Args_);
        return;
    }
    Callbacks_.push_back(callback);
}

template <class TResult, class... TArgs>
bool TSingleShotCallbackList<TResult(TArgs...)>::TrySubscribe(const TCallback& callback)
{
    auto guard = WriterGuard(SpinLock_);
    if (Fired_.load(std::memory_order::acquire)) {
        return false;
    }
    Callbacks_.push_back(callback);
    return true;
}

template <class TResult, class... TArgs>
void TSingleShotCallbackList<TResult(TArgs...)>::Unsubscribe(const TCallback& callback)
{
    auto guard = WriterGuard(SpinLock_);
    for (auto it = Callbacks_.begin(); it != Callbacks_.end(); ++it) {
        if (*it == callback) {
            Callbacks_.erase(it);
            break;
        }
    }
}

template <class TResult, class... TArgs>
std::vector<TCallback<TResult(TArgs...)>> TSingleShotCallbackList<TResult(TArgs...)>::ToVector() const
{
    auto guard = ReaderGuard(SpinLock_);
    return std::vector<TCallback>(Callbacks_.begin(), Callbacks_.end());
}

template <class TResult, class... TArgs>
template <class... TCallArgs>
bool TSingleShotCallbackList<TResult(TArgs...)>::Fire(TCallArgs&&... args)
{
    TCallbackVector callbacks;
    {
        auto guard = WriterGuard(SpinLock_);
        if (Fired_.load(std::memory_order::acquire)) {
            return false;
        }
        Args_ = std::tuple(std::forward<TCallArgs>(args)...);
        callbacks.swap(Callbacks_);
        Fired_.store(true, std::memory_order::release);
    }

    for (const auto& callback : callbacks) {
        std::apply(callback, Args_);
    }

    return true;
}

template <class TResult, class... TArgs>
bool TSingleShotCallbackList<TResult(TArgs...)>::IsFired() const
{
    return Fired_.load(std::memory_order::acquire);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
