#pragma once

#include <utility>
#include <type_traits>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A simple guard that executes a given function at the end of scope.

template <class TCallback>
class TFinallyGuard
{
public:
    template <class T>
    explicit TFinallyGuard(T&& finally)
        : Finally_(std::forward<T>(finally))
    { }

    TFinallyGuard(TFinallyGuard&& guard) noexcept
        : Released_(guard.Released_)
        , Finally_(std::move(guard.Finally_))
    {
        guard.Release();
    }

    TFinallyGuard(const TFinallyGuard&) = delete;
    TFinallyGuard& operator=(const TFinallyGuard&) = delete;

    TFinallyGuard& operator=(TFinallyGuard&& other) noexcept
    {
        if (this != &other) {
            Released_ = other.Released_;
            Finally_ = std::move(other.Finally_);
            other.Release();
        }
        return *this;
    }

    void Release() noexcept
    {
        Released_ = true;
    }

    ~TFinallyGuard()
    {
        if (!Released_) {
            Finally_();
        }
    }

private:
    bool Released_ = false;
    TCallback Finally_;
};

template <class TCallback>
[[nodiscard]] TFinallyGuard<typename std::decay<TCallback>::type> Finally(TCallback&& callback);

template <class TCallback>
TFinallyGuard<typename std::decay<TCallback>::type> Finally(TCallback&& callback)
{
    return TFinallyGuard<typename std::decay<TCallback>::type>(std::forward<TCallback>(callback));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
