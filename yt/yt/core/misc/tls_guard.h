#pragma once

#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TTlsGuard
    : public NConcurrency::TForbidContextSwitchGuard
{
public:
    TTlsGuard(const TTlsGuard&) = delete;
    TTlsGuard(TTlsGuard&&) = delete;

    TTlsGuard(T* ptr, T newValue)
        : TlsPtr_(ptr)
        , SavedValue_(newValue)
    {
        YT_VERIFY(ptr);
        std::swap(SavedValue_, *TlsPtr_);
    }

    ~TTlsGuard()
    {
        std::swap(SavedValue_, *TlsPtr_);
    }

private:
    T* const TlsPtr_;
    T SavedValue_;
};

// Modifies thread local variable and saves current value.
// On scope exit restores saved value.
// Forbids context switch.

template <class T>
auto TlsGuard(T* ptr, T newValue)
{
    return TTlsGuard<T>(ptr, std::move(newValue));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

