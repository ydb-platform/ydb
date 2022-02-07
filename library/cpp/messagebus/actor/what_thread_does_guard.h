#pragma once

#include "what_thread_does.h"

template <class T>
class TWhatThreadDoesAcquireGuard: public TNonCopyable {
public:
    inline TWhatThreadDoesAcquireGuard(const T& t, const char* acquire) noexcept {
        Init(&t, acquire);
    }

    inline TWhatThreadDoesAcquireGuard(const T* t, const char* acquire) noexcept {
        Init(t, acquire);
    }

    inline ~TWhatThreadDoesAcquireGuard() {
        Release();
    }

    inline void Release() noexcept {
        if (WasAcquired()) {
            const_cast<T*>(T_)->Release();
            T_ = nullptr;
        }
    }

    inline bool WasAcquired() const noexcept {
        return T_ != nullptr;
    }

private:
    inline void Init(const T* t, const char* acquire) noexcept {
        T_ = const_cast<T*>(t);
        TWhatThreadDoesPushPop pp(acquire);
        T_->Acquire();
    }

private:
    T* T_;
};
