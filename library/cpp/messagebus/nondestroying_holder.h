#pragma once

#include <util/generic/ptr.h>

template <typename T>
class TNonDestroyingHolder: public THolder<T> {
public:
    TNonDestroyingHolder(T* t = nullptr) noexcept
        : THolder<T>(t)
    {
    }

    TNonDestroyingHolder(TAutoPtr<T> t) noexcept
        : THolder<T>(t)
    {
    }

    ~TNonDestroyingHolder() {
        Y_ABORT_UNLESS(!*this, "stored object must be explicitly released");
    }
};

template <class T>
class TNonDestroyingAutoPtr: public TAutoPtr<T> {
public:
    inline TNonDestroyingAutoPtr(T* t = 0) noexcept
        : TAutoPtr<T>(t)
    {
    }

    inline TNonDestroyingAutoPtr(const TAutoPtr<T>& t) noexcept
        : TAutoPtr<T>(t.Release())
    {
    }

    inline ~TNonDestroyingAutoPtr() {
        Y_ABORT_UNLESS(!*this, "stored object must be explicitly released");
    }
};
