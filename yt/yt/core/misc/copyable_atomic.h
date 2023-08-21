#pragma once

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TCopyableAtomic
{   
public:
    T Load() const
    {
        return T_.load();
    }

    void Store(T v)
    {
        T_.store(v);
    }

    TCopyableAtomic() = default;

    TCopyableAtomic(T v)
        : T_(v)
    { }

    TCopyableAtomic(const TCopyableAtomic& other)
        : T_(other.T_.load())
    { }

    TCopyableAtomic& operator = (const TCopyableAtomic& other)
    {
        T_ = other.T_.load();
        return *this;
    }

    TCopyableAtomic(TCopyableAtomic&& other)
        : T_(other.T_.load())
    { }

    TCopyableAtomic& operator = (TCopyableAtomic&& other)
    {
        T_ = other.T_.load();
        return *this;
    }

private:
    std::atomic<T> T_ = {};
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
