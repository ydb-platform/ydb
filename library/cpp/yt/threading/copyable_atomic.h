#pragma once

#include <atomic>

namespace NYT::NThreading {

////////////////////////////////////////////////////////////////////////////////

//! A drop-in replacement for |std::atomic| that can be copied and moved,
//! which makes it usable as a member of a copyable class.
/*!
 *  Copying is not atomic: the source is loaded and the result is used to
 *  initialize (or is stored into) the target.
 */
template <class T>
struct TCopyableAtomic
    : public std::atomic<T>
{
    using std::atomic<T>::atomic;
    using std::atomic<T>::operator=;

    TCopyableAtomic() = default;

    TCopyableAtomic(const TCopyableAtomic& other) noexcept
        : std::atomic<T>(other.load())
    { }

    TCopyableAtomic(TCopyableAtomic&& other) noexcept
        : std::atomic<T>(other.load())
    { }

    TCopyableAtomic& operator=(const TCopyableAtomic& other) noexcept
    {
        this->store(other.load());
        return *this;
    }

    TCopyableAtomic& operator=(TCopyableAtomic&& other) noexcept
    {
        this->store(other.load());
        return *this;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NThreading
