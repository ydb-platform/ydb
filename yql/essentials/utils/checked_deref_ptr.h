#pragma once

#include "yql_panic.h"

#include <util/generic/utility.h>

namespace NYql {

/**
 * @brief Template wrapper for raw pointers with null-safety checks.
 *
 * Provides automatic null-checking on dereference operations (-> and *).
 * Implicitly converts to raw pointer for seamless integration with existing code.
 */
template <typename T>
class TCheckedDerefPtr {
public:
    constexpr TCheckedDerefPtr() noexcept
        : Ptr_(nullptr)
    {
    }

    explicit constexpr TCheckedDerefPtr(T* ptr) noexcept
        : Ptr_(ptr)
    {
    }

    constexpr TCheckedDerefPtr(const TCheckedDerefPtr& other) noexcept = default;
    constexpr TCheckedDerefPtr(TCheckedDerefPtr&& other) noexcept = default;

    TCheckedDerefPtr& operator=(const TCheckedDerefPtr& other) noexcept = default;
    TCheckedDerefPtr& operator=(TCheckedDerefPtr&& other) noexcept = default;

    TCheckedDerefPtr& operator=(T* ptr) noexcept {
        Ptr_ = ptr;
        return *this;
    }

    T& operator*() const {
        YQL_ENSURE(Ptr_ != nullptr, "Attempt to dereference null pointer");
        return *Ptr_;
    }

    T* operator->() const {
        YQL_ENSURE(Ptr_ != nullptr, "Attempt to access member through null pointer");
        return Ptr_;
    }

    // NOLINTNEXTLINE(google-explicit-constructor)
    constexpr operator T*() const noexcept {
        return Ptr_;
    }

    constexpr explicit operator bool() const noexcept {
        return Ptr_ != nullptr;
    }

    constexpr T* Get() const noexcept {
        return Ptr_;
    }

    void Reset() noexcept {
        Ptr_ = nullptr;
    }

    void Reset(T* ptr) noexcept {
        Ptr_ = ptr;
    }

    void Swap(TCheckedDerefPtr& other) noexcept {
        DoSwap(Ptr_, other.Ptr_);
    }

private:
    T* Ptr_;
};

template <typename T>
// NOLINTNEXTLINE(readability-identifier-naming)
void swap(TCheckedDerefPtr<T>& lhs, TCheckedDerefPtr<T>& rhs) noexcept {
    lhs.Swap(rhs);
}

} // namespace NYql
