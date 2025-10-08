#pragma once

#include <atomic>
#include <optional>
#include <utility>

namespace NKikimr::NOlap {

template <class T>
class TThreadSafeOptional {
private:
    alignas(T) unsigned char Storage[sizeof(T)];
    std::atomic<bool> Defined{ false };

    T *Ptr() {
        return reinterpret_cast<T *>(&Storage[0]);
    }

    const T *Ptr() const {
        return reinterpret_cast<const T *>(&Storage[0]);
    }

public:
    TThreadSafeOptional() = default;

    ~TThreadSafeOptional() {
        if (Has()) {
            Ptr()->~T();
        }
    }

    TThreadSafeOptional(const TThreadSafeOptional& other) {
        const bool has = other.Defined.load(std::memory_order_acquire);
        if (has) {
            ::new (Ptr()) T(*other.Ptr());
            Defined.store(true, std::memory_order_release);
        }
    }

    TThreadSafeOptional& operator=(const TThreadSafeOptional& other) {
        if (this == &other) {
            return *this;
        }

        const bool has = other.Defined.load(std::memory_order_acquire);
        if (Has()) {
            Ptr()->~T();
            Defined.store(false, std::memory_order_release);
        }

        if (has) {
            ::new (Ptr()) T(*other.Ptr());
            Defined.store(true, std::memory_order_release);
        }

        return *this;
    }

    TThreadSafeOptional(TThreadSafeOptional&& other) noexcept {
        const bool has = other.Defined.load(std::memory_order_acquire);
        if (has) {
            ::new (Ptr()) T(std::move(*other.Ptr()));
            Defined.store(true, std::memory_order_release);
        }
    }

    TThreadSafeOptional& operator=(TThreadSafeOptional&& other) noexcept {
        if (this == &other) {
            return *this;
        }

        const bool has = other.Defined.load(std::memory_order_acquire);
        if (Has()) {
            Ptr()->~T();
            Defined.store(false, std::memory_order_release);
        }

        if (has) {
            ::new (Ptr()) T(std::move(*other.Ptr()));
            Defined.store(true, std::memory_order_release);
        }

        return *this;
    }

    void Reset() {
        if (Has()) {
            Ptr()->~T();
            Defined.store(false, std::memory_order_release);
        }
    }

    void Set(const T& value) {
        if (Has()) {
            Ptr()->~T();
            Defined.store(false, std::memory_order_release);
        }

        ::new (Ptr()) T(value);
        Defined.store(true, std::memory_order_release);
    }

    void Set(T&& value) {
        if (Has()) {
            Ptr()->~T();
            Defined.store(false, std::memory_order_release);
        }

        ::new (Ptr()) T(std::move(value));
        Defined.store(true, std::memory_order_release);
    }

    bool Has() const {
        return Defined.load(std::memory_order_acquire);
    }

    const T& Get() const {
        return *Ptr();
    }

    std::optional<T> GetOptional() const {
        if (Has()) {
            return *Ptr();
        }

        return {};
    }
};

}   // namespace NKikimr::NOlap
