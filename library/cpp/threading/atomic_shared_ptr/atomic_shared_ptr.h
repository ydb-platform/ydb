#pragma once

#include <atomic>
#include <cstdint>
#include <type_traits>
#include <utility>
#include <util/system/yassert.h>

#if !defined(_x86_64_) && !defined(__aarch64__)
#error "only 64 bit platforms are supported"
#endif

namespace NPrivate {
#if defined(_x86_64_)
    static constexpr unsigned char USABLE_ADDRESS_BITS = 48;
#elif defined(__aarch64__)
    static constexpr unsigned char USABLE_ADDRESS_BITS = 52;
#else
#error "number of using bits is undefined"
#endif

    struct TRefCounter {
        static constexpr ui64 USE_INCREMENT = 2;
        static constexpr ui64 DESTROYED_FLAG = 1;

        std::atomic<ui64> ref_count_{USE_INCREMENT};
        std::atomic<ui64> weak_count_{1};

        virtual ~TRefCounter() noexcept(false) = default;
        virtual void* GetPtr() noexcept = 0;
        virtual void DestroyPayload() = 0;

        template <class PayloadType>
        PayloadType* GetPayload() noexcept {
            return reinterpret_cast<PayloadType*>(GetPtr());
        }

        ui64 Ref(ui64 add = USE_INCREMENT) noexcept {
            return ref_count_.fetch_add(add, std::memory_order_relaxed) + add;
        }

        ui64 Unref(ui64 sub = USE_INCREMENT) noexcept {
            return ref_count_.fetch_sub(sub, std::memory_order_seq_cst) - sub;
        }

        void UnrefAndDelete(ui64 sub = USE_INCREMENT) {
            if (Unref(sub) != 0)
                return;
            ui64 expect = 0;
            bool flag_is_set = ref_count_.compare_exchange_strong(
                expect, DESTROYED_FLAG,
                std::memory_order_seq_cst, std::memory_order_relaxed);
            if (!flag_is_set)
                return;
            DestroyPayload();
            UnrefWeakAndDelete();
        }

        void* UnrefAndReleaseLast(ui64 sub = USE_INCREMENT) noexcept {
            if (Unref(sub) != 0)
                return nullptr;
            ui64 expect = 0;
            bool flag_is_set = ref_count_.compare_exchange_strong(
                expect, DESTROYED_FLAG,
                std::memory_order_seq_cst, std::memory_order_relaxed);
            if (!flag_is_set)
                return nullptr;
            void* result = GetPtr();
            UnrefWeakAndDelete();
            return result;
        }

        bool RefFromWeak(ui64 add = USE_INCREMENT) noexcept {
            ui64 result = Ref(add);
            if (!(result & DESTROYED_FLAG))
                return true;
            UnrefAndDelete(add);
            return false;
        }

        ui64 RefWeak(ui64 add = 1) noexcept {
            return weak_count_.fetch_add(add, std::memory_order_relaxed) + add;
        }

        ui64 UnrefWeak(ui64 sub = 1) noexcept {
            return weak_count_.fetch_sub(sub, std::memory_order_seq_cst) - sub;
        }

        void UnrefWeakAndDelete(ui64 sub = 1) noexcept {
            if (UnrefWeak(sub) != 0)
                return;
            delete this;
        }
    };

    template <class PayloadType>
    struct TRefCounterWithPointer: public TRefCounter {
        PayloadType* obj;

        explicit TRefCounterWithPointer(PayloadType* ptr) noexcept
            : obj(ptr)
        {
        }

        ~TRefCounterWithPointer() noexcept(noexcept(delete obj)) override {
            delete obj;
        }

        void* GetPtr() noexcept override {
            return obj;
        }

        void DestroyPayload() noexcept(noexcept(delete obj)) override {
            delete obj;
            obj = nullptr;
        }
    };

    class TSharedBasePtr {
    public:
        TSharedBasePtr() noexcept = default;
        TSharedBasePtr(TSharedBasePtr&) = delete;
        void operator=(TSharedBasePtr&) = delete;

        explicit TSharedBasePtr(TRefCounter* counter_ptr) noexcept
            : ptr_((uintptr_t)counter_ptr)
        {
            Y_ABORT_UNLESS((ptr_.load(std::memory_order_relaxed) & ~PTR_MASK) == 0,
                     "you must provide a clean ptr");
        }

        template <class PayloadType>
        explicit TSharedBasePtr(PayloadType* obj_ptr) noexcept
            : TSharedBasePtr(
                  (TRefCounter*)new NPrivate::TRefCounterWithPointer<PayloadType>(obj_ptr)) {
        }

        static constexpr uintptr_t CONCURRENT_INCREMENT = (uintptr_t)1 << USABLE_ADDRESS_BITS;
        static constexpr uintptr_t PTR_MASK = CONCURRENT_INCREMENT - 1;

        TRefCounter* ConcurrentAcquire() noexcept {
            auto result =
                ptr_.fetch_add(CONCURRENT_INCREMENT, std::memory_order_seq_cst);
            auto ptr_result = CleanUpPtr(result);
            if (ptr_result)
                ptr_result->Ref(CONCURRENT_INCREMENT + TRefCounter::USE_INCREMENT);
            return ptr_result;
        }

        TRefCounter* ConcurrentWeakAcquire() noexcept {
            auto result =
                ptr_.fetch_add(CONCURRENT_INCREMENT, std::memory_order_seq_cst);
            auto ptr_result = CleanUpPtr(result);
            if (ptr_result) {
                ptr_result->RefWeak();
                ptr_result->Ref(CONCURRENT_INCREMENT);
            }
            return ptr_result;
        }

        static void DestroyPtr(TRefCounter* ptr) {
            auto clean_ptr = CleanUpPtr(ptr);
            if (!clean_ptr)
                return;
            ui64 cnt = GetCounter(ptr);
            clean_ptr->UnrefAndDelete(cnt + TRefCounter::USE_INCREMENT);
        }

        static void* ReleaseLast(TRefCounter* ptr) noexcept {
            auto clean_ptr = CleanUpPtr(ptr);
            if (!clean_ptr)
                return nullptr;
            ui64 cnt = GetCounter(ptr);
            return clean_ptr->UnrefAndReleaseLast(cnt + TRefCounter::USE_INCREMENT);
        }

        static void DestroyWeakPtr(TRefCounter* ptr) noexcept {
            auto clean_ptr = CleanUpPtr(ptr);
            if (clean_ptr) {
                ui64 cnt = GetCounter(ptr);
                clean_ptr->UnrefWeakAndDelete(cnt);
            }
        }

        static TRefCounter* CleanUpPtr(TRefCounter* ptr) noexcept {
            return (TRefCounter*)((uintptr_t)ptr & PTR_MASK);
        }

        static TRefCounter* CleanUpPtr(uintptr_t ptr) noexcept {
            return (TRefCounter*)(ptr & PTR_MASK);
        }

        static uintptr_t GetCounter(TRefCounter* ptr) noexcept {
            return (uintptr_t)ptr & ~PTR_MASK;
        }

        TRefCounter* GetClean() const noexcept {
            return CleanUpPtr(ptr_.load(std::memory_order_relaxed));
        }

        TRefCounter* GetRaw() const noexcept {
            return (TRefCounter*)ptr_.load(std::memory_order_relaxed);
        }

        TRefCounter* Swap(TRefCounter* other) noexcept {
            return (TRefCounter*)ptr_.exchange(
                (uintptr_t)other, std::memory_order_seq_cst);
        }

        size_t UseCount() const noexcept {
            auto ptr = GetRaw();
            auto clean_ptr = CleanUpPtr(ptr);
            if (!clean_ptr)
                return 0;
            ui64 result = clean_ptr->ref_count_.load(std::memory_order_relaxed);
            result -= GetCounter(ptr);
            result /= TRefCounter::USE_INCREMENT;
            return result;
        }

    private:
        std::atomic<uintptr_t> ptr_{0};
    };

}

// This class provides thread-safe and atomic operations
// with wait-free guarantee for copying and destroying shared pointers.
// You may safely copy this shared pointer from multiple threads without
// explicit locks, hazard pointers or other synchonization.
// This class makes use of high bits of 64-bit pointers which are normally
// ignored by hardware. Thus it works on x86_64 (amd64) platforms.
// Wait-free guarantee is supported under the following circumstances:
// - atomic<void*>.fetch_add is wait-free.
// - atomic<void*>.exchange is wait-free.
// - atomic<ui64>.compare_exchange_strong is wait-free.
// This class does not provide wait-free guarantee for arm64 or power_pc
// platforms because these platforms do not provide wait-free guarantee for
// necessary atomic operations. It requires more sophisticated algorithms
// to achieve wait-free guarantee using lock-free atomic operations.
template <class PayloadType>
class TTrueAtomicSharedPtr {
public:
    TTrueAtomicSharedPtr() noexcept = default;

    explicit TTrueAtomicSharedPtr(PayloadType* obj_ptr) noexcept
        : ptr_(obj_ptr)
    {
    }

    ~TTrueAtomicSharedPtr() noexcept(PAYLOAD_DESTRUCTOR_IS_NOEXCEPT) {
        auto ptr = ptr_.GetRaw();
        NPrivate::TSharedBasePtr::DestroyPtr(ptr);
    }

    TTrueAtomicSharedPtr(const TTrueAtomicSharedPtr& other) noexcept
        : ptr_(other.ptr_.ConcurrentAcquire())
    {
    }

    TTrueAtomicSharedPtr(TTrueAtomicSharedPtr&& other) noexcept
        : ptr_(other.ptr_.Swap(nullptr))
    {
    }

    // WARNING: it is thread-safe but it is not atomic
    TTrueAtomicSharedPtr& operator=(const TTrueAtomicSharedPtr& other) noexcept(PAYLOAD_DESTRUCTOR_IS_NOEXCEPT) {
        auto new_ptr = other.ptr_.ConcurrentAcquire();
        auto back_ptr = ptr_.Swap(new_ptr);
        NPrivate::TSharedBasePtr::DestroyPtr(back_ptr);
        return *this;
    }

    // WARNING: it is thread-safe but it is not atomic
    TTrueAtomicSharedPtr& operator=(TTrueAtomicSharedPtr&& other) noexcept(PAYLOAD_DESTRUCTOR_IS_NOEXCEPT) {
        auto back_ptr = other.ptr_.Swap(nullptr);
        back_ptr = ptr_.Swap(back_ptr);
        NPrivate::TSharedBasePtr::DestroyPtr(back_ptr);
        return *this;
    }

    // WARNING: it is not thread safe, consider acquire a local copy
    // before dereferencing
    PayloadType* operator->() const noexcept {
        NPrivate::TRefCounter* ptr = ptr_.GetClean();
        Y_ABORT_UNLESS(ptr, "dereferencing nullptr");
        return ptr->GetPayload<PayloadType>();
    }

    // WARNING: it is not thread safe, consider acquire a local copy
    // before dereferencing
    PayloadType& operator*() const noexcept {
        NPrivate::TRefCounter* ptr = ptr_.GetClean();
        Y_ABORT_UNLESS(ptr, "dereferencing nullptr");
        return *ptr->GetPayload<PayloadType>();
    }

    // WARNING: it is not thread safe, consider acquire a local copy
    // before getting a pointer
    PayloadType* get() const noexcept {
        auto ptr = ptr_.GetClean();
        return ptr ? ptr->template GetPayload<PayloadType>() : nullptr;
    }

    explicit operator bool() const noexcept {
        return ptr_.GetClean();
    }

    // WARNING: swap is thread-safe but it is not atomic at all circumstances.
    // Having A with possible accesses from multiple threads and B with exclusive
    // use from a single thread, then calling A.swap(B) is atomic.
    void swap(TTrueAtomicSharedPtr<PayloadType>& other) noexcept {
        NPrivate::TRefCounter* acquired = other.ptr_.ConcurrentAcquire();
        acquired = ptr_.Swap(acquired);
        acquired = other.ptr_.Swap(acquired);
        NPrivate::TSharedBasePtr::DestroyPtr(acquired);
    }

    void reset() noexcept {
        auto back_ptr = ptr_.Swap(nullptr);
        NPrivate::TSharedBasePtr::DestroyPtr(back_ptr);
    }

    // ReleaseLast release and returns a controlled object if there is no other
    // references to the object, otherwise returns nullptr.
    // This pointer becomes empty.
    // This is useful in order to postpone destruction of
    // a heavy-destructible object.
    PayloadType* ReleaseLast() noexcept {
        NPrivate::TRefCounter* cnt_obj = ptr_.Swap(nullptr);
        void* obj_ptr = NPrivate::TSharedBasePtr::ReleaseLast(cnt_obj);
        return reinterpret_cast<PayloadType*>(obj_ptr);
    }

private:
    mutable NPrivate::TSharedBasePtr ptr_;

    explicit TTrueAtomicSharedPtr(NPrivate::TRefCounter* control) noexcept
        : ptr_(control)
    {
    }

    static constexpr bool PAYLOAD_DESTRUCTOR_IS_NOEXCEPT =
        std::is_nothrow_destructible<PayloadType>::value;

    template <class T>
    friend class TTrueAtomicWeakPtr;
};

template <class PayloadType, class... ArgTypes>
TTrueAtomicSharedPtr<PayloadType> MakeTrueAtomicShared(ArgTypes&&... args) {
    auto obj_ptr = new PayloadType(std::forward<ArgTypes>(args)...);
    return TTrueAtomicSharedPtr<PayloadType>(obj_ptr);
}

// This class provides thread-safe and atomic operations
// with wait-free guarantee for copying and destroying weak pointers.
// See description of TTrueAtomicSharedPtr.
// All guarantees are the same as for TTrueAtomicSharedPtr.
template <class PayloadType>
class TTrueAtomicWeakPtr {
public:
    TTrueAtomicWeakPtr() noexcept = default;

    TTrueAtomicWeakPtr(const TTrueAtomicSharedPtr<PayloadType>& shared_ptr) noexcept
        : ptr_(shared_ptr.ptr_.ConcurrentWeakAcquire())
    {
    }

    TTrueAtomicWeakPtr(const TTrueAtomicWeakPtr& other) noexcept
        : ptr_(other.ptr_.ConcurrentWeakAcquire())
    {
    }

    TTrueAtomicWeakPtr& operator=(const TTrueAtomicWeakPtr& other) noexcept {
        auto new_ptr = other.ptr_.ConcurrentWeakAcquire();
        auto back_ptr = ptr_.Swap(new_ptr);
        NPrivate::TSharedBasePtr::DestroyWeakPtr(back_ptr);
        return *this;
    }

    TTrueAtomicWeakPtr& operator=(TTrueAtomicWeakPtr&& other) noexcept {
        auto new_ptr = other.ptr_.Swap(nullptr);
        auto back_ptr = ptr_.Swap(new_ptr);
        NPrivate::TSharedBasePtr::DestroyWeakPtr(back_ptr);
        return *this;
    }

    TTrueAtomicSharedPtr<PayloadType> lock() noexcept {
        // create local TTrueAtomicWeakPtr to avoid concurrent changes of this
        TTrueAtomicWeakPtr<PayloadType> local(*this);
        if (!local.ptr_.GetRaw()->RefFromWeak())
            return TTrueAtomicSharedPtr<PayloadType>();
        return TTrueAtomicSharedPtr<PayloadType>(ptr_.GetClean());
    }

    void reset() noexcept {
        auto back_ptr = ptr_.Swap(nullptr);
        NPrivate::TSharedBasePtr::DestroyWeakPtr(back_ptr);
    }

    // WARNING: swap is thread-safe but it is not atomic at all circumstances.
    // Having A with possible accesses from multiple threads and B with exclusive
    // use from a single thread, then calling A.swap(B) is atomic.
    void swap(TTrueAtomicWeakPtr& other) noexcept {
        auto copy = other.ptr_.ConcurrentWeakAcquire();
        auto back_ptr = ptr_.Swap(copy);
        back_ptr = other.ptr_.Swap(back_ptr);
        NPrivate::TSharedBasePtr::DestroyWeakPtr(back_ptr);
    }

    size_t use_count() const noexcept {
        return ptr_.UseCount();
    }

private:
    mutable NPrivate::TSharedBasePtr ptr_{nullptr};
};
