// Exercise the opposite header order to memory_header_ut.cpp.
#include <atomic>

#ifdef __cpp_lib_atomic_shared_ptr
#error atomic smart pointers must not be advertised by <atomic> yet
#endif

#include <memory>

#include <library/cpp/testing/gtest/gtest.h>

#include <array>
#include <barrier>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace {
    template <class T>
    std::shared_ptr<T> Lock(const std::shared_ptr<T>& ptr) {
        return ptr;
    }

    template <class T>
    std::shared_ptr<T> Lock(const std::weak_ptr<T>& ptr) {
        return ptr.lock();
    }

    template <class T>
    bool SameOwner(const T& lhs, const T& rhs) {
        return !lhs.owner_before(rhs) && !rhs.owner_before(lhs);
    }

    template <class T>
    class TAtomicSmartPtr: public testing::Test {};

    using TSmartPointers = testing::Types<std::shared_ptr<std::int32_t>, std::weak_ptr<std::int32_t>>;
    TYPED_TEST_SUITE(TAtomicSmartPtr, TSmartPointers);

    TYPED_TEST(TAtomicSmartPtr, ConstructionAndAssignment) {
        using TPtr = TypeParam;
        using TAtomic = std::atomic<TPtr>;
        static_assert(std::is_same_v<typename TAtomic::value_type, TPtr>);
        static_assert(std::is_nothrow_default_constructible_v<TAtomic>);
        static_assert(std::is_nothrow_constructible_v<TAtomic, TPtr>);
        static_assert(std::is_nothrow_convertible_v<const TAtomic&, TPtr>);
        static_assert(!std::is_copy_constructible_v<TAtomic>);
        static_assert(!std::is_move_constructible_v<TAtomic>);
        static_assert(!std::is_copy_assignable_v<TAtomic>);
        static_assert(!std::is_move_assignable_v<TAtomic>);
        static_assert(std::is_same_v<decltype(std::declval<TAtomic&>() = std::declval<TPtr>()), void>);
        static_assert(noexcept(std::declval<TAtomic&>().store(TPtr())));
        static_assert(noexcept(std::declval<const TAtomic&>().load()));
        static_assert(noexcept(std::declval<const TAtomic&>().operator TPtr()));
        static_assert(noexcept(std::declval<TAtomic&>().exchange(TPtr())));
        static_assert(noexcept(std::declval<TAtomic&>().compare_exchange_weak(std::declval<TPtr&>(), TPtr())));
        static_assert(noexcept(std::declval<TAtomic&>().compare_exchange_strong(std::declval<TPtr&>(), TPtr())));
        static_assert(noexcept(std::declval<const TAtomic&>().wait(TPtr())));
        static_assert(noexcept(std::declval<TAtomic&>().notify_one()));
        static_assert(noexcept(std::declval<TAtomic&>().notify_all()));

        TAtomic empty;
        EXPECT_FALSE(Lock(empty.load()));
        EXPECT_FALSE(empty.is_lock_free());
        EXPECT_FALSE(TAtomic::is_always_lock_free);

        auto owner = std::make_shared<std::int32_t>(42);
        TPtr initial(owner);
        const TAtomic value(initial);
        EXPECT_EQ(Lock(value.load()), owner);
        TPtr converted = value;
        EXPECT_EQ(Lock(converted), owner);
        EXPECT_TRUE(SameOwner(converted, initial));
        empty = initial;
        EXPECT_EQ(Lock(empty.load()), owner);

        TAtomic moved(std::move(initial));
        EXPECT_TRUE(SameOwner(initial, TPtr()));
        EXPECT_EQ(Lock(moved.load()), owner);
    }

    TYPED_TEST(TAtomicSmartPtr, StoreLoadAndExchangeOrders) {
        using TPtr = TypeParam;
        auto first = std::make_shared<std::int32_t>(1);
        auto second = std::make_shared<std::int32_t>(2);
        std::atomic<TPtr> value{TPtr(first)};
        for (auto order : {std::memory_order_relaxed, std::memory_order_release, std::memory_order_seq_cst}) {
            value.store(TPtr(second), order);
            EXPECT_EQ(Lock(value.load()), second);
            value.store(TPtr(first), order);
        }

        for (auto order : {std::memory_order_relaxed, std::memory_order_consume,
                           std::memory_order_acquire, std::memory_order_seq_cst}) {
            EXPECT_EQ(Lock(value.load(order)), first);
        }

        for (auto order : {std::memory_order_relaxed, std::memory_order_consume, std::memory_order_acquire,
                           std::memory_order_release, std::memory_order_acq_rel, std::memory_order_seq_cst}) {
            EXPECT_EQ(Lock(value.exchange(TPtr(second), order)), first);
            EXPECT_EQ(Lock(value.exchange(TPtr(first), order)), second);
        }

        value.store(TPtr());
        EXPECT_FALSE(Lock(value.load()));
    }

    TYPED_TEST(TAtomicSmartPtr, CompareExchangeOrders) {
        using TPtr = TypeParam;
        auto first = std::make_shared<std::int32_t>(1);
        auto second = std::make_shared<std::int32_t>(2);
        std::atomic<TPtr> value{TPtr(first)};
        for (auto order : {std::memory_order_relaxed, std::memory_order_consume, std::memory_order_acquire,
                           std::memory_order_release, std::memory_order_acq_rel, std::memory_order_seq_cst}) {
            auto expected = TPtr(second);
            EXPECT_FALSE(value.compare_exchange_strong(expected, TPtr(second), order));
            EXPECT_EQ(Lock(expected), first);
            EXPECT_TRUE(value.compare_exchange_strong(expected, TPtr(second), order));
            EXPECT_EQ(Lock(expected), first);
            expected = TPtr(second);
            while (!value.compare_exchange_weak(expected, TPtr(first), order)) {}
        }

        for (auto failure : {std::memory_order_relaxed, std::memory_order_consume,
                             std::memory_order_acquire, std::memory_order_seq_cst}) {
            auto expected = TPtr(second);
            EXPECT_FALSE(value.compare_exchange_weak(expected, TPtr(second), std::memory_order_seq_cst, failure));
            EXPECT_EQ(Lock(expected), first);
            EXPECT_TRUE(value.compare_exchange_strong(expected, TPtr(second), std::memory_order_seq_cst, failure));
            expected = TPtr(second);
            while (!value.compare_exchange_weak(expected, TPtr(first), std::memory_order_seq_cst, failure)) {}
        }
    }

    TYPED_TEST(TAtomicSmartPtr, CompareExchangeUsesPointerAndOwner) {
        using TPtr = TypeParam;
        auto owner = std::make_shared<std::array<std::int32_t, 2>>();
        auto first = std::shared_ptr<std::int32_t>(owner, &(*owner)[0]);
        auto second = std::shared_ptr<std::int32_t>(owner, &(*owner)[1]);
        auto otherOwner = std::shared_ptr<std::int32_t>(first.get(), [](std::int32_t*) {});
        auto desired = std::make_shared<std::int32_t>(7);
        std::atomic<TPtr> value{TPtr(first)};

        auto expected = TPtr(second);
        EXPECT_FALSE(value.compare_exchange_strong(expected, TPtr(desired)));
        EXPECT_EQ(Lock(expected).get(), first.get());
        expected = TPtr(otherOwner);
        EXPECT_FALSE(value.compare_exchange_strong(expected, TPtr(desired)));
        EXPECT_TRUE(SameOwner(expected, TPtr(first)));
        EXPECT_TRUE(value.compare_exchange_strong(expected, TPtr(desired)));
        EXPECT_EQ(Lock(value.load()), desired);

        auto nullAlias = std::shared_ptr<std::int32_t>(owner, nullptr);
        value.store(TPtr(nullAlias));
        expected = TPtr();
        EXPECT_FALSE(value.compare_exchange_strong(expected, TPtr()));
        EXPECT_TRUE(SameOwner(expected, TPtr(nullAlias)));
        EXPECT_TRUE(value.compare_exchange_strong(expected, TPtr()));
    }

    TYPED_TEST(TAtomicSmartPtr, FreeFunctions) {
        using TPtr = TypeParam;
        auto owner = std::make_shared<std::int32_t>(4);
        std::atomic<TPtr> value;
        EXPECT_FALSE(std::atomic_is_lock_free(&value));
        std::atomic_init(&value, TPtr(owner));
        EXPECT_EQ(Lock(value.load()), owner);
        std::atomic_init<TPtr>(&value, TPtr());
        EXPECT_FALSE(Lock(value.load()));
        std::atomic_store(&value, TPtr(owner));
        EXPECT_EQ(Lock(std::atomic_load(&value)), owner);
        std::atomic_store_explicit(&value, TPtr(owner), std::memory_order_release);
        EXPECT_EQ(Lock(std::atomic_load_explicit(&value, std::memory_order_acquire)), owner);
        EXPECT_EQ(Lock(std::atomic_exchange(&value, TPtr())), owner);
        EXPECT_FALSE(Lock(std::atomic_exchange_explicit(&value, TPtr(owner), std::memory_order_acq_rel)));
        auto expected = TPtr(owner);
        EXPECT_TRUE(std::atomic_compare_exchange_strong(&value, &expected, TPtr()));
        expected = TPtr();
        while (!std::atomic_compare_exchange_weak(&value, &expected, TPtr(owner))) {}

        expected = TPtr(owner);
        EXPECT_TRUE(std::atomic_compare_exchange_strong_explicit(
            &value, &expected, TPtr(), std::memory_order_release, std::memory_order_relaxed));
        expected = TPtr();
        while (!std::atomic_compare_exchange_weak_explicit(
            &value, &expected, TPtr(owner), std::memory_order_acq_rel, std::memory_order_acquire)) {}

        std::atomic_wait(&value, TPtr());
        std::atomic_wait_explicit(&value, TPtr(), std::memory_order_acquire);
        std::atomic_notify_one(&value);
        std::atomic_notify_all(&value);
    }

    template <class TPtr>
    void CheckWait(const TPtr& before, const TPtr& after, bool all) {
        std::atomic<TPtr> value(before);
        std::atomic<std::int32_t> entered{0};
        std::atomic<std::int32_t> finished{0};
        const std::int32_t count = all ? 4 : 1;
        std::vector<std::thread> threads;
        for (std::int32_t i = 0; i < count; ++i) {
            threads.emplace_back([&] {
                entered.fetch_add(1);
                value.wait(before, std::memory_order_acquire);
                finished.fetch_add(1);
            });
        }

        while (entered.load() != count) {
            std::this_thread::yield();
        }

        // Exercise the blocking path, including notifications that do not change the value.
        value.notify_all();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        EXPECT_EQ(finished.load(), 0);
        value.store(after, std::memory_order_release);
        if (all) {
            value.notify_all();
        } else {
            value.notify_one();
        }

        for (auto& thread : threads) {
            thread.join();
        }

        EXPECT_EQ(finished.load(), count);
        // Also exercise a notification preceding the wait and every valid load order.
        for (auto order : {std::memory_order_relaxed, std::memory_order_consume,
                           std::memory_order_acquire, std::memory_order_seq_cst}) {
            value.wait(before, order);
        }
    }

    TYPED_TEST(TAtomicSmartPtr, WaitNotify) {
        using TPtr = TypeParam;
        auto owner = std::make_shared<std::array<std::int32_t, 2>>();
        auto first = std::shared_ptr<std::int32_t>(owner, &(*owner)[0]);
        auto alias = std::shared_ptr<std::int32_t>(owner, &(*owner)[1]);
        auto otherOwner = std::shared_ptr<std::int32_t>(first.get(), [](std::int32_t*) {});
        for (bool all : {false, true}) {
            CheckWait(TPtr(), TPtr(first), all);
            CheckWait(TPtr(first), TPtr(alias), all);
            CheckWait(TPtr(first), TPtr(otherOwner), all);
            CheckWait(TPtr(first), TPtr(), all);
            auto borrowed = std::shared_ptr<std::int32_t>(std::shared_ptr<std::int32_t>(), first.get());
            CheckWait(TPtr(), TPtr(borrowed), all);
        }
    }

    TYPED_TEST(TAtomicSmartPtr, OwnerlessAliasing) {
        using TPtr = TypeParam;
        std::int32_t payload = 42;
        auto borrowed = std::shared_ptr<std::int32_t>(std::shared_ptr<std::int32_t>(), &payload);
        std::atomic<TPtr> value{TPtr(borrowed)};
        TPtr expected;
        EXPECT_FALSE(value.compare_exchange_strong(expected, TPtr()));
        EXPECT_EQ(expected.use_count(), 0);
        EXPECT_TRUE(value.compare_exchange_strong(expected, TPtr()));
    }

    TYPED_TEST(TAtomicSmartPtr, ConcurrentPublication) {
        using TPtr = TypeParam;
        constexpr std::int32_t iterations = 2000;
        std::atomic<TPtr> value;
        std::thread producer([&] {
            for (std::int32_t i = 1; i <= iterations; ++i) {
                auto owner = std::make_shared<std::int32_t>(i);
                value.store(TPtr(owner), std::memory_order_release);
                value.notify_one();
                while (Lock(value.load(std::memory_order_acquire))) {
                    std::this_thread::yield();
                }
            }
        });

        for (std::int32_t i = 1; i <= iterations; ++i) {
            value.wait(TPtr(), std::memory_order_acquire);
            auto owner = Lock(value.load(std::memory_order_acquire));
            EXPECT_TRUE(owner);
            if (owner) {
                EXPECT_EQ(*owner, i);
            }

            value.store(TPtr(), std::memory_order_release);
        }

        producer.join();
    }

    TEST(TAtomicSharedPtr, EmptyAliasingAndTypes) {
        std::int32_t payload = 1;
        auto alias = std::shared_ptr<std::int32_t>(std::shared_ptr<std::int32_t>(), &payload);
        std::atomic<std::shared_ptr<std::int32_t>> value(alias);
        auto expected = std::shared_ptr<std::int32_t>();
        EXPECT_FALSE(value.compare_exchange_strong(expected, nullptr));
        EXPECT_EQ(expected.get(), &payload);
        EXPECT_EQ(expected.use_count(), 0);
        EXPECT_TRUE(value.compare_exchange_strong(expected, nullptr));
        value = nullptr;
        EXPECT_FALSE(value.load());

        std::atomic<std::shared_ptr<std::int32_t[]>> array(std::make_shared<std::int32_t[]>(3));
        EXPECT_EQ(array.load()[2], 0);
        std::atomic<std::shared_ptr<std::int32_t[2]>> bounded(std::make_shared<std::int32_t[2]>());
        EXPECT_EQ(bounded.load()[1], 0);
        std::atomic<std::shared_ptr<const std::int32_t>> constant(std::make_shared<const std::int32_t>(7));
        EXPECT_EQ(*constant.load(), 7);
        std::atomic<std::shared_ptr<const void>> erased(constant.load());
        EXPECT_EQ(erased.load().get(), constant.load().get());
        std::atomic<std::weak_ptr<std::int32_t[]>> weakArray(array.load());
        EXPECT_EQ(weakArray.load().lock()[2], 0);
    }

    TEST(TAtomicWeakPtr, ExpiredIdentity) {
        auto owner = std::make_shared<std::array<std::int32_t, 2>>();
        auto first = std::shared_ptr<std::int32_t>(owner, &(*owner)[0]);
        auto second = std::shared_ptr<std::int32_t>(owner, &(*owner)[1]);
        auto otherOwner = std::make_shared<std::int32_t>(9);
        auto weakFirst = std::weak_ptr<std::int32_t>(first);
        auto weakSecond = std::weak_ptr<std::int32_t>(second);
        auto weakOther = std::weak_ptr<std::int32_t>(otherOwner);
        owner.reset();
        first.reset();
        second.reset();
        otherOwner.reset();
        ASSERT_TRUE(weakFirst.expired());
        ASSERT_TRUE(weakSecond.expired());
        ASSERT_TRUE(weakOther.expired());

        std::atomic<std::weak_ptr<std::int32_t>> value(weakFirst);
        auto expected = weakSecond;
        EXPECT_FALSE(value.compare_exchange_strong(expected, {}));
        EXPECT_TRUE(value.compare_exchange_strong(expected, weakFirst));
        expected = weakOther;
        EXPECT_FALSE(value.compare_exchange_strong(expected, {}));
        EXPECT_TRUE(SameOwner(expected, weakFirst));
        expected.reset();
        EXPECT_FALSE(value.compare_exchange_strong(expected, {}));
        CheckWait(weakFirst, weakSecond, false);
        CheckWait(weakFirst, weakOther, true);
        CheckWait(weakFirst, std::weak_ptr<std::int32_t>(), true);
    }

    TEST(TAtomicSharedPtr, ReferenceCountsAndReentrantDeleters) {
        std::atomic<std::shared_ptr<std::int32_t>> value;
        std::int32_t destroyed = 0;
        auto make = [&] {
            return std::shared_ptr<std::int32_t>(new std::int32_t(3), [&](std::int32_t* ptr) {
                value.load();
                ++destroyed;
                delete ptr;
            });
        };

        value.store(make());
        EXPECT_EQ(value.load().use_count(), 2);
        auto loaded = value.load();
        value.store(nullptr);
        EXPECT_EQ(destroyed, 0);
        loaded.reset();
        EXPECT_EQ(destroyed, 1);
        value.store(make());
        value.store(nullptr);
        EXPECT_EQ(destroyed, 2);
        value.store(make());
        auto previous = value.exchange(nullptr);
        EXPECT_EQ(destroyed, 2);
        previous.reset();
        EXPECT_EQ(destroyed, 3);

        auto expected = make();
        EXPECT_FALSE(value.compare_exchange_strong(expected, {}));
        EXPECT_FALSE(expected);
        EXPECT_EQ(destroyed, 4);
        value.store(make());
        expected = value.load();
        EXPECT_TRUE(value.compare_exchange_strong(expected, {}));
        EXPECT_EQ(destroyed, 4);
        expected.reset();
        EXPECT_EQ(destroyed, 5);
        value.store(make());
        EXPECT_FALSE(value.compare_exchange_strong(expected, make()));
        EXPECT_EQ(destroyed, 6);
        value.store(nullptr);
        expected.reset();
        EXPECT_EQ(destroyed, 7);
    }

    TEST(TAtomicSharedPtr, AtomicInitAndLegacyFunctions) {
        std::atomic<std::int32_t> value;
        std::atomic_init(&value, 42);
        EXPECT_EQ(value.load(), 42);
        volatile std::atomic<std::int32_t> volatileValue;
        std::atomic_init(&volatileValue, 7);
        EXPECT_EQ(volatileValue.load(), 7);

        auto owner = std::make_shared<std::int32_t>(9);
        std::shared_ptr<std::int32_t> legacy;
        std::atomic_store(&legacy, owner);
        EXPECT_EQ(std::atomic_load(&legacy), owner);
        auto expected = owner;
        EXPECT_TRUE(std::atomic_compare_exchange_strong(&legacy, &expected, std::shared_ptr<std::int32_t>()));
        EXPECT_FALSE(std::atomic_load(&legacy));
    }

    TEST(TAtomicSharedPtr, ConcurrentCompareExchange) {
        std::atomic<std::shared_ptr<std::int32_t>> value(std::make_shared<std::int32_t>(0));
        constexpr std::int32_t count = 4;
        constexpr std::int32_t iterations = 3000;
        std::barrier start(count);
        std::vector<std::thread> threads;
        for (std::int32_t i = 0; i < count; ++i) {
            threads.emplace_back([&] {
                start.arrive_and_wait();
                for (std::int32_t j = 0; j < iterations; ++j) {
                    auto expected = value.load(std::memory_order_relaxed);
                    while (!value.compare_exchange_weak(expected, std::make_shared<std::int32_t>(*expected + 1),
                                                        std::memory_order_acq_rel, std::memory_order_acquire)) {}
                }
            });
        }

        for (auto& thread : threads) {
            thread.join();
        }

        EXPECT_EQ(*value.load(), count * iterations);
        EXPECT_EQ(value.load().use_count(), 2);
    }

    TYPED_TEST(TAtomicSmartPtr, SequentialConsistencyWithOrdinaryAtomics) {
        using TPtr = TypeParam;
        auto owner = std::make_shared<std::int32_t>(1);
        std::atomic<TPtr> pointer;
        std::atomic<std::int32_t> number{0};
        std::int32_t observedNumber = 0;
        bool observedPointer = false;
        constexpr std::int32_t iterations = 2000;
        std::barrier phase(3);
        std::thread first([&] {
            for (std::int32_t i = 0; i < iterations; ++i) {
                phase.arrive_and_wait();
                pointer.store(TPtr(owner));
                observedNumber = number.load();
                phase.arrive_and_wait();
            }
        });

        std::thread second([&] {
            for (std::int32_t i = 0; i < iterations; ++i) {
                phase.arrive_and_wait();
                number.store(1);
                observedPointer = bool(Lock(pointer.load()));
                phase.arrive_and_wait();
            }
        });

        for (std::int32_t i = 0; i < iterations; ++i) {
            pointer.store(TPtr());
            number.store(0);
            phase.arrive_and_wait();
            phase.arrive_and_wait();
            EXPECT_TRUE(observedNumber != 0 || observedPointer);
        }

        first.join();
        second.join();
    }

    template <class T>
    struct TReentrantAllocator {
        using value_type = T;
        std::atomic<std::weak_ptr<std::int32_t>>* Atomic;
        std::int32_t* Deallocated;

        TReentrantAllocator(std::atomic<std::weak_ptr<std::int32_t>>* atomic, std::int32_t* deallocated)
            : Atomic(atomic)
            , Deallocated(deallocated)
        {}

        template <class U>
        TReentrantAllocator(const TReentrantAllocator<U>& other)
            : Atomic(other.Atomic)
            , Deallocated(other.Deallocated)
        {}

        T* allocate(std::size_t count) {
            return std::allocator<T>().allocate(count);
        }

        void deallocate(T* ptr, std::size_t count) {
            Atomic->load();
            ++*Deallocated;
            std::allocator<T>().deallocate(ptr, count);
        }
    };

    TEST(TAtomicWeakPtr, ReentrantControlBlockDeallocation) {
        std::atomic<std::weak_ptr<std::int32_t>> value;
        std::int32_t deallocated = 0;
        auto makeExpired = [&] {
            auto owner = std::allocate_shared<std::int32_t>(TReentrantAllocator<std::int32_t>(&value, &deallocated), 42);
            return std::weak_ptr<std::int32_t>(owner);
        };

        value.store(makeExpired());
        EXPECT_TRUE(value.load().expired());
        EXPECT_EQ(deallocated, 0);
        value.store({});
        EXPECT_EQ(deallocated, 1);
        auto expected = makeExpired();
        EXPECT_FALSE(value.compare_exchange_strong(expected, {}));
        EXPECT_EQ(deallocated, 2);
        value.store(makeExpired());
        auto previous = value.exchange({});
        EXPECT_EQ(deallocated, 2);
        previous.reset();
        EXPECT_EQ(deallocated, 3);
    }

    struct TTracked {
        std::atomic<std::int32_t>& Destroyed;
        const std::int32_t Value = 42;

        explicit TTracked(std::atomic<std::int32_t>& destroyed)
            : Destroyed(destroyed)
        {}

        ~TTracked() {
            Destroyed.fetch_add(1, std::memory_order_relaxed);
        }
    };

    TEST(TAtomicSharedPtr, ConcurrentLifetime) {
        std::atomic<std::int32_t> destroyed{0};
        std::atomic<std::shared_ptr<TTracked>> shared;
        std::atomic<std::weak_ptr<TTracked>> weak;
        constexpr std::int32_t iterations = 5000;
        std::barrier start(4);
        std::vector<std::thread> threads;
        for (std::int32_t i = 0; i < 2; ++i) {
            threads.emplace_back([&] {
                start.arrive_and_wait();
                for (std::int32_t j = 0; j < iterations; ++j) {
                    auto owner = std::make_shared<TTracked>(destroyed);
                    shared.store(owner);
                    weak.store(owner);
                    shared.exchange(nullptr);
                }
            });
        }

        for (std::int32_t i = 0; i < 2; ++i) {
            threads.emplace_back([&] {
                start.arrive_and_wait();
                for (std::int32_t j = 0; j < iterations; ++j) {
                    if (auto ptr = shared.load()) {
                        EXPECT_EQ(ptr->Value, 42);
                    }

                    auto expected = weak.load();
                    if (auto ptr = expected.lock()) {
                        EXPECT_EQ(ptr->Value, 42);
                    }

                    weak.compare_exchange_weak(expected, {});
                }
            });
        }

        for (auto& thread : threads) {
            thread.join();
        }

        shared.store(nullptr);
        weak.store({});
        EXPECT_EQ(destroyed.load(), 2 * iterations);
    }
}
