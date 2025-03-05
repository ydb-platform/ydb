#include "spinparkpad.h"

#include <util/system/platform.h>
#include <util/system/spinlock.h>
#include <util/system/yassert.h>
#include <atomic>
#include <new>

#ifdef _linux_

#include "futex.h"

namespace NActors {

    struct TSpinParkPad::TImpl {
        // Bit 0: waiting flag
        // Bit 1: interrupted flag
        // Bit 2..31: the number of Unpark calls
        int Value = 0;

        static constexpr int FlagWaiting = 1;
        static constexpr int FlagInterrupted = 2;
        static constexpr int UnparkIncrement = 4;

        bool Park() noexcept {
            // Fast path: assume not interrupted and single Unpark() already called
            int current = UnparkIncrement;
            if (__atomic_compare_exchange_n(&Value, &current, 0, false, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE)) {
                return false;
            }

            // Fast path: spin, assuming Unpark() will be called very soon
            for (int i = 0; i < 64; ++i) {
                Y_DEBUG_ABORT_UNLESS(current >= 0, "Unexpected negative value");
                Y_DEBUG_ABORT_UNLESS(!(current & FlagWaiting), "Another thread is already waiting");

                if (current >= UnparkIncrement) {
                    do {
                        if (__atomic_compare_exchange_n(&Value, &current, current - UnparkIncrement, true, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE)) {
                            return false;
                        }
                    } while (current >= UnparkIncrement);
                } else if (current == FlagInterrupted) {
                    return true;
                } else {
                    if (i >= 32) {
                        SpinLockPause();
                    }
                    current = __atomic_load_n(&Value, __ATOMIC_ACQUIRE);
                }
            }

            // Try to set FlagWaiting or consume an unpark attempt
            for (;;) {
                Y_DEBUG_ABORT_UNLESS(current >= 0, "Unexpected negative value");
                Y_DEBUG_ABORT_UNLESS(!(current & FlagWaiting), "Another thread is already waiting");

                if (current >= UnparkIncrement) {
                    if (__atomic_compare_exchange_n(&Value, &current, current - UnparkIncrement, true, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE)) {
                        return false;
                    }
                    continue;
                }

                if (current == FlagInterrupted) {
                    return true;
                }

                if (__atomic_compare_exchange_n(&Value, &current, current | FlagWaiting, true, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE)) {
                    current |= FlagWaiting;
                    break;
                }

                if (current == FlagInterrupted) {
                    return true;
                }
            }

            // Now we just keep trying to unpark while waiting on a futex
            for (;;) {
                Y_DEBUG_ABORT_UNLESS(current >= 0 && (current & FlagWaiting));

                if (current >= UnparkIncrement) {
                    if (__atomic_compare_exchange_n(&Value, &current, current - UnparkIncrement - FlagWaiting, true, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE)) {
                        return false;
                    }
                    continue;
                }

                if (current & FlagInterrupted) {
                    if (__atomic_compare_exchange_n(&Value, &current, current - FlagWaiting, true, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE)) {
                        return true;
                    }
                    continue;
                }

                SysFutex(&Value, FUTEX_WAIT_PRIVATE, current, nullptr, nullptr, 0);
                current = __atomic_load_n(&Value, __ATOMIC_ACQUIRE);
            }
        }

        void Unpark() noexcept {
            int prev = __atomic_fetch_add(&Value, UnparkIncrement, __ATOMIC_RELEASE);
            if (prev & FlagWaiting) {
                SysFutex(&Value, FUTEX_WAKE_PRIVATE, 1, nullptr, nullptr, 0);
            }
        }

        void Interrupt() noexcept {
            int prev = __atomic_fetch_or(&Value, FlagInterrupted, __ATOMIC_RELEASE);
            if ((prev & FlagWaiting) && !(prev & FlagInterrupted)) {
                SysFutex(&Value, FUTEX_WAKE_PRIVATE, -1, nullptr, nullptr, 0);
            }
        }
    };

} // namespace NActors

#else

namespace NActors {

    struct TSpinParkPad::TImpl {
#if __cpp_lib_atomic_lock_free_type_aliases     >= 201907L
        using atomic_type = std::atomic_signed_lock_free;
#elif defined(__linux__)
        // Linux uses int for a futex
        using atomic_type = std::atomic<int>;
#else
        // Asume other OSes use int64 for now
        using atomic_type = std::atomic<int64_t>;
#endif
        using value_type = typename atomic_type::value_type;

        atomic_type Value{ 0 };

        static constexpr value_type FlagWaiting = 1;
        static constexpr value_type FlagInterrupted = 2;
        static constexpr value_type UnparkIncrement = 4;

        bool Park() noexcept {
            // Fast path: assume not interrupted and a single Unpark() already called
            value_type current = UnparkIncrement;
            if (Value.compare_exchange_strong(current, 0, std::memory_order_acquire)) {
                return false;
            }

            // TODO: implement spinning for the generic implementation
            // Note: underlying implementations of atomic wait also spin unfortunately

            // Try to set FlagWaiting or consume an unpark attempt
            for (;;) {
                Y_DEBUG_ABORT_UNLESS(current >= 0, "Unexpected negative value");
                Y_DEBUG_ABORT_UNLESS(!(current & FlagWaiting), "Another thread is already waiting");

                if (current >= UnparkIncrement) {
                    if (Value.compare_exchange_weak(current, current - UnparkIncrement, std::memory_order_acquire)) {
                        return false;
                    }
                    continue;
                }

                if (current == FlagInterrupted) {
                    return true;
                }

                if (Value.compare_exchange_weak(current, current | FlagWaiting, std::memory_order_acquire)) {
                    current |= FlagWaiting;
                    break;
                }
            }

            // Now we just keep trying to unpark while waiting on a futex
            for (;;) {
                Y_DEBUG_ABORT_UNLESS(current >= 0 && (current & FlagWaiting));

                if (current >= UnparkIncrement) {
                    if (Value.compare_exchange_weak(current, current - UnparkIncrement - FlagWaiting, std::memory_order_acquire)) {
                        return false;
                    }
                    continue;
                }

                if (current & FlagInterrupted) {
                    if (Value.compare_exchange_weak(current, current - FlagWaiting, std::memory_order_acquire)) {
                        return true;
                    }
                    continue;
                }

                Value.wait(current, std::memory_order_acquire);
                current = Value.load(std::memory_order_acquire);
            }
        }

        void Unpark() noexcept {
            value_type prev = Value.fetch_add(UnparkIncrement, std::memory_order_release);
            if (prev & FlagWaiting) {
                Value.notify_one();
            }
        }

        void Interrupt() noexcept {
            value_type prev = Value.fetch_or(FlagInterrupted, std::memory_order_release);
            if ((prev & FlagWaiting) && !(prev & FlagInterrupted)) {
                Value.notify_all();
            }
        }
    };

} // namespace NActors

#endif

namespace NActors {

    TSpinParkPad::TSpinParkPad() {
        static_assert(sizeof(TImpl) <= 8);
        static_assert(alignof(TImpl) <= 8);
        ::new (Data) TImpl();
    }

    TSpinParkPad::~TSpinParkPad() {
        Impl().~TImpl();
    }

    TSpinParkPad::TImpl& TSpinParkPad::Impl() {
        return *reinterpret_cast<TImpl*>(Data);
    }

    bool TSpinParkPad::Park() noexcept {
        return Impl().Park();
    }

    void TSpinParkPad::Unpark() noexcept {
        return Impl().Unpark();
    }

    void TSpinParkPad::Interrupt() noexcept {
        return Impl().Interrupt();
    }

} // namespace NActors
