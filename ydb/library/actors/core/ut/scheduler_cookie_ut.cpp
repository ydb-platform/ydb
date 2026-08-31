#include "../scheduler_cookie.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>

#include <atomic>
#include <thread>

using namespace NActors;

namespace {

    constexpr size_t CookieCount = 10000;

    struct TDetachState {
        std::atomic<size_t> Entered = 0;
        std::atomic<size_t> Returned = 0;
        std::atomic<bool> Destroyed = false;
    };

    class TObservedSchedulerCookie final : public ISchedulerCookie {
        TDetachState& State;

    public:
        explicit TObservedSchedulerCookie(TDetachState& state)
            : ISchedulerCookie(2)
            , State(state)
        {}

        ~TObservedSchedulerCookie() override {
            State.Destroyed.store(true, std::memory_order_release);
        }

        bool DetachImpl() noexcept override {
            if (State.Entered.fetch_add(1, std::memory_order_acq_rel) == 0) {
                while (State.Returned.load(std::memory_order_acquire) == 0) {
                    std::this_thread::yield();
                }
                UNIT_ASSERT(!State.Destroyed.load(std::memory_order_acquire));
            }
            return false;
        }

        bool DetachEventImpl() noexcept override {
            Y_ABORT();
        }

        bool IsArmed() noexcept override {
            return true;
        }
    };

    void WaitForParticipants(std::atomic<size_t>& ready, size_t expected) {
        ready.fetch_add(1, std::memory_order_acq_rel);
        while (ready.load(std::memory_order_acquire) < expected) {
            std::this_thread::yield();
        }
    }

}

Y_UNIT_TEST_SUITE(SchedulerCookie) {
    Y_UNIT_TEST(WaitsForConcurrentDetachImpl) {
        TDetachState state;
        auto* cookie = new TObservedSchedulerCookie(state);

        auto detach = [&] {
            cookie->Detach();
            state.Returned.fetch_add(1, std::memory_order_release);
        };

        std::thread first(detach);
        std::thread second(detach);
        first.join();
        second.join();

        UNIT_ASSERT(state.Destroyed.load(std::memory_order_acquire));
    }

    Y_UNIT_TEST(ConcurrentDetach2Way) {
        TVector<ISchedulerCookie*> cookies(CookieCount);
        for (auto& cookie : cookies) {
            cookie = ISchedulerCookie::Make2Way();
        }

        std::atomic<size_t> ready = 0;
        TVector<ui8> firstResults(CookieCount);
        TVector<ui8> secondResults(CookieCount);

        std::thread first([&] {
            for (size_t i = 0; i < CookieCount; ++i) {
                WaitForParticipants(ready, 2 * (i + 1));
                firstResults[i] = cookies[i]->Detach();
            }
        });
        std::thread second([&] {
            for (size_t i = 0; i < CookieCount; ++i) {
                WaitForParticipants(ready, 2 * (i + 1));
                secondResults[i] = cookies[i]->Detach();
            }
        });

        first.join();
        second.join();

        for (size_t i = 0; i < CookieCount; ++i) {
            UNIT_ASSERT_VALUES_UNEQUAL(firstResults[i], secondResults[i]);
        }
    }

    Y_UNIT_TEST(ConcurrentDetach3Way) {
        TVector<ISchedulerCookie*> cookies(CookieCount);
        for (auto& cookie : cookies) {
            cookie = ISchedulerCookie::Make3Way();
        }

        std::atomic<size_t> ready = 0;
        TVector<ui8> firstResults(CookieCount);
        TVector<ui8> secondResults(CookieCount);
        TVector<ui8> eventResults(CookieCount);

        std::thread first([&] {
            for (size_t i = 0; i < CookieCount; ++i) {
                WaitForParticipants(ready, 3 * (i + 1));
                firstResults[i] = cookies[i]->Detach();
            }
        });
        std::thread second([&] {
            for (size_t i = 0; i < CookieCount; ++i) {
                WaitForParticipants(ready, 3 * (i + 1));
                secondResults[i] = cookies[i]->Detach();
            }
        });
        std::thread event([&] {
            for (size_t i = 0; i < CookieCount; ++i) {
                WaitForParticipants(ready, 3 * (i + 1));
                eventResults[i] = cookies[i]->DetachEvent();
            }
        });

        first.join();
        second.join();
        event.join();

        for (size_t i = 0; i < CookieCount; ++i) {
            UNIT_ASSERT(firstResults[i] + secondResults[i] + eventResults[i] <= 2);
        }
    }
}
