#include "spinparkpad.h"

#include <library/cpp/testing/unittest/registar.h>
#include <atomic>
#include <deque>
#include <mutex>
#include <condition_variable>
#include <thread>

namespace NActors {

Y_UNIT_TEST_SUITE(SpinParkPad) {

    template<class T>
    class TSlowQueue {
    public:
        void Push(T item) {
            std::unique_lock g(Lock);
            Items.push_back(std::move(item));
            NotEmpty.notify_one();
        }

        T Pop() {
            std::unique_lock g(Lock);
            while (Items.empty()) {
                NotEmpty.wait(g);
            }
            T item = std::move(Items.front());
            Items.pop_front();
            return item;
        }

    private:
        std::mutex Lock;
        std::condition_variable NotEmpty;
        std::deque<T> Items;
    };

    Y_UNIT_TEST(FastParkUnparkMemoryOrder) {
        std::atomic<TSpinParkPad*> padPtr{ nullptr };
        int value = 0;

        std::thread t([&]{
            TSpinParkPad pad;
            Y_ABORT_UNLESS(value == 0);
            value = 1;
            padPtr.store(&pad, std::memory_order_release);
            pad.Park();
            // Note: Unpark() must happen before Park() with release-acquire memory order
            Y_ABORT_UNLESS(value == 2);
            value = 3;
            padPtr.store(&pad, std::memory_order_release);
            pad.Park();
            // Note: Unpark() must happen before Park() with release-acquire memory order
            Y_ABORT_UNLESS(value == 4);
        });

        for (;;) {
            TSpinParkPad* pad = padPtr.load(std::memory_order_acquire);
            if (!pad) continue;
            Y_ABORT_UNLESS(value == 1);
            padPtr.store(nullptr, std::memory_order_relaxed);
            value = 2;
            pad->Unpark();
            break;
        }

        for (;;) {
            TSpinParkPad* pad = padPtr.load(std::memory_order_acquire);
            if (!pad) continue;
            Y_ABORT_UNLESS(value == 3);
            padPtr.store(nullptr, std::memory_order_relaxed);
            value = 4;
            pad->Unpark();
            break;
        }

        t.join();
    }

    Y_UNIT_TEST(SlowParkUnparkMemoryOrder) {
        TSlowQueue<TSpinParkPad*> q;
        int value = 0;

        std::thread t([&]{
            TSpinParkPad pad;
            Y_ABORT_UNLESS(value == 0);
            value = 1;
            q.Push(&pad);
            pad.Park();
            // Note: Unpark() must happen before Park() with release-acquire memory order
            Y_ABORT_UNLESS(value == 2);
            value = 3;
            q.Push(&pad);
            pad.Park();
            // Note: Unpark() must happen before Park() with release-acquire memory order
            Y_ABORT_UNLESS(value == 4);
        });

        if (TSpinParkPad* pad = q.Pop()) {
            Y_ABORT_UNLESS(value == 1);
            value = 2;
            pad->Unpark();
        }

        if (TSpinParkPad* pad = q.Pop()) {
            Y_ABORT_UNLESS(value == 3);
            value = 4;
            pad->Unpark();
        }

        t.join();
    }

    Y_UNIT_TEST(UnparkThenInterrupt) {
        TSpinParkPad pad;

        std::thread t([&]{
            bool interrupted;
            interrupted = pad.Park();
            Y_ABORT_UNLESS(!interrupted);
            interrupted = pad.Park();
            Y_ABORT_UNLESS(interrupted);
            interrupted = pad.Park();
            Y_ABORT_UNLESS(interrupted);
        });

        pad.Unpark();
        pad.Interrupt();
        t.join();
    }

}

} // namespace NActors
