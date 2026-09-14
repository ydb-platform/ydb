#include "parallel.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/mutex.h>
#include <util/thread/pool.h>

using namespace NLsp;

Y_UNIT_TEST_SUITE(ParallelConsumerTests) {

class TVectorConsumer final: public IConsumer<int> {
public:
    void Receive(int value) override {
        Sleep(TDuration::MilliSeconds(200));

        with_lock (Mutex_) {
            if (IsStopped_) {
                return;
            }

            Values_.push_back(value);
        }
    }

    void Stop() override {
        with_lock (Mutex_) {
            IsStopped_ = true;
        }
    }

    TVector<int>& Values() {
        with_lock (Mutex_) {
            return Values_;
        }
    }

private:
    TMutex Mutex_;
    TVector<int> Values_;
    bool IsStopped_ = false;
};

Y_UNIT_TEST(OnlyOnceDelivery) {
    auto c = MakeIntrusive<TVectorConsumer>();

    {
        auto pool = CreateThreadPool(/*threadCount=*/4);
        auto isPure = [](auto) { return true; };
        auto p = Parallel<int>(std::move(pool), isPure, c);

        p->Receive(1);
        p->Receive(2);
        p->Receive(3);
        p->Stop();
    }

    auto vs = c->Values();
    Sort(vs);
    UNIT_ASSERT_VALUES_EQUAL(vs, (TVector<int>{1, 2, 3}));
}

} // Y_UNIT_TEST_SUITE(ParallelConsumerTests)
