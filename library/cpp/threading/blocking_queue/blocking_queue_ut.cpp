#include "blocking_queue.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>
#include <util/system/thread.h>

namespace {
    class TFunctionThread: public ISimpleThread {
    public:
        using TFunc = std::function<void()>;

    private:
        TFunc Func;

    public:
        TFunctionThread(const TFunc& func)
            : Func(func)
        {
        }

        void* ThreadProc() noexcept override {
            Func();
            return nullptr;
        }
    };

}

IOutputStream& operator<<(IOutputStream& o, const TMaybe<int>& val) {
    if (val) {
        o << "TMaybe<int>(" << val.GetRef() << ')';
    } else {
        o << "TMaybe<int>()";
    }
    return o;
}

Y_UNIT_TEST_SUITE(BlockingQueueTest) {
    Y_UNIT_TEST(SimplePushPopTest) {
        const size_t limit = 100;

        NThreading::TBlockingQueue<int> queue(100);

        for (int i = 0; i != limit; ++i) {
            queue.Push(i);
        }

        for (int i = 0; i != limit; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(queue.Pop(), i);
        }

        UNIT_ASSERT(queue.Empty());
    }

    Y_UNIT_TEST(SimpleStopTest) {
        const size_t limit = 100;

        NThreading::TBlockingQueue<int> queue(100);

        for (int i = 0; i != limit; ++i) {
            queue.Push(i);
        }
        queue.Stop();

        bool ok = queue.Push(100500);
        UNIT_ASSERT_VALUES_EQUAL(ok, false);

        for (int i = 0; i != limit; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(queue.Pop(), i);
        }

        UNIT_ASSERT_VALUES_EQUAL(queue.Pop(), TMaybe<int>());
    }

    Y_UNIT_TEST(BigPushPop) {
        const int limit = 100000;

        NThreading::TBlockingQueue<int> queue(10);

        TFunctionThread pusher([&] {
            for (int i = 0; i != limit; ++i) {
                if (!queue.Push(i)) {
                    break;
                }
            }
        });

        pusher.Start();

        try {
            for (int i = 0; i != limit; ++i) {
                size_t size = queue.Size();
                UNIT_ASSERT_C(size <= 10, (TStringBuilder() << "Size exceeds 10: " << size).data());
                UNIT_ASSERT_VALUES_EQUAL(queue.Pop(), i);
            }
        } catch (...) {
            // gracefull shutdown of pusher thread if assertion fails
            queue.Stop();
            throw;
        }

        pusher.Join();
    }

    Y_UNIT_TEST(StopWhenMultiplePoppers) {
        NThreading::TBlockingQueue<int> queue(10);
        TFunctionThread popper1([&] {
            UNIT_ASSERT_VALUES_EQUAL(queue.Pop(), TMaybe<int>());
        });
        TFunctionThread popper2([&] {
            UNIT_ASSERT_VALUES_EQUAL(queue.Pop(), TMaybe<int>());
        });
        popper1.Start();
        popper2.Start();

        queue.Stop();

        popper1.Join();
        popper2.Join();
    }

    Y_UNIT_TEST(StopWhenMultiplePushers) {
        NThreading::TBlockingQueue<int> queue(1);
        queue.Push(1);
        TFunctionThread pusher1([&] {
            UNIT_ASSERT_VALUES_EQUAL(queue.Push(2), false);
        });
        TFunctionThread pusher2([&] {
            UNIT_ASSERT_VALUES_EQUAL(queue.Push(2), false);
        });
        pusher1.Start();
        pusher2.Start();

        queue.Stop();

        pusher1.Join();
        pusher2.Join();
    }

    Y_UNIT_TEST(InterruptPopByDeadline) {
        NThreading::TBlockingQueue<int> queue1(10);
        NThreading::TBlockingQueue<int> queue2(10);

        const auto popper1DeadLine = TInstant::Now();
        const auto popper2DeadLine = TInstant::Now() + TDuration::Seconds(2);

        TFunctionThread popper1([&] {
            UNIT_ASSERT_VALUES_EQUAL(queue1.Pop(popper1DeadLine), TMaybe<int>());
            UNIT_ASSERT_VALUES_EQUAL(queue1.IsStopped(), false);
        });

        TFunctionThread popper2([&] {
            UNIT_ASSERT_VALUES_EQUAL(queue2.Pop(popper2DeadLine), 2);
            UNIT_ASSERT_VALUES_EQUAL(queue2.IsStopped(), false);
        });

        popper1.Start();
        popper2.Start();

        Sleep(TDuration::Seconds(1));

        queue1.Push(1);
        queue2.Push(2);

        Sleep(TDuration::Seconds(1));

        queue1.Stop();
        queue2.Stop();

        popper1.Join();
        popper2.Join();
    }

    Y_UNIT_TEST(InterruptPushByDeadline) {
        NThreading::TBlockingQueue<int> queue1(1);
        NThreading::TBlockingQueue<int> queue2(1);

        queue1.Push(0);
        queue2.Push(0);

        const auto pusher1DeadLine = TInstant::Now();
        const auto pusher2DeadLine = TInstant::Now() + TDuration::Seconds(2);

        TFunctionThread pusher1([&] {
            UNIT_ASSERT_VALUES_EQUAL(queue1.Push(1, pusher1DeadLine), false);
            UNIT_ASSERT_VALUES_EQUAL(queue1.IsStopped(), false);
        });

        TFunctionThread pusher2([&] {
            UNIT_ASSERT_VALUES_EQUAL(queue2.Push(2, pusher2DeadLine), true);
            UNIT_ASSERT_VALUES_EQUAL(queue2.IsStopped(), false);
        });

        pusher1.Start();
        pusher2.Start();

        Sleep(TDuration::Seconds(1));

        queue1.Pop();
        queue2.Pop();

        Sleep(TDuration::Seconds(1));

        queue1.Stop();
        queue2.Stop();

        pusher1.Join();
        pusher2.Join();
    }
}
