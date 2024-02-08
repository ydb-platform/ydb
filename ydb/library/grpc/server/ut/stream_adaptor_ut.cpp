#include <ydb/library/grpc/server/grpc_request.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/system/thread.h>
#include <util/thread/pool.h>

using namespace NYdbGrpc;

// Here we emulate stream data producer
class TOrderedProducer: public TThread {
public:
    TOrderedProducer(IStreamAdaptor* adaptor, ui64 max, bool withSleep, std::function<void(ui64)>&& consumerOp)
        : TThread(&ThreadProc, this)
        , Adaptor_(adaptor)
        , Max_(max)
        , WithSleep_(withSleep)
        , ConsumerOp_(std::move(consumerOp))
    {}

    static void* ThreadProc(void* _this) {
        SetCurrentThreadName("OrderedProducerThread");
        static_cast<TOrderedProducer*>(_this)->Exec();
        return nullptr;
    }

    void Exec() {
        for (ui64 i = 0; i < Max_; i++) {
            auto cb = [i, this]() mutable {
                ConsumerOp_(i);
            };
            Adaptor_->Enqueue(std::move(cb), false);
            if (WithSleep_ && (i % 256 == 0)) {
                Sleep(TDuration::MilliSeconds(10));
            }
        }
    }

private:
    IStreamAdaptor* Adaptor_;
    const ui64 Max_;
    const bool WithSleep_;
    std::function<void(ui64)> ConsumerOp_;
};

Y_UNIT_TEST_SUITE(StreamAdaptor) {
    static void OrderingTest(size_t threads, bool withSleep) {

        auto adaptor = CreateStreamAdaptor();

        const i64 max = 10000;

        // Here we will emulate grpc stream (NextReply call after writing)
        std::unique_ptr<IThreadPool> consumerQueue(new TThreadPool(TThreadPool::TParams().SetBlocking(false).SetCatching(false)));
        // And make sure only one request inflight (see UNIT_ASSERT on adding to the queue)
        consumerQueue->Start(threads, 1);

        // Non atomic!!! Stream adaptor must protect us
        ui64 curVal = 0;

        // Used just to wait in the main thread
        TAtomic finished = false;
        auto consumerOp = [&finished, &curVal, ptr{adaptor.get()}, queue{consumerQueue.get()}](ui64 i) {
            // Check no reordering inside stream adaptor
            // and no simultanious consumer Op call
            UNIT_ASSERT_VALUES_EQUAL(curVal, i);
            curVal++;
            // We must set finished flag after last ProcessNext, but we can`t compare curVal and max after ProcessNext
            // so compare here and set after
            bool tmp = curVal == max;
            bool res = queue->AddFunc([ptr, &finished, tmp, &curVal, i]() {
                // Additional check the value still same
                // run under tsan makes sure no consumer Op call before we call ProcessNext
                UNIT_ASSERT_VALUES_EQUAL(curVal, i + 1);
                ptr->ProcessNext();
                // Reordering after ProcessNext is possible, so check tmp and set finished to true
                if (tmp)
                    AtomicSet(finished, true);
            });
            UNIT_ASSERT(res);
        };

        TOrderedProducer producer(adaptor.get(), max, withSleep, std::move(consumerOp));

        producer.Start();
        producer.Join();

        while (!AtomicGet(finished))
        {
            Sleep(TDuration::MilliSeconds(100));
        }

        consumerQueue->Stop();

        UNIT_ASSERT_VALUES_EQUAL(curVal, max);
    }

    Y_UNIT_TEST(OrderingOneThread) {
        OrderingTest(1, false);
    }

    Y_UNIT_TEST(OrderingTwoThreads) {
        OrderingTest(2, false);
    }

    Y_UNIT_TEST(OrderingManyThreads) {
        OrderingTest(10, false);
    }

    Y_UNIT_TEST(OrderingOneThreadWithSleep) {
        OrderingTest(1, true);
    }

    Y_UNIT_TEST(OrderingTwoThreadsWithSleep) {
        OrderingTest(2, true);
    }

    Y_UNIT_TEST(OrderingManyThreadsWithSleep) {
        OrderingTest(10, true);
    }
}
