#include <library/cpp/threading/light_rw_lock/lightrwlock.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/ptr.h>
#include <util/random/random.h>
#include <util/thread/pool.h>

class TRWMutexTest: public TTestBase {
    UNIT_TEST_SUITE(TRWMutexTest);
    UNIT_TEST(TestConcurrentReadAccess)
    UNIT_TEST(TestExclusiveWriteAccess)
    UNIT_TEST(TestSharedData)
    UNIT_TEST_SUITE_END();

    class TOneShotEvent {
    public:
        void Wait() {
            Released_.wait(false, std::memory_order_acquire);
        }

        void Release() {
            Released_.store(true, std::memory_order_release);
            Released_.notify_all();
        }

    private:
        std::atomic<bool> Released_{false};
    };

    struct TSharedData {
        TSharedData()
            : WritersIn(0)
            , ReadersIn(0)
            , Counter(0)
        {
        }

        std::atomic<ui32> WritersIn;
        std::atomic<ui32> ReadersIn;

        void IncWriters() {
            WritersIn.fetch_add(1, std::memory_order_relaxed);
        }

        void DecWriters() {
            WritersIn.fetch_sub(1, std::memory_order_relaxed);
        }

        ui32 LoadWriters() {
            return WritersIn.load(std::memory_order_relaxed);
        }

        void IncReaders() {
            ReadersIn.fetch_add(1, std::memory_order_relaxed);
        }

        void DecReaders() {
            ReadersIn.fetch_sub(1, std::memory_order_relaxed);
        }

        ui32 LoadReaders() {
            return ReadersIn.load(std::memory_order_relaxed);
        }

        std::atomic_flag Failed = ATOMIC_FLAG_INIT;

        void SetFailed() {
            Failed.test_and_set(std::memory_order_relaxed);
        }

        bool TestFailed() {
            return Failed.test(std::memory_order_relaxed);
        }

        ui64 Counter;

        TLightRWLock Mutex;
        TOneShotEvent Event;
    };

    class TThreadTask: public IObjectInQueue {
    public:
        using PFunc = void (TThreadTask::*)(void);

        TThreadTask(PFunc func, TSharedData& data, size_t id, size_t total)
            : Func_(func)
            , Data_(data)
            , Id_(id)
            , Total_(total)
        {
        }

        void Process(void*) override {
            THolder<TThreadTask> This(this);

            (this->*Func_)();
        }

#define FAIL_ASSERT(cond)  \
    if (!(cond)) {         \
        Data_.SetFailed(); \
    }
        void RunConcurrentReadAccess() {
            Data_.Mutex.AcquireRead();

            Data_.IncReaders();
            if (Data_.LoadReaders() != Total_) {
                Data_.Event.Wait();
            }
            Data_.Event.Release();
            Data_.DecReaders();

            Data_.Mutex.ReleaseRead();
        }

        void RunExclusiveWriteAccess() {
            if (Id_ % 2 == 0) {
                for (size_t i = 0; i < 10; ++i) {
                    Data_.Mutex.AcquireRead();

                    Data_.IncReaders();
                    FAIL_ASSERT(Data_.LoadWriters() == 0);
                    usleep(RandomNumber<ui32>() % 5);
                    Data_.DecReaders();

                    Data_.Mutex.ReleaseRead();
                }
            } else {
                for (size_t i = 0; i < 10; ++i) {
                    Data_.Mutex.AcquireWrite();

                    Data_.IncWriters();
                    FAIL_ASSERT(Data_.LoadReaders() == 0 && Data_.LoadWriters() == 1);
                    usleep(RandomNumber<ui32>() % 5);
                    Data_.DecWriters();

                    Data_.Mutex.ReleaseWrite();
                }
            }
        }

        void RunSharedData() {
            if (Id_ % 2 == 0) {
                ui64 localCounter = 0;
                Y_UNUSED(localCounter);
                for (size_t i = 0; i < 1000; ++i) {
                    Data_.Mutex.AcquireRead();
                    localCounter = Data_.Counter;
                    Data_.Mutex.ReleaseRead();
                }
            } else {
                for (size_t i = 0; i < 1000; ++i) {
                    Data_.Mutex.AcquireWrite();
                    ++Data_.Counter;
                    Data_.Mutex.ReleaseWrite();
                }
            }
        }
#undef FAIL_ASSERT

    private:
        PFunc Func_;
        TSharedData& Data_;
        size_t Id_;
        size_t Total_;
    };

private:
#define RUN_CYCLE(what, count)                                                      \
    Data_.Reset(MakeHolder<TSharedData>());                                         \
    Q_.Start(count);                                                                \
    for (size_t i = 0; i < count; ++i) {                                            \
        UNIT_ASSERT(Q_.Add(new TThreadTask(&TThreadTask::what, *Data_, i, count))); \
    }                                                                               \
    Q_.Stop();                                                                      \
    UNIT_ASSERT(!Data_->TestFailed());

    void TestConcurrentReadAccess() {
        RUN_CYCLE(RunConcurrentReadAccess, 5);
    }

    void TestExclusiveWriteAccess() {
        RUN_CYCLE(RunExclusiveWriteAccess, 4);
    }

    void TestSharedData() {
        // TODO: Fix Tsan error
        // RUN_CYCLE(RunSharedData, 4);
    }

#undef RUN_CYCLE
private:
    THolder<TSharedData> Data_;
    TThreadPool Q_;
};

UNIT_TEST_SUITE_REGISTRATION(TRWMutexTest)
