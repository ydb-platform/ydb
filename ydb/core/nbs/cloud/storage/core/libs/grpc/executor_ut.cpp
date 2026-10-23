#include "executor.h"

#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/gmock_in_unittest/gmock.h>
#include <library/cpp/testing/unittest/registar.h>

using ::testing::_;
using ::testing::AtLeast;
using ::testing::DoAll;
using ::testing::Expectation;
using ::testing::Return;
using ::testing::SetArgPointee;

namespace NYdb::NBS::NGrpc {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TRequestHandlerMock: public TRequestHandlerBase
{
    MOCK_METHOD(void, Process, (bool));
    MOCK_METHOD(void, Cancel, ());
};

// WARNING: created only to be used with TExecutor for testing purposes
struct TCompletionQueueMock
{
    MOCK_METHOD(void, Shutdown, ());
    MOCK_METHOD(bool, Next, (void** tag, bool* ok));
};

struct TRequestsInFlightPlaceHolder
{
    using TRequestHanlder = TRequestHandlerMock;
    MOCK_METHOD(void, Shutdown, ());
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TExecutorTest)
{
    Y_UNIT_TEST(MustStopCallingHandlerOnShutdown)
    {
        ILoggingServicePtr logging = CreateLoggingService("console");
        auto Log = logging->CreateLog("NFS_SERVER");

        TExecutor<TCompletionQueueMock, TRequestsInFlightPlaceHolder> executor(
            "executor test",
            std::make_unique<TCompletionQueueMock>(),
            Log);

        auto& cq = *executor.CompletionQueue.get();

        TRequestHandlerMock handler;

        std::atomic_bool processCalled = false;
        EXPECT_CALL(handler, Process(_))
            .WillRepeatedly(
                [&]
                {
                    handler.AcquireCompletionTag();
                    if (!processCalled.load()) {
                        processCalled.store(true);
                        processCalled.notify_one();
                    }
                });

        void* handlerPointer = static_cast<void*>(&handler);
        EXPECT_CALL(cq, Next(_, _))
            .WillRepeatedly(DoAll(
                SetArgPointee<0>(handlerPointer),
                testing::Invoke(
                    []
                    {
                        static int counter = 0;
                        // we return `true` 10 times to emulate usual flushing
                        // once `Shutdown()` is being called, then we return
                        // `false` to finish shutdown call
                        return counter++ < 10;
                    })));

        executor.Start();
        processCalled.wait(false);

        EXPECT_CALL(cq, Shutdown());

        EXPECT_CALL(executor.RequestsInFlight, Shutdown());
        executor.Shutdown();
    }
}

}   // namespace NYdb::NBS::NGrpc
