#include "suite_runner.h"

#include "helpers.h"
#include "request_generator.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/model/public.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/logging.h>

namespace NCloud::NBlockStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

TSuiteRunner::TSuiteRunner(
        TAppContext& appContext,
        const TString& testName,
        TTestContext& testContext,
        CB finishCallBack)
    : AppContext(appContext)
    , LoggingTag(MakeLoggingTag(testName))
    , TestContext(testContext)
    , StartTime(Now())
    , FinishCallBack(std::move(finishCallBack))
{
    TLogSettings logSettings;
    logSettings.FiltrationLevel = ELogPriority::TLOG_ERR;
    Logging = CreateLoggingService("console", logSettings);
}

void TSuiteRunner::StartSubtest(const NProto::TRangeTest& range)
{
    auto runner = CreateTestRunner(
        Logging,
        LoggingTag,
        CreateArtificialRequestGenerator(Logging, range),
        range.GetIoDepth(),
        TestContext.ShouldStop);

    RegisterSubtest(std::move(runner));
}

void TSuiteRunner::RegisterSubtest(ITestRunnerPtr runner)
{
    runner->Run().Subscribe([&] (const auto& future) mutable {
        const auto& testResults = future.GetValue();

        with_lock (TestContext.WaitMutex) {
            ++CompletedSubtests;

            if (Results.Status == NProto::TEST_STATUS_OK) {
                Results.Status = testResults->Status;
            }

            Results.RequestsCompleted += testResults->RequestsCompleted;

            Results.BlocksRead += testResults->BlocksRead;
            Results.BlocksWritten += testResults->BlocksWritten;
            Results.BlocksZeroed += testResults->BlocksZeroed;

            Results.ReadHist.Add(testResults->ReadHist);
            Results.WriteHist.Add(testResults->WriteHist);
            Results.ZeroHist.Add(testResults->ZeroHist);

            switch (testResults->Status) {
                case NProto::TEST_STATUS_FAILURE:
                    AppContext.FailedTests.fetch_add(1);
                    break;
                case NProto::TEST_STATUS_EXPECTED_ERROR:
                    StopTest(TestContext);
                    break;
                default:
                    break;
            }

            TestContext.WaitCondVar.Signal();
            TestContext.Finished.store(true, std::memory_order_release);
        }
        //FinishCallBack();
    });

    Subtests.push_back(std::move(runner));
}

void TSuiteRunner::Wait(ui64 duration)
{
    //Y_ABORT("TSuiteRunner::Wait is temporarily unsupported");
    TInstant expectedEnd = TInstant::Max();
    if (duration) {
        expectedEnd = StartTime + TDuration::Seconds(duration);
    }

    with_lock (TestContext.WaitMutex) {
        while (CompletedSubtests < Subtests.size()) {
            TestContext.WaitCondVar.WaitD(
                TestContext.WaitMutex, expectedEnd);
            if (TInstant::Now() > expectedEnd
                    && !AppContext.ShouldStop.load(std::memory_order_acquire))
            {
                StopTest(TestContext);
            }
        }
    }
}

}   // namespace NCloud::NBlockStore::NLoadTest
