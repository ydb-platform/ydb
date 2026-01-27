#pragma once

#include "app_context.h"
#include "test_runner.h"

#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/public.h>
#include <ydb/core/protos/load_test.pb.h>
#include <ydb/core/protos/nbs2_load.pb.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

class TSuiteRunner: public TThrRefBase
{
public:
    using CB = std::function<void()>;

private:
    TAppContext& AppContext;
    ILoggingServicePtr Logging;
    TString LoggingTag;
    TTestContext& TestContext;

    TInstant StartTime;
    TTestResults Results;
    TVector<ITestRunnerPtr> Subtests;
    size_t CompletedSubtests = 0;
    CB FinishCallBack;

public:
    TSuiteRunner(
        TAppContext& appContext,
        const TString& testName,
        TTestContext& testContext,
        CB cb = nullptr);

public:
    TInstant GetStartTime() const
    {
        return StartTime;
    }

    const TTestResults& GetResults() const
    {
        return Results;
    }

    void StartSubtest(const NProto::TRangeTest& range);
    void Wait(ui64 duration);

private:
    void RegisterSubtest(ITestRunnerPtr runner);
};

}   // namespace NCloud::NBlockStore::NLoadTest
