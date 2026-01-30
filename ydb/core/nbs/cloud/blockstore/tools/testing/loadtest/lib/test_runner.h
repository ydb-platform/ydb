#pragma once

#include "public.h"
#include <ydb/core/nbs/cloud/blockstore/tools/testing/loadtest/lib/protos/nbs2_load.pb.h>

#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/public.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/histogram.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

struct TTestResults
{
    NProto::ETestStatus Status = NProto::TEST_STATUS_OK;

    size_t RequestsCompleted = 0;

    size_t BlocksRead = 0;
    size_t BlocksWritten = 0;
    size_t BlocksZeroed = 0;

    TLatencyHistogram ReadHist;
    TLatencyHistogram WriteHist;
    TLatencyHistogram ZeroHist;
};

////////////////////////////////////////////////////////////////////////////////

struct ITestRunner
{
    virtual ~ITestRunner() = default;

    virtual void Start() = 0;
    virtual TInstant GetStartTime() const = 0;
    virtual const TTestResults& GetResults() const = 0;
    virtual void Stop() = 0;
    virtual bool IsFinished() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

ITestRunnerPtr CreateTestRunner(
    ILoggingServicePtr loggingService,
    TString loggingTag,
    IRequestGeneratorPtr requestGenerator,
    ui32 maxIoDepth,
    std::atomic<bool>& shouldStop,
    TLoadTestRequestCallbacks requestCallbacks,
    const void *udata);

}   // namespace NCloud::NBlockStore::NLoadTest
