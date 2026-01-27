#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/public.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/histogram.h>
#include <ydb/core/protos/load_test.pb.h>
#include <ydb/core/protos/nbs2_load.pb.h>

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

    virtual NThreading::TFuture<TTestResultsPtr> Run() = 0;
};

////////////////////////////////////////////////////////////////////////////////

ITestRunnerPtr CreateTestRunner(
    ILoggingServicePtr loggingService,
    TString loggingTag,
    IRequestGeneratorPtr requestGenerator,
    ui32 maxIoDepth,
    std::atomic<bool>& shouldStop);

}   // namespace NCloud::NBlockStore::NLoadTest
