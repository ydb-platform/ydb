#include "test_runner.h"

#include "buffer_pool.h"
#include "request_generator.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/thread.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/datetime/base.h>
#include <util/generic/deque.h>
#include <util/generic/vector.h>
#include <util/random/random.h>
#include <util/string/builder.h>

#include <atomic>

namespace NCloud::NBlockStore::NLoadTest {

using namespace NThreading;

//using namespace NCloud::NBlockStore::NClient;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TCompletedRequest
{
    EBlockStoreRequest RequestType;
    TBlockRange64 BlockRange;
    NYdb::NBS::NProto::TError Error;
    TDuration Elapsed;

    TCompletedRequest(
            EBlockStoreRequest requestType,
            const TBlockRange64& blockRange,
            const NYdb::NBS::NProto::TError& error,
            TDuration elapsed)
        : RequestType(requestType)
        , BlockRange(blockRange)
        , Error(error)
        , Elapsed(elapsed)
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TTestRunner final
    : public ITestRunner
    , public std::enable_shared_from_this<TTestRunner>
{
private:
    TLog Log;
    TString LoggingTag;
    IRequestGeneratorPtr Requests;
    ui32 MaxIoDepth;

    TInstant StartTs;

    std::atomic<bool>& ShouldStop;
    LoadTestSendRequestCallbacks RequestCallbacks;

    ui64 RequestsSent = 0;
    ui64 RequestsCompleted = 0;
    ui32 CurrentIoDepth = 0;

    TInstant LastReportTs;
    ui64 LastRequestsCompleted = 0;
    TTestResults TestResults;

    const void *Udata = nullptr;

public:
    TTestRunner(
            ILoggingServicePtr loggingService,
            TString loggingTag,
            IRequestGeneratorPtr requests,
            ui32 maxIoDepth,
            std::atomic<bool>& shouldStop,
            LoadTestSendRequestCallbacks requestCallbacks,
            const void *udata)
        : Log(loggingService->CreateLog(requests->Describe()))
        , LoggingTag(std::move(loggingTag))
        , Requests(std::move(requests))
        , MaxIoDepth(maxIoDepth)
        , ShouldStop(shouldStop)
        , RequestCallbacks(std::move(requestCallbacks))
    {
        Udata = udata;
    }

    void Start() override;
    void Stop() override;
    bool IsFinished()  const override;

    TInstant GetStartTime() const override;
    const TTestResults& GetResults() const override;

private:
    void SendAvailableRequests();
    bool SendNextRequest();
    void SendReadRequest(const TBlockRange64& range);
    void SendWriteRequest(const TBlockRange64& range);
    void SendZeroRequest(const TBlockRange64& range);

    void HandleCompletedRequest(
        EBlockStoreRequest requestType,
        const TBlockRange64& range,
        const NYdb::NBS::NProto::TError& error,
        TDuration elapsed
    );
    bool StopRequested() const;
    bool CheckSendRequestCondition() const;
    bool CheckExitCondition() const;

    void ProcessCompletedRequests(std::unique_ptr<TCompletedRequest> request);

    void ReportProgress();
};

////////////////////////////////////////////////////////////////////////////////

TInstant TTestRunner::GetStartTime() const
{
    return StartTs;
}

const TTestResults& TTestRunner::GetResults() const
{
    return TestResults;
}

void TTestRunner::SendAvailableRequests()
{
    while (CheckSendRequestCondition() && SendNextRequest()) {
        ++RequestsSent;
    }
}

void TTestRunner::Start()
{
    StartTs = Now();
    LastReportTs = Now();

    SendAvailableRequests();
}

void TTestRunner::Stop()
{
    ShouldStop = true;
}

bool TTestRunner::IsFinished() const {
    return CheckExitCondition();
}

bool TTestRunner::SendNextRequest()
{
    if (MaxIoDepth && CurrentIoDepth >= MaxIoDepth) {
        return false;
    }

    TRequest request;
    if (!Requests->Next(&request)) {
        return false;
    }

    ++CurrentIoDepth;

    switch (request.RequestType) {
        case EBlockStoreRequest::ReadBlocks:
            SendReadRequest(request.BlockRange);
            break;

        case EBlockStoreRequest::WriteBlocks:
            SendWriteRequest(request.BlockRange);
            break;

        case EBlockStoreRequest::ZeroBlocks:
            SendZeroRequest(request.BlockRange);
            break;

        default:
            STORAGE_ERROR(LoggingTag
                << "unexpected request type: "
                << request.RequestType << ". Test has been stopped");
            Stop();
    }

    return true;
}
void TTestRunner::SendReadRequest(const TBlockRange64& range)
{
    STORAGE_DEBUG(LoggingTag
        << "Sending ReadBlocks request: " << range);

    auto started = TInstant::Now();

    auto cb = [started, range, this, p=shared_from_this()] (NYdb::NBS::NProto::TError result, const void* udata) mutable {
        STORAGE_DEBUG(LoggingTag
                << "TTestRunner::SendReadRequest cb for range: " << range
                << ", result: " << NYdb::NBS::FormatError(result));
        Udata = udata;
        if (FAILED(result.GetCode())) {
            STORAGE_ERROR(LoggingTag
                << "ReadBlocks request failed with error: "
                << NYdb::NBS::FormatError(result));
        }
        p->HandleCompletedRequest(
            EBlockStoreRequest::ReadBlocks,
            range,
            result,
            TInstant::Now() - started);
    };
    RequestCallbacks.Read(range, cb, Udata);
}

void TTestRunner::HandleCompletedRequest(
    EBlockStoreRequest requestType,
    const TBlockRange64& range,
    const NYdb::NBS::NProto::TError& error,
    TDuration elapsed)
{
    ProcessCompletedRequests(
std::make_unique<TCompletedRequest>(
            requestType,
            range,
            error,
            elapsed));

    SendAvailableRequests();

    if (IsFinished()) {
        RequestCallbacks.NotifyCompleted(Udata);
    }
}

void TTestRunner::SendWriteRequest(const TBlockRange64& range)
{
    STORAGE_ERROR(LoggingTag
        << "WriteBlocks request: ("
        << range.Start << ", " << range.Size() << "). Test has been stopped");
    Stop();

}

void TTestRunner::SendZeroRequest(const TBlockRange64& range)
{
    STORAGE_ERROR(LoggingTag
        << "ZeroBlocks request: ("
        << range.Start << ", " << range.Size() << "). Test has been stopped");
    Stop();
}

bool TTestRunner::StopRequested() const
{
    return ShouldStop.load(std::memory_order_acquire)
        || TestResults.Status != NProto::TEST_STATUS_OK;
}

bool TTestRunner::CheckExitCondition() const
{
    return (RequestsSent == RequestsCompleted
        && (StopRequested() || !Requests->HasMoreRequests()));
}

bool TTestRunner::CheckSendRequestCondition() const
{
    return !StopRequested() && Requests->HasMoreRequests();
}

void TTestRunner::ProcessCompletedRequests(std::unique_ptr<TCompletedRequest> request)
{
    Requests->Complete(request->BlockRange);

    Y_ABORT_UNLESS(CurrentIoDepth > 0);
    --CurrentIoDepth;

    if (FAILED(request->Error.GetCode())) {
        if (TestResults.Status != NProto::TEST_STATUS_FAILURE) {
            STORAGE_ERROR(LoggingTag
                << "Request failed with error: "
                << NYdb::NBS::FormatError(request->Error));
        }

        TestResults.Status = NProto::TEST_STATUS_FAILURE;
    }

    ++RequestsCompleted;

    ++TestResults.RequestsCompleted;

    switch (request->RequestType) {
        case EBlockStoreRequest::ReadBlocks:
            TestResults.BlocksRead += request->BlockRange.Size();
            TestResults.ReadHist.RecordValue(request->Elapsed);
            break;

        case EBlockStoreRequest::WriteBlocks:
            TestResults.BlocksWritten += request->BlockRange.Size();
            TestResults.WriteHist.RecordValue(request->Elapsed);
            break;

        case EBlockStoreRequest::ZeroBlocks:
            TestResults.BlocksZeroed += request->BlockRange.Size();
            TestResults.ZeroHist.RecordValue(request->Elapsed);
            break;

        default:
            STORAGE_ERROR(LoggingTag
                << "unexpected request type: "
                << request->RequestType << ". Test has been stopped");
            Stop();
    }
}

void TTestRunner::ReportProgress()
{
    const auto reportInterval = TDuration::Seconds(5);

    auto now = Now();
    if (now - LastReportTs > reportInterval) {
        const auto timePassed = now - LastReportTs;
        const auto requestsCompleted = RequestsCompleted - LastRequestsCompleted;

        STORAGE_INFO(LoggingTag
            << "Current IOPS: " << (requestsCompleted / timePassed.Seconds()));

        LastReportTs = now;
        LastRequestsCompleted = RequestsCompleted;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ITestRunnerPtr CreateTestRunner(
    ILoggingServicePtr loggingService,
    TString loggingTag,
    IRequestGeneratorPtr requests,
    ui32 maxIoDepth,
    std::atomic<bool>& shouldStop,
    LoadTestSendRequestCallbacks RequestCallbacks,
    const void *udata)
{
    return std::make_shared<TTestRunner>(
        std::move(loggingService),
        std::move(loggingTag),
        std::move(requests),
        maxIoDepth,
        shouldStop,
        std::move(RequestCallbacks),
        udata);
}

}   // namespace NCloud::NBlockStore::NLoadTest
