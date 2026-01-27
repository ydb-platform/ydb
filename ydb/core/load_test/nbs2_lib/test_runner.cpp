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
#include <util/system/condvar.h>
#include <util/system/event.h>
#include <util/system/mutex.h>
#include <util/system/thread.h>

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
    NProto::TError Error;
    TDuration Elapsed;

    TCompletedRequest(
            EBlockStoreRequest requestType,
            const TBlockRange64& blockRange,
            const NProto::TError& error,
            TDuration elapsed)
        : RequestType(requestType)
        , BlockRange(blockRange)
        , Error(error)
        , Elapsed(elapsed)
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TRequestsCompletionQueue
{
private:
    TDeque<std::unique_ptr<TCompletedRequest>> Items;
    TMutex Lock;

public:
    void Enqueue(std::unique_ptr<TCompletedRequest> request)
    {
        with_lock (Lock) {
            Items.emplace_back(std::move(request));
        }
    }

    std::unique_ptr<TCompletedRequest> Dequeue()
    {
        with_lock (Lock) {
            std::unique_ptr<TCompletedRequest> ptr;
            if (Items) {
                ptr = std::move(Items.front());
                Items.pop_front();
            }
            return ptr;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTestRunner final
    : public ITestRunner
    , public ISimpleThread
    , public std::enable_shared_from_this<TTestRunner>
{
private:
    TLog Log;
    TString LoggingTag;
    IRequestGeneratorPtr Requests;
    ui32 MaxIoDepth;

    TInstant StartTs;

    std::atomic<bool>& ShouldStop;

    ui64 RequestsSent = 0;
    ui64 RequestsCompleted = 0;
    ui32 CurrentIoDepth = 0;

    TInstant LastReportTs;
    ui64 LastRequestsCompleted = 0;

    TRequestsCompletionQueue CompletionQueue;
    TAutoEvent Event;

    TPromise<TTestResultsPtr> Response = NewPromise<TTestResultsPtr>();
    TTestResultsPtr TestResults = std::make_unique<TTestResults>();

    IAllocator* Allocator = BufferPool();  // TDefaultAllocator::Instance()

public:
    TTestRunner(
            ILoggingServicePtr loggingService,
            TString loggingTag,
            IRequestGeneratorPtr requests,
            ui32 maxIoDepth,
            std::atomic<bool>& shouldStop)
        : Log(loggingService->CreateLog(requests->Describe()))
        , LoggingTag(std::move(loggingTag))
        , Requests(std::move(requests))
        , MaxIoDepth(maxIoDepth)
        , ShouldStop(shouldStop)
    {
    }

    TFuture<TTestResultsPtr> Run() override
    {
        Start();
        return Response;
    }

    void* ThreadProc() override;

private:
    bool SendNextRequest();
    void SendReadRequest(const TBlockRange64& range);
    void SendWriteRequest(const TBlockRange64& range);
    void SendZeroRequest(const TBlockRange64& range);

    bool StopRequested() const;
    bool CheckSendRequestCondition() const;
    bool CheckExitCondition() const;

    void SignalCompletion(
        EBlockStoreRequest requestType,
        const TBlockRange64& range,
        const NProto::TError& error,
        TDuration elapsed);

    void ProcessCompletedRequests();

    void ReportProgress();
};

////////////////////////////////////////////////////////////////////////////////

void* TTestRunner::ThreadProc()
{
    SetCurrentThreadName("Test");
    StartTs = Now();
    LastReportTs = Now();

    while (!CheckExitCondition()) {
        while (CheckSendRequestCondition() && SendNextRequest()) {
            ++RequestsSent;
        }

        if (!CheckExitCondition()) {
            Event.WaitD(Requests->Peek());
            ProcessCompletedRequests();
        }

        ReportProgress();
    }

    Response.SetValue(std::move(TestResults));
    return nullptr;
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
            Y_ABORT_UNLESS(TStringBuilder()
                << "unexpected request type: " << request.RequestType);
                //<< GetBlockStoreRequestName(request.RequestType));
    }

    return true;
}

void doDummyWork(TPromise<NProto::TError> response)
{
    Sleep(TDuration::MilliSeconds(50));
    NProto::TError result;
    result.SetCode(NCloud::EWellKnownResultCodes::S_OK);
    response.SetValue(result);
}

NThreading::TFuture<NProto::TError> getFuture()
{
    auto response = NewPromise<NProto::TError>();
    doDummyWork(response);
    return response;
}

void TTestRunner::SendReadRequest(const TBlockRange64& range)
{
    STORAGE_DEBUG(LoggingTag
        << "ReadBlocks request: ("
        << range.Start << ", " << range.Size() << ")");

    auto started = TInstant::Now();
    auto future = getFuture();
    future.Subscribe(
    [started, range, future, this, p=shared_from_this()] (const auto& f) mutable {
        STORAGE_INFO(LoggingTag
                << "maks_ololo TTestRunner::SendReadRequest test future cb");
        const auto& error = f.GetValue();
        //const auto& error = response.GetError();
        if (FAILED(error.GetCode())) {
            STORAGE_ERROR(LoggingTag
                << "ReadBlocks request failed with error: "
                << FormatError(error));
        }

        p->SignalCompletion(
            EBlockStoreRequest::ReadBlocks,
            range,
            error,
            TInstant::Now() - started);
    });
}

void TTestRunner::SendWriteRequest(const TBlockRange64& range)
{
    STORAGE_DEBUG(LoggingTag
        << "WriteBlocks request: ("
        << range.Start << ", " << range.Size() << ")");

    //Y_DEBUG_ABORT_UNLESS(Volume.GetBlockSize() >= sizeof(RequestsSent));
    Y_ABORT("WriteBlocks is not supported");

}

void TTestRunner::SendZeroRequest(const TBlockRange64& range)
{
    STORAGE_DEBUG(LoggingTag
        << "ZeroBlocks request: ("
        << range.Start << ", " << range.Size() << ")");
    Y_ABORT("ZeroBlocks is not supported");
}

bool TTestRunner::StopRequested() const
{
    return ShouldStop.load(std::memory_order_acquire)
        || TestResults->Status != NProto::TEST_STATUS_OK;
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

void TTestRunner::SignalCompletion(
    EBlockStoreRequest requestType,
    const TBlockRange64& range,
    const NProto::TError& error,
    TDuration elapsed)
{
    CompletionQueue.Enqueue(
        std::make_unique<TCompletedRequest>(requestType, range, error, elapsed));

    Event.Signal();
}

void TTestRunner::ProcessCompletedRequests()
{
    while (auto request = CompletionQueue.Dequeue()) {
        Requests->Complete(request->BlockRange);

        Y_ABORT_UNLESS(CurrentIoDepth > 0);
        --CurrentIoDepth;

        if (FAILED(request->Error.GetCode())) {
            if (TestResults->Status != NProto::TEST_STATUS_FAILURE) {
                STORAGE_ERROR(LoggingTag
                    << "Request failed with error: "
                    << FormatError(request->Error));
            }

            TestResults->Status = NProto::TEST_STATUS_FAILURE;
        }

        ++RequestsCompleted;

        ++TestResults->RequestsCompleted;

        switch (request->RequestType) {
            case EBlockStoreRequest::ReadBlocks:
                TestResults->BlocksRead += request->BlockRange.Size();
                TestResults->ReadHist.RecordValue(request->Elapsed);
                break;

            case EBlockStoreRequest::WriteBlocks:
                TestResults->BlocksWritten += request->BlockRange.Size();
                TestResults->WriteHist.RecordValue(request->Elapsed);
                break;

            case EBlockStoreRequest::ZeroBlocks:
                TestResults->BlocksZeroed += request->BlockRange.Size();
                TestResults->ZeroHist.RecordValue(request->Elapsed);
                break;

            default:
                Y_ABORT_UNLESS(TStringBuilder()
                    << "unexpected request type: " << request->RequestType);
                    //<< GetBlockStoreRequestName(request->RequestType));
        }
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
    std::atomic<bool>& shouldStop)
{
    return std::make_shared<TTestRunner>(
        std::move(loggingService),
        //std::move(session),
        //std::move(volume),
        std::move(loggingTag),
        std::move(requests),
        maxIoDepth,
        shouldStop);
}

}   // namespace NCloud::NBlockStore::NLoadTest
