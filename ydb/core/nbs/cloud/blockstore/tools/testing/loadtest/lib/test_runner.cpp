#include "test_runner.h"

#include "request_generator.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/thread.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/logging.h>

#include <util/datetime/base.h>
#include <util/generic/deque.h>
#include <util/generic/vector.h>
#include <util/generic/size_literals.h>
#include <util/random/random.h>
#include <util/string/builder.h>

#include <algorithm>
#include <atomic>
#include <unordered_set>

namespace NYdb::NBS::NBlockStore::NLoadTest {

using namespace NThreading;

// using namespace NYdb::NBS::NBlockStore::NClient;

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
    TLoadTestRequestCallbacks RequestCallbacks;

    ui64 RequestsSent = 0;
    ui64 RequestsCompleted = 0;
    ui32 CurrentIoDepth = 0;

    TInstant LastReportTs;
    ui64 LastRequestsCompleted = 0;
    TTestResults TestResults;

    const void *Udata = nullptr;


    static constexpr ui64 BlockSize = 4_KB;
    static constexpr ui32 BlocksNumberForPreGeneratedWriteData = 1024;
    TVector<TVector<ui8>> DataForWriteRequests;
    std::unordered_set<ui64> AvailableWriteBlocks;
public:
    TTestRunner(
            ILoggingServicePtr loggingService,
            TString loggingTag,
            IRequestGeneratorPtr requests,
            ui32 maxIoDepth,
            std::atomic<bool>& shouldStop,
            TLoadTestRequestCallbacks requestCallbacks,
            const void *udata)
        : Log(loggingService->CreateLog(requests->Describe()))
        , LoggingTag(std::move(loggingTag))
        , Requests(std::move(requests))
        , MaxIoDepth(maxIoDepth)
        , ShouldStop(shouldStop)
        , RequestCallbacks(std::move(requestCallbacks))
        , Udata(udata)
    {
        STORAGE_WARN(LoggingTag
                << "TTestRunner initializing");

        if (Requests->HasWriteRequests()) {
            STORAGE_WARN(LoggingTag
                << "Test has write requests. Generating data");
            GenerateWriteData();
        }
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

    void AddAvailableWriteBlockIndex(ui64 index);
    ui64 GetNextWriteBlockIndex();
    void ProcessCompletedRequests(std::unique_ptr<TCompletedRequest> request);

    void ReportProgress();
    void GenerateWriteData();
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

    auto cb =
    [started, range, this, p=shared_from_this()]
    (const NYdb::NBS::NProto::TError& result, const void* udata) mutable {
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

void TTestRunner::AddAvailableWriteBlockIndex(ui64 index)
{
    AvailableWriteBlocks.insert(index);
}

ui64 TTestRunner::GetNextWriteBlockIndex()
{
    ui64 index = RandomNumber<ui64>(AvailableWriteBlocks.size());
    auto it = std::next(AvailableWriteBlocks.begin(), static_cast<int>(index));
    ui64 result = *it;
    AvailableWriteBlocks.erase(it);

    return result;
}

void TTestRunner::SendWriteRequest(const TBlockRange64& range)
{
    if (range.Size() != 1) {
        STORAGE_ERROR(LoggingTag
            << "Trying to generate > 1 block write request which "
            " is not supported. Test has been stopped");
        Stop();
    }

    auto started = TInstant::Now();

    ui64 randomBlockIndex = GetNextWriteBlockIndex();

    auto cb =
        [started, range, this, p=shared_from_this(), randomBlockIndex]
        (const NYdb::NBS::NProto::TError& result, const void* udata) mutable {
        STORAGE_DEBUG(LoggingTag
                << "TTestRunner::SendWriteRequest cb for range: " << range
                << ", result: " << NYdb::NBS::FormatError(result));
        Udata = udata;
        if (FAILED(result.GetCode())) {
            STORAGE_ERROR(LoggingTag
                << "WriteBlocks request failed with error: "
                << NYdb::NBS::FormatError(result));
        }
        p->HandleCompletedRequest(
            EBlockStoreRequest::WriteBlocks,
            range,
            result,
            TInstant::Now() - started);
        p->AddAvailableWriteBlockIndex(randomBlockIndex);
    };


    auto& block = DataForWriteRequests[randomBlockIndex];
    RequestCallbacks.Write(range.Start, block.data(), block.size(), cb, Udata);
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
            << "Average IOPS: " << (requestsCompleted / timePassed.Seconds()));

        LastReportTs = now;
        LastRequestsCompleted = RequestsCompleted;
    }
}

void TTestRunner::GenerateWriteData()
{
    DataForWriteRequests.resize(BlocksNumberForPreGeneratedWriteData);
    for (ui64 i = 0; i < DataForWriteRequests.size(); ++i) {
        auto &block = DataForWriteRequests[i];
        AddAvailableWriteBlockIndex(i);
        block.resize(BlockSize);
        auto random = []() -> ui8 {
            return 1 + RandomNumber<ui8>(Max<ui8>());
        };
        std::ranges::generate(block, random);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ITestRunnerPtr CreateTestRunner(
    ILoggingServicePtr loggingService,
    TString loggingTag,
    IRequestGeneratorPtr requestGenerator,
    ui32 maxIoDepth,
    std::atomic<bool>& shouldStop,
    TLoadTestRequestCallbacks requestCallbacks,
    const void *udata)
{
    return std::make_shared<TTestRunner>(
        std::move(loggingService),
        std::move(loggingTag),
        std::move(requestGenerator),
        maxIoDepth,
        shouldStop,
        std::move(requestCallbacks),
        udata);
}

}   // namespace NYdb::NBS::NBlockStore::NLoadTest
