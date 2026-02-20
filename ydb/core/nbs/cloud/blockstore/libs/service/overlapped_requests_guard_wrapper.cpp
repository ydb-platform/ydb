#include "overlapped_requests_guard_wrapper.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_range_map.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/storage.h>

#include <util/string/builder.h>

#include <memory>

namespace NYdb::NBS::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TInflight;

class TOverlappedRequestsGuardStorageWrapper final
    : public IStorage
    , public std::enable_shared_from_this<
          TOverlappedRequestsGuardStorageWrapper>
{
private:
    const IStoragePtr Storage;

    TAdaptiveLock Lock;
    ui64 RequestIdGenerator = 0;
    TBlockRangeMap<ui64, std::unique_ptr<TInflight>> InflightRequests;

public:
    explicit TOverlappedRequestsGuardStorageWrapper(IStoragePtr storage);
    ~TOverlappedRequestsGuardStorageWrapper() override;

    // implements IStorage
    NThreading::TFuture<TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request) override;

    NThreading::TFuture<TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TWriteBlocksLocalRequest> request) override;

    NThreading::TFuture<TZeroBlocksLocalResponse> ZeroBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TZeroBlocksLocalRequest> request) override;

    void ReportIOError() override;

private:
    void OnRequestFinished(ui64 requestId, const NProto::TError& error);
};

template <typename TRequest, typename TResponse>
struct TCoveredRequest
{
    NThreading::TPromise<TResponse> Promise =
        NThreading::NewPromise<TResponse>();
    std::shared_ptr<TRequest> Request;
};

using TCoveredWriteRequest =
    TCoveredRequest<TWriteBlocksLocalRequest, TWriteBlocksLocalResponse>;
using TCoveredZeroRequest =
    TCoveredRequest<TZeroBlocksLocalRequest, TZeroBlocksLocalResponse>;

template <typename TRequest, typename TResponse>
struct TDelayedRequest
{
    TCallContextPtr CallContext;
    NThreading::TPromise<TResponse> Promise =
        NThreading::NewPromise<TResponse>();
    std::shared_ptr<TRequest> Request;
};

using TDelayedWriteRequest =
    TDelayedRequest<TWriteBlocksLocalRequest, TWriteBlocksLocalResponse>;
using TDelayedZeroRequest =
    TDelayedRequest<TZeroBlocksLocalRequest, TZeroBlocksLocalResponse>;

struct TInflight
{
    TVector<TCoveredWriteRequest> CoveredWrites;
    TVector<TCoveredZeroRequest> CoveredZeroes;
    TVector<TDelayedWriteRequest> DelayedWrites;
    TVector<TDelayedZeroRequest> DelayedZeroes;

    void OnRequestExecuted(IStorage* storage, const NProto::TError& error);
};

////////////////////////////////////////////////////////////////////////////////

void TInflight::OnRequestExecuted(
    IStorage* storage,
    const NProto::TError& error)
{
    for (auto& [promise, _]: CoveredWrites) {
        promise.SetValue(TWriteBlocksLocalResponse{.Error = error});
    }

    for (auto& [promise, _]: CoveredZeroes) {
        promise.SetValue(TZeroBlocksLocalResponse{.Error = error});
    }

    for (auto& delayed: DelayedWrites) {
        auto result = storage->WriteBlocksLocal(
            std::move(delayed.CallContext),
            std::move(delayed.Request));

        result.Apply(
            [promise = std::move(delayed.Promise)](
                const NThreading::TFuture<TWriteBlocksLocalResponse>& f) mutable
            {
                promise.SetValue(f.GetValue());   //
            });
    }

    for (auto& delayed: DelayedZeroes) {
        auto result = storage->ZeroBlocksLocal(
            std::move(delayed.CallContext),
            std::move(delayed.Request));

        result.Apply(
            [promise = std::move(delayed.Promise)](
                const NThreading::TFuture<TZeroBlocksLocalResponse>& f) mutable
            {
                promise.SetValue(f.GetValue());   //
            });
    }
}

////////////////////////////////////////////////////////////////////////////////

TOverlappedRequestsGuardStorageWrapper::TOverlappedRequestsGuardStorageWrapper(
    IStoragePtr storage)
    : Storage(std::move(storage))
{}

TOverlappedRequestsGuardStorageWrapper::
    ~TOverlappedRequestsGuardStorageWrapper() = default;

NThreading::TFuture<TReadBlocksLocalResponse>
TOverlappedRequestsGuardStorageWrapper::ReadBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request)
{
    return Storage->ReadBlocksLocal(std::move(callContext), std::move(request));
}

NThreading::TFuture<TWriteBlocksLocalResponse>
TOverlappedRequestsGuardStorageWrapper::WriteBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TWriteBlocksLocalRequest> request)
{
    auto guard = Guard(Lock);

    const auto* overlaps =
        InflightRequests.FindFirstOverlapping(request->Range);

    if (!overlaps) {
        const ui64 requestId = ++RequestIdGenerator;
        InflightRequests.AddRange(
            requestId,
            request->Range,
            nullptr   // will create TInflight when we find the first
                      // intersection with the request.
        );
        guard.Release();   // Access to InflightRequests completed.

        auto result = Storage->WriteBlocksLocal(
            std::move(callContext),
            std::move(request));
        result.Subscribe(
            [weakSelf = weak_from_this(),
             requestId](const NThreading::TFuture<TWriteBlocksLocalResponse>& f)
            {
                if (auto self = weakSelf.lock()) {
                    self->OnRequestFinished(requestId, f.GetValue().Error);
                }
            });
        return result;
    }

    std::unique_ptr<TInflight>& inflightPtr = overlaps->AccessValue();
    if (!inflightPtr) {
        inflightPtr = std::make_unique<TInflight>();
    }

    if (overlaps->Range.Contains(request->Range)) {
        // The new request is fully covered by the executing one.
        inflightPtr->CoveredWrites.push_back({.Request = std::move(request)});
        return inflightPtr->CoveredWrites.back().Promise;
    }

    inflightPtr->DelayedWrites.push_back(
        {.CallContext = std::move(callContext), .Request = std::move(request)});
    return inflightPtr->DelayedWrites.back().Promise;
}

NThreading::TFuture<TZeroBlocksLocalResponse>
TOverlappedRequestsGuardStorageWrapper::ZeroBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TZeroBlocksLocalRequest> request)
{
    auto guard = Guard(Lock);

    const auto* overlaps =
        InflightRequests.FindFirstOverlapping(request->Range);

    if (!overlaps) {
        const ui64 requestId = ++RequestIdGenerator;
        InflightRequests.AddRange(
            requestId,
            request->Range,
            nullptr   // will create TInflight when we find the first
                      // intersection with the request.
        );
        guard.Release();

        auto result = Storage->ZeroBlocksLocal(
            std::move(callContext),
            std::move(request));
        result.Subscribe(
            [weakSelf = weak_from_this(),
             requestId](const NThreading::TFuture<TZeroBlocksLocalResponse>& f)
            {
                if (auto self = weakSelf.lock()) {
                    self->OnRequestFinished(requestId, f.GetValue().Error);
                }
            });
        return result;
    }

    std::unique_ptr<TInflight>& inflightPtr = overlaps->AccessValue();
    if (!inflightPtr) {
        inflightPtr = std::make_unique<TInflight>();
    }

    if (overlaps->Range.Contains(request->Range)) {
        inflightPtr->CoveredZeroes.push_back({.Request = std::move(request)});
        return inflightPtr->CoveredZeroes.back().Promise;
    }

    inflightPtr->DelayedZeroes.push_back(
        {.CallContext = std::move(callContext), .Request = std::move(request)});
    return inflightPtr->DelayedZeroes.back().Promise;
}

void TOverlappedRequestsGuardStorageWrapper::ReportIOError()
{
    Storage->ReportIOError();
}

void TOverlappedRequestsGuardStorageWrapper::OnRequestFinished(
    ui64 requestId,
    const NProto::TError& error)
{
    Lock.Acquire();
    auto inflight = InflightRequests.ExtractRange(requestId);
    Lock.Release();

    if (inflight && inflight->Value) {
        inflight->Value->OnRequestExecuted(this, error);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStoragePtr CreateOverlappedRequestsGuardStorageWrapper(IStoragePtr storage)
{
    return std::make_shared<TOverlappedRequestsGuardStorageWrapper>(storage);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
