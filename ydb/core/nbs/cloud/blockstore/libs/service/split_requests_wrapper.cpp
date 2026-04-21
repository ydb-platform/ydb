#include "split_requests_wrapper.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/storage.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>

#include <util/string/builder.h>
#include <util/system/mutex.h>

#include <memory>

namespace NYdb::NBS::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

bool NeedToSplitRequest(const TRequestHeaders& headers)
{
    const ui64 blocksPerStripe = headers.VolumeConfig->BlocksPerStripe;
    if (!blocksPerStripe) {
        return false;
    }
    const bool needToSplit = headers.Range.Start / blocksPerStripe !=
                             headers.Range.End / blocksPerStripe;
    return needToSplit;
}

template <typename TRequest>
TVector<std::shared_ptr<TRequest>> SplitRequest(const TRequest& request)
{
    const ui64 stripeSize = request.Headers.VolumeConfig->BlocksPerStripe;
    const ui32 blockSize = request.Headers.VolumeConfig->BlockSize;

    TVector<std::shared_ptr<TRequest>> result;

    auto guard = request.Sglist.Acquire();
    if (!guard) {
        return result;
    }
    const TSgList& sgList = guard.Get();

    auto subRanges = request.Headers.Range.Split(stripeSize);
    result.reserve(subRanges.size());
    for (auto subRange: subRanges) {
        auto subRequest =
            std::make_shared<TRequest>(request.Headers.Clone(subRange));
        auto subSgList = CreateSgListSubRange(
            sgList,
            (subRange.Start - request.Headers.Range.Start) * blockSize,
            subRange.Size() * blockSize);
        subRequest->Sglist =
            request.Sglist.CreateDepender(std::move(subSgList));
        result.push_back(std::move(subRequest));
    }

    return result;
}

template <>
TVector<std::shared_ptr<TZeroBlocksLocalRequest>> SplitRequest(
    const TZeroBlocksLocalRequest& request)
{
    const ui64 stripeSize = request.Headers.VolumeConfig->BlocksPerStripe;

    TVector<std::shared_ptr<TZeroBlocksLocalRequest>> result;

    auto subRanges = request.Headers.Range.Split(stripeSize);
    result.reserve(subRanges.size());
    for (auto subRange: subRanges) {
        auto subRequest = std::make_shared<TZeroBlocksLocalRequest>(
            request.Headers.Clone(subRange));
        result.push_back(std::move(subRequest));
    }

    return result;
}

template <typename TRequest, typename TResponse>
class TSplittedRequest
    : public std::enable_shared_from_this<TSplittedRequest<TRequest, TResponse>>
{
private:
    std::shared_ptr<TRequest> Request;
    TVector<std::shared_ptr<TRequest>> SubRequests;
    TPromise<TResponse> Promise = NewPromise<TResponse>();

    TMutex Lock;
    size_t SubResponseReceived = 0;

public:
    explicit TSplittedRequest(std::shared_ptr<TRequest> request)
        : Request(std::move(request))
        , SubRequests(SplitRequest(*Request))
    {}

    TFuture<TResponse> RunSubRequests(
        TCallContextPtr callContext,
        IStorage* storage)
    {
        if (SubRequests.empty()) {
            return MakeFuture(TResponse{
                .Error = MakeError(E_CANCELLED, "failed to acquire sglist")});
        }

        for (const auto& subRequest: SubRequests) {
            auto subFuture =
                TStorageAdapter::Execute(storage, callContext, subRequest);
            subFuture.Subscribe(
                [self = this->shared_from_this()]   //
                (const TFuture<TResponse>& f)
                {
                    self->OnSubResponse(UnsafeExtractValue(f));   //
                });
        }

        return Promise.GetFuture();
    }

private:
    void OnSubResponse(TResponse response)
    {
        const bool hasError = HasError(response.Error);
        TPromise<TResponse> promise;

        // Access Promise field with lock.
        with_lock (Lock) {
            ++SubResponseReceived;

            if (!Promise.Initialized()) {
                // We have already responded with an error.
                return;
            }

            const bool isLastResponse =
                SubResponseReceived == SubRequests.size();

            if (!isLastResponse && !hasError) {
                return;
            }

            promise.Swap(Promise);
        }

        // Reply to client without lock.
        promise.SetValue(std::move(response));

        if (hasError) {
            for (auto& subRequest: SubRequests) {
                if constexpr (TRequestTraits<TRequest>::IsReadWriteRequest()) {
                    // Cancel request in case of error.
                    subRequest->Sglist.Close();
                }
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSplitRequestsStorageWrapper final
    : public IStorage
    , public std::enable_shared_from_this<TSplitRequestsStorageWrapper>
{
private:
    const IStoragePtr Storage;

public:
    explicit TSplitRequestsStorageWrapper(IStoragePtr storage);
    ~TSplitRequestsStorageWrapper() override;

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
    template <typename TResponse, typename TRequest>
    NThreading::TFuture<TResponse> ExecuteRequest(
        TCallContextPtr callContext,
        std::shared_ptr<TRequest> request);
};

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////

TSplitRequestsStorageWrapper::TSplitRequestsStorageWrapper(IStoragePtr storage)
    : Storage(std::move(storage))
{}

TSplitRequestsStorageWrapper::~TSplitRequestsStorageWrapper() = default;

NThreading::TFuture<TReadBlocksLocalResponse>
TSplitRequestsStorageWrapper::ReadBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request)
{
    return ExecuteRequest<TReadBlocksLocalResponse>(
        std::move(callContext),
        std::move(request));
}

NThreading::TFuture<TWriteBlocksLocalResponse>
TSplitRequestsStorageWrapper::WriteBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TWriteBlocksLocalRequest> request)
{
    return ExecuteRequest<TWriteBlocksLocalResponse>(
        std::move(callContext),
        std::move(request));
}

NThreading::TFuture<TZeroBlocksLocalResponse>
TSplitRequestsStorageWrapper::ZeroBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TZeroBlocksLocalRequest> request)
{
    return ExecuteRequest<TZeroBlocksLocalResponse>(
        std::move(callContext),
        std::move(request));
}

void TSplitRequestsStorageWrapper::ReportIOError()
{
    Storage->ReportIOError();
}

template <typename TResponse, typename TRequest>
NThreading::TFuture<TResponse> TSplitRequestsStorageWrapper::ExecuteRequest(
    TCallContextPtr callContext,
    std::shared_ptr<TRequest> request)
{
    if (NeedToSplitRequest(request->Headers)) {
        auto splittedRequest =
            std::make_shared<TSplittedRequest<TRequest, TResponse>>(
                std::move(request));

        return splittedRequest->RunSubRequests(
            std::move(callContext),
            Storage.get());
    }

    return TStorageAdapter::Execute(
        Storage.get(),
        std::move(callContext),
        std::move(request));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStoragePtr CreateSplitRequestsStorageWrapper(IStoragePtr storage)
{
    return std::make_shared<TSplitRequestsStorageWrapper>(std::move(storage));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
