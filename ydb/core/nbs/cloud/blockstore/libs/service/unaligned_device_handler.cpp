
#include "unaligned_device_handler.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>

namespace NYdb::NBS::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 MaxUnalignedRequestSize = 32_MB;

TErrorResponse CreateErrorAcquireResponse()
{
    return {E_CANCELLED, "failed to acquire sglist in DeviceHandler"};
}

TErrorResponse CreateRequestDestroyedResponse()
{
    return {E_CANCELLED, "request destroyed"};
}

TErrorResponse CreateBackendDestroyedResponse()
{
    return {E_CANCELLED, "backend destroyed"};
}

TErrorResponse CreateUnalignedTooBigResponse(ui32 blockCount)
{
    return {
        E_ARGUMENT,
        TStringBuilder() << "Unaligned request is too big. BlockCount="
                         << blockCount};
}

TStorageBuffer AllocateBuffer(size_t bytesCount)
{
    return std::shared_ptr<char>(
        new char[bytesCount],
        std::default_delete<char[]>());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

// The base class for wrapping over a write or zero request.
class TModifyRequest: public std::enable_shared_from_this<TModifyRequest>
{
private:
    enum class EStatus
    {
        Created,
        Postponed,
        InFlight,
        Completed,
    };
    // The current status of the request execution can be read and written
    // only under the RequestsLock.
    EStatus Status = EStatus::Created;

    // A list of requests that prevent the execution.
    TVector<TModifyRequestWeakPtr> Dependencies;

    // A list iterator that points to an object in the list of running requests.
    // Makes it easier to delete an object after the request is completed.
    TModifyRequestIt It;

protected:
    const std::weak_ptr<TAlignedDeviceHandler> Backend;
    const TBlocksInfo BlocksInfo;
    TCallContextPtr CallContext;

    // The buffer that was allocated to execute the unaligned request. Memory is
    // allocated only if the request is not aligned.
    TStorageBuffer RMWBuffer;
    TSgList RMWBufferSgList;

public:
    TModifyRequest(
        std::weak_ptr<TAlignedDeviceHandler> backend,
        TCallContextPtr callContext,
        const TBlocksInfo& blocksInfo);

    virtual ~TModifyRequest() = default;

    void SetIt(TModifyRequestIt it);
    TModifyRequestIt GetIt() const;

    bool IsAligned() const;

    // Adds a list of overlapped requests that this request depends on. The
    // execution of this request will not start until all this requests have
    // been completed.
    void AddDependencies(const TList<TModifyRequestPtr>& requests);

    // Returns a flag indicating whether the request can be executed. The method
    // modifies the internal state and gives permission to execute only once. It
    // is necessary to call the method under RequestsLock.
    [[nodiscard]] bool PrepareToRun();

    // Starts the execution of the postponed request. The method can only be
    // called if the request has been postponed.
    void ExecutePostponed();

    // It is necessary to call the method under RequestsLock.
    void SetCompleted();

protected:
    void AllocateRMWBuffer();

    virtual void DoPostpone() = 0;
    virtual void DoExecutePostponed() = 0;
};

//  Wrapper for a write request.
class TWriteRequest final: public TModifyRequest
{
public:
    using TResponsePromise = TPromise<TWriteBlocksLocalResponse>;
    using TResponseFuture = TFuture<TWriteBlocksLocalResponse>;

private:
    TResponsePromise Promise;
    TGuardedSgList SgList;

public:
    TWriteRequest(
        std::weak_ptr<TAlignedDeviceHandler> backend,
        TCallContextPtr callContext,
        const TBlocksInfo& blocksInfo,
        TGuardedSgList sgList);

    TResponseFuture ExecuteOrPostpone(bool readyToRun);

protected:
    void DoPostpone() override;
    void DoExecutePostponed() override;

private:
    TResponseFuture DoExecute();
    TResponseFuture ReadModifyWrite(TAlignedDeviceHandler& backend);
    TResponseFuture ModifyAndWrite();
};

//  Wrapper for a zero request.
class TZeroRequest final: public TModifyRequest
{
public:
    using TResponsePromise = TPromise<TZeroBlocksLocalResponse>;
    using TResponseFuture = TFuture<TZeroBlocksLocalResponse>;

private:
    TResponsePromise Promise;
    TGuardedSgList SgList;

public:
    TZeroRequest(
        std::weak_ptr<TAlignedDeviceHandler> backend,
        TCallContextPtr callContext,
        const TBlocksInfo& blocksInfo);
    ~TZeroRequest() override;

    TResponseFuture ExecuteOrPostpone(bool readyToRun);

protected:
    void DoPostpone() override;
    void DoExecutePostponed() override;

private:
    TResponseFuture DoExecute();
    TResponseFuture ReadModifyWrite(TAlignedDeviceHandler& backend);
    TResponseFuture ModifyAndWrite();
};

////////////////////////////////////////////////////////////////////////////////

TModifyRequest::TModifyRequest(
    std::weak_ptr<TAlignedDeviceHandler> backend,
    TCallContextPtr callContext,
    const TBlocksInfo& blocksInfo)
    : Backend(std::move(backend))
    , BlocksInfo(blocksInfo)
    , CallContext(std::move(callContext))
{}

void TModifyRequest::AddDependencies(const TList<TModifyRequestPtr>& requests)
{
    for (const auto& request: requests) {
        if (BlocksInfo.Range.Overlaps(request->BlocksInfo.Range)) {
            Dependencies.push_back(request);
        }
    }
}

void TModifyRequest::SetIt(TModifyRequestIt it)
{
    It = it;
}

TModifyRequestIt TModifyRequest::GetIt() const
{
    return It;
}

bool TModifyRequest::IsAligned() const
{
    return BlocksInfo.IsAligned();
}

bool TModifyRequest::PrepareToRun()
{
    if (Status == EStatus::InFlight) {
        // can't run a request that is already running
        return false;
    }

    bool allDependenciesCompleted = AllOf(
        Dependencies,
        [](const TModifyRequestWeakPtr& weakRequest)
        {
            if (auto request = weakRequest.lock()) {
                return request->Status == EStatus::Completed;
            }
            return true;
        });
    if (!allDependenciesCompleted) {
        if (Status == EStatus::Created) {
            // Postponing the execution of the newly created request.
            Status = EStatus::Postponed;
            CallContext->Postpone(GetCycleCount());
            DoPostpone();
        }
        return false;
    }

    // The request can be executed. We update status and give a sign to caller
    // that the request needs to be run for execution. We don't run here because
    // we want to minimize the code executed under RequestsLock.
    Status = EStatus::InFlight;
    return true;
}

void TModifyRequest::ExecutePostponed()
{
    CallContext->Advance(GetCycleCount());
    DoExecutePostponed();
}

void TModifyRequest::SetCompleted()
{
    Status = EStatus::Completed;
}

void TModifyRequest::AllocateRMWBuffer()
{
    auto bufferSize = BlocksInfo.MakeAligned().BufferSize();
    RMWBuffer = AllocateBuffer(bufferSize);

    auto sgListOrError = SgListNormalize(
        TBlockDataRef(RMWBuffer.get(), bufferSize),
        BlocksInfo.BlockSize);
    Y_DEBUG_ABORT_UNLESS(!HasError(sgListOrError));

    RMWBufferSgList = sgListOrError.ExtractResult();
}

////////////////////////////////////////////////////////////////////////////////

TWriteRequest::TWriteRequest(
    std::weak_ptr<TAlignedDeviceHandler> backend,
    TCallContextPtr callContext,
    const TBlocksInfo& blocksInfo,
    TGuardedSgList sgList)
    : TModifyRequest(std::move(backend), std::move(callContext), blocksInfo)
    , SgList(std::move(sgList))
{}

TWriteRequest::TResponseFuture TWriteRequest::ExecuteOrPostpone(bool readyToRun)
{
    return readyToRun ? DoExecute() : Promise.GetFuture();
}

void TWriteRequest::DoPostpone()
{
    Y_ABORT_UNLESS(!Promise.Initialized());
    Promise = NewPromise<TWriteBlocksLocalResponse>();
}

void TWriteRequest::DoExecutePostponed()
{
    Y_ABORT_UNLESS(Promise.Initialized());
    auto future = DoExecute();
    future.Subscribe([promise = Promise](const TResponseFuture& f) mutable
                     { promise.SetValue(f.GetValue()); });
}

TWriteRequest::TResponseFuture TWriteRequest::DoExecute()
{
    if (auto backend = Backend.lock()) {
        return IsAligned() ? backend->ExecuteWriteRequest(
                                 std::move(CallContext),
                                 BlocksInfo,
                                 std::move(SgList))
                           : ReadModifyWrite(*backend);
    }

    return MakeFuture<TWriteBlocksLocalResponse>(
        {.Error = CreateBackendDestroyedResponse()});
}

TWriteRequest::TResponseFuture TWriteRequest::ReadModifyWrite(
    TAlignedDeviceHandler& backend)
{
    AllocateRMWBuffer();

    auto read = backend.ExecuteReadRequest(
        CallContext,
        BlocksInfo.MakeAligned(),
        SgList.Create(RMWBufferSgList),
        {});

    return read.Apply(
        [weakPtr = weak_from_this()](
            const TFuture<TReadBlocksLocalResponse>& future) mutable
        {
            const auto& response = future.GetValue();
            if (HasError(response.Error)) {
                return MakeFuture<TWriteBlocksLocalResponse>(
                    {.Error = response.Error});
            }

            if (auto p = weakPtr.lock()) {
                return static_cast<TWriteRequest*>(p.get())->ModifyAndWrite();
            }

            return MakeFuture<TWriteBlocksLocalResponse>(
                {.Error = CreateRequestDestroyedResponse()});
        });
}

TWriteRequest::TResponseFuture TWriteRequest::ModifyAndWrite()
{
    auto backend = Backend.lock();
    if (!backend) {
        return MakeFuture<TWriteBlocksLocalResponse>(
            {.Error = CreateBackendDestroyedResponse()});
    }

    if (auto guard = SgList.Acquire()) {
        const auto& srcSgList = guard.Get();
        auto size = SgListGetSize(srcSgList);
        TBlockDataRef dstBuf(RMWBuffer.get() + BlocksInfo.BeginOffset, size);
        auto cpSize = SgListCopy(srcSgList, {dstBuf});
        Y_ABORT_UNLESS(cpSize == size);
    } else {
        return MakeFuture<TWriteBlocksLocalResponse>(
            {.Error = CreateErrorAcquireResponse()});
    }

    return backend->ExecuteWriteRequest(
        CallContext,
        BlocksInfo.MakeAligned(),
        SgList.Create(RMWBufferSgList));
}

////////////////////////////////////////////////////////////////////////////////

TZeroRequest::TZeroRequest(
    std::weak_ptr<TAlignedDeviceHandler> backend,
    TCallContextPtr callContext,
    const TBlocksInfo& blocksInfo)
    : TModifyRequest(std::move(backend), std::move(callContext), blocksInfo)
{}

TZeroRequest::~TZeroRequest()
{
    SgList.Close();
}

TZeroRequest::TResponseFuture TZeroRequest::ExecuteOrPostpone(bool readyToRun)
{
    return readyToRun ? DoExecute() : Promise.GetFuture();
}

void TZeroRequest::DoPostpone()
{
    Y_ABORT_UNLESS(!Promise.Initialized());
    Promise = NewPromise<TZeroBlocksLocalResponse>();
}

void TZeroRequest::DoExecutePostponed()
{
    Y_ABORT_UNLESS(Promise.Initialized());
    auto future = DoExecute();
    future.Subscribe([promise = Promise](const TResponseFuture& f) mutable
                     { promise.SetValue(f.GetValue()); });
}

TZeroRequest::TResponseFuture TZeroRequest::DoExecute()
{
    if (auto backend = Backend.lock()) {
        return IsAligned() ? backend->ExecuteZeroRequest(
                                 std::move(CallContext),
                                 BlocksInfo)
                           : ReadModifyWrite(*backend);
    }

    return MakeFuture<TZeroBlocksLocalResponse>(
        {.Error = CreateBackendDestroyedResponse()});
}

TZeroRequest::TResponseFuture TZeroRequest::ReadModifyWrite(
    TAlignedDeviceHandler& backend)
{
    AllocateRMWBuffer();
    SgList.SetSgList(RMWBufferSgList);

    auto read = backend.ExecuteReadRequest(
        CallContext,
        BlocksInfo.MakeAligned(),
        SgList,
        {});

    return read.Apply(
        [weakPtr = weak_from_this()](
            const TFuture<TReadBlocksLocalResponse>& future) mutable
        {
            const auto& response = future.GetValue();
            if (HasError(response.Error)) {
                return MakeFuture<TZeroBlocksLocalResponse>(
                    {.Error = response.Error});
            }

            if (auto p = weakPtr.lock()) {
                return static_cast<TZeroRequest*>(p.get())->ModifyAndWrite();
            }

            return MakeFuture<TZeroBlocksLocalResponse>(
                {.Error = CreateRequestDestroyedResponse()});
        });
}

TZeroRequest::TResponseFuture TZeroRequest::ModifyAndWrite()
{
    auto backend = Backend.lock();
    if (!backend) {
        return MakeFuture<TZeroBlocksLocalResponse>(
            {.Error = CreateBackendDestroyedResponse()});
    }

    std::memset(
        RMWBuffer.get() + BlocksInfo.BeginOffset,
        0,
        BlocksInfo.BufferSize());

    auto result = backend->ExecuteWriteRequest(
        CallContext,
        BlocksInfo.MakeAligned(),
        SgList);
    return result.Apply(
        [](const TFuture<TWriteBlocksLocalResponse>& future)
        {
            return MakeFuture<TZeroBlocksLocalResponse>(
                {.Error = future.GetValue().Error});
        });
}

////////////////////////////////////////////////////////////////////////////////

TUnalignedDeviceHandler::TUnalignedDeviceHandler(
    TDeviceHandlerParams params,
    ui32 maxSubRequestSize)
    : Backend(
          std::make_shared<TAlignedDeviceHandler>(
              std::move(params),
              maxSubRequestSize))
    , BlockSize(Backend->GetBlockSize())
    , MaxUnalignedBlockCount(MaxUnalignedRequestSize / BlockSize)
{}

TUnalignedDeviceHandler::~TUnalignedDeviceHandler()
{
    with_lock (RequestsLock) {
        AlignedRequests.clear();
        UnalignedRequests.clear();
    }
}

TFuture<TReadBlocksLocalResponse> TUnalignedDeviceHandler::Read(
    TCallContextPtr ctx,
    ui64 from,
    ui64 length,
    TGuardedSgList sgList,
    const TString& checkpointId)
{
    auto blocksInfo = TBlocksInfo(from, length, BlockSize);
    auto normalizeError = TryToNormalize(sgList, blocksInfo);
    if (HasError(normalizeError)) {
        return MakeFuture<TReadBlocksLocalResponse>({.Error = normalizeError});
    }
    return blocksInfo.IsAligned() ? Backend->ExecuteReadRequest(
                                        std::move(ctx),
                                        blocksInfo,
                                        std::move(sgList),
                                        checkpointId)
                                  : ExecuteUnalignedReadRequest(
                                        std::move(ctx),
                                        blocksInfo,
                                        std::move(sgList),
                                        checkpointId);
}

TFuture<TWriteBlocksLocalResponse> TUnalignedDeviceHandler::Write(
    TCallContextPtr ctx,
    ui64 from,
    ui64 length,
    TGuardedSgList sgList)
{
    auto blocksInfo = TBlocksInfo(from, length, BlockSize);
    auto normalizeError = TryToNormalize(sgList, blocksInfo);
    if (HasError(normalizeError)) {
        return MakeFuture<TWriteBlocksLocalResponse>({.Error = normalizeError});
    }

    if (!blocksInfo.IsAligned() &&
        blocksInfo.Range.Size() > MaxUnalignedBlockCount)
    {
        return MakeFuture<TWriteBlocksLocalResponse>(
            {.Error = CreateUnalignedTooBigResponse(blocksInfo.Range.Size())});
    }

    auto request = std::make_shared<TWriteRequest>(
        Backend,
        ctx,
        blocksInfo,
        std::move(sgList));
    auto weakRequest = request->weak_from_this();
    auto* rawRequest = request.get();

    const bool readyToRun = RegisterRequest(std::move(request));
    auto result = rawRequest->ExecuteOrPostpone(readyToRun);
    return result.Apply(
        [weakDeviceHandler = weak_from_this(),
         weakRequest = std::move(weakRequest)](
            const TFuture<TWriteBlocksLocalResponse>& f) mutable
        {
            if (auto p = weakDeviceHandler.lock()) {
                p->OnRequestFinished(std::move(weakRequest));
            }
            return f.GetValue();
        });
}

TFuture<TZeroBlocksLocalResponse>
TUnalignedDeviceHandler::Zero(TCallContextPtr ctx, ui64 from, ui64 length)
{
    return Zero(std::move(ctx), TBlocksInfo(from, length, BlockSize));
}

NThreading::TFuture<TZeroBlocksLocalResponse> TUnalignedDeviceHandler::Zero(
    TCallContextPtr ctx,
    const TBlocksInfo& blocksInfo)
{
    auto [currentBlocksInfo, nextBlocksInfo] = blocksInfo.Split();
    auto result = ExecuteZeroBlocksRequest(ctx, currentBlocksInfo);
    return result.Apply(
        [weakPtr = weak_from_this(),
         ctx = std::move(ctx),
         nextBlocksInfo = std::move(nextBlocksInfo)](
            const TFuture<TZeroBlocksLocalResponse>& f) mutable
        {
            const auto& response = f.GetValue();
            if (HasError(response.Error) || !nextBlocksInfo) {
                return f;
            }

            if (auto self = weakPtr.lock()) {
                return self->Zero(std::move(ctx), *nextBlocksInfo);
            }
            return MakeFuture<TZeroBlocksLocalResponse>(
                {.Error = MakeError(E_CANCELLED)});
        });
}

NThreading::TFuture<TZeroBlocksLocalResponse>
TUnalignedDeviceHandler::ExecuteZeroBlocksRequest(
    TCallContextPtr ctx,
    const TBlocksInfo& blocksInfo)
{
    auto request = std::make_shared<TZeroRequest>(Backend, ctx, blocksInfo);
    auto weakRequest = request->weak_from_this();
    auto* rawRequest = request.get();

    const bool readyToRun = RegisterRequest(std::move(request));
    auto result = rawRequest->ExecuteOrPostpone(readyToRun);
    return result.Apply(
        [weakDeviceHandler = weak_from_this(),
         weakRequest = std::move(weakRequest)](
            const TFuture<TZeroBlocksLocalResponse>& f) mutable
        {
            if (auto p = weakDeviceHandler.lock()) {
                p->OnRequestFinished(std::move(weakRequest));
            }
            return f.GetValue();
        });
}

bool TUnalignedDeviceHandler::RegisterRequest(TModifyRequestPtr request)
{
    with_lock (RequestsLock) {
        request->AddDependencies(UnalignedRequests);
        if (request->IsAligned()) {
            AlignedRequests.push_front(request);
            request->SetIt(AlignedRequests.begin());
        } else {
            request->AddDependencies(AlignedRequests);
            UnalignedRequests.push_front(request);
            request->SetIt(UnalignedRequests.begin());
        }
        return request->PrepareToRun();
    }
}

TFuture<TReadBlocksLocalResponse>
TUnalignedDeviceHandler::ExecuteUnalignedReadRequest(
    TCallContextPtr ctx,
    TBlocksInfo blocksInfo,
    TGuardedSgList sgList,
    TString checkpointId) const
{
    if (blocksInfo.Range.Size() > MaxUnalignedBlockCount) {
        return MakeFuture<TReadBlocksLocalResponse>(
            {.Error = CreateUnalignedTooBigResponse(blocksInfo.Range.Size())});
    }

    auto bufferSize = blocksInfo.MakeAligned().BufferSize();
    auto buffer = AllocateBuffer(bufferSize);

    auto sgListOrError =
        SgListNormalize(TBlockDataRef(buffer.get(), bufferSize), BlockSize);

    if (HasError(sgListOrError)) {
        return MakeFuture<TReadBlocksLocalResponse>(
            {.Error = sgListOrError.GetError()});
    }

    auto alignedRequest = Backend->ExecuteReadRequest(
        std::move(ctx),
        blocksInfo.MakeAligned(),
        sgList.Create(sgListOrError.ExtractResult()),
        std::move(checkpointId));

    return alignedRequest.Apply(
        [sgList = std::move(sgList),
         buffer = std::move(buffer),
         beginOffset = blocksInfo.BeginOffset](
            const TFuture<TReadBlocksLocalResponse>& future)
        {
            const auto& response = future.GetValue();
            if (HasError(response.Error)) {
                return response;
            }

            if (auto guard = sgList.Acquire()) {
                const auto& dstSgList = guard.Get();
                auto size = SgListGetSize(dstSgList);
                TBlockDataRef srcBuf(buffer.get() + beginOffset, size);
                auto cpSize = SgListCopy({srcBuf}, dstSgList);
                Y_ABORT_UNLESS(cpSize == size);
                return response;
            }

            return TReadBlocksLocalResponse{
                .Error = CreateErrorAcquireResponse()};
        });
}

void TUnalignedDeviceHandler::OnRequestFinished(
    TModifyRequestWeakPtr weakRequest)
{
    // When the request is complete, it must be unregistered. We
    // also need to find and run postponed requests that are waiting for
    // a completetion this one.

    auto request = weakRequest.lock();
    if (!request) {
        return;
    }

    TVector<TModifyRequest*> readyToRunPostponedRequests;

    auto collectReadyToRunRequests =
        [&](const TList<TModifyRequestPtr>& requests)
    {
        for (const auto& request: requests) {
            if (request->PrepareToRun()) {
                readyToRunPostponedRequests.push_back(request.get());
            }
        }
    };

    with_lock (RequestsLock) {
        request->SetCompleted();
        const bool isAligned = request->IsAligned();
        if (isAligned) {
            AlignedRequests.erase(request->GetIt());
        } else {
            UnalignedRequests.erase(request->GetIt());
        }
        // Here we reset the last reference to the request.
        request.reset();
        Y_DEBUG_ABORT_UNLESS(weakRequest.expired());

        collectReadyToRunRequests(UnalignedRequests);
        if (!isAligned) {
            collectReadyToRunRequests(AlignedRequests);
        }
    }

    for (auto* readyToRunRequest: readyToRunPostponedRequests) {
        readyToRunRequest->ExecutePostponed();
    }
}

}   // namespace NYdb::NBS::NBlockStore
