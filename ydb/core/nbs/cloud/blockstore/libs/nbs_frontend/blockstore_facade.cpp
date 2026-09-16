#include "blockstore_facade.h"

#include "frontend_state.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/device_handler.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/logging.h>

#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/service/service_method.h>

#include <util/generic/size_literals.h>
#include <util/system/yassert.h>

#include <limits>

namespace NYdb::NBS::NBlockStore {

namespace {

namespace NCompatProto = NNbs1CompatApi::NBlockStore::NProto;

constexpr ui64 MaxIoBytes = 32_MB;

NProto::TError ValidateIoMode(ui32 flags, const NCompatProto::THeaders& headers)
{
    if (flags || headers.GetReplicaIndex() || headers.GetReplicaCount()) {
        return MakeError(E_NOT_IMPLEMENTED, "Unsupported NBS2 I/O mode");
    }
    return {};
}

NProto::TError ValidateIoRange(
    const TVolumeConfig& config,
    ui32 blockSize,
    ui64 startIndex,
    ui64 blocksCount)
{
    if (blockSize && blockSize != config.BlockSize) {
        return MakeError(E_ARGUMENT, "BlockSize does not match the partition");
    }
    if (!blocksCount || blocksCount > MaxIoBytes / config.BlockSize) {
        return MakeError(
            E_ARGUMENT,
            "I/O must be nonempty and contain at most 32 MiB");
    }
    if (startIndex >= config.BlockCount ||
        blocksCount > config.BlockCount - startIndex)
    {
        return MakeError(E_ARGUMENT, "I/O range is outside the partition");
    }
    // Validate the byte end before any multiplication or addition in handlers.
    const ui64 maxByteBlocks =
        std::numeric_limits<ui64>::max() / config.BlockSize;
    if (startIndex > maxByteBlocks || blocksCount > maxByteBlocks - startIndex)
    {
        return MakeError(E_ARGUMENT, "I/O byte range overflows uint64");
    }
    return {};
}

template <typename TResponse>
NThreading::TFuture<TResponse> ErrorResponse(NProto::TError error)
{
    TResponse response;
    *response.MutableError() = std::move(error);
    return NThreading::MakeFuture(std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

// Adapts classic control and block I/O requests to the NBS2 frontend/backend.
class TNbsFrontendBlockStore final
    : public NYdb::NBS::NNbs1CompatApi::NBlockStore::TBlockStoreImpl<
          TNbsFrontendBlockStore,
          NYdb::NBS::NNbs1CompatApi::NBlockStore::IBlockStore>
{
public:
    // Shares metadata, admission and session state with the frontend runtime.
    explicit TNbsFrontendBlockStore(
        std::shared_ptr<TFrontendState> frontendState,
        TLog log);

    // Opens the admission gate for requests.
    void Start() override;

    // Closes the admission gate and revokes the active session.
    void Stop() override;

    // Classic callers allocate their protobuf buffers; no external buffer pool.
    NYdb::NBS::NNbs1CompatApi::NBlockStore::TStorageBuffer AllocateBuffer(
        size_t bytesCount) override;

    // Dispatches classic requests through shared admission and session state.
    template <typename TMethod>
    NThreading::TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request);

private:
    NThreading::TFuture<NCompatProto::TReadBlocksResponse> ExecuteReadBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NCompatProto::TReadBlocksRequest> request,
        TFrontendIoBackend backend);

    NThreading::TFuture<NCompatProto::TWriteBlocksResponse> ExecuteWriteBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NCompatProto::TWriteBlocksRequest> request,
        TFrontendIoBackend backend);

    const std::shared_ptr<TFrontendState> FrontendState;
    TLog Log;
};

////////////////////////////////////////////////////////////////////////////////

TNbsFrontendBlockStore::TNbsFrontendBlockStore(
    std::shared_ptr<TFrontendState> frontendState,
    TLog log)
    : FrontendState(std::move(frontendState))
    , Log(std::move(log))
{
    Y_ABORT_UNLESS(FrontendState);
}

void TNbsFrontendBlockStore::Start()
{
    FrontendState->Start();
}

void TNbsFrontendBlockStore::Stop()
{
    FrontendState->Stop();
}

NYdb::NBS::NNbs1CompatApi::NBlockStore::TStorageBuffer
TNbsFrontendBlockStore::AllocateBuffer(size_t bytesCount)
{
    Y_UNUSED(bytesCount);
    return nullptr;
}

template <typename TMethod>
NThreading::TFuture<typename TMethod::TResponse>
TNbsFrontendBlockStore::Execute(
    TCallContextPtr callContext,
    std::shared_ptr<typename TMethod::TRequest> request)
{
    STORAGE_DEBUG(
        TMethod::Name << " RequestId=" << request->GetHeaders().GetRequestId());

    using TResponse = typename TMethod::TResponse;

    TResponse response;
    if constexpr (
        std::is_same_v<
            TMethod,
            NNbs1CompatApi::NBlockStore::TBlockStoreMountVolumeMethod>)
    {
        response = FrontendState->MountVolume(*request);
    } else if constexpr (
        std::is_same_v<
            TMethod,
            NNbs1CompatApi::NBlockStore::TBlockStoreUnmountVolumeMethod>)
    {
        *response.MutableError() = FrontendState->UnmountVolume(
            request->GetDiskId(),
            request->GetHeaders().GetClientId(),
            request->GetSessionId());
    } else if constexpr (
        std::is_same_v<
            TMethod,
            NYdb::NBS::NNbs1CompatApi::NBlockStore::TBlockStorePingMethod>)
    {
        *response.MutableError() = FrontendState->CheckAcceptingRequests();
        if (HasError(response)) {
            STORAGE_DEBUG(
                "Ping RequestId=" << request->GetHeaders().GetRequestId()
                                  << " Error="
                                  << FormatError(response.GetError()));
        }
    } else {
        auto backend = FrontendState->AcquireIoBackend(
            request->GetDiskId(),
            request->GetHeaders().GetClientId(),
            request->GetSessionId());
        if (HasError(backend)) {
            return ErrorResponse<TResponse>(backend.GetError());
        }
        if constexpr (
            std::is_same_v<
                TMethod,
                NNbs1CompatApi::NBlockStore::TBlockStoreReadBlocksMethod>)
        {
            return ExecuteReadBlocks(
                std::move(callContext),
                std::move(request),
                backend.ExtractResult());
        } else {
            return ExecuteWriteBlocks(
                std::move(callContext),
                std::move(request),
                backend.ExtractResult());
        }
    }

    return NThreading::MakeFuture(std::move(response));
}

NThreading::TFuture<NCompatProto::TReadBlocksResponse>
TNbsFrontendBlockStore::ExecuteReadBlocks(
    TCallContextPtr callContext,
    std::shared_ptr<NCompatProto::TReadBlocksRequest> request,
    TFrontendIoBackend backend)
{
    using TResponse = NCompatProto::TReadBlocksResponse;
    if (const auto error =
            ValidateIoMode(request->GetFlags(), request->GetHeaders());
        HasError(error))
    {
        return ErrorResponse<TResponse>(error);
    }
    if (!request->GetCheckpointId().empty()) {
        return ErrorResponse<TResponse>(MakeError(
            E_NOT_IMPLEMENTED,
            "NBS2 checkpoints are not implemented"));
    }
    const auto& config = *backend.IoGeometry;
    if (const auto error = ValidateIoRange(
            config,
            request->GetBlockSize(),
            request->GetStartIndex(),
            request->GetBlocksCount());
        HasError(error))
    {
        return ErrorResponse<TResponse>(error);
    }

    TGuardedBuffer owner(std::make_shared<TResponse>());
    TSgList buffers;
    buffers.reserve(request->GetBlocksCount());
    for (ui32 i = 0; i != request->GetBlocksCount(); ++i) {
        auto* buffer = owner.Get()->MutableBlocks()->AddBuffers();
        buffer->resize(config.BlockSize);
        buffers.emplace_back(buffer->data(), buffer->size());
    }
    auto sglist = owner.CreateGuardedSgList(std::move(buffers));
    const ui64 from = request->GetStartIndex() * config.BlockSize;
    const ui64 length = ui64(request->GetBlocksCount()) * config.BlockSize;
    return backend.Handler
        ->Read(std::move(callContext), from, length, sglist, {})
        .Apply(
            [owner = std::move(owner), sglist, handler = backend.Handler](
                const auto& future) mutable
            {
                Y_UNUSED(handler);
                // Close memory access before moving the protobuf or clearing
                // data.
                sglist.Close();
                const auto& result = future.GetValue();
                auto response = owner.Extract();
                *response->MutableError() = result.Error;
                if (HasError(result.Error)) {
                    response->ClearBlocks();
                }
                return std::move(*response);
            });
}

NThreading::TFuture<NCompatProto::TWriteBlocksResponse>
TNbsFrontendBlockStore::ExecuteWriteBlocks(
    TCallContextPtr callContext,
    std::shared_ptr<NCompatProto::TWriteBlocksRequest> request,
    TFrontendIoBackend backend)
{
    using TResponse = NCompatProto::TWriteBlocksResponse;
    if (const auto error =
            ValidateIoMode(request->GetFlags(), request->GetHeaders());
        HasError(error))
    {
        return ErrorResponse<TResponse>(error);
    }
    if (request->ChecksumsSize()) {
        return ErrorResponse<TResponse>(MakeError(
            E_NOT_IMPLEMENTED,
            "NBS2 write checksums are not implemented"));
    }
    const auto& config = *backend.IoGeometry;
    ui64 length = 0;
    for (const auto& buffer: request->GetBlocks().GetBuffers()) {
        if (buffer.empty() || buffer.size() > MaxIoBytes - length) {
            return ErrorResponse<TResponse>(MakeError(
                E_ARGUMENT,
                "Empty write buffer or payload exceeds 32 MiB"));
        }
        length += buffer.size();
    }
    if (length % config.BlockSize) {
        return ErrorResponse<TResponse>(MakeError(
            E_ARGUMENT,
            "Write payload must contain whole partition blocks"));
    }
    if (const auto error = ValidateIoRange(
            config,
            request->GetBlockSize(),
            request->GetStartIndex(),
            length / config.BlockSize);
        HasError(error))
    {
        return ErrorResponse<TResponse>(error);
    }

    const ui64 from = request->GetStartIndex() * config.BlockSize;
    TGuardedBuffer owner(std::move(request));
    TSgList buffers;
    for (const auto& buffer: owner.Get()->GetBlocks().GetBuffers()) {
        buffers.emplace_back(buffer.data(), buffer.size());
    }
    auto sglist = owner.CreateGuardedSgList(std::move(buffers));
    return backend.Handler->Write(std::move(callContext), from, length, sglist)
        .Apply(
            [owner = std::move(owner), sglist, handler = backend.Handler](
                const auto& future) mutable
            {
                Y_UNUSED(owner);
                Y_UNUSED(handler);
                // The input protobuf owns the payload until all guards are
                // closed.
                sglist.Close();
                const auto& result = future.GetValue();
                TResponse response;
                *response.MutableError() = result.Error;
                return response;
            });
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace

NYdb::NBS::NNbs1CompatApi::NBlockStore::IBlockStorePtr
CreateNbsFrontendBlockStore(
    std::shared_ptr<TFrontendState> frontendState,
    TLog log)
{
    return std::make_shared<TNbsFrontendBlockStore>(
        std::move(frontendState),
        std::move(log));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
