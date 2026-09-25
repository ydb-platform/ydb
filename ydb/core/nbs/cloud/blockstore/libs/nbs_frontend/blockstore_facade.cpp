#include "blockstore_facade.h"

#include "partition_registry.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/device_handler.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/model/log_prefix.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/session/partition_session.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/session/partition_session_control.h>

#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/logging.h>

#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/service/service_method.h>
#include <ydb/core/protos/blockstore_config.pb.h>

#include <util/string/builder.h>

#include <atomic>
#include <limits>

namespace NYdb::NBS::NBlockStore {

namespace NCompatProto = NNbs1CompatApi::NBlockStore::NProto;
using namespace NStorage::NPartitionDirect;

namespace {

NProto::TError ValidateMountParameters(
    const NCompatProto::TMountVolumeRequest& request)
{
    if (request.GetVolumeAccessMode() !=
            NCompatProto::VOLUME_ACCESS_READ_WRITE ||
        (request.GetVolumeMountMode() != NCompatProto::VOLUME_MOUNT_LOCAL &&
         request.GetVolumeMountMode() != NCompatProto::VOLUME_MOUNT_REMOTE))
    {
        return MakeError(E_NOT_IMPLEMENTED, "Unsupported NBS2 mount mode");
    }

    // TODO: Implement mount/writer generation and fencing, as well as disk fill
    // sequence/generation checks, before accepting nonzero generation values.
    if (request.GetMountSeqNumber() || request.GetFillSeqNumber() ||
        request.GetFillGeneration())
    {
        return MakeError(
            E_NOT_IMPLEMENTED,
            "NBS2 mount generations are not implemented");
    }

    const auto& encryption = request.GetEncryptionSpec();
    if (request.GetMountFlags() || request.GetThrottlingDisabled() ||
        !request.GetToken().empty() || request.GetForceDisableEncryption() ||
        encryption.GetMode() != NCompatProto::NO_ENCRYPTION ||
        encryption.HasKeyPath() || encryption.HasKeyHash())
    {
        return MakeError(
            E_NOT_IMPLEMENTED,
            "Unsupported NBS2 mount parameters");
    }
    // MVP simplification: for a given disk, only ClientId identifies the
    // session owner. InstanceId, IPC/version information and ForceRemoteBinding
    // are ignored; revisit their role with the full session/access contract.
    // RequestId and tracing headers describe individual requests, not sessions.
    return {};
}

NNbs1CompatApi::NBlockStore::NProto::TVolume MakeClassicVolume(
    const NKikimrBlockStore::TVolumeConfig& config)
{
    NNbs1CompatApi::NBlockStore::NProto::TVolume volume;
    volume.SetDiskId(config.GetDiskId());
    volume.SetBlockSize(config.GetBlockSize());
    volume.SetBlocksCount(config.GetPartitions(0).GetBlockCount());
    volume.SetPartitionsCount(config.PartitionsSize());
    // Registration accepts only native SSD; map explicitly to the wire enum.
    volume.SetStorageMediaKind(NNbs1CompatApi::NProto::STORAGE_MEDIA_SSD);
    volume.SetConfigVersion(config.GetVersion());
    volume.SetProjectId(config.GetProjectId());
    volume.SetFolderId(config.GetFolderId());
    volume.SetCloudId(config.GetCloudId());
    return volume;
}

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
    if (!blocksCount || blocksCount > MaxGrpcIoBytes / config.BlockSize) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "I/O must be nonempty and contain at most "
                             << MaxGrpcIoBytes << " bytes");
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

// Keeps frontend rejection and backend failure diagnostics in the same format.
template <typename TMethod>
void LogRequestError(
    TLog& Log,
    const typename TMethod::TRequest& request,
    const NProto::TError& error)
{
    STORAGE_WARN(
        (TLogPrefix(
            {{"d", request.GetDiskId()},
             {"r", request.GetHeaders().GetRequestId()}}))
        << " " << TMethod::Name << " Error=" << FormatError(error));
}

template <typename TResponse>
NThreading::TFuture<TResponse> ErrorResponse(NProto::TError error)
{
    TResponse response;
    *response.MutableError() = std::move(error);
    return NThreading::MakeFuture(std::move(response));
}

// Implements the classic API; registry and session publication stay behind
// their own interfaces.
class TNbsBlockStoreFacade final
    : public NNbs1CompatApi::NBlockStore::
          TBlockStoreImpl<TNbsBlockStoreFacade, INbsBlockStoreFacade>
{
public:
    explicit TNbsBlockStoreFacade(TLog log);
    ~TNbsBlockStoreFacade() noexcept override;

    void Start() override;
    void Stop() override;
    NNbs1CompatApi::NBlockStore::TStorageBuffer AllocateBuffer(
        size_t bytesCount) override;
    TResultOrError<TString> RegisterVolume(
        TPartitionSessionPtr session,
        IPartitionSessionControlPtr control) override;
    void UnregisterVolume(
        const TString& diskId,
        const TString& registrationId) override;

    template <typename TMethod>
    NThreading::TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request);

private:
    NThreading::TFuture<NCompatProto::TMountVolumeResponse> ExecuteMountVolume(
        const NCompatProto::TMountVolumeRequest& request,
        const TPartitionRegistration& registration);
    NThreading::TFuture<NCompatProto::TUnmountVolumeResponse>
    ExecuteUnmountVolume(
        const NCompatProto::TUnmountVolumeRequest& request,
        const TPartitionRegistration& registration);
    NThreading::TFuture<NCompatProto::TReadBlocksResponse> ExecuteReadBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NCompatProto::TReadBlocksRequest> request,
        TPartitionIoBackend backend);
    NThreading::TFuture<NCompatProto::TWriteBlocksResponse> ExecuteWriteBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NCompatProto::TWriteBlocksRequest> request,
        TPartitionIoBackend backend);

    std::atomic_bool AcceptingRequests = false;
    // Serializes admission closure with session command dispatch.
    TMutex SessionDispatchMutex;
    TPartitionRegistry PartitionRegistry;
    TLog Log;
};

TNbsBlockStoreFacade::TNbsBlockStoreFacade(TLog log)
    : Log(std::move(log))
{}

TNbsBlockStoreFacade::~TNbsBlockStoreFacade() noexcept = default;

void TNbsBlockStoreFacade::Start()
{
    AcceptingRequests.store(true);
}

void TNbsBlockStoreFacade::Stop()
{
    // Synchronize with control calls so none is started after Stop() returns
    // unless Start() reopens admission. Already submitted requests
    // and admitted I/O are not drained.
    with_lock (SessionDispatchMutex) {
        AcceptingRequests.store(false);
    }
}

NNbs1CompatApi::NBlockStore::TStorageBuffer
TNbsBlockStoreFacade::AllocateBuffer(size_t bytesCount)
{
    Y_UNUSED(bytesCount);
    return nullptr;
}

TResultOrError<TString> TNbsBlockStoreFacade::RegisterVolume(
    TPartitionSessionPtr session,
    IPartitionSessionControlPtr control)
{
    return PartitionRegistry.Register(std::move(session), std::move(control));
}

void TNbsBlockStoreFacade::UnregisterVolume(
    const TString& diskId,
    const TString& registrationId)
{
    PartitionRegistry.Unregister(diskId, registrationId);
}

template <typename TMethod>
NThreading::TFuture<typename TMethod::TResponse> TNbsBlockStoreFacade::Execute(
    TCallContextPtr callContext,
    std::shared_ptr<typename TMethod::TRequest> request)
{
    using namespace NNbs1CompatApi::NBlockStore;
    using TResponse = typename TMethod::TResponse;

    STORAGE_DEBUG(
        TMethod::Name << " RequestId=" << request->GetHeaders().GetRequestId());

    if (!AcceptingRequests.load()) {
        const auto error =
            MakeError(E_REJECTED, "NBS2 frontend is not accepting requests");
        if constexpr (!std::is_same_v<TMethod, TBlockStorePingMethod>) {
            LogRequestError<TMethod>(Log, *request, error);
        }
        return ErrorResponse<TResponse>(error);
    }

    if constexpr (std::is_same_v<TMethod, TBlockStorePingMethod>) {
        return NThreading::MakeFuture<TResponse>();
    } else {
        const auto partition = PartitionRegistry.Find(request->GetDiskId());
        if (!partition) {
            const auto error = MakeError(
                E_NOT_FOUND,
                "Disk is not registered on this NBS2 host");
            LogRequestError<TMethod>(Log, *request, error);
            return ErrorResponse<TResponse>(error);
        }

        if constexpr (std::is_same_v<TMethod, TBlockStoreMountVolumeMethod>) {
            return ExecuteMountVolume(*request, *partition);
        } else if constexpr (
            std::is_same_v<TMethod, TBlockStoreUnmountVolumeMethod>)
        {
            return ExecuteUnmountVolume(*request, *partition);
        } else {
            static_assert(
                std::is_same_v<TMethod, TBlockStoreReadBlocksMethod> ||
                    std::is_same_v<TMethod, TBlockStoreWriteBlocksMethod>,
                "Unsupported classic NBS method");

            auto backend = partition->Session->AcquireIoBackend(
                request->GetHeaders().GetClientId(),
                request->GetSessionId());
            if (HasError(backend)) {
                LogRequestError<TMethod>(Log, *request, backend.GetError());
                return ErrorResponse<TResponse>(backend.GetError());
            }
            if constexpr (std::is_same_v<TMethod, TBlockStoreReadBlocksMethod>)
            {
                return ExecuteReadBlocks(
                    std::move(callContext),
                    std::move(request),
                    backend.ExtractResult());
            } else if constexpr (
                std::is_same_v<TMethod, TBlockStoreWriteBlocksMethod>)
            {
                return ExecuteWriteBlocks(
                    std::move(callContext),
                    std::move(request),
                    backend.ExtractResult());
            }
        }
    }
}

NThreading::TFuture<NCompatProto::TMountVolumeResponse>
TNbsBlockStoreFacade::ExecuteMountVolume(
    const NCompatProto::TMountVolumeRequest& request,
    const TPartitionRegistration& registration)
{
    using TMethod = NNbs1CompatApi::NBlockStore::TBlockStoreMountVolumeMethod;
    if (request.GetHeaders().GetClientId().empty()) {
        return ErrorResponse<NCompatProto::TMountVolumeResponse>(
            MakeError(E_ARGUMENT, "MountVolume requires ClientId"));
    }
    if (const auto error = ValidateMountParameters(request); HasError(error)) {
        return ErrorResponse<NCompatProto::TMountVolumeResponse>(error);
    }
    auto session = registration.Session;
    NThreading::TFuture<TResultOrError<TString>> mounted;
    // Stop() may close admission after lookup; recheck it under the same lock
    // as dispatch. Partition teardown is handled by the command recipient.
    with_lock (SessionDispatchMutex) {
        if (!AcceptingRequests.load()) {
            const auto error = MakeError(
                E_REJECTED,
                "NBS2 frontend is not accepting requests");
            LogRequestError<TMethod>(Log, request, error);
            return ErrorResponse<NCompatProto::TMountVolumeResponse>(error);
        }
        mounted =
            registration.Control->Mount(request.GetHeaders().GetClientId());
    }
    return mounted.Apply(
        [session = std::move(session)](const auto& future)
        {
            NCompatProto::TMountVolumeResponse response;
            const auto& result = future.GetValue();
            if (HasError(result)) {
                *response.MutableError() = result.GetError();
            } else {
                response.SetSessionId(result.GetResult());
                *response.MutableVolume() =
                    MakeClassicVolume(session->GetVolumeMetadata());
                // No inactivity expiry in MVP.
                response.SetInactiveClientsTimeout(0);
            }
            return response;
        });
}

NThreading::TFuture<NCompatProto::TUnmountVolumeResponse>
TNbsBlockStoreFacade::ExecuteUnmountVolume(
    const NCompatProto::TUnmountVolumeRequest& request,
    const TPartitionRegistration& registration)
{
    using TMethod = NNbs1CompatApi::NBlockStore::TBlockStoreUnmountVolumeMethod;
    NThreading::TFuture<NProto::TError> unmounted;
    with_lock (SessionDispatchMutex) {
        if (!AcceptingRequests.load()) {
            const auto error = MakeError(
                E_REJECTED,
                "NBS2 frontend is not accepting requests");
            LogRequestError<TMethod>(Log, request, error);
            return ErrorResponse<NCompatProto::TUnmountVolumeResponse>(error);
        }
        unmounted = registration.Control->Unmount(
            request.GetHeaders().GetClientId(),
            request.GetSessionId());
    }
    return unmounted.Apply(
        [](const auto& future)
        {
            NCompatProto::TUnmountVolumeResponse response;
            *response.MutableError() = future.GetValue();
            return response;
        });
}

NThreading::TFuture<NCompatProto::TReadBlocksResponse>
TNbsBlockStoreFacade::ExecuteReadBlocks(
    TCallContextPtr callContext,
    std::shared_ptr<NCompatProto::TReadBlocksRequest> request,
    TPartitionIoBackend backend)
{
    using TMethod = NNbs1CompatApi::NBlockStore::TBlockStoreReadBlocksMethod;
    using TResponse = TMethod::TResponse;
    if (const auto error =
            ValidateIoMode(request->GetFlags(), request->GetHeaders());
        HasError(error))
    {
        LogRequestError<TMethod>(Log, *request, error);
        return ErrorResponse<TResponse>(error);
    }
    if (!request->GetCheckpointId().empty()) {
        const auto error = MakeError(
            E_NOT_IMPLEMENTED,
            "NBS2 checkpoints are not implemented");
        LogRequestError<TMethod>(Log, *request, error);
        return ErrorResponse<TResponse>(error);
    }
    const auto& config = *backend.IoGeometry;
    if (const auto error = ValidateIoRange(
            config,
            request->GetBlockSize(),
            request->GetStartIndex(),
            request->GetBlocksCount());
        HasError(error))
    {
        LogRequestError<TMethod>(Log, *request, error);
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
            [owner = std::move(owner),
             request = std::move(request),
             sglist,
             handler = backend.Handler,
             Log = Log](const auto& future) mutable
            {
                Y_UNUSED(handler);
                // Close memory access before moving the protobuf or clearing
                // data.
                sglist.Close();
                const auto& result = future.GetValue();
                auto response = owner.Extract();
                *response->MutableError() = result.Error;
                if (HasError(result.Error)) {
                    LogRequestError<TMethod>(Log, *request, result.Error);
                    response->ClearBlocks();
                }
                return std::move(*response);
            });
}

NThreading::TFuture<NCompatProto::TWriteBlocksResponse>
TNbsBlockStoreFacade::ExecuteWriteBlocks(
    TCallContextPtr callContext,
    std::shared_ptr<NCompatProto::TWriteBlocksRequest> request,
    TPartitionIoBackend backend)
{
    using TMethod = NNbs1CompatApi::NBlockStore::TBlockStoreWriteBlocksMethod;
    using TResponse = TMethod::TResponse;
    if (const auto error =
            ValidateIoMode(request->GetFlags(), request->GetHeaders());
        HasError(error))
    {
        LogRequestError<TMethod>(Log, *request, error);
        return ErrorResponse<TResponse>(error);
    }
    if (request->ChecksumsSize()) {
        const auto error = MakeError(
            E_NOT_IMPLEMENTED,
            "NBS2 write checksums are not implemented");
        LogRequestError<TMethod>(Log, *request, error);
        return ErrorResponse<TResponse>(error);
    }
    const auto& config = *backend.IoGeometry;
    ui64 length = 0;
    for (const auto& buffer: request->GetBlocks().GetBuffers()) {
        if (buffer.empty() || buffer.size() > MaxGrpcIoBytes - length) {
            const auto error = MakeError(
                E_ARGUMENT,
                TStringBuilder() << "Empty write buffer or payload exceeds "
                                 << MaxGrpcIoBytes << " bytes");
            LogRequestError<TMethod>(Log, *request, error);
            return ErrorResponse<TResponse>(error);
        }
        length += buffer.size();
    }
    if (length % config.BlockSize) {
        const auto error = MakeError(
            E_ARGUMENT,
            "Write payload must contain whole partition blocks");
        LogRequestError<TMethod>(Log, *request, error);
        return ErrorResponse<TResponse>(error);
    }
    if (const auto error = ValidateIoRange(
            config,
            request->GetBlockSize(),
            request->GetStartIndex(),
            length / config.BlockSize);
        HasError(error))
    {
        LogRequestError<TMethod>(Log, *request, error);
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
            [owner = std::move(owner),
             sglist,
             handler = backend.Handler,
             Log = Log](const auto& future) mutable
            {
                Y_UNUSED(handler);
                // The input protobuf owns the payload until all guards are
                // closed.
                sglist.Close();
                const auto& result = future.GetValue();
                if (HasError(result.Error)) {
                    LogRequestError<TMethod>(Log, *owner.Get(), result.Error);
                }
                TResponse response;
                *response.MutableError() = result.Error;
                return response;
            });
}

}   // namespace

INbsBlockStoreFacadePtr CreateNbsBlockStoreFacade(TLog log)
{
    return std::make_shared<TNbsBlockStoreFacade>(std::move(log));
}

}   // namespace NYdb::NBS::NBlockStore
