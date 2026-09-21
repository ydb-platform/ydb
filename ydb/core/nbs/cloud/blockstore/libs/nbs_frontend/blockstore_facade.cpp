#include "blockstore_facade.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/device_handler.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/session/events.h>

#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/logging.h>

#include <ydb/core/protos/blockstore_config.pb.h>

#include <ydb/library/actors/core/actorsystem.h>

#include <util/generic/hash.h>
#include <util/generic/size_literals.h>

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

// Payload limit, excluding protobuf overhead. GRpcConfig.MaxMessageSize and
// client message limits must allow the serialized request/response. Smaller
// transport limits can fail with RESOURCE_EXHAUSTED instead of an NBS error.
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

// Keeps frontend rejection and backend failure diagnostics in the same format.
template <typename TMethod>
void LogRequestError(
    TLog& Log,
    const typename TMethod::TRequest& request,
    const NProto::TError& error)
{
    STORAGE_WARN(
        TMethod::Name << " RequestId=" << request.GetHeaders().GetRequestId()
                      << " DiskId=" << request.GetDiskId()
                      << " Error=" << FormatError(error));
}

template <typename TResponse>
NThreading::TFuture<TResponse> ErrorResponse(NProto::TError error)
{
    TResponse response;
    *response.MutableError() = std::move(error);
    return NThreading::MakeFuture(std::move(response));
}

}   // namespace

// Control and data references must always come from the same registration.
struct TNbsFrontendBlockStore::TPartitionRegistration
{
    NActors::TActorSystem* ActorSystem;
    NActors::TActorId ActorId;
    std::shared_ptr<TPartitionSessionState> SessionState;
};

// Registry updates are rare; I/O reads a single immutable map snapshot.
struct TNbsFrontendBlockStore::TSnapshot
{
    bool AcceptingRequests = false;
    THashMap<TString, std::shared_ptr<const TPartitionRegistration>> Partitions;
};

TNbsFrontendBlockStore::TNbsFrontendBlockStore(TLog log)
    : Snapshot(new TSnapshot())
    , Log(std::move(log))
{}

TNbsFrontendBlockStore::~TNbsFrontendBlockStore() noexcept = default;

void TNbsFrontendBlockStore::Start()
{
    with_lock (Mutex) {
        auto next = std::make_unique<TSnapshot>(*Snapshot);
        next->AcceptingRequests = true;
        Snapshot.atomic_store(TTrueAtomicSharedPtr<TSnapshot>(next.release()));
    }
}

void TNbsFrontendBlockStore::Stop()
{
    with_lock (Mutex) {
        auto next = std::make_unique<TSnapshot>(*Snapshot);
        next->AcceptingRequests = false;
        Snapshot.atomic_store(TTrueAtomicSharedPtr<TSnapshot>(next.release()));
    }
}

NNbs1CompatApi::NBlockStore::TStorageBuffer
TNbsFrontendBlockStore::AllocateBuffer(size_t bytesCount)
{
    Y_UNUSED(bytesCount);
    return nullptr;
}

TResultOrError<TString> TNbsFrontendBlockStore::RegisterVolume(
    NActors::TActorSystem* actorSystem,
    const NActors::TActorId& actorId,
    std::shared_ptr<TPartitionSessionState> sessionState)
{
    if (!actorSystem || !actorId || !sessionState) {
        return MakeError(
            E_ARGUMENT,
            "Missing partition control or session target");
    }
    const auto& diskId = sessionState->GetVolumeMetadata().GetDiskId();
    const TString registrationId = sessionState->GetRegistrationId();
    with_lock (Mutex) {
        auto next = std::make_unique<TSnapshot>(*Snapshot);
        next->Partitions[diskId] =
            std::make_shared<TPartitionRegistration>(TPartitionRegistration{
                actorSystem,
                actorId,
                std::move(sessionState)});
        Snapshot.atomic_store(TTrueAtomicSharedPtr<TSnapshot>(next.release()));
    }
    return registrationId;
}

void TNbsFrontendBlockStore::UnregisterVolume(
    const TString& diskId,
    const TString& registrationId)
{
    with_lock (Mutex) {
        const auto it = Snapshot->Partitions.find(diskId);
        if (it == Snapshot->Partitions.end() ||
            it->second->SessionState->GetRegistrationId() != registrationId)
        {
            return;
        }
        auto next = std::make_unique<TSnapshot>(*Snapshot);
        next->Partitions.erase(diskId);
        Snapshot.atomic_store(TTrueAtomicSharedPtr<TSnapshot>(next.release()));
    }
}

TResultOrError<
    std::shared_ptr<const TNbsFrontendBlockStore::TPartitionRegistration>>
TNbsFrontendBlockStore::FindPartition(const TString& diskId) const
{
    const auto snapshot = Snapshot.atomic_load();
    if (!snapshot->AcceptingRequests) {
        return MakeError(E_REJECTED, "NBS2 frontend is not accepting requests");
    }
    const auto it = snapshot->Partitions.find(diskId);
    if (it == snapshot->Partitions.end()) {
        return MakeError(
            E_NOT_FOUND,
            "Disk is not registered on this NBS2 host");
    }
    return it->second;
}

template <typename TEvent>
auto TNbsFrontendBlockStore::SendSessionRequest(
    const std::shared_ptr<const TPartitionRegistration>& partition,
    std::unique_ptr<TEvent> event)
{
    const auto future = event->Result.GetFuture();
    // Serialize sending with unregister, so actor-system cleanup cannot leave
    // a sender using the ActorSystem pointer from an already removed entry.
    with_lock (Mutex) {
        const auto current = FindPartition(
            partition->SessionState->GetVolumeMetadata().GetDiskId());
        if (HasError(current)) {
            event->Result.TrySetValue(current.GetError());
        } else if (current.GetResult() != partition) {
            event->Result.TrySetValue(
                MakeError(E_REJECTED, "Partition registration changed"));
        } else {
            partition->ActorSystem->Send(partition->ActorId, event.release());
        }
    }
    return future;
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

    if constexpr (std::is_same_v<
                      TMethod,
                      NNbs1CompatApi::NBlockStore::TBlockStorePingMethod>)
    {
        TResponse response;
        if (!Snapshot.atomic_load()->AcceptingRequests) {
            *response.MutableError() = MakeError(
                E_REJECTED,
                "NBS2 frontend is not accepting requests");
        }
        return NThreading::MakeFuture(std::move(response));
    } else {
        auto found = FindPartition(request->GetDiskId());
        if (HasError(found)) {
            LogRequestError<TMethod>(Log, *request, found.GetError());
            return ErrorResponse<TResponse>(found.GetError());
        }
        auto partition = found.ExtractResult();
        if constexpr (
            std::is_same_v<
                TMethod,
                NNbs1CompatApi::NBlockStore::TBlockStoreMountVolumeMethod>)
        {
            return ExecuteMountVolume(*request, std::move(partition));
        } else if constexpr (
            std::is_same_v<
                TMethod,
                NNbs1CompatApi::NBlockStore::TBlockStoreUnmountVolumeMethod>)
        {
            return ExecuteUnmountVolume(*request, std::move(partition));
        } else {
            auto backend = partition->SessionState->AcquireIoBackend(
                request->GetHeaders().GetClientId(),
                request->GetSessionId());
            if (HasError(backend)) {
                LogRequestError<TMethod>(Log, *request, backend.GetError());
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
    }
}

NThreading::TFuture<NCompatProto::TMountVolumeResponse>
TNbsFrontendBlockStore::ExecuteMountVolume(
    const NCompatProto::TMountVolumeRequest& request,
    std::shared_ptr<const TPartitionRegistration> partition)
{
    if (request.GetHeaders().GetClientId().empty()) {
        return ErrorResponse<NCompatProto::TMountVolumeResponse>(
            MakeError(E_ARGUMENT, "MountVolume requires ClientId"));
    }
    if (const auto error = ValidateMountParameters(request); HasError(error)) {
        return ErrorResponse<NCompatProto::TMountVolumeResponse>(error);
    }
    auto event = std::make_unique<TEvPartitionSession::TEvMount>(
        partition->SessionState->GetRegistrationId(),
        request.GetHeaders().GetClientId());
    return SendSessionRequest(partition, std::move(event))
        .Apply(
            [partition = std::move(partition)](const auto& future)
            {
                NCompatProto::TMountVolumeResponse response;
                const auto& result = future.GetValue();
                if (HasError(result)) {
                    *response.MutableError() = result.GetError();
                } else {
                    response.SetSessionId(result.GetResult());
                    *response.MutableVolume() = MakeClassicVolume(
                        partition->SessionState->GetVolumeMetadata());
                    // No inactivity expiry in MVP.
                    response.SetInactiveClientsTimeout(0);
                }
                return response;
            });
}

NThreading::TFuture<NCompatProto::TUnmountVolumeResponse>
TNbsFrontendBlockStore::ExecuteUnmountVolume(
    const NCompatProto::TUnmountVolumeRequest& request,
    std::shared_ptr<const TPartitionRegistration> partition)
{
    auto event = std::make_unique<TEvPartitionSession::TEvUnmount>(
        partition->SessionState->GetRegistrationId(),
        request.GetHeaders().GetClientId(),
        request.GetSessionId());
    return SendSessionRequest(partition, std::move(event))
        .Apply(
            [](const auto& future)
            {
                NCompatProto::TUnmountVolumeResponse response;
                *response.MutableError() = future.GetValue();
                return response;
            });
}

NThreading::TFuture<NCompatProto::TReadBlocksResponse>
TNbsFrontendBlockStore::ExecuteReadBlocks(
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
TNbsFrontendBlockStore::ExecuteWriteBlocks(
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
        if (buffer.empty() || buffer.size() > MaxIoBytes - length) {
            const auto error = MakeError(
                E_ARGUMENT,
                "Empty write buffer or payload exceeds 32 MiB");
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

std::shared_ptr<TNbsFrontendBlockStore> CreateNbsFrontendBlockStore(TLog log)
{
    return std::make_shared<TNbsFrontendBlockStore>(std::move(log));
}

}   // namespace NYdb::NBS::NBlockStore
