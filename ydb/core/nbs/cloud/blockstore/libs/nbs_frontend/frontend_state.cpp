#include "frontend_state.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/device_handler.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/overlapped_requests_guard_wrapper.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/split_requests_wrapper.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/storage_gate.h>

#include <ydb/core/nbs/cloud/storage/core/protos/media.pb.h>

#include <ydb/core/protos/blockstore_config.pb.h>

#include <util/generic/guid.h>

#include <memory>
#include <optional>

namespace NYdb::NBS::NBlockStore {

namespace {

namespace NCompatProto = NNbs1CompatApi::NBlockStore::NProto;

// The MVP supports a single client/writer, without persistent state.
struct TSession
{
    TString ClientId;
    TString SessionId;
    IDeviceHandlerPtr Handler;
};

NProto::TError NotAcceptingRequests()
{
    return MakeError(E_REJECTED, "NBS2 frontend is not accepting requests");
}

NProto::TError InvalidSession()
{
    return MakeError(E_BS_INVALID_SESSION, "Invalid NBS2 frontend session");
}

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
    // InstanceId, IPC/version information and ForceRemoteBinding are not part
    // of MVP session identity. RequestId and other headers are not
    // either.
    return {};
}

NProto::TError ValidateVolumeMetadata(
    const NKikimrBlockStore::TVolumeConfig& config)
{
    if (config.GetDiskId().empty() || config.PartitionsSize() != 1 ||
        !IsSupportedBlockSize(config.GetBlockSize()) ||
        !config.GetPartitions(0).GetBlockCount() ||
        config.GetStorageMediaKind() != NProto::STORAGE_MEDIA_SSD)
    {
        return MakeError(
            E_ARGUMENT,
            "NBS2 frontend requires a disk id, one nonempty partition, "
            "a supported block size and SSD media");
    }
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

}   // namespace

// A request holds one immutable version of admission, registration and session
// state.
struct TFrontendState::TSnapshot
{
    bool AcceptingRequests = false;
    std::optional<NKikimrBlockStore::TVolumeConfig> VolumeMetadata;
    TString RegistrationId;
    std::optional<TSession> Session;
    TStorageGatePtr StorageGate;
    IStoragePtr Storage;
    TVolumeConfigPtr IoGeometry;
};

TFrontendState::TFrontendState()
    : Snapshot(new TSnapshot())
{}

TFrontendState::~TFrontendState() noexcept = default;

void TFrontendState::Start()
{
    with_lock (Mutex) {
        auto next = std::make_unique<TSnapshot>(*Snapshot);
        next->AcceptingRequests = true;
        Snapshot.atomic_store(TTrueAtomicSharedPtr<TSnapshot>(next.release()));
    }
}

void TFrontendState::Stop()
{
    with_lock (Mutex) {
        auto next = std::make_unique<TSnapshot>(*Snapshot);
        next->AcceptingRequests = false;
        next->Session.reset();
        Snapshot.atomic_store(TTrueAtomicSharedPtr<TSnapshot>(next.release()));
    }
}

NProto::TError TFrontendState::CheckAcceptingRequests() const
{
    const auto snapshot = Snapshot.atomic_load();
    return snapshot->AcceptingRequests ? NProto::TError{}
                                       : NotAcceptingRequests();
}

TResultOrError<TString> TFrontendState::RegisterVolume(
    const NKikimrBlockStore::TVolumeConfig& volumeMetadata,
    IStoragePtr storage,
    TVolumeConfigPtr ioGeometry)
{
    if (const auto error = ValidateVolumeMetadata(volumeMetadata);
        HasError(error))
    {
        return error;
    }

    if (!storage || !ioGeometry ||
        ioGeometry->DiskId != volumeMetadata.GetDiskId() ||
        ioGeometry->BlockSize != volumeMetadata.GetBlockSize() ||
        ioGeometry->BlockCount !=
            volumeMetadata.GetPartitions(0).GetBlockCount() ||
        !ioGeometry->BlocksPerStripe || !ioGeometry->VChunkSize)
    {
        return MakeError(
            E_ARGUMENT,
            "Missing or inconsistent partition backend");
    }

    with_lock (Mutex) {
        if (Snapshot->VolumeMetadata &&
            Snapshot->VolumeMetadata->GetDiskId() != volumeMetadata.GetDiskId())
        {
            return MakeError(
                E_ARGUMENT,
                "NBS2 frontend supports only one disk");
        }

        auto snapshot = std::make_unique<TSnapshot>(*Snapshot);
        snapshot->VolumeMetadata = volumeMetadata;
        snapshot->Session.reset();
        snapshot->RegistrationId = CreateGuidAsString();
        snapshot->StorageGate =
            std::make_shared<TStorageGate>(std::move(storage));
        snapshot->Storage = CreateOverlappedRequestsGuardStorageWrapper(
            CreateSplitRequestsStorageWrapper(snapshot->StorageGate));
        snapshot->IoGeometry = std::move(ioGeometry);
        const TString registrationId = snapshot->RegistrationId;
        // A retained old handler must never start using a replacement backend.
        if (Snapshot->StorageGate) {
            Snapshot->StorageGate->Detach();
        }
        Snapshot.atomic_store(
            TTrueAtomicSharedPtr<TSnapshot>(snapshot.release()));
        return registrationId;
    }
}

void TFrontendState::UnregisterVolume(const TString& registrationId)
{
    with_lock (Mutex) {
        if (registrationId.empty() ||
            Snapshot->RegistrationId != registrationId)
        {
            return;
        }
        auto snapshot = std::make_unique<TSnapshot>(*Snapshot);
        snapshot->StorageGate->Detach();
        snapshot->VolumeMetadata.reset();
        snapshot->RegistrationId.clear();
        snapshot->Session.reset();
        snapshot->StorageGate.reset();
        snapshot->Storage.reset();
        snapshot->IoGeometry.reset();
        Snapshot.atomic_store(
            TTrueAtomicSharedPtr<TSnapshot>(snapshot.release()));
    }
}

TResultOrError<NNbs1CompatApi::NBlockStore::NProto::TVolume>
TFrontendState::GetVolume(const TString& diskId) const
{
    const auto snapshot = Snapshot.atomic_load();
    if (const auto error = CheckDisk(*snapshot, diskId); HasError(error)) {
        return error;
    }
    return MakeClassicVolume(*snapshot->VolumeMetadata);
}

NNbs1CompatApi::NBlockStore::NProto::TMountVolumeResponse
TFrontendState::MountVolume(const NCompatProto::TMountVolumeRequest& request)
{
    NCompatProto::TMountVolumeResponse response;
    with_lock (Mutex) {
        if (const auto error = CheckDisk(*Snapshot, request.GetDiskId());
            HasError(error))
        {
            *response.MutableError() = error;
            return response;
        }
        const auto& clientId = request.GetHeaders().GetClientId();
        if (clientId.empty()) {
            *response.MutableError() =
                MakeError(E_ARGUMENT, "MountVolume requires ClientId");
            return response;
        }
        if (const auto error = ValidateMountParameters(request);
            HasError(error))
        {
            *response.MutableError() = error;
            return response;
        }
        if (Snapshot->Session && Snapshot->Session->ClientId != clientId) {
            *response.MutableError() = MakeError(
                E_BS_MOUNT_CONFLICT,
                "NBS2 disk is mounted by another client");
            return response;
        }

        if (!Snapshot->Session) {
            auto next = std::make_unique<TSnapshot>(*Snapshot);
            const auto& config = *next->IoGeometry;
            auto handler =
                CreateDefaultDeviceHandlerFactory()->CreateDeviceHandler({
                    .Storage = next->Storage,
                    .DiskId = config.DiskId,
                    .ClientId = clientId,
                    .BlockSize = config.BlockSize,
                    .BlockCount = config.BlockCount,
                    .BlocksPerStripeCount = config.BlocksPerStripe,
                    .VChunkSize = config.VChunkSize,
                    .StorageMediaKind = static_cast<NProto::EStorageMediaKind>(
                        next->VolumeMetadata->GetStorageMediaKind()),
                });
            next->Session =
                TSession{clientId, CreateGuidAsString(), std::move(handler)};
            Snapshot.atomic_store(
                TTrueAtomicSharedPtr<TSnapshot>(next.release()));
        }
        *response.MutableVolume() =
            MakeClassicVolume(*Snapshot->VolumeMetadata);
        response.SetSessionId(Snapshot->Session->SessionId);
        // No inactivity expiry in MVP; zero disables NBS1 periodic remount.
        response.SetInactiveClientsTimeout(0);
    }
    return response;
}

NProto::TError TFrontendState::UnmountVolume(
    const TString& diskId,
    const TString& clientId,
    const TString& sessionId)
{
    with_lock (Mutex) {
        if (const auto error = CheckDisk(*Snapshot, diskId); HasError(error)) {
            return error;
        }
        if (clientId.empty() || sessionId.empty()) {
            return InvalidSession();
        }
        // No token history: S_ALREADY means nothing is currently mounted,
        // not that the supplied session ever existed.
        if (!Snapshot->Session) {
            return MakeError(S_ALREADY, "NBS2 disk has no active session");
        }
        if (const auto error = CheckSession(*Snapshot, clientId, sessionId);
            HasError(error))
        {
            return error;
        }
        auto next = std::make_unique<TSnapshot>(*Snapshot);
        next->Session.reset();
        Snapshot.atomic_store(TTrueAtomicSharedPtr<TSnapshot>(next.release()));
    }
    return {};
}

TResultOrError<TFrontendIoBackend> TFrontendState::AcquireIoBackend(
    const TString& diskId,
    const TString& clientId,
    const TString& sessionId) const
{
    // TODO: Implement authoritative session ownership and coordinated
    // write-access revocation inside the partition, including already admitted
    // writes.
    const auto snapshot = Snapshot.atomic_load();
    if (const auto error = CheckDisk(*snapshot, diskId); HasError(error)) {
        return error;
    }
    if (const auto error = CheckSession(*snapshot, clientId, sessionId);
        HasError(error))
    {
        return error;
    }
    return TFrontendIoBackend{snapshot->Session->Handler, snapshot->IoGeometry};
}

// static
NProto::TError TFrontendState::CheckDisk(
    const TSnapshot& snapshot,
    const TString& diskId)
{
    if (!snapshot.AcceptingRequests) {
        return NotAcceptingRequests();
    }
    if (!snapshot.VolumeMetadata ||
        snapshot.VolumeMetadata->GetDiskId() != diskId)
    {
        return MakeError(
            E_NOT_FOUND,
            "Disk is not registered on this NBS2 host");
    }
    return {};
}

// static
NProto::TError TFrontendState::CheckSession(
    const TSnapshot& snapshot,
    const TString& clientId,
    const TString& sessionId)
{
    if (clientId.empty() || sessionId.empty() || !snapshot.Session ||
        snapshot.Session->ClientId != clientId ||
        snapshot.Session->SessionId != sessionId)
    {
        return InvalidSession();
    }
    return {};
}

}   // namespace NYdb::NBS::NBlockStore
