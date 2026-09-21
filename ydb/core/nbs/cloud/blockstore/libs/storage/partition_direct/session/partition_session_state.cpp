#include "partition_session_state.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/device_handler.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/overlapped_requests_guard_wrapper.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/split_requests_wrapper.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/storage_gate.h>

#include <ydb/core/protos/blockstore_config.pb.h>

#include <util/generic/guid.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

NProto::TError InvalidSession()
{
    return MakeError(E_BS_INVALID_SESSION, "Invalid NBS2 partition session");
}

NProto::TError PartitionStopped()
{
    return MakeError(E_REJECTED, "NBS2 partition is stopped");
}

}   // namespace

// The actor publishes identity and its handler together, never in separate
// steps.
struct TPartitionSessionState::TSnapshot
{
    bool Stopped = false;
    TString ClientId;
    TString SessionId;
    IDeviceHandlerPtr Handler;
};

// static
TResultOrError<std::shared_ptr<TPartitionSessionState>>
TPartitionSessionState::Create(
    const NKikimrBlockStore::TVolumeConfig& volumeMetadata,
    IStoragePtr storage,
    TVolumeConfigPtr ioGeometry)
{
    if (volumeMetadata.GetDiskId().empty() ||
        volumeMetadata.PartitionsSize() != 1 ||
        !IsSupportedBlockSize(volumeMetadata.GetBlockSize()) ||
        !volumeMetadata.GetPartitions(0).GetBlockCount() ||
        volumeMetadata.GetStorageMediaKind() != NProto::STORAGE_MEDIA_SSD)
    {
        return MakeError(E_ARGUMENT, "Invalid NBS2 partition metadata");
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
    return std::shared_ptr<TPartitionSessionState>(new TPartitionSessionState(
        volumeMetadata,
        std::move(storage),
        std::move(ioGeometry)));
}

TPartitionSessionState::~TPartitionSessionState() = default;

const NKikimrBlockStore::TVolumeConfig&
TPartitionSessionState::GetVolumeMetadata() const
{
    return *VolumeMetadata;
}

const TString& TPartitionSessionState::GetRegistrationId() const
{
    return RegistrationId;
}

TResultOrError<TString> TPartitionSessionState::Mount(const TString& clientId)
{
    const auto snapshot = Snapshot.atomic_load();
    if (snapshot->Stopped) {
        return PartitionStopped();
    }
    if (clientId.empty()) {
        return MakeError(E_ARGUMENT, "MountVolume requires ClientId");
    }
    if (snapshot->Handler) {
        if (snapshot->ClientId != clientId) {
            return MakeError(
                E_BS_MOUNT_CONFLICT,
                "NBS2 disk is mounted by another client");
        }
        return snapshot->SessionId;
    }

    auto next = std::make_unique<TSnapshot>();
    next->ClientId = clientId;
    next->SessionId = CreateGuidAsString();
    next->Handler = CreateDefaultDeviceHandlerFactory()->CreateDeviceHandler({
        .Storage = Storage,
        .DiskId = IoGeometry->DiskId,
        .ClientId = clientId,
        .BlockSize = IoGeometry->BlockSize,
        .BlockCount = IoGeometry->BlockCount,
        .BlocksPerStripeCount = IoGeometry->BlocksPerStripe,
        .VChunkSize = IoGeometry->VChunkSize,
        .StorageMediaKind = NProto::STORAGE_MEDIA_SSD,
    });
    const TString sessionId = next->SessionId;
    Snapshot.atomic_store(TTrueAtomicSharedPtr<TSnapshot>(next.release()));
    return sessionId;
}

NProto::TError TPartitionSessionState::Unmount(
    const TString& clientId,
    const TString& sessionId)
{
    const auto snapshot = Snapshot.atomic_load();
    if (snapshot->Stopped) {
        return PartitionStopped();
    }
    if (clientId.empty() || sessionId.empty()) {
        return InvalidSession();
    }
    if (!snapshot->Handler) {
        return MakeError(S_ALREADY, "NBS2 disk has no active session");
    }
    if (snapshot->ClientId != clientId || snapshot->SessionId != sessionId) {
        return InvalidSession();
    }
    Snapshot.atomic_store(TTrueAtomicSharedPtr<TSnapshot>(new TSnapshot()));
    return {};
}

void TPartitionSessionState::Stop()
{
    StorageGate->Detach();
    auto next = std::make_unique<TSnapshot>();
    next->Stopped = true;
    Snapshot.atomic_store(TTrueAtomicSharedPtr<TSnapshot>(next.release()));
}

TResultOrError<TPartitionIoBackend> TPartitionSessionState::AcquireIoBackend(
    const TString& clientId,
    const TString& sessionId) const
{
    // TODO: Persist session/writer state and coordinate revocation with already
    // admitted writes before enabling writer handoff or restart recovery.
    const auto snapshot = Snapshot.atomic_load();
    if (snapshot->Stopped) {
        return PartitionStopped();
    }
    if (clientId.empty() || sessionId.empty() || !snapshot->Handler ||
        snapshot->ClientId != clientId || snapshot->SessionId != sessionId)
    {
        return InvalidSession();
    }
    return TPartitionIoBackend{snapshot->Handler, IoGeometry};
}

TPartitionSessionState::TPartitionSessionState(
    const NKikimrBlockStore::TVolumeConfig& volumeMetadata,
    IStoragePtr storage,
    TVolumeConfigPtr ioGeometry)
    : VolumeMetadata(
          std::make_unique<NKikimrBlockStore::TVolumeConfig>(volumeMetadata))
    , RegistrationId(CreateGuidAsString())
    , IoGeometry(std::move(ioGeometry))
    , StorageGate(std::make_shared<TStorageGate>(std::move(storage)))
    , Storage(CreateOverlappedRequestsGuardStorageWrapper(
          CreateSplitRequestsStorageWrapper(StorageGate)))
    , Snapshot(new TSnapshot())
{}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
