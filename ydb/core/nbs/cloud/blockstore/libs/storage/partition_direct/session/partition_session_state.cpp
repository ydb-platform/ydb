#include "partition_session_state.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/device_handler.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/overlapped_requests_guard_wrapper.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/split_requests_wrapper.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/storage_gate.h>

#include <ydb/core/protos/blockstore_config.pb.h>

#include <util/generic/guid.h>
#include <util/system/yassert.h>

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

// static
TResultOrError<TIntrusivePtr<TPartitionSessionState>>
TPartitionSessionState::Create(
    const NKikimrBlockStore::TVolumeConfig& volumeMetadata,
    IStoragePtr storage,
    TVolumeConfigPtr ioGeometry)
{
    // The owner supplies validated partition metadata and its matching backend.
    Y_ABORT_UNLESS(!volumeMetadata.GetDiskId().empty());
    Y_ABORT_UNLESS(volumeMetadata.GetPartitions(0).GetBlockCount());
    Y_ABORT_UNLESS(storage);
    Y_ABORT_UNLESS(ioGeometry);
    Y_ABORT_UNLESS(ioGeometry->DiskId == volumeMetadata.GetDiskId());
    Y_ABORT_UNLESS(ioGeometry->BlockSize == volumeMetadata.GetBlockSize());
    Y_ABORT_UNLESS(
        ioGeometry->BlockCount ==
        volumeMetadata.GetPartitions(0).GetBlockCount());
    Y_ABORT_UNLESS(ioGeometry->BlocksPerStripe);
    Y_ABORT_UNLESS(ioGeometry->VChunkSize);

    // MVP: both the device handler and the classic Mount response assume SSD.
    if (volumeMetadata.GetStorageMediaKind() != NProto::STORAGE_MEDIA_SSD) {
        return MakeError(
            E_ARGUMENT,
            "NBS2 frontend MVP supports only SSD partitions");
    }
    return TIntrusivePtr<TPartitionSessionState>(new TPartitionSessionState(
        volumeMetadata,
        std::move(storage),
        std::move(ioGeometry)));
}

TPartitionSessionState::TPartitionSessionState(
    const TPartitionSessionState& other) = default;

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
    if (Stopped) {
        return PartitionStopped();
    }
    if (clientId.empty()) {
        return MakeError(E_ARGUMENT, "MountVolume requires ClientId");
    }
    if (Handler) {
        if (ClientId != clientId) {
            return MakeError(
                E_BS_MOUNT_CONFLICT,
                "NBS2 disk is mounted by another client");
        }
        return SessionId;
    }

    ClientId = clientId;
    SessionId = CreateGuidAsString();
    Handler = CreateDefaultDeviceHandlerFactory()->CreateDeviceHandler({
        .Storage = Storage,
        .DiskId = IoGeometry->DiskId,
        .ClientId = clientId,
        .BlockSize = IoGeometry->BlockSize,
        .BlockCount = IoGeometry->BlockCount,
        .BlocksPerStripeCount = IoGeometry->BlocksPerStripe,
        .VChunkSize = IoGeometry->VChunkSize,
        .StorageMediaKind = NProto::STORAGE_MEDIA_SSD,
    });
    return SessionId;
}

NProto::TError TPartitionSessionState::Unmount(
    const TString& clientId,
    const TString& sessionId)
{
    if (Stopped) {
        return PartitionStopped();
    }
    if (clientId.empty() || sessionId.empty()) {
        return InvalidSession();
    }
    if (!Handler) {
        return MakeError(S_ALREADY, "NBS2 disk has no active session");
    }
    if (ClientId != clientId || SessionId != sessionId) {
        return InvalidSession();
    }
    ClientId.clear();
    SessionId.clear();
    Handler.reset();
    return {};
}

void TPartitionSessionState::Stop()
{
    StorageGate->Detach();
    Stopped = true;
    ClientId.clear();
    SessionId.clear();
    Handler.reset();
}

TResultOrError<TPartitionIoBackend> TPartitionSessionState::AcquireIoBackend(
    const TString& clientId,
    const TString& sessionId) const
{
    // TODO: Persist session/writer state and coordinate revocation with already
    // admitted writes before enabling writer handoff or restart recovery.
    if (Stopped) {
        return PartitionStopped();
    }
    if (clientId.empty() || sessionId.empty() || !Handler ||
        ClientId != clientId || SessionId != sessionId)
    {
        return InvalidSession();
    }
    return TPartitionIoBackend{Handler, IoGeometry};
}

TPartitionSessionState::TPartitionSessionState(
    const NKikimrBlockStore::TVolumeConfig& volumeMetadata,
    IStoragePtr storage,
    TVolumeConfigPtr ioGeometry)
    : VolumeMetadata(std::make_shared<const NKikimrBlockStore::TVolumeConfig>(
          volumeMetadata))
    , IoGeometry(std::move(ioGeometry))
    , RegistrationId(CreateGuidAsString())
    , StorageGate(std::make_shared<TStorageGate>(std::move(storage)))
    , Storage(CreateOverlappedRequestsGuardStorageWrapper(
          CreateSplitRequestsStorageWrapper(StorageGate)))
{}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
