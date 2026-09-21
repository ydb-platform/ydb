#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/atomic_shared_ptr/atomic_shared_ptr.h>

namespace NKikimrBlockStore {
class TVolumeConfig;
}

namespace NYdb::NBS::NBlockStore {
class TStorageGate;
}

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

// A handler and geometry from the same admitted partition session.
struct TPartitionIoBackend
{
    IDeviceHandlerPtr Handler;
    TVolumeConfigPtr IoGeometry;
};

// One process-local session per partition. The owner actor serializes
// mutations; I/O threads only read immutable snapshots. Unmount does not drain
// admitted I/O.
class TPartitionSessionState final
{
public:
    // Validates partition metadata and builds its shared storage chain.
    static TResultOrError<std::shared_ptr<TPartitionSessionState>> Create(
        const NKikimrBlockStore::TVolumeConfig& volumeMetadata,
        IStoragePtr storage,
        TVolumeConfigPtr ioGeometry);

    ~TPartitionSessionState();

    // Returns immutable metadata owned by this partition incarnation.
    const NKikimrBlockStore::TVolumeConfig& GetVolumeMetadata() const;

    // Identifies this incarnation, including its control and I/O targets.
    const TString& GetRegistrationId() const;

    // Creates the single writer session, or reuses it for the same client.
    TResultOrError<TString> Mount(const TString& clientId);

    // Revokes a matching session; subsequent I/O must mount again.
    NProto::TError Unmount(const TString& clientId, const TString& sessionId);

    // Permanently closes this incarnation and detaches its storage backend.
    void Stop();

    // Checks session identity without a mutex or actor hop on the I/O path.
    TResultOrError<TPartitionIoBackend> AcquireIoBackend(
        const TString& clientId,
        const TString& sessionId) const;

private:
    struct TSnapshot;

    TPartitionSessionState(
        const NKikimrBlockStore::TVolumeConfig& volumeMetadata,
        IStoragePtr storage,
        TVolumeConfigPtr ioGeometry);

    // Disk metadata for registration and the classic MountVolume response.
    const std::unique_ptr<const NKikimrBlockStore::TVolumeConfig>
        VolumeMetadata;
    // Effective backend geometry for I/O validation and device handler
    // creation.
    const TVolumeConfigPtr IoGeometry;
    const TString RegistrationId;
    const std::shared_ptr<TStorageGate> StorageGate;
    const IStoragePtr Storage;
    // Active session identity and its handler.
    TTrueAtomicSharedPtr<TSnapshot> Snapshot;
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
