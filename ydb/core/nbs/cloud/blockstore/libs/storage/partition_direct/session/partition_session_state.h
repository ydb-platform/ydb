#pragma once

#include "partition_session.h"

namespace NKikimrBlockStore {
class TVolumeConfig;
}

namespace NYdb::NBS::NBlockStore {
class TStorageGate;
}

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

// One published version of a partition's process-local session. Readers never
// mutate it; TPartitionSession owns mutations and publication.
// Unmount does not drain admitted I/O.
class TPartitionSessionState final
    : public TAtomicRefCount<TPartitionSessionState>
{
public:
    // Builds the storage chain for validated partition metadata and a matching
    // backend. Only SSD partitions are supported in MVP.
    static TResultOrError<TIntrusivePtr<TPartitionSessionState>> Create(
        const NKikimrBlockStore::TVolumeConfig& volumeMetadata,
        IStoragePtr storage,
        TVolumeConfigPtr ioGeometry);

    // Copies the session while sharing immutable metadata and the same backend.
    TPartitionSessionState(const TPartitionSessionState& other);
    ~TPartitionSessionState();

    // Returns immutable metadata owned by this partition incarnation.
    const NKikimrBlockStore::TVolumeConfig& GetVolumeMetadata() const;

    // Prevents a stale unregister from removing a replacement registration.
    const TString& GetRegistrationId() const;

    // Creates or reuses the writer session.
    TResultOrError<TString> Mount(const TString& clientId);

    // Ends the matching session without cancelling already admitted I/O.
    NProto::TError Unmount(const TString& clientId, const TString& sessionId);

    // Disables session and backend access for this partition incarnation.
    // Does not wait for in-flight I/O to complete.
    void Stop();

    // Checks session identity without a mutex or actor hop on the I/O path.
    TResultOrError<TPartitionIoBackend> AcquireIoBackend(
        const TString& clientId,
        const TString& sessionId) const;

private:
    TPartitionSessionState(
        const NKikimrBlockStore::TVolumeConfig& volumeMetadata,
        IStoragePtr storage,
        TVolumeConfigPtr ioGeometry);

    // Disk metadata for registration and the classic MountVolume response.
    const std::shared_ptr<const NKikimrBlockStore::TVolumeConfig>
        VolumeMetadata;
    // Effective backend geometry for I/O validation and device handler
    // creation.
    const TVolumeConfigPtr IoGeometry;
    const TString RegistrationId;
    const std::shared_ptr<TStorageGate> StorageGate;
    const IStoragePtr Storage;
    // These fields may only be changed before this version is published.
    bool Stopped = false;
    TString ClientId;
    TString SessionId;
    IDeviceHandlerPtr Handler;
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
