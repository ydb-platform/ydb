#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/public/api/protos/mount.pb.h>

#include <library/cpp/threading/atomic_shared_ptr/atomic_shared_ptr.h>

#include <util/system/mutex.h>

namespace NKikimrBlockStore {
class TVolumeConfig;
}

namespace NYdb::NBS::NBlockStore {

// The handler and geometry belong to the same checked registration/session.
struct TFrontendIoBackend
{
    IDeviceHandlerPtr Handler;
    TVolumeConfigPtr IoGeometry;
};

// Owns request admission, disk backend and single process-local session.
// Unmount revokes admission but does not drain accepted I/O. The caller
// must stop submitting requests and await their completion before unmounting.
class TFrontendState final
{
public:
    // Creates closed frontend state without a registered disk or session.
    TFrontendState();
    ~TFrontendState() noexcept;

    // Opens the admission gate without changing the registered partition.
    void Start();

    // Closes admission and revokes the session, preserving the registered
    // partition.
    void Stop();

    // Check if the admission gate is open so request can be handled.
    NProto::TError CheckAcceptingRequests() const;

    // Registers partition metadata/backend and returns a unique registration
    // ID. UnregisterVolume uses this ID to avoid removing a newer registration.
    // Replacing a registration revokes its session.
    TResultOrError<TString> RegisterVolume(
        const NKikimrBlockStore::TVolumeConfig& volumeMetadata,
        IStoragePtr storage,
        TVolumeConfigPtr ioGeometry);

    // Revokes the matching registration and session; stale tokens are harmless.
    void UnregisterVolume(const TString& registrationId);

    // Forms the classic Volume after checks of admission and disk identity.
    TResultOrError<NNbs1CompatApi::NBlockStore::NProto::TVolume> GetVolume(
        const TString& diskId) const;

    // Creates a session or returns the current one for an equivalent mount.
    NNbs1CompatApi::NBlockStore::NProto::TMountVolumeResponse MountVolume(
        const NNbs1CompatApi::NBlockStore::NProto::TMountVolumeRequest&
            request);

    // Revokes a matching session.
    NProto::TError UnmountVolume(
        const TString& diskId,
        const TString& clientId,
        const TString& sessionId);

    // Checks admission and session in one snapshot and retains its I/O backend.
    TResultOrError<TFrontendIoBackend> AcquireIoBackend(
        const TString& diskId,
        const TString& clientId,
        const TString& sessionId) const;

private:
    struct TSnapshot;

    static NProto::TError CheckDisk(
        const TSnapshot& snapshot,
        const TString& diskId);
    static NProto::TError CheckSession(
        const TSnapshot& snapshot,
        const TString& clientId,
        const TString& sessionId);

    TMutex Mutex;
    TTrueAtomicSharedPtr<TSnapshot> Snapshot;
};

}   // namespace NYdb::NBS::NBlockStore
