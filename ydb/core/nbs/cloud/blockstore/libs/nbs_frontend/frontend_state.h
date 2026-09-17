#pragma once

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/public/api/protos/mount.pb.h>

#include <library/cpp/threading/atomic_shared_ptr/atomic_shared_ptr.h>

#include <util/system/mutex.h>

namespace NKikimrBlockStore {
class TVolumeConfig;
}

namespace NYdb::NBS::NBlockStore {

// Owns request admission, disk metadata and process-local session.
// For now this is a prototype and there is no a goal to support full sessions
// logic for all possible disks.
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

    // Registers partition metadata and returns a unique registration ID.
    // UnregisterVolume uses this ID to avoid removing a newer registration.
    // Replacing a registration revokes its session.
    TResultOrError<TString> RegisterVolume(
        const NKikimrBlockStore::TVolumeConfig& volumeConfig);

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

    // Checks if session can handle IO requests.
    NProto::TError ValidateIoSession(
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
