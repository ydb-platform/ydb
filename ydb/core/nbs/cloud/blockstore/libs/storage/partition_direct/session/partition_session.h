#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/hot_swap/hot_swap.h>

namespace NKikimrBlockStore {
class TVolumeConfig;
}

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

class TPartitionSessionState;

// A handler and geometry from the same admitted partition session.
struct TPartitionIoBackend
{
    IDeviceHandlerPtr Handler;
    TVolumeConfigPtr IoGeometry;
};

// Owns a partition incarnation's session and publishes consistent versions to
// concurrent I/O readers. Only the owner actor may call Mount/Unmount/Stop;
// teardown may also call Stop after that actor has ceased processing events.
class TPartitionSession final
{
public:
    TPartitionSession(const TPartitionSession& other) = delete;
    TPartitionSession& operator=(const TPartitionSession& other) = delete;
    ~TPartitionSession();

    // Creates an unmounted session for validated partition metadata and a
    // matching backend. Only SSD partitions are supported in MVP.
    static TResultOrError<TPartitionSessionPtr> Create(
        const NKikimrBlockStore::TVolumeConfig& volumeMetadata,
        IStoragePtr storage,
        TVolumeConfigPtr ioGeometry);

    // Metadata is immutable and remains valid for this object's lifetime.
    const NKikimrBlockStore::TVolumeConfig& GetVolumeMetadata() const;

    // Identifies the partition incarnation, unchanged by session operations.
    TString GetRegistrationId() const;

    // Creates or reuses a writer session; failures leave it unchanged.
    TResultOrError<TString> Mount(const TString& clientId);

    // Ends the matching session without draining already admitted I/O.
    NProto::TError Unmount(const TString& clientId, const TString& sessionId);

    // Disables session and backend access without waiting for in-flight I/O.
    void Stop();

    // Validates session identity and acquires its backend without an actor hop.
    TResultOrError<TPartitionIoBackend> AcquireIoBackend(
        const TString& clientId,
        const TString& sessionId) const;

private:
    explicit TPartitionSession(TIntrusivePtr<TPartitionSessionState> state);

    THotSwap<TPartitionSessionState> State;
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
