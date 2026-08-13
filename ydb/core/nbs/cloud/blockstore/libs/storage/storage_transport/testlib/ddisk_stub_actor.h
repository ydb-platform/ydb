#pragma once

#include <ydb/core/blobstorage/ddisk/ddisk.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/util/rope.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/system/mutex.h>

namespace NYdb::NBS::NBlockStore::NStorage::NTransport::NTestLib {

////////////////////////////////////////////////////////////////////////////////

// Thread-safe state shared between the test thread and the stub actor
// running inside the test actor runtime.
struct TDDiskStubState: public TThrRefBase
{
    TMutex Lock;

    // When set, the stub does not answer the corresponding request and holds
    // it until the test drains Held* via the test adapter.
    bool PendingConnect = false;
    bool PendingRead = false;
    bool PendingWrite = false;
    bool PendingWritePBuffer = false;
    bool PendingErase = false;
    bool PendingSync = false;

    // When set, TEvWritePersistentBuffers replies are split into two events
    // (first half of PersistentBufferIds, then the rest).
    bool SplitWriteToManyReplies = false;

    // DDiskInstanceGuid reported on a successful connect.
    ui64 DDiskInstanceGuid = 1;

    // Credentials observed in every TEvConnect received by the stub.
    TVector<NKikimr::NDDisk::TQueryCredentials> ConnectCredentials;

    // Payload echo storage. Keyed by (vChunkIndex, offset, lsn) for PBuffer
    // and by (vChunkIndex, offset, 0) for DDisk.
    struct TPayloadKey
    {
        ui64 VChunkIndex = 0;
        ui64 OffsetInBytes = 0;
        ui64 Lsn = 0;

        bool operator==(const TPayloadKey& other) const = default;
    };

    struct TPayloadKeyHash
    {
        size_t operator()(const TPayloadKey& key) const
        {
            return CombineHashes(
                CombineHashes(
                    IntHash(key.VChunkIndex),
                    IntHash(key.OffsetInBytes)),
                IntHash(key.Lsn));
        }
    };

    THashMap<TPayloadKey, TRope, TPayloadKeyHash> Payloads;

    enum class EHeldKind
    {
        Read,
        ReadPBuffer,
        Write,
        WritePBuffer,
        WritePBuffers,
        Erase,
        Sync,
    };

    struct THeldRequest
    {
        EHeldKind Kind = EHeldKind::Read;
        NActors::TActorId Sender;
        ui64 Cookie = 0;
        TRope Payload;
        TPayloadKey Key;
        TVector<NKikimrBlobStorage::NDDisk::TDDiskId> PersistentBufferIds;
    };

    TVector<THeldRequest> HeldRequests;
};

using TDDiskStubStatePtr = TIntrusivePtr<TDDiskStubState>;

// Drain held requests of the given kind from the test thread, storing any
// write payloads and sending OK replies through the actor system.
void ReleaseHeldRequests(
    TDDiskStubStatePtr state,
    NActors::TActorSystem* actorSystem,
    TDDiskStubState::EHeldKind kind);

// For a held TEvWritePersistentBuffers request: send an OK result covering
// only the first half of PersistentBufferIds and leave the remainder held
// (PendingWritePBuffer stays true). Used to exercise mid-fanout session death.
void ReleaseHeldWritePBuffersFirstHalf(
    TDDiskStubStatePtr state,
    NActors::TActorSystem* actorSystem);

////////////////////////////////////////////////////////////////////////////////

// DDisk/PersistentBuffer stub. Replies OK by default; honours pending flags to
// keep requests in flight. Echoes written payloads on subsequent reads.
class TDDiskStubActor: public NActors::TActorBootstrapped<TDDiskStubActor>
{
public:
    explicit TDDiskStubActor(TDDiskStubStatePtr state);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    TDDiskStubStatePtr State;

    STFUNC(StateWork);

    void HandleConnect(
        const NKikimr::NDDisk::TEvConnect::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleRead(
        const NKikimr::NDDisk::TEvRead::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleWrite(
        const NKikimr::NDDisk::TEvWrite::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleWritePersistentBuffer(
        const NKikimr::NDDisk::TEvWritePersistentBuffer::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleWritePersistentBuffers(
        const NKikimr::NDDisk::TEvWritePersistentBuffers::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleReadPersistentBuffer(
        const NKikimr::NDDisk::TEvReadPersistentBuffer::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleBatchErasePersistentBuffer(
        const NKikimr::NDDisk::TEvBatchErasePersistentBuffer::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleErasePersistentBuffer(
        const NKikimr::NDDisk::TEvErasePersistentBuffer::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleSync(
        const NKikimr::NDDisk::TEvSync::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleListPersistentBuffer(
        const NKikimr::NDDisk::TEvListPersistentBuffer::TPtr& ev,
        const NActors::TActorContext& ctx);

    void Hold(TDDiskStubState::THeldRequest held);
    void StorePayload(const TDDiskStubState::TPayloadKey& key, TRope payload);
    [[nodiscard]] TRope LoadPayload(
        const TDDiskStubState::TPayloadKey& key) const;

    void ReplyRead(
        const NActors::TActorContext& ctx,
        NActors::TActorId sender,
        ui64 cookie,
        const TDDiskStubState::TPayloadKey& key,
        bool pbuffer);
    void ReplyWrite(
        const NActors::TActorContext& ctx,
        NActors::TActorId sender,
        ui64 cookie);
    void ReplyWritePBuffer(
        const NActors::TActorContext& ctx,
        NActors::TActorId sender,
        ui64 cookie);
    void ReplyWritePBuffers(
        const NActors::TActorContext& ctx,
        NActors::TActorId sender,
        ui64 cookie,
        const TVector<NKikimrBlobStorage::NDDisk::TDDiskId>& ids);
    void ReplyErase(
        const NActors::TActorContext& ctx,
        NActors::TActorId sender,
        ui64 cookie);
    void ReplySync(
        const NActors::TActorContext& ctx,
        NActors::TActorId sender,
        ui64 cookie);
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport::NTestLib
