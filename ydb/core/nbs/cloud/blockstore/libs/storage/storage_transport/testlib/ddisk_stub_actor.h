#pragma once

#include <ydb/core/blobstorage/ddisk/ddisk.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/vector.h>
#include <util/system/mutex.h>

namespace NYdb::NBS::NBlockStore::NStorage::NTransport::NTestLib {

////////////////////////////////////////////////////////////////////////////////

// Thread-safe state shared between the test thread (which configures pending
// behaviour) and the stub actor running inside the test actor runtime.
struct TDDiskStubState: public TThrRefBase
{
    TMutex Lock;

    // When set, the stub does not answer the corresponding request, leaving it
    // in flight inside TICStorageTransportActor (so it can later be rejected by
    // a disconnect).
    bool PendingConnect = false;
    bool PendingRead = false;
    bool PendingWrite = false;

    // DDiskInstanceGuid reported on a successful connect.
    ui64 DDiskInstanceGuid = 1;

    // Credentials observed in every TEvConnect received by the stub.
    TVector<NKikimr::NDDisk::TQueryCredentials> ConnectCredentials;
};

using TDDiskStubStatePtr = TIntrusivePtr<TDDiskStubState>;

////////////////////////////////////////////////////////////////////////////////

// Minimal DDisk/PersistentBuffer stub. Replies OK to TEvConnect, TEvRead,
// TEvWrite and TEvListPersistentBuffer by default; honours the pending flags in
// the shared state to keep a request in flight.
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
    void HandleListPersistentBuffer(
        const NKikimr::NDDisk::TEvListPersistentBuffer::TPtr& ev,
        const NActors::TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NTransport::NTestLib
