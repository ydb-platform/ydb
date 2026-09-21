#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/service/public.h>

#include <library/cpp/logger/log.h>

namespace NActors {
class TActorSystem;
struct TActorId;
}   // namespace NActors

namespace NYdb::NBS::NBlockStore {

class TNbsFrontendBlockStore;

namespace NStorage::NPartitionDirect {
class TPartitionSessionState;
}

////////////////////////////////////////////////////////////////////////////////

// Owns and controls the classic-compatible facade of the NBS2 frontend.
class TNbsFrontendRuntime final
{
public:
    explicit TNbsFrontendRuntime(TLog log);

    // Opens the frontend admission gate.
    void Start();

    // Closes request admission without revoking partition-owned sessions.
    void Stop();

    // Returns the shared classic-compatible facade.
    [[nodiscard]] NYdb::NBS::NNbs1CompatApi::NBlockStore::IBlockStorePtr
    GetBlockStore() const;

    // Publishes partition metadata/backend and returns the token to revoke it.
    TResultOrError<TString> RegisterVolume(
        NActors::TActorSystem* actorSystem,
        const NActors::TActorId& actorId,
        std::shared_ptr<NStorage::NPartitionDirect::TPartitionSessionState>
            sessionState);

    // Revokes a matching registration without affecting a newer instance.
    void UnregisterVolume(const TString& diskId, const TString& registrationId);

private:
    const std::shared_ptr<TNbsFrontendBlockStore> BlockStore;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
