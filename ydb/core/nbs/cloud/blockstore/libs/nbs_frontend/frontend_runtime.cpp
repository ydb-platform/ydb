#include "frontend_runtime.h"

#include "blockstore_facade.h"

#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/service/service.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

TNbsFrontendRuntime::TNbsFrontendRuntime(TLog log)
    : BlockStore(CreateNbsFrontendBlockStore(std::move(log)))
{}

void TNbsFrontendRuntime::Start()
{
    BlockStore->Start();
}

void TNbsFrontendRuntime::Stop()
{
    BlockStore->Stop();
}

NYdb::NBS::NNbs1CompatApi::NBlockStore::IBlockStorePtr
TNbsFrontendRuntime::GetBlockStore() const
{
    return BlockStore;
}

TResultOrError<TString> TNbsFrontendRuntime::RegisterVolume(
    NActors::TActorSystem* actorSystem,
    const NActors::TActorId& actorId,
    std::shared_ptr<NStorage::NPartitionDirect::TPartitionSessionState>
        sessionState)
{
    return BlockStore->RegisterVolume(
        actorSystem,
        actorId,
        std::move(sessionState));
}

void TNbsFrontendRuntime::UnregisterVolume(
    const TString& diskId,
    const TString& registrationId)
{
    BlockStore->UnregisterVolume(diskId, registrationId);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
