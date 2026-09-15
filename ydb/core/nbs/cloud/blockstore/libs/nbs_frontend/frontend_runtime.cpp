#include "frontend_runtime.h"

#include "blockstore_facade.h"
#include "frontend_state.h"

#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/service/service.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

TNbsFrontendRuntime::TNbsFrontendRuntime(TLog log)
    : FrontendState(std::make_shared<TFrontendState>())
    , BlockStore(CreateNbsFrontendBlockStore(FrontendState, std::move(log)))
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
    const NKikimrBlockStore::TVolumeConfig& volumeConfig)
{
    return FrontendState->RegisterVolume(volumeConfig);
}

void TNbsFrontendRuntime::UnregisterVolume(const TString& registrationId)
{
    FrontendState->UnregisterVolume(registrationId);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
