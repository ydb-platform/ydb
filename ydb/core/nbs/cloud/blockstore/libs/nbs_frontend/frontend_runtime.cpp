#include "frontend_runtime.h"

#include "blockstore_facade.h"

#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/service/service.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

TNbsFrontendRuntime::TNbsFrontendRuntime()
    : BlockStore(CreateNbsFrontendBlockStore())
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

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
