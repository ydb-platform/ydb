#include "frontend_runtime.h"

#include "blockstore_facade.h"

#include <ydb/core/nbs/cloud/blockstore/compat/libs/service/service.h>

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

NCloud::NBlockStore::IBlockStorePtr TNbsFrontendRuntime::GetBlockStore() const
{
    return BlockStore;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
