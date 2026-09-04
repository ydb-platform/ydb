#pragma once

#include <ydb/core/nbs/cloud/blockstore/compat/libs/service/public.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// Creates the classic-compatible block store facade
NCloud::NBlockStore::IBlockStorePtr CreateNbsFrontendBlockStore();

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
