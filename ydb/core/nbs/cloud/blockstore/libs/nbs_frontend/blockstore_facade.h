#pragma once

#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/service/public.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// Creates the classic-compatible block store facade
NYdb::NBS::NNbs1CompatApi::NBlockStore::IBlockStorePtr
CreateNbsFrontendBlockStore();

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
