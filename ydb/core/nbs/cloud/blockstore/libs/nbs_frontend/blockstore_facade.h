#pragma once

#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/service/public.h>

#include <library/cpp/logger/log.h>

#include <memory>

namespace NYdb::NBS::NBlockStore {

class TFrontendState;

////////////////////////////////////////////////////////////////////////////////

// Creates the classic-compatible facade sharing the supplied frontend state.
NYdb::NBS::NNbs1CompatApi::NBlockStore::IBlockStorePtr
CreateNbsFrontendBlockStore(
    std::shared_ptr<TFrontendState> frontendState,
    TLog log);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
