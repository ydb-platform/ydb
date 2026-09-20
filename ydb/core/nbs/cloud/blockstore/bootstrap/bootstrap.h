#pragma once

#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/service/public.h>

namespace NKikimrConfig {
class TNbsConfig;
}

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

void CreateNbsService(const NKikimrConfig::TNbsConfig& config);
void StartNbsService();
void StopNbsService();

// Returns NBS2 frontend facade.
NYdb::NBS::NNbs1CompatApi::NBlockStore::IBlockStorePtr
GetNbsFrontendBlockStore();

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
