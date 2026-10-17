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

// Joins NBS executor threads. Call after ActorSystem::Cleanup so disconnect
// tasks posted during actor shutdown still run, and before TActorSystem is
// destroyed so those threads cannot log through a freed actor system.
void StopNbsExecutors();

// Returns NBS2 frontend facade.
NYdb::NBS::NNbs1CompatApi::NBlockStore::IBlockStorePtr
GetNbsFrontendBlockStore();

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
