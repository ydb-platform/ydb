#pragma once

namespace NKikimrConfig {
class TNbsConfig;
}

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

void CreateNbsService(const NKikimrConfig::TNbsConfig& config);
void StartNbsService();
void StopNbsService();

<<<<<<< HEAD
=======
// Joins NBS executor threads. Call after ActorSystem::Cleanup so disconnect
// tasks posted during actor shutdown still run, and before TActorSystem is
// destroyed so those threads cannot log through a freed actor system.
void StopNbsExecutors();

// Returns NBS2 frontend facade.
NYdb::NBS::NNbs1CompatApi::NBlockStore::IBlockStorePtr
GetNbsFrontendBlockStore();

>>>>>>> 078873ca180 ([YDBBUGS-790] Fix use-after-free in nbs2 (#53752))
////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
