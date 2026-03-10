#pragma once

namespace NKikimrConfig {
class TNbsConfig;
}

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

void CreateNbsService(const NKikimrConfig::TNbsConfig& config);
void StartNbsService();
void StopNbsService();

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
