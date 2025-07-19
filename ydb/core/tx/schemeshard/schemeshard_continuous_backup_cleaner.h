#pragma once

#include "defs.h"

namespace NKikimr::NSchemeShard {

IActor* CreateContinuousBackupCleaner(TActorId txAllocatorClient,
                                      TActorId schemeShard,
                                      const TString& workingDir,
                                      const TString& tableName,
                                      const TString& streamName);

} // namespace NKikimr::NSchemeShard
