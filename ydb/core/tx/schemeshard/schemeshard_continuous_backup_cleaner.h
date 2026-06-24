#pragma once

#include "defs.h"

#include <ydb/core/scheme/scheme_pathid.h>

#include <util/generic/maybe.h>

namespace NKikimr::NSchemeShard {

IActor* CreateContinuousBackupCleaner(TActorId txAllocatorClient,
                                      TActorId schemeShard,
                                      ui64 backupId,
                                      TPathId item,
                                      const TString& workingDir,
                                      const TString& tableName,
                                      const TString& streamName);

} // namespace NKikimr::NSchemeShard
