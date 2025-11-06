#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NKikimrConfig {
    
class TLogConfig;

} // namespace NKikimrConfig

namespace NKikimr {

NActors::IActor* CreateYqlLogsUpdater(const NKikimrConfig::TLogConfig& logConfig);

NActors::TActorId MakeYqlLogsUpdaterId();

} /* namespace NKikimr */
