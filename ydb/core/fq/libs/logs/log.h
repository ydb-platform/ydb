#pragma once

#include <ydb/core/protos/config.pb.h>

#include <ydb/library/actors/core/actor.h>

namespace NKikimr {

NActors::IActor* CreateYqlLogsUpdater(const NKikimrConfig::TLogConfig& logConfig);

NActors::TActorId MakeYqlLogsUpdaterId();

} /* namespace NKikimr */
