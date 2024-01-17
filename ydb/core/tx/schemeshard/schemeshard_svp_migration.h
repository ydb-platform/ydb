#pragma once

#include <ydb/library/actors/core/actor.h>
#include <queue>

namespace NKikimr::NSchemeShard {

struct TSVPMigrationInfo {
    TString WorkingDir;
    TString DbName;
};

THolder<NActors::IActor> CreateSVPMigrator(ui64 ssTabletId, NActors::TActorId ssActorId,
    std::queue<TSVPMigrationInfo>&& migrations);

} // namespace NKikimr::NSchemeShard
