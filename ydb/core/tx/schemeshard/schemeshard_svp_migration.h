#pragma once

#include <ydb/library/actors/core/actor.h>
#include <queue>

namespace NKikimr::NSchemeShard {

struct TMigrationInfo {
    TString WorkingDir;
    TString DbName;
    bool CreateSVP = false;
    bool CreateSA = false;
    bool CreateBCT = false;
};

THolder<NActors::IActor> CreateTabletMigrator(ui64 ssTabletId, NActors::TActorId ssActorId,
    std::queue<TMigrationInfo>&& migrations);

} // namespace NKikimr::NSchemeShard
