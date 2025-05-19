#pragma once

#include <ydb/library/actors/core/harmonizer/harmonizer.h>
#include <ydb/library/actors/core/harmonizer/pool.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/core/hfunc.h>

#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr::NActorSystemMon {

void RegisterPages(NMonitoring::TIndexMonPage* index, NActors::TActorSystem* actorSystem);

NActors::TActorId MakeActorSystemMonId();

NActors::IActor* CreateActorSystemMon();

} // NKikimr::NActorSystemMon
