#pragma once

#include <ydb/library/actors/core/actor.h>


namespace NKikimr::NWorkloadManager {

NActors::IActor* CreateResourcePoolsCacheActor(NActors::TActorId workloadManagerServiceId);

}
