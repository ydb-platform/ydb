#pragma once

#include <ydb/services/workload_manager/gateway_internal.h>

#include <ydb/library/actors/core/actor.h>


namespace NKikimr::NWorkloadManager {

NActors::TActorId MakeWorkloadManagerStateActorId(ui32 nodeId);

NActors::IActor* CreateWorkloadManagerStateActor(std::shared_ptr<NPrivate::TWorkloadManagerGateway> gateway);

}
