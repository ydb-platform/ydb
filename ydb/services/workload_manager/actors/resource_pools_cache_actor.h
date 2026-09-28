#pragma once

#include <ydb/services/workload_manager/gateway_internal.h>

#include <ydb/library/actors/core/actor.h>


namespace NKikimr::NWorkloadManager {

NActors::IActor* CreateResourcePoolsCacheActor(std::shared_ptr<NPrivate::TWorkloadManagerGateway> gateway);

}
