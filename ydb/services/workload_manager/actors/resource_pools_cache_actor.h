#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/services/workload_manager/gateway_internal.h>

namespace NKikimr::NWorkloadManager {

NActors::IActor* CreateResourcePoolsCacheActor(std::shared_ptr<NPrivate::TWorkloadManagerGateway> gateway);

}
