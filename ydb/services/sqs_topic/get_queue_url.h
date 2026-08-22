#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NGRpcService {
    class IRequestOpCtx;
} // namespace NKikimr::NGRpcService

namespace NKikimr::NSqsTopic::V1 {
    std::unique_ptr<NActors::IActor> CreateGetQueueUrlActor(NKikimr::NGRpcService::IRequestOpCtx* msg);
} // namespace NKikimr::NSqsTopic::V1
