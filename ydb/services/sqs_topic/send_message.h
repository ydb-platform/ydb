#pragma once

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/library/actors/core/actor.h>
#include <memory>

namespace NKikimr::NSqsTopic::V1 {
    std::unique_ptr<NActors::IActor> CreateSendMessageActor(NKikimr::NGRpcService::IRequestOpCtx* msg);
    std::unique_ptr<NActors::IActor> CreateSendMessageBatchActor(NKikimr::NGRpcService::IRequestOpCtx* msg);
} // namespace NKikimr::NSqsTopic::V1
