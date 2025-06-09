#pragma once

#include "etcd_shared.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/library/actors/core/actor.h>

namespace NEtcd {
    NActors::IActor* MakeRange(std::unique_ptr<NKikimr::NGRpcService::IRequestCtx> p, TSharedStuff::TPtr stuff);
    NActors::IActor* MakePut(std::unique_ptr<NKikimr::NGRpcService::IRequestCtx> p, TSharedStuff::TPtr stuff);
    NActors::IActor* MakeDeleteRange(std::unique_ptr<NKikimr::NGRpcService::IRequestCtx> p, TSharedStuff::TPtr stuff);
    NActors::IActor* MakeTxn(std::unique_ptr<NKikimr::NGRpcService::IRequestCtx> p, TSharedStuff::TPtr stuff);
    NActors::IActor* MakeCompact(std::unique_ptr<NKikimr::NGRpcService::IRequestCtx> p, TSharedStuff::TPtr stuff);

    NActors::IActor* MakeLeaseGrant(std::unique_ptr<NKikimr::NGRpcService::IRequestCtx> p, TSharedStuff::TPtr stuff);
    NActors::IActor* MakeLeaseRevoke(std::unique_ptr<NKikimr::NGRpcService::IRequestCtx> p, TSharedStuff::TPtr stuff);
    NActors::IActor* MakeLeaseTimeToLive(std::unique_ptr<NKikimr::NGRpcService::IRequestCtx> p, TSharedStuff::TPtr stuff);
    NActors::IActor* MakeLeaseLeases(std::unique_ptr<NKikimr::NGRpcService::IRequestCtx> p, TSharedStuff::TPtr stuff);

    template<typename TValueType>
    std::string AddParam(const std::string_view& name, NYdb::TParamsBuilder& params, const TValueType& value, size_t* counter = nullptr);

    std::string MakeSimplePredicate(const std::string_view& key, const std::string_view& rangeEnd, std::ostream& sql, NYdb::TParamsBuilder& params, size_t* paramsCounter = nullptr);
}
