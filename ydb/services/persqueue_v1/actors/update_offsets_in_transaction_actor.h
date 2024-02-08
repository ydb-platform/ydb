#pragma once

#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/grpc_services/rpc_kqp_base.h>

#include <ydb/core/kqp/common/kqp.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <memory>

namespace NKikimr::NGRpcService {

using TEvUpdateOffsetsInTransactionRequest =
    TGrpcRequestOperationCall<Ydb::Topic::UpdateOffsetsInTransactionRequest, Ydb::Topic::UpdateOffsetsInTransactionResponse>;

class TUpdateOffsetsInTransactionActor :
    public TRpcKqpRequestActor<TUpdateOffsetsInTransactionActor, TEvUpdateOffsetsInTransactionRequest> {
public:
    using TBase = TRpcKqpRequestActor<TUpdateOffsetsInTransactionActor, TEvUpdateOffsetsInTransactionRequest>;
    using TResult = Ydb::Topic::UpdateOffsetsInTransactionResult;

    explicit TUpdateOffsetsInTransactionActor(IRequestOpCtx* msg);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NKqp::TEvKqp::TEvQueryResponse, Handle);
        default:
            TBase::StateWork(ev);
            break;
        }
    }

    void Handle(const NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const NActors::TActorContext& ctx);

    void Proceed(const NActors::TActorContext& ctx);
};

}
