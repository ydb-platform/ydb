#pragma once

#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/grpc_services/rpc_kqp_base.h>

#include <ydb/core/kqp/kqp.h>

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>

#include <memory>

namespace NKikimr::NGRpcService {

using TEvAddOffsetsToTransactionRequest =
    TGrpcRequestOperationCall<Ydb::Topic::AddOffsetsToTransactionRequest, Ydb::Topic::AddOffsetsToTransactionResponse>;

class TAddOffsetsToTransactionActor : public TRpcKqpRequestActor<TAddOffsetsToTransactionActor, TEvAddOffsetsToTransactionRequest> {
public:
    using TBase = TRpcKqpRequestActor<TAddOffsetsToTransactionActor, TEvAddOffsetsToTransactionRequest>;
    using TResult = Ydb::Topic::AddOffsetsToTransactionResult;

    explicit TAddOffsetsToTransactionActor(IRequestOpCtx* msg);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NKqp::TEvKqp::TEvQueryResponse, Handle);
        default:
            TBase::StateWork(ev, ctx);
            break;
        }
    }

    void Handle(const NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const NActors::TActorContext& ctx);

    void Proceed(const NActors::TActorContext& ctx);
};

}
