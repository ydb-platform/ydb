#pragma once
#include "common.h"
#include "config.h"

#include <library/cpp/actors/core/actor_virtual.h>
#include <library/cpp/actors/core/av_bootstrapped.h>
#include <library/cpp/actors/core/log.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NInternal::NRequest {

template <class TRequestExt, class TResponseExt, ui32 EvStartExt, ui32 EvResultInternalExt, ui32 EvResultExt>
class TDialogPolicyImpl {
public:
    using TRequest = TRequestExt;
    using TResponse = TResponseExt;
    static constexpr ui32 EvStart = EvStartExt;
    static constexpr ui32 EvResultInternal = EvResultInternalExt;
    static constexpr ui32 EvResult = EvResultExt;
};

using TDialogCreateTable = TDialogPolicyImpl<Ydb::Table::CreateTableRequest, Ydb::Table::CreateTableResponse,
    EEvents::EvCreateTableRequest, EEvents::EvCreateTableInternalResponse, EEvents::EvCreateTableResponse>;
using TDialogSelect = TDialogPolicyImpl<Ydb::Table::ExecuteDataQueryRequest, Ydb::Table::ExecuteDataQueryResponse,
    EEvents::EvSelectRequest, EEvents::EvSelectInternalResponse, EEvents::EvSelectResponse>;
using TDialogCreateSession = TDialogPolicyImpl<Ydb::Table::CreateSessionRequest, Ydb::Table::CreateSessionResponse,
    EEvents::EvCreateSessionRequest, EEvents::EvCreateSessionInternalResponse, EEvents::EvCreateSessionResponse>;

template <class TDialogPolicy>
class TEvRequestResult: public NActors::TEventLocal<TEvRequestResult<TDialogPolicy>, TDialogPolicy::EvResult> {
private:
    YDB_READONLY_DEF(typename TDialogPolicy::TResponse, Result);
public:
    TEvRequestResult(typename TDialogPolicy::TResponse&& result)
        : Result(std::move(result)) {

    }
};

class TEvRequestFinished: public NActors::TEventLocal<TEvRequestFinished, EEvents::EvRequestFinished> {
public:
    TEvRequestFinished() = default;
};

template <class TResponse>
class TOperatorChecker {
public:
    static bool IsSuccess(const TResponse& r) {
        return r.operation().status() == Ydb::StatusIds::SUCCESS;
    }
};

template <>
class TOperatorChecker<Ydb::Table::CreateTableResponse> {
public:
    static bool IsSuccess(const Ydb::Table::CreateTableResponse& r) {
        return r.operation().status() == Ydb::StatusIds::SUCCESS ||
            r.operation().status() != Ydb::StatusIds::ALREADY_EXISTS;
    }
};

template <class TDialogPolicy>
class TYDBRequest: public NActors::TActorBootstrapped<TYDBRequest<TDialogPolicy>> {
private:
    using TBase = NActors::TActorBootstrapped<TYDBRequest<TDialogPolicy>>;
    using TRequest = typename TDialogPolicy::TRequest;
    using TResponse = typename TDialogPolicy::TResponse;
    using TSelf = TYDBRequest<TDialogPolicy>;
    TRequest ProtoRequest;
    const NActors::TActorId ActorFinishId;
    const TConfig& Config;
    ui32 Retry = 0;
protected:
    class TEvRequestInternalResult: public NActors::TEventLocal<TEvRequestInternalResult, TDialogPolicy::EvResultInternal> {
    private:
        YDB_READONLY_DEF(NThreading::TFuture<typename TDialogPolicy::TResponse>, Future);
    public:
        TEvRequestInternalResult(const NThreading::TFuture<typename TDialogPolicy::TResponse>& f)
            : Future(f) {

        }
    };
    class TEvRequestStart: public NActors::TEventLocal<TEvRequestStart, TDialogPolicy::EvStart> {
    public:
    };

public:
    void Bootstrap(const TActorContext& /*ctx*/) {
        TBase::Become(&TBase::TThis::StateMain);
        TBase::template Sender<TEvRequestStart>().SendTo(TBase::SelfId());
    }

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvRequestInternalResult, Handle);
            hFunc(TEvRequestStart, Handle);
            default:
                break;
        }
    }
    void Handle(typename TEvRequestInternalResult::TPtr& ev) {
        if (!ev->Get()->GetFuture().HasValue() || ev->Get()->GetFuture().HasException()) {
            ALS_ERROR(NKikimrServices::METADATA_PROVIDER) << "cannot receive result on initialization";
            TBase::Schedule(Config.GetRetryPeriod(++Retry), new TEvRequestStart);
            return;
        }
        auto f = ev->Get()->GetFuture();
        TResponse response = f.ExtractValue();
        if (!TOperatorChecker<TResponse>::IsSuccess(response)) {
            ALS_ERROR(NKikimrServices::METADATA_PROVIDER) << "incorrect reply: " << response.DebugString();
            TBase::Schedule(Config.GetRetryPeriod(++Retry), new TEvRequestStart);
            return;
        }
        TBase::template Sender<TEvRequestResult<TDialogPolicy>>(std::move(response)).SendTo(ActorFinishId);
        TBase::template Sender<TEvRequestFinished>().SendTo(ActorFinishId);
        TBase::Die(TActivationContext::AsActorContext());
    }

    void Handle(typename TEvRequestStart::TPtr& /*ev*/) {
        auto aSystem = TActivationContext::ActorSystem();
        using TRpcRequest = NGRpcService::TGrpcRequestOperationCall<TRequest, TResponse>;
        auto request = ProtoRequest;
        auto result = NRpcService::DoLocalRpc<TRpcRequest>(std::move(request), AppData()->TenantName, "", aSystem);
        const NActors::TActorId selfId = TBase::SelfId();
        const auto replyCallback = [aSystem, selfId](const NThreading::TFuture<TResponse>& f) {
            aSystem->Send(selfId, new TEvRequestInternalResult(f));
        };
        result.Subscribe(replyCallback);
    }

    TYDBRequest(const TRequest& request, const NActors::TActorId actorFinishId, const TConfig& config)
        : ProtoRequest(request)
        , ActorFinishId(actorFinishId)
        , Config(config)
    {

    }
};

}
