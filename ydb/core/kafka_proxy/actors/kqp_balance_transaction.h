#pragma once

#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/grpc_services/rpc_deferrable.h>
#include <ydb/core/client/server/msgbus_server_pq_metacache.h>
#include <ydb/core/persqueue/events/global.h>

namespace NKikimr::NGRpcProxy::V1 {

using namespace NKikimr::NGRpcService; // savnik change namespace

class TKqpTxHelper {
public:
    TKqpTxHelper(TString database);
    void SendCreateSessionRequest(const TActorContext& ctx);
    void BeginTransaction(ui64 cookie, const NActors::TActorContext& ctx);
    bool HandleCreateSessionResponse(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx);
    void CloseKqpSession(const TActorContext& ctx);
    void SendRequest(THolder<NKqp::TEvKqp::TEvQueryRequest> request, ui64 cookie, const NActors::TActorContext& ctx);
    void CommitTx(ui64 cookie, const NActors::TActorContext& ctx);
    void SendYqlRequest(TString yqlRequest, NYdb::TParams sqlParams, ui64 cookie, const NActors::TActorContext& ctx);

private:
    THolder<NKqp::TEvKqp::TEvCreateSessionRequest> MakeCreateSessionRequest();
    THolder<NKqp::TEvKqp::TEvCloseSessionRequest> MakeCloseSessionRequest();


private:
    TString DataBase;
    TString Consumer;
    TString Path;

    TString TxId;
    TString KqpSessionId;
};

}  // namespace NKikimr::NGRpcProxy::V1
