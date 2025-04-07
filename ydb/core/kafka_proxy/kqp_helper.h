#pragma once

#include <ydb/core/client/server/msgbus_server_pq_metacache.h>
#include <ydb/core/grpc_services/rpc_deferrable.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/services/metadata/abstract/initialization.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/services/metadata/abstract/kqp_common.h>

namespace NKafka {

using namespace NKikimr;
using namespace NKikimr::NKqp;
using namespace NActors;

class TKqpTxHelper {
public:
    TKqpTxHelper(TString database);
    void SendCreateSessionRequest(const TActorContext& ctx);
    void BeginTransaction(ui64 cookie, const TActorContext& ctx);
    bool HandleCreateSessionResponse(TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx);
    void CloseKqpSession(const TActorContext& ctx);
    void SendRequest(THolder<TEvKqp::TEvQueryRequest> request, ui64 cookie, const TActorContext& ctx);
    void CommitTx(ui64 cookie, const TActorContext& ctx);
    void SendYqlRequest(const TString& yqlRequest, NYdb::TParams sqlParams, ui64 cookie, const TActorContext& ctx, bool commit = false);
    void SendInitTableRequest(const TActorContext& ctx, std::shared_ptr<NKikimr::NMetadata::IClassBehaviour> prepareManager);
    void SetTxId(const TString& txId);
    void ResetTxId();

public:
    TString DataBase;
    TString TxId;

private:
    THolder<TEvKqp::TEvCreateSessionRequest> MakeCreateSessionRequest();
    THolder<TEvKqp::TEvCloseSessionRequest> MakeCloseSessionRequest();


private:
    TString Consumer;
    TString Path;

    TString KqpSessionId;
};

}  // namespace NKikimr::NGRpcProxy::V1
