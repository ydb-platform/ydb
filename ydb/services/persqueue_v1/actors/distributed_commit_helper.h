#pragma once

#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/grpc_services/rpc_deferrable.h>
#include <ydb/core/client/server/msgbus_server_pq_metacache.h>
#include <ydb/core/persqueue/events/global.h>

namespace NKikimr::NGRpcProxy::V1 {

using namespace NKikimr::NGRpcService;

class TDistributedCommitHelper {
public:
    enum ECurrentStep {
        BEGIN_TRANSACTION_SENDED,
        OFFSETS_SENDED,
        COMMIT_SENDED,
        DONE
    };

    struct TCommitInfo {
        ui64 PartitionId;
        i64 Offset;
        bool KillReadSession;
        bool OnlyCheckCommitedToFinish;
        TString ReadSessionId;
    };

    TDistributedCommitHelper(TString database, TString consumer, TString path, std::vector<TCommitInfo> commits, ui64 cookie = 0);

    ECurrentStep Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx);
    void SendCreateSessionRequest(const TActorContext& ctx);
    void BeginTransaction(const NActors::TActorContext& ctx);
    bool Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& ctx);

private:
    void CloseKqpSession(const TActorContext& ctx);
    THolder<NKqp::TEvKqp::TEvCreateSessionRequest> MakeCreateSessionRequest();
    THolder<NKqp::TEvKqp::TEvCloseSessionRequest> MakeCloseSessionRequest();
    void SendCommits(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const NActors::TActorContext& ctx);
    void CommitTx(const NActors::TActorContext& ctx);

private:
    TString DataBase;
    TString Consumer;
    TString Path;
    std::vector<TCommitInfo> Commits;
    ECurrentStep Step;
    ui64 Cookie;

    TString TxId;
    TString KqpSessionId;
};

}  // namespace NKikimr::NGRpcProxy::V1
