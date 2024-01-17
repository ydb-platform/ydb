#pragma once

#include "grpc_pq_actor.h"
#include <ydb/core/grpc_services/rpc_operation_request_base.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/services/deprecated/persqueue_v0/api/protos/persqueue.pb.h>

namespace NKikimr {
namespace NGRpcService {

class TMoveTopicActor : public TActorBootstrapped<TMoveTopicActor> {
public:

    TMoveTopicActor(NYdbGrpc::IRequestContextBase* request);

    ~TMoveTopicActor() = default;

    void Bootstrap(const NActors::TActorContext& ctx);

    STRICT_STFUNC(StateWork, {
        HFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, HandleDescribeResponse)
        HFunc(TEvTxUserProxy::TEvProposeTransactionStatus, HandleProposeStatus)
        HFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult, HandleTxComplete)
        HFunc(TEvTicketParser::TEvAuthorizeTicketResult, HandleAclResponse)
        HFunc(TEvTabletPipe::TEvClientConnected, Handle)
        HFunc(TEvTabletPipe::TEvClientDestroyed, Handle)
        IgnoreFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered)
    })
private:
    //SS requests
    void SendAllocateRequest(const NKikimrSchemeOp::TPersQueueGroupAllocate& pqAllocate, const TActorContext& ctx);
    void SendDeallocateRequest(const TActorContext& ctx);
    void SendDescribeRequest(const TString& path, const TActorContext& ctx);

    // Handlers
    void HandleDescribeResponse(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev,
                                const TActorContext& ctx);

    void HandleProposeStatus(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev, const TActorContext& ctx);
    void HandleTxComplete(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&, const TActorContext& ctx);

    //Helpers
    void ProcessDescriptions(const TActorContext& ctx);
    void OnTxComplete(ui64 txId, const TActorContext& ctx);
    void OpenPipe(const TActorContext &ctx);
    void OnPipeDestroyed(const TActorContext &ctx);
    void SendNotifyRequest(const TActorContext &ctx);
    const NPersQueue::TMoveTopicRequest* GetProtoRequest();
    void Reply(const Ydb::StatusIds::StatusCode status, const TString& error = TString());
    bool Verify(bool condition, const TString& error, const TActorContext& ctx);

    //Auth
    void SendAclRequest(const TActorContext& ctx);
    void HandleAclResponse(TEvTicketParser::TEvAuthorizeTicketResult::TPtr& ev, const TActorContext& ctx);

private:
    enum {
        E_UNDEFINED,
        E_ALLOCATE,
        E_DEALLOCATE
    } TxPending;

    ui64 TxId = 0;
    ui64 AllocateTxId = 0;
    ui64 SchemeShardTabletId;
    TActorId Pipe;

    TString SrcPath;
    TString DstPath;
    NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr SrcDescription;
    NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr DstDescription;
    TMaybe<bool> SrcExists;
    TMaybe<bool> DstExists;

    TIntrusivePtr<NYdbGrpc::IRequestContextBase> RequestCtx;
};

} //namespace NGRpcProxy
} // namespace NKikimr
