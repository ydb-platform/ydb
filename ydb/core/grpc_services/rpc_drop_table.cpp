#include "grpc_request_proxy.h" 
 
#include "rpc_calls.h" 
#include "rpc_scheme_base.h"
#include "rpc_common.h" 
 
namespace NKikimr { 
namespace NGRpcService { 
 
using namespace NActors; 
using namespace Ydb;
 
class TDropTableRPC : public TRpcSchemeRequestActor<TDropTableRPC, TEvDropTableRequest> {
    using TBase = TRpcSchemeRequestActor<TDropTableRPC, TEvDropTableRequest>;

public: 
    TDropTableRPC(IRequestOpCtx* msg) 
        : TBase(msg) {}
 
    void Bootstrap(const TActorContext &ctx) { 
        TBase::Bootstrap(ctx);

        SendProposeRequest(ctx); 
        Become(&TDropTableRPC::StateWork); 
    } 
 
private: 
    void SendProposeRequest(const TActorContext &ctx) { 
        const auto req = GetProtoRequest(); 
        std::pair<TString, TString> pathPair; 
        try { 
            pathPair = SplitPath(req->path()); 
        } catch (const std::exception& ex) { 
            Request_->RaiseIssue(NYql::ExceptionToIssue(ex)); 
            return ReplyWithResult(StatusIds::BAD_REQUEST, ctx); 
        } 
 
        const auto& workingDir = pathPair.first; 
        const auto& name = pathPair.second; 
 
        std::unique_ptr<TEvTxUserProxy::TEvProposeTransaction> proposeRequest = CreateProposeTransaction();
        NKikimrTxUserProxy::TEvProposeTransaction& record = proposeRequest->Record; 
        NKikimrSchemeOp::TModifyScheme* modifyScheme = record.MutableTransaction()->MutableModifyScheme();
        modifyScheme->SetWorkingDir(workingDir); 
        modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropTable);
        auto drop = modifyScheme->MutableDrop(); 
        drop->SetName(name); 
        ctx.Send(MakeTxProxyID(), proposeRequest.release()); 
    } 
 
    void ReplyWithResult(StatusIds::StatusCode status, const TActorContext &ctx) { 
        Request_->ReplyWithYdbStatus(status); 
        Die(ctx); 
    } 
}; 
 
void TGRpcRequestProxy::Handle(TEvDropTableRequest::TPtr& ev, const TActorContext& ctx) { 
    ctx.Register(new TDropTableRPC(ev->Release().Release())); 
} 
 
template<> 
IActor* TEvDropTableRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) { 
    return new TDropTableRPC(msg); 
} 
 
} // namespace NKikimr 
} // namespace NGRpcService 
