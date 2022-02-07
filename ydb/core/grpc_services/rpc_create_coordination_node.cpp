#include "grpc_request_proxy.h"

#include "rpc_calls.h"
#include "rpc_scheme_base.h"
#include "rpc_common.h"

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace Ydb;

class TCreateCoordinationNodeRPC : public TRpcSchemeRequestActor<TCreateCoordinationNodeRPC, TEvCreateCoordinationNode> {
    using TBase = TRpcSchemeRequestActor<TCreateCoordinationNodeRPC, TEvCreateCoordinationNode>;

public:
    TCreateCoordinationNodeRPC(TEvCreateCoordinationNode* msg)
        : TBase(msg) {}

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);

        SendProposeRequest(ctx);
        Become(&TCreateCoordinationNodeRPC::StateWork);
    }

private:
    void SendProposeRequest(const TActorContext &ctx) {
        const auto req = GetProtoRequest();
        std::pair<TString, TString> pathPair;
        try {
            pathPair = SplitPath(Request_->GetDatabaseName(), req->path());
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
        modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateKesus);
        auto kesus = modifyScheme->MutableKesus();
        kesus->SetName(name);
        kesus->MutableConfig()->CopyFrom(req->config());
        ctx.Send(MakeTxProxyID(), proposeRequest.release());
    }

    void ReplyWithResult(StatusIds::StatusCode status, const TActorContext &ctx) {
        Request_->ReplyWithYdbStatus(status);
        Die(ctx);
    }
};

void TGRpcRequestProxy::Handle(TEvCreateCoordinationNode::TPtr& ev, const TActorContext& ctx) {
    ctx.Register(new TCreateCoordinationNodeRPC(ev->Release().Release()));
}

} // namespace NKikimr
} // namespace NGRpcService
