#include "grpc_request_proxy.h"

#include "rpc_calls.h"
#include "rpc_scheme_base.h"
#include "rpc_common.h"

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace Ydb;

class TAlterCoordinationNodeRPC : public TRpcSchemeRequestActor<TAlterCoordinationNodeRPC, TEvAlterCoordinationNode> {
    using TBase = TRpcSchemeRequestActor<TAlterCoordinationNodeRPC, TEvAlterCoordinationNode>;

public:
    TAlterCoordinationNodeRPC(TEvAlterCoordinationNode* msg)
        : TBase(msg) {}

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);

        SendProposeRequest(ctx);
        Become(&TAlterCoordinationNodeRPC::StateWork);
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
        modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterKesus);

        auto desc = modifyScheme->MutableKesus();
        desc->SetName(name);
        desc->MutableConfig()->CopyFrom(req->config());

        ctx.Send(MakeTxProxyID(), proposeRequest.release());
    }

    void ReplyWithResult(StatusIds::StatusCode status, const TActorContext &ctx) {
        Request_->ReplyWithYdbStatus(status);
        Die(ctx);
    }
};

void TGRpcRequestProxy::Handle(TEvAlterCoordinationNode::TPtr& ev, const TActorContext& ctx) {
    ctx.Register(new TAlterCoordinationNodeRPC(ev->Release().Release()));
}

} // namespace NKikimr
} // namespace NGRpcService
