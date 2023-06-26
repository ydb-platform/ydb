#include "service_coordination.h"
#include <ydb/core/grpc_services/base/base.h>

#include "rpc_scheme_base.h"
#include "rpc_common/rpc_common.h"

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace Ydb;

using TEvAlterCoordinationNode = TGrpcRequestOperationCall<Ydb::Coordination::AlterNodeRequest,
    Ydb::Coordination::AlterNodeResponse>;

class TAlterCoordinationNodeRPC : public TRpcSchemeRequestActor<TAlterCoordinationNodeRPC, TEvAlterCoordinationNode> {
    using TBase = TRpcSchemeRequestActor<TAlterCoordinationNodeRPC, TEvAlterCoordinationNode>;

public:
    TAlterCoordinationNodeRPC(IRequestOpCtx* msg)
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

void DoAlterCoordinationNode(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TAlterCoordinationNodeRPC(p.release()));
}

} // namespace NKikimr
} // namespace NGRpcService
