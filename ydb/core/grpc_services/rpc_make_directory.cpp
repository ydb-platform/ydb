#include "service_scheme.h"

#include "rpc_scheme_base.h"
#include "rpc_common/rpc_common.h"
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/public/api/protos/ydb_scheme.pb.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace Ydb;

using TEvMakeDirectoryRequest = TGrpcRequestOperationCall<Ydb::Scheme::MakeDirectoryRequest,
    Ydb::Scheme::MakeDirectoryResponse>;

class TMakeDirectoryRPC : public TRpcSchemeRequestActor<TMakeDirectoryRPC, TEvMakeDirectoryRequest> {
    using TBase = TRpcSchemeRequestActor<TMakeDirectoryRPC, TEvMakeDirectoryRequest>;

public:
    TMakeDirectoryRPC(IRequestOpCtx* msg)
        : TBase(msg) {}

    void Bootstrap(const TActorContext &ctx) {
        TBase::Bootstrap(ctx);

        SendProposeRequest(ctx);
        Become(&TMakeDirectoryRPC::StateWork);
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
        modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpMkDir);
        modifyScheme->MutableMkDir()->SetName(name);
        ctx.Send(MakeTxProxyID(), proposeRequest.release());
    }

    void ReplyWithResult(const StatusIds::StatusCode status,
                         const TActorContext &ctx) {
        Request_->ReplyWithYdbStatus(status);
        Die(ctx);
    }
};

void DoMakeDirectoryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TMakeDirectoryRPC(p.release()));
}

template<>
IActor* TEvMakeDirectoryRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
    return new TMakeDirectoryRPC(msg);
}

} // namespace NGRpcService
} // namespace NKikimr

