#include "service_table.h"
#include <ydb/core/grpc_services/base/base.h>

#include "rpc_calls.h"
#include "rpc_scheme_base.h"
#include "rpc_common/rpc_common.h"

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace Ydb;

using TEvCopyTableRequest = TGrpcRequestOperationCall<Ydb::Table::CopyTableRequest,
    Ydb::Table::CopyTableResponse>;

class TCopyTableRPC : public TRpcSchemeRequestActor<TCopyTableRPC, TEvCopyTableRequest> {
    using TBase = TRpcSchemeRequestActor<TCopyTableRPC, TEvCopyTableRequest>;

public:
    TCopyTableRPC(IRequestOpCtx* msg)
        : TBase(msg) {}

    void Bootstrap(const TActorContext &ctx) {
        TBase::Bootstrap(ctx);

        SendProposeRequest(ctx);
        Become(&TCopyTableRPC::StateWork);
    }

private:
    void SendProposeRequest(const TActorContext &ctx) {
        const auto req = GetProtoRequest();
        std::pair<TString, TString> destinationPathPair;
        try {
            destinationPathPair = SplitPath(req->destination_path());
        } catch (const std::exception& ex) {
            Request_->RaiseIssue(NYql::ExceptionToIssue(ex));
            return Reply(StatusIds::BAD_REQUEST, ctx);
        }

        const auto& workingDir = destinationPathPair.first;
        const auto& name = destinationPathPair.second;

        std::unique_ptr<TEvTxUserProxy::TEvProposeTransaction> proposeRequest = CreateProposeTransaction();
        NKikimrTxUserProxy::TEvProposeTransaction& record = proposeRequest->Record;
        NKikimrSchemeOp::TModifyScheme* modifyScheme = record.MutableTransaction()->MutableModifyScheme();
        modifyScheme->SetWorkingDir(workingDir);
        modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable);
        auto create = modifyScheme->MutableCreateTable();
        create->SetName(name);
        create->SetCopyFromTable(req->source_path());
        ctx.Send(MakeTxProxyID(), proposeRequest.release());
    }
};

void DoCopyTableRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TCopyTableRPC(p.release()));
}

} // namespace NKikimr
} // namespace NGRpcService
