#include "service_table.h"
#include <ydb/core/grpc_services/base/base.h>

#include "service_table.h"
#include "rpc_calls.h"
#include "rpc_scheme_base.h"
#include "rpc_common/rpc_common.h"

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace Ydb;

using TEvRenameTablesRequest = TGrpcRequestOperationCall<Ydb::Table::RenameTablesRequest,
    Ydb::Table::RenameTablesResponse>;

class TRenameTablesRPC : public TRpcSchemeRequestActor<TRenameTablesRPC, TEvRenameTablesRequest> {
    using TBase = TRpcSchemeRequestActor<TRenameTablesRPC, TEvRenameTablesRequest>;

public:
    TRenameTablesRPC(IRequestOpCtx* msg)
        : TBase(msg) {}

    void Bootstrap(const TActorContext &ctx) {
        TBase::Bootstrap(ctx);

        SendProposeRequest(ctx);
        Become(&TRenameTablesRPC::StateWork);
    }

private:
    void SendProposeRequest(const TActorContext &ctx) {
        const auto req = GetProtoRequest();

        auto proposeRequest = TBase::CreateProposeTransaction();
        auto& record = proposeRequest->Record;
        auto& transaction = *record.MutableTransaction();

        if (req->tables().empty()) {
            Request_->RaiseIssue(NYql::TIssue("Emply move list"));
            return Reply(StatusIds::BAD_REQUEST, ctx);
        }

        try {
            for (const auto& item: req->tables()) {
                if (item.replace_destination()) {
                    auto* modifyScheme = transaction.AddTransactionalModification();
                    modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropTable);

                    auto [workingDir, name] = SplitPath(item.destination_path());
                    modifyScheme->SetWorkingDir(workingDir);
                    modifyScheme->MutableDrop()->SetName(name);
                }

                auto* modifyScheme = transaction.AddTransactionalModification();
                modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpMoveTable);
                auto* description = modifyScheme->MutableMoveTable();
                description->SetSrcPath(item.source_path());
                description->SetDstPath(item.destination_path());
            }
        } catch (const std::exception& ex) {
            Request_->RaiseIssue(NYql::ExceptionToIssue(ex));
            return Reply(StatusIds::BAD_REQUEST, ctx);
        }

        ctx.Send(MakeTxProxyID(), proposeRequest.release());
    }
};

void DoRenameTablesRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TRenameTablesRPC(p.release()));
}

} // namespace NKikimr
} // namespace NGRpcService
