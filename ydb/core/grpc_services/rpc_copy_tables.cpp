#include "service_table.h"
#include <ydb/core/grpc_services/base/base.h>

#include "rpc_calls.h"
#include "rpc_scheme_base.h"
#include "rpc_common/rpc_common.h"

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace Ydb;

using TEvCopyTablesRequest = TGrpcRequestOperationCall<Ydb::Table::CopyTablesRequest,
    Ydb::Table::CopyTablesResponse>;

class TCopyTablesRPC : public TRpcSchemeRequestActor<TCopyTablesRPC, TEvCopyTablesRequest> {
    using TBase = TRpcSchemeRequestActor<TCopyTablesRPC, TEvCopyTablesRequest>;

public:
    TCopyTablesRPC(IRequestOpCtx* msg)
        : TBase(msg) {}

    void Bootstrap(const TActorContext &ctx) {
        TBase::Bootstrap(ctx);

        SendProposeRequest(ctx);
        Become(&TCopyTablesRPC::StateWork);
    }

private:
    void SendProposeRequest(const TActorContext &ctx) {
        std::unique_ptr<TEvTxUserProxy::TEvProposeTransaction> proposeRequest = CreateProposeTransaction();
        NKikimrTxUserProxy::TEvProposeTransaction& record = proposeRequest->Record;
        NKikimrSchemeOp::TModifyScheme* modifyScheme = record.MutableTransaction()->MutableModifyScheme();
        modifyScheme->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateConsistentCopyTables);
        auto copy = modifyScheme->MutableCreateConsistentCopyTables();

        const auto req = GetProtoRequest();
        for (const auto& item: req->tables()) {
            auto description = copy->AddCopyTableDescriptions();
            description->SetSrcPath(item.source_path());
            description->SetDstPath(item.destination_path());
            description->SetOmitIndexes(item.omit_indexes());
        }

        ctx.Send(MakeTxProxyID(), proposeRequest.release());
    }
};

void DoCopyTablesRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TCopyTablesRPC(p.release()));
}

} // namespace NKikimr
} // namespace NGRpcService
