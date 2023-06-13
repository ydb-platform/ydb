#include "service_table.h"
#include <ydb/core/grpc_services/base/base.h>

#include "rpc_calls.h"
#include "rpc_kqp_base.h"
#include "service_table.h"

#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace Ydb;
using namespace NKqp;

using TEvDeleteSessionRequest = TGrpcRequestOperationCall<Ydb::Table::DeleteSessionRequest,
    Ydb::Table::DeleteSessionResponse>;

class TDeleteSessionRPC : public TRpcKqpRequestActor<TDeleteSessionRPC, TEvDeleteSessionRequest> {
    using TBase = TRpcKqpRequestActor<TDeleteSessionRPC, TEvDeleteSessionRequest>;

public:
    TDeleteSessionRPC(IRequestOpCtx* msg)
        : TBase(msg) {}

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);

        DeleteSessionImpl(ctx);
        Become(&TDeleteSessionRPC::StateWork);
    }

private:
    void DeleteSessionImpl(const TActorContext& ctx) {
        const auto req = GetProtoRequest();

        auto ev = MakeHolder<NKqp::TEvKqp::TEvCloseSessionRequest>();

        NYql::TIssues issues;
        if (CheckSession(req->session_id(), issues)) {
            ev->Record.MutableRequest()->SetSessionId(req->session_id());
        } else {
            return Reply(Ydb::StatusIds::BAD_REQUEST, issues, ctx);
        }

        ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release()); //no respose will be sended, so don't wait for anything
        Request_->ReplyWithYdbStatus(Ydb::StatusIds::SUCCESS);
        this->Die(ctx);
    }
};

void DoDeleteSessionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TDeleteSessionRPC(p.release()));
}

template<>
IActor* TEvDeleteSessionRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
    return new TDeleteSessionRPC(msg);
}

} // namespace NGRpcService
} // namespace NKikimr
