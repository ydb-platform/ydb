#include "service.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/rpc_deferrable.h>

namespace NKikimr::NGRpcService::NYdbOverFq {

class TCreateSessionRPC : public TRpcOperationRequestActor<
    TCreateSessionRPC, TGrpcRequestOperationCall<Ydb::Table::CreateSessionRequest, Ydb::Table::CreateSessionResponse>> {
public:
    using TBase = TRpcOperationRequestActor<
        TCreateSessionRPC,
        TGrpcRequestOperationCall<Ydb::Table::CreateSessionRequest, Ydb::Table::CreateSessionResponse>>;
    using TBase::Become;
    using TBase::Send;
    using TBase::PassAway;
    using TBase::Request_;
    using TBase::GetProtoRequest;

    using TBase::TBase;

    void Bootstrap(const TActorContext& ctx) {
        CreateSessionImpl(ctx);
    }

private:
    void CreateSessionImpl(const TActorContext& ctx) {
        const TString FakeSessionId = "fake-session-id";

        Ydb::Table::CreateSessionResult res;
        res.set_session_id(FakeSessionId);
        ReplyWithResult(Ydb::StatusIds::SUCCESS, res, ctx);
    }
};

void DoCreateSessionRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TCreateSessionRPC(p.release()));
}

} // namespace NKikimr::NGRpcService::NYdbOverFq