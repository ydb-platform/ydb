#include "service.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/rpc_deferrable.h>

namespace NKikimr::NGRpcService::NYdbOverFq {

class TKeepAliveRPC : public TRpcOperationRequestActor<
    TKeepAliveRPC, TGrpcRequestOperationCall<Ydb::Table::KeepAliveRequest, Ydb::Table::KeepAliveResponse>> {
public:
    using TBase = TRpcOperationRequestActor<
        TKeepAliveRPC,
        TGrpcRequestOperationCall<Ydb::Table::KeepAliveRequest, Ydb::Table::KeepAliveResponse>>;
    using TBase::TBase;

    void Bootstrap(const TActorContext& ctx) {
        KeepAliveImpl(ctx);
    }

private:
    void KeepAliveImpl(const TActorContext& ctx) {
        const TString FakeSessionId = "fake-session-id";

        Ydb::Table::KeepAliveResult result;
        if (TBase::GetProtoRequest()->session_id() == FakeSessionId) {
            result.set_session_status(Ydb::Table::KeepAliveResult_SessionStatus_SESSION_STATUS_READY);
        }
        ReplyWithResult(Ydb::StatusIds::SUCCESS, result, ctx);
    }
};

std::function<void(std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&)> GetKeepAliveExecutor(NActors::TActorId) {
    return [](std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
        f.RegisterActor(new TKeepAliveRPC(p.release()));
    };
}
}
