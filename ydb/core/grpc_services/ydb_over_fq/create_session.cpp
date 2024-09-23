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

class TDeleteSessionRPC : public TRpcOperationRequestActor<
    TCreateSessionRPC, TGrpcRequestOperationCall<Ydb::Table::DeleteSessionRequest, Ydb::Table::DeleteSessionResponse>> {
public:
    using TBase = TRpcOperationRequestActor<
        TCreateSessionRPC,
        TGrpcRequestOperationCall<Ydb::Table::DeleteSessionRequest, Ydb::Table::DeleteSessionResponse>>;
    using TBase::Become;
    using TBase::Send;
    using TBase::PassAway;
    using TBase::Request_;
    using TBase::GetProtoRequest;

    using TBase::TBase;

    void Bootstrap(const TActorContext& ctx) {
        DeleteSessionImpl(ctx);
    }

private:
    void DeleteSessionImpl(const TActorContext& ctx) {
        const TString FakeSessionId = "fake-session-id";

        if (GetProtoRequest()->session_id() == FakeSessionId) {
            Reply(Ydb::StatusIds::SUCCESS, ctx);
        } else {
            Reply(Ydb::StatusIds::BAD_REQUEST, ctx);
        }
    }
};

std::function<void(std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&)> GetCreateSessionExecutor(NActors::TActorId) {
    return [](std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
        f.RegisterActor(new TCreateSessionRPC(p.release()));
    };
}

std::function<void(std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&)> GetDeleteSessionExecutor(NActors::TActorId) {
    return [](std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
        f.RegisterActor(new TDeleteSessionRPC(p.release()));
    };
}

} // namespace NKikimr::NGRpcService::NYdbOverFq