#include "service.h"
#include "ydb/core/grpc_services/local_rpc/local_rpc.h"
#include "ydb/core/grpc_services/service_fq.h"

#include <ydb/core/grpc_services/rpc_deferrable.h>

namespace NKikimr::NGRpcService {

namespace NYdbOverFq {

using CreateQueryRpc = TGrpcFqRequestOperationCall<FederatedQuery::CreateQueryRequest, FederatedQuery::CreateQueryResponse>;

template<typename TRpc, typename TCbWrapper>
class TLocalRpcCtxWithPerms
    : public NRpcService::TLocalRpcCtx<TRpc, TCbWrapper>
    , public TFqPermissionsBase<typename TRpc::TRequest> {
public:
    using TLocalRpcCtx = NRpcService::TLocalRpcCtx<TRpc, TCbWrapper>;
    using TProto = typename TRpc::TRequest;
    using TPermissions = TFqPermissionsBase<TProto>;
    using TPermissionsFunc = typename TPermissions::TPermissionsFunc;

    TLocalRpcCtxWithPerms(TProto&& proto, TCbWrapper&& wrapper, std::shared_ptr<IRequestCtx> req,
            TPermissionsFunc&& permissions)
        : TLocalRpcCtx(std::move(proto), std::move(wrapper), req->GetDatabaseName().GetOrElse(""),
                       /*serialized token*/Nothing(), /*requestType*/ Nothing(), /*internalCall*/ true)
        , TPermissions(std::move(permissions))
        , BaseRequest(std::move(req))
    {
        TPermissions::FillSids(*BaseRequest->GetDatabaseName(), *static_cast<const TProto*>(TLocalRpcCtx::GetRequest()));
        Cerr << "MY_MARKER Creating context with database '" << BaseRequest->GetDatabaseName() << "', "
                "token '" << NFederatedQuery::GetYdbToken(*BaseRequest) << "', "
                "has internal token: '" << (BaseRequest->GetInternalToken() != nullptr) << "'" << Endl;
    }

    const TMaybe<TString> GetPeerMetaValues(const TString& key) const override {
        return BaseRequest->GetPeerMetaValues(key);
    }

private:
    std::shared_ptr<IRequestCtx> BaseRequest;
};

class ExecuteDataQueryRPC : public TRpcOperationRequestActor<
    ExecuteDataQueryRPC, TGrpcRequestOperationCall<Ydb::Table::ExecuteDataQueryRequest, Ydb::Table::ExecuteDataQueryResponse>> {
public:
    using TBase = TRpcOperationRequestActor<
        ExecuteDataQueryRPC,
        TGrpcRequestOperationCall<Ydb::Table::ExecuteDataQueryRequest, Ydb::Table::ExecuteDataQueryResponse>>;
    using TBase::Become;
    using TBase::Send;
    using TBase::PassAway;
    using TBase::Request_;
    using TBase::GetProtoRequest;

    using TBase::TBase;

    void Bootstrap(const TActorContext& ctx) {
        ExecuteDataQueryImpl(ctx);
    }

private:
    void ExecuteDataQueryImpl(const TActorContext& ctx) {
        auto createdQueryFut = CreateQuery(ctx);
        Reply(Ydb::StatusIds_StatusCode_CANCELLED, ctx);
    }

    NThreading::TFuture<FederatedQuery::CreateQueryResult> CreateQuery(const TActorContext& ctx) {
        if (!GetProtoRequest()->query().has_yql_text()) {
            Reply(
                Ydb::StatusIds::BAD_REQUEST,
                "query id in ExecuteDataQuery is not supported",
                NKikimrIssues::TIssuesIds::EIssueCode::TIssuesIds_EIssueCode_DEFAULT_ERROR,
                ctx);
            return {};
        }
        const auto& text = GetProtoRequest()->query().yql_text();

        Cerr << "MY_MARKER CreateQuery(" << text << "), has internal token: " << (!!Request_->GetInternalToken()) << ", request: " << (const void*)Request_.get() << Endl;

        FederatedQuery::CreateQueryRequest req;
        req.set_execute_mode(FederatedQuery::ExecuteMode::RUN);
        auto& queryContent = *req.mutable_content();
        queryContent.set_type(FederatedQuery::QueryContent_QueryType_ANALYTICS);
        queryContent.set_name("Generated query from within");
        queryContent.mutable_text()->assign(text);
        queryContent.set_automatic(true);

        using TCbWrapper = NRpcService::TPromiseWrapper<typename CreateQueryRpc::TResponse>;
        auto makeContext = [this, as = ctx.ActorSystem()](FederatedQuery::CreateQueryRequest&& proto, TCbWrapper&& wrapper) {
            return new TLocalRpcCtxWithPerms<CreateQueryRpc, TCbWrapper>(std::move(proto), std::move(wrapper), Request_, GetFederatedQueryCreateQueryPermissions());
        };

        return NRpcService::DoLocalRpc<CreateQueryRpc>(std::move(req), ctx.ActorSystem(), std::move(makeContext))
            .Apply([](const NThreading::TFuture<FederatedQuery::CreateQueryResponse>& respFut) {
                Cerr << "MY_MARKER CreateQuery responded ";
                const auto& resp = respFut.GetValue();
                Cerr << "status: " << Ydb::StatusIds_StatusCode_Name(resp.operation().status());

                FederatedQuery::CreateQueryResult result;
                respFut.GetValue().operation().result().UnpackTo(&result);
                Cerr << "id: " << result.query_id() << Endl;
                return result;
            });
    }
};

void DoExecuteDataQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    Cerr << "MY_MARKER DoExecuteDataQueryRequest()" << Endl;
    f.RegisterActor(new ExecuteDataQueryRPC(p.release()));
}

} // namespace NYdbOverFq
} // namespace NKikimr::NGRpcService