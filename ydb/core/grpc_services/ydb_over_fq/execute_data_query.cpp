#include "service.h"

#include <ydb/core/fq/libs/control_plane_storage/util.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/grpc_services/service_fq.h>
#include <ydb/core/grpc_services/rpc_deferrable.h>

namespace NKikimr::NGRpcService {

namespace NYdbOverFq {

using CreateQueryRpc = TGrpcFqRequestOperationCall<FederatedQuery::CreateQueryRequest, FederatedQuery::CreateQueryResponse>;
using GetQueryStatusRpc = TGrpcFqRequestOperationCall<FederatedQuery::GetQueryStatusRequest, FederatedQuery::GetQueryStatusResponse>;
using DescribeQueryRpc = TGrpcFqRequestOperationCall<FederatedQuery::DescribeQueryRequest, FederatedQuery::DescribeQueryResponse>;
using GetResultDataRpc = TGrpcFqRequestOperationCall<FederatedQuery::GetResultDataRequest, FederatedQuery::GetResultDataResponse>;

template<typename TRpc, typename TCbWrapper>
class TLocalRpcCtxWithPerms
    : public NRpcService::TLocalRpcCtx<TRpc, TCbWrapper>
    , public TFqPermissionsBase<typename TRpc::TRequest> {
public:
    using TLocalRpcCtx = NRpcService::TLocalRpcCtx<TRpc, TCbWrapper>;
    using TProto = typename TRpc::TRequest;
    using TPermissions = TFqPermissionsBase<TProto>;
    using TPermissionsFunc = typename TPermissions::TPermissionsFunc;

    template <typename TReq, typename TResp>
    TLocalRpcCtxWithPerms(TProto&& proto, TCbWrapper&& wrapper, std::shared_ptr<TGrpcYdbOverFqOpCall<TReq, TResp>> req)
        : TLocalRpcCtx(std::move(proto), std::move(wrapper), req->GetDatabaseName().GetOrElse(""),
                       /*serialized token*/req->GetSerializedToken().Empty() ? Nothing() : TMaybe<TString>{req->GetSerializedToken()}, /*requestType*/ Nothing(), /*internalCall*/ true)
        , TPermissions(nullptr)
        , BaseRequest(std::move(req))
        , Scope("yandexcloud:/" + BaseRequest->GetDatabaseName().GetOrElse("/"))
    {
        TPermissions::Sids = std::static_pointer_cast<TGrpcYdbOverFqOpCall<TReq, TResp>>(BaseRequest)->GetSids();
    }

    const TMaybe<TString> GetPeerMetaValues(const TString& key) const override {
        if (key == "x-ydb-fq-project") {
            return Scope;
        }
        return BaseRequest->GetPeerMetaValues(key);
    }

private:
    std::shared_ptr<IRequestCtx> BaseRequest;
    TString Scope;
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
        Become(&ExecuteDataQueryRPC::CreateQueryState);
        CreateQuery(ctx);
    }

private:
    // overall algorithm: CreateQuery -> WaitForExecution -> GatherResultSetSizes -> GatherResultSets -> SendReply

    void CreateQuery(const TActorContext& ctx) {
        if (!GetProtoRequest()->query().has_yql_text()) {
            LOG_INFO_S(ctx, NKikimrServices::FQ_INTERNAL_SERVICE,
                            "pseudo ExecuteDataQuery actorId: " << SelfId().ToString() << ", got request with id instead of text");
            Reply(
                Ydb::StatusIds::BAD_REQUEST,
                "query id in ExecuteDataQuery is not supported",
                NKikimrIssues::TIssuesIds::EIssueCode::TIssuesIds_EIssueCode_DEFAULT_ERROR,
                ctx);
            return;
        }
        const auto& text = GetProtoRequest()->query().yql_text();

        FederatedQuery::CreateQueryRequest req;
        req.set_execute_mode(FederatedQuery::ExecuteMode::RUN);
        auto& queryContent = *req.mutable_content();
        queryContent.set_type(FederatedQuery::QueryContent_QueryType_ANALYTICS);
        queryContent.set_name("Generated query from within");
        queryContent.mutable_text()->assign(text);
        queryContent.set_automatic(true);
        auto& acl = *queryContent.mutable_acl();
        acl.set_visibility(FederatedQuery::Acl_Visibility::Acl_Visibility_SCOPE);

        LOG_DEBUG_S(ctx, NKikimrServices::FQ_INTERNAL_SERVICE,
                        "pseudo ExecuteDataQuery actorId: " << SelfId().ToString() << ", creating query");

        MakeLocalCall<CreateQueryRpc, TEvCreateQueryResponse>(std::move(req), ctx);
    }

    void WaitForExecution(const TString& queryId, const TActorContext& ctx) {
        QueryId = queryId;
        auto reqEv = CreateStatusRequest();

        Become(&ExecuteDataQueryRPC::WaitForExecutionState);
        ctx.Send(SelfId(), reqEv);
    }

    void GatherResultSetSizes(const TActorContext& ctx) {
        FederatedQuery::DescribeQueryRequest req;
        req.set_query_id(QueryId);

        Become(&ExecuteDataQueryRPC::GatherResultSetSizesState);
        MakeLocalCall<DescribeQueryRpc, TEvDescribeQueryResponse>(std::move(req), ctx);
    }

    void GatherResultSets(const TActorContext& ctx) {
        Become(&ExecuteDataQueryRPC::GatherResultSetsState);
        ctx.Send(SelfId(), CreateResultSetRequest(0, 0));
    }

    void SendReply(const TActorContext& ctx) {
        Ydb::Table::ExecuteQueryResult result;
        for (const auto& resultSet : ResultSets) {
            *result.add_result_sets() = resultSet;
        }
        result.mutable_query_meta()->set_id(QueryId);
        ReplyWithResult(Ydb::StatusIds_StatusCode_SUCCESS, result, ctx);
    }

    // CreateQueryImpl

    STRICT_STFUNC(CreateQueryState,
        HFunc(TEvCreateQueryResponse, HandleCreatedQuery);
    )

    void HandleCreatedQuery(typename TEvCreateQueryResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& resp = ev->Get()->Message;
        if (HandleFailure(resp.operation(), "CreateQuery", ctx)) [[unlikely]] {
            return;
        }

        FederatedQuery::CreateQueryResult result;
        resp.operation().result().UnpackTo(&result);

        LOG_DEBUG_S(ctx, NKikimrServices::FQ_INTERNAL_SERVICE,
            "pseudo ExecuteDataQuery actorId: " << SelfId().ToString() << ", created query: " << result.query_id());

        WaitForExecution(result.query_id(), ctx);
    }

    // WaitForExecutionImpl

    STRICT_STFUNC(WaitForExecutionState,
        HFunc(TEvGetQueryStatusRequest, HandleStatusRequest);
        HFunc(TEvGetQueryStatusResponse, HandleStatusResponse);
    )

    TEvGetQueryStatusRequest* CreateStatusRequest() {
        FederatedQuery::GetQueryStatusRequest req;
        req.set_query_id(QueryId);
        return new TEvGetQueryStatusRequest(std::move(req));
    }

    void HandleStatusRequest(typename TEvGetQueryStatusRequest::TPtr& ev, const TActorContext& ctx) {
        MakeLocalCall<GetQueryStatusRpc, TEvGetQueryStatusResponse>(std::move(ev->Get()->Message), ctx);
    }

    void HandleStatusResponse(typename TEvGetQueryStatusResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& resp = ev->Get()->Message;
        if (HandleFailure(resp.operation(), "GetQueryStatus", ctx)) [[unlikely]] {
            return;
        }

        FederatedQuery::GetQueryStatusResult result;
        resp.operation().result().UnpackTo(&result);

        if (!NFq::IsTerminalStatus(result.status())) {
            LOG_TRACE_S(ctx, NKikimrServices::FQ_INTERNAL_SERVICE,
                "pseudo ExecuteDataQuery actorId: " << SelfId().ToString() << ", still waiting for query: " << QueryId <<
                ", current status: " << FederatedQuery::QueryMeta::ComputeStatus_Name(result.status()));
            ctx.Schedule(TDuration::Seconds(3), CreateStatusRequest());
            return;
        }

        if (result.status() != FederatedQuery::QueryMeta_ComputeStatus_COMPLETED) {
            LOG_INFO_S(ctx, NKikimrServices::FQ_INTERNAL_SERVICE,
                "pseudo ExecuteDataQuery actorId: " << SelfId().ToString() << ", queryId: " << QueryId <<
                ", finished with bad status: " << FederatedQuery::QueryMeta::ComputeStatus_Name(result.status()));
            Reply(Ydb::StatusIds_StatusCode_INTERNAL_ERROR,
                TString{TStringBuilder{} << "Created query " << QueryId << " finished with non-success status: " <<
                    FederatedQuery::QueryMeta::ComputeStatus_Name(result.status())}, NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ctx);
            return;
        }

        LOG_INFO_S(ctx, NKikimrServices::FQ_INTERNAL_SERVICE,
            "pseudo ExecuteDataQuery actorId: " << SelfId().ToString() << ", queryId: " << QueryId <<
            ", finished query execution");

        GatherResultSetSizes(ctx);
    }

    // GatherResultSetSizesImpl

    STRICT_STFUNC(GatherResultSetSizesState,
        HFunc(TEvDescribeQueryResponse, HandleDescribeResponse);
    )

    void HandleDescribeResponse(typename TEvDescribeQueryResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& resp = ev->Get()->Message;
        if (HandleFailure(resp.operation(), "DescribeQuery", ctx)) [[unlikely]] {
            return;
        }

        FederatedQuery::DescribeQueryResult result;
        resp.operation().result().UnpackTo(&result);
        ResultSetSizes.reserve(result.query().result_set_meta_size());

        for (const auto& meta : result.query().result_set_meta()) {
            ResultSetSizes.push_back(meta.rows_count());
        }

        GatherResultSets(ctx);
    }

    // GatherResultSetsImpl

    STRICT_STFUNC(GatherResultSetsState,
        HFunc(TEvGetResultDataRequest, HandleResultSetRequest);
        HFunc(TEvGetResultDataResponse, HandleResultSetPart);
    )

    TEvGetResultDataRequest* CreateResultSetRequest(i32 index, i64 offset) {
        auto req = new TEvGetResultDataRequest();
        auto& msg = req->Message;

        constexpr i64 RowsLimit = 333;

        msg.set_query_id(QueryId);
        msg.set_result_set_index(index);
        msg.set_offset(offset);
        msg.set_limit(RowsLimit);
        return req;
    }

    void HandleResultSetRequest(typename TEvGetResultDataRequest::TPtr& ev, const TActorContext& ctx) {
        MakeLocalCall<GetResultDataRpc, TEvGetResultDataResponse>(std::move(ev->Get()->Message), ctx);
    }

    void HandleResultSetPart(typename TEvGetResultDataResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& resp = ev->Get()->Message;
        if (HandleFailure(resp.operation(), "GetResultData", ctx)) [[unlikely]] {
            return;
        }

        FederatedQuery::GetResultDataResult result;
        resp.operation().result().UnpackTo(&result);

        Y_ABORT_UNLESS(CurrentResultSet >= static_cast<i64>(ResultSets.size()));

        Ydb::ResultSet* resultSet = nullptr;
        if (CurrentResultSet == static_cast<i64>(ResultSets.size())) {
            ResultSets.push_back(result.result_set());
            resultSet = &ResultSets.back();
        } else {
            // TODO: validate columns
            resultSet = &ResultSets.back();
            for (const auto& srcRow : result.result_set().rows()) {
                *resultSet->add_rows() = srcRow;
            }
        }

        if (resultSet->rows_size() < ResultSetSizes[CurrentResultSet]) {
            ctx.Send(SelfId(), CreateResultSetRequest(CurrentResultSet, resultSet->rows_size()));
        } else {
            Y_ABORT_UNLESS(resultSet->rows_size() == ResultSetSizes[CurrentResultSet]);
            if (++CurrentResultSet < static_cast<i64>(ResultSetSizes.size())) {
                ctx.Send(SelfId(), CreateResultSetRequest(CurrentResultSet, 0));
            } else {
                SendReply(ctx);
            }
        }
    }

    // helpers

    template <typename TRpc, typename TEvResponse>
    void MakeLocalCall(typename TRpc::TRequest&& req, const TActorContext& ctx) {
        using TRequest = typename TRpc::TRequest;
        using TResp = typename TRpc::TResponse;
        using TCbWrapper = NRpcService::TPromiseWrapper<TResp>;

        auto baseRequest = std::dynamic_pointer_cast<TGrpcYdbOverFqOpCall<Ydb::Table::ExecuteDataQueryRequest, Ydb::Table::ExecuteDataQueryResponse>>(Request_);
        auto makeContext = [baseRequest, as = ctx.ActorSystem()](TRequest&& proto, TCbWrapper&& wrapper) {
            return new TLocalRpcCtxWithPerms<TRpc, TCbWrapper>(std::move(proto), std::move(wrapper), baseRequest);
        };
        NRpcService::DoLocalRpc<TRpc>(std::move(req), ctx.ActorSystem(), std::move(makeContext))
            .Subscribe([selfId = SelfId(), as = ctx.ActorSystem()](const NThreading::TFuture<TResp>& respFut) {
                auto respEvent = new TEvResponse(respFut.GetValue());
                as->Send(selfId, respEvent);
            });
    }

    // if status is not success, replies error, returns true
    bool HandleFailure(const Ydb::Operations::Operation& operation, std::string_view opName, const TActorContext& ctx) {
        if (operation.status() == Ydb::StatusIds::SUCCESS) {
            return false;
        }

        NYql::TIssues issues;
        NYql::IssuesFromMessage(operation.issues(), issues);
        LOG_INFO_S(ctx, NKikimrServices::FQ_INTERNAL_SERVICE,
            "pseudo ExecuteDataQuery actorId: " << SelfId().ToString() << ", failed to " << opName << ", status: " <<
            Ydb::StatusIds::StatusCode_Name(operation.status()) << ", issues: " << issues.ToOneLineString());
        Reply(Ydb::StatusIds_StatusCode_INTERNAL_ERROR, operation.issues(), ctx);
        return true;
    }

    TString QueryId;
    std::vector<i64> ResultSetSizes;
    i32 CurrentResultSet = 0;
    std::vector<Ydb::ResultSet> ResultSets;
};

void DoExecuteDataQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new ExecuteDataQueryRPC(p.release()));
}

} // namespace NYdbOverFq
} // namespace NKikimr::NGRpcService