#include "service_table.h"
#include <ydb/core/grpc_services/base/base.h>

#include "rpc_calls.h"
#include "rpc_kqp_base.h"
#include "rpc_common/rpc_common.h"
#include "service_table.h"
#include "audit_dml_operations.h"

#include <ydb/core/protos/console_config.pb.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/public/lib/operation_id/operation_id.h>

#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace NOperationId;
using namespace Ydb;
using namespace NKqp;

using TEvPrepareDataQueryRequest = TGrpcRequestOperationCall<Ydb::Table::PrepareDataQueryRequest,
    Ydb::Table::PrepareDataQueryResponse>;


class TPrepareDataQueryRPC : public TRpcKqpRequestActor<TPrepareDataQueryRPC, TEvPrepareDataQueryRequest> {
    using TBase = TRpcKqpRequestActor<TPrepareDataQueryRPC, TEvPrepareDataQueryRequest>;

public:
    TPrepareDataQueryRPC(IRequestOpCtx* msg)
        : TBase(msg) {}

    void Bootstrap(const TActorContext &ctx) {
        TBase::Bootstrap(ctx);

        this->Become(&TPrepareDataQueryRPC::StateWork);
        Proceed(ctx);
    }

    void StateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NKqp::TEvKqp::TEvQueryResponse, Handle);
            default: TBase::StateWork(ev);
        }
    }

    void Proceed(const TActorContext &ctx) {
        const auto req = GetProtoRequest();
        const auto traceId = Request_->GetTraceId();
        const auto requestType = Request_->GetRequestType();
        auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        SetAuthToken(ev, *Request_);
        SetDatabase(ev, *Request_);
        ev->Record.MutableRequest()->SetClientAddress(Request_->GetPeerName());

        AuditContextAppend(Request_.get(), *req);

        if (traceId) {
            ev->Record.SetTraceId(traceId.GetRef());
        }

        if (requestType) {
            ev->Record.SetRequestType(requestType.GetRef());
        }

        if (CheckSession(req->session_id(), Request_.get())) {
            ev->Record.MutableRequest()->SetSessionId(req->session_id());
        } else {
            return Reply(Ydb::StatusIds::BAD_REQUEST, ctx);
        }

        if (!CheckQuery(req->yql_text(), Request_.get())) {
            return Reply(Ydb::StatusIds::BAD_REQUEST, ctx);
        }

        ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_PREPARE);
        ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
        ev->Record.MutableRequest()->SetQuery(req->yql_text());

        ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release(), 0, 0, Span_.GetTraceId());
    }

    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record.GetRef();
        SetCost(record.GetConsumedRu());
        AddServerHintsIfAny(record);

        if (record.GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
            const auto& kqpResponse = record.GetResponse();
            const auto& queryId = kqpResponse.GetPreparedQuery();
            const auto& issueMessage = kqpResponse.GetQueryIssues();
            const auto& queryParameters = kqpResponse.GetQueryParameters();

            Ydb::Table::PrepareQueryResult queryResult;
            queryResult.set_query_id(FormatPreparedQueryIdCompat(queryId));
            for (const auto& queryParameter: queryParameters) {
                Ydb::Type parameterType;
                try {
                    ConvertMiniKQLTypeToYdbType(queryParameter.GetType(), parameterType);
                } catch (const std::exception& ex) {
                    NYql::TIssues issues;
                    issues.AddIssue(NYql::ExceptionToIssue(ex));
                    return Reply(Ydb::StatusIds::INTERNAL_ERROR, issues, ctx);
                }
                queryResult.mutable_parameters_types()->insert({queryParameter.GetName(), parameterType});
            }

            AuditContextAppend(Request_.get(), *GetProtoRequest(), queryResult);

            ReplyWithResult(Ydb::StatusIds::SUCCESS, issueMessage, queryResult, ctx);
        } else {
            return OnGenericQueryResponseError(record, ctx);
        }
    }
};

void DoPrepareDataQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TPrepareDataQueryRPC(p.release()));
}

} // namespace NGRpcService
} // namespace NKikimr
