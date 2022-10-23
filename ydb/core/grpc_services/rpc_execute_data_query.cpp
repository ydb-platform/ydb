#include "service_table.h"
#include <ydb/core/grpc_services/base/base.h>
#include "rpc_kqp_base.h"
#include "rpc_common.h"
#include "service_table.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/public/api/protos/ydb_scheme.pb.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/protos/console_config.pb.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/public/lib/operation_id/operation_id.h>

#include <ydb/library/yql/public/issue/yql_issue.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace NOperationId;
using namespace Ydb;
using namespace Ydb::Table;
using namespace NKqp;

using TEvExecuteDataQueryRequest = TGrpcRequestOperationCall<Ydb::Table::ExecuteDataQueryRequest,
    Ydb::Table::ExecuteDataQueryResponse>;

class TExecuteDataQueryRPC : public TRpcKqpRequestActor<TExecuteDataQueryRPC, TEvExecuteDataQueryRequest> {
    using TBase = TRpcKqpRequestActor<TExecuteDataQueryRPC, TEvExecuteDataQueryRequest>;

public:
    using TResult = Ydb::Table::ExecuteQueryResult;

    TExecuteDataQueryRPC(IRequestOpCtx* msg)
        : TBase(msg) {}

    void Bootstrap(const TActorContext &ctx) {
        TBase::Bootstrap(ctx);

        this->Become(&TExecuteDataQueryRPC::StateWork);
        Proceed(ctx);
    }

    void StateWork(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NKqp::TEvKqp::TEvQueryResponse, Handle);
            default: TBase::StateWork(ev, ctx);
        }
    }

    void Proceed(const TActorContext& ctx) {
        const auto req = GetProtoRequest();
        const auto traceId = Request_->GetTraceId();
        const auto requestType = Request_->GetRequestType();

        auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        SetAuthToken(ev, *Request_);
        SetDatabase(ev, *Request_);

        NYql::TIssues issues;
        if (CheckSession(req->session_id(), issues)) {
            ev->Record.MutableRequest()->SetSessionId(req->session_id());
        } else {
            return Reply(Ydb::StatusIds::BAD_REQUEST, issues, ctx);
        }

        if (traceId) {
            ev->Record.SetTraceId(traceId.GetRef());
        }

        if (requestType) {
            ev->Record.SetRequestType(requestType.GetRef());
        }

        ev->Record.MutableRequest()->SetCancelAfterMs(GetCancelAfter().MilliSeconds());
        ev->Record.MutableRequest()->SetTimeoutMs(GetOperationTimeout().MilliSeconds());

        if (req->parametersSize() != 0) {
            try {
                NKikimrMiniKQL::TParams params;
                ConvertYdbParamsToMiniKQLParams(req->parameters(), params);
                ev->Record.MutableRequest()->MutableParameters()->CopyFrom(params);
            } catch (const std::exception& ex) {
                auto issue = MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, "Failed to parse query parameters.");
                issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(NYql::ExceptionToIssue(ex)));

                NYql::TIssues issues;
                issues.AddIssue(issue);

                return Reply(Ydb::StatusIds::BAD_REQUEST, issues, ctx);
            }
        }

        if (!req->has_tx_control()) {
            NYql::TIssues issues;
            issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, "Empty tx_control."));
            return Reply(Ydb::StatusIds::BAD_REQUEST, issues, ctx);
        }

        if (req->tx_control().has_begin_tx() && !req->tx_control().commit_tx()) {
            switch (req->tx_control().begin_tx().tx_mode_case()) {
                case Table::TransactionSettings::kOnlineReadOnly:
                case Table::TransactionSettings::kStaleReadOnly: {
                    NYql::TIssues issues;
                    issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, TStringBuilder()
                        << "Failed to execute query: open transactions not supported for transaction mode: "
                        << GetTransactionModeName(req->tx_control().begin_tx())
                        << ", use commit_tx flag to explicitely commit transaction."));
                    return Reply(Ydb::StatusIds::BAD_REQUEST, issues, ctx);
                }
                default:
                    break;
            }
        }

        ev->Record.MutableRequest()->MutableTxControl()->CopyFrom(req->tx_control());
        ev->Record.MutableRequest()->MutableQueryCachePolicy()->CopyFrom(req->query_cache_policy());
        ev->Record.MutableRequest()->SetStatsMode(GetKqpStatsMode(req->collect_stats()));
        ev->Record.MutableRequest()->SetCollectStats(req->collect_stats());

        auto& query = req->query();
        switch (req->query().query_case()) {
            case Query::kYqlText: {
                NYql::TIssues issues;
                if (!CheckQuery(query.yql_text(), issues)) {
                    return Reply(Ydb::StatusIds::BAD_REQUEST, issues, ctx);
                }

                ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
                ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
                ev->Record.MutableRequest()->SetQuery(query.yql_text());
                break;
            }

            case Query::kId: {
                if (query.id().empty()) {
                    NYql::TIssues issues;
                    issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, "Empty query id"));
                    return Reply(Ydb::StatusIds::BAD_REQUEST, issues, ctx);
                }

                ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED);
                ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_PREPARED_DML);

                TString preparedQueryId;
                try {
                    preparedQueryId = DecodePreparedQueryId(query.id());
                } catch (const std::exception& ex) {
                    NYql::TIssues issues;
                    issues.AddIssue(NYql::ExceptionToIssue(ex));
                    return Reply(Ydb::StatusIds::BAD_REQUEST, issues, ctx);
                }

                ev->Record.MutableRequest()->SetPreparedQuery(preparedQueryId);
                break;
            }

            default: {
                NYql::TIssues issues;
                issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, "Unexpected query option"));
                return Reply(Ydb::StatusIds::BAD_REQUEST, issues, ctx);
            }
        }

        ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());
    }

    static void ConvertReadStats(const NKikimrQueryStats::TReadOpStats& from, Ydb::TableStats::OperationStats* to) {
        to->set_rows(to->rows() + from.GetRows());
        to->set_bytes(to->bytes() + from.GetBytes());
    }

    static void ConvertWriteStats(const NKikimrQueryStats::TWriteOpStats& from, Ydb::TableStats::OperationStats* to) {
        to->set_rows(from.GetCount());
        to->set_bytes(from.GetBytes());
    }

    static void ConvertQueryStats(const NKikimrKqp::TQueryResponse& from, Ydb::Table::ExecuteQueryResult* to) {
        if (from.HasQueryStats()) {
            FillQueryStats(*to->mutable_query_stats(), from);
            to->mutable_query_stats()->set_query_ast(from.GetQueryAst());
            return;
        }

        // TODO: For compatibility with old kqp workers, deprecate.
        if (from.GetProfile().KqlProfilesSize() == 1) {
            const auto& kqlProlfile = from.GetProfile().GetKqlProfiles(0);
            const auto& phases = kqlProlfile.GetMkqlProfiles();
            for (const auto& s : phases) {
                if (s.HasTxStats()) {
                    const auto& tableStats = s.GetTxStats().GetTableAccessStats();
                    auto* phase = to->mutable_query_stats()->add_query_phases();
                    phase->set_duration_us(s.GetTxStats().GetDurationUs());
                    for (const auto& ts : tableStats) {
                        auto* tableAccess = phase->add_table_access();
                        tableAccess->set_name(ts.GetTableInfo().GetName());
                        if (ts.HasSelectRow()) {
                            ConvertReadStats(ts.GetSelectRow(), tableAccess->mutable_reads());
                        }
                        if (ts.HasSelectRange()) {
                            ConvertReadStats(ts.GetSelectRange(), tableAccess->mutable_reads());
                        }
                        if (ts.HasUpdateRow()) {
                            ConvertWriteStats(ts.GetUpdateRow(), tableAccess->mutable_updates());
                        }
                        if (ts.HasEraseRow()) {
                            ConvertWriteStats(ts.GetEraseRow(), tableAccess->mutable_deletes());
                        }
                    }
                }
            }
        }
    }

    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record.GetRef();
        SetCost(record.GetConsumedRu());
        AddServerHintsIfAny(record);

        if (record.GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
            const auto& kqpResponse = record.GetResponse();
            const auto& issueMessage = kqpResponse.GetQueryIssues();

            auto queryResult = TEvExecuteDataQueryRequest::AllocateResult<Ydb::Table::ExecuteQueryResult>(Request_);

            try {
                ConvertKqpQueryResultsToDbResult(kqpResponse, queryResult);
                ConvertQueryStats(kqpResponse, queryResult);
                if (kqpResponse.HasTxMeta()) {
                    queryResult->mutable_tx_meta()->CopyFrom(kqpResponse.GetTxMeta());
                }
                if (!kqpResponse.GetPreparedQuery().empty()) {
                    auto& queryMeta = *queryResult->mutable_query_meta();
                    Ydb::TOperationId opId;
                    opId.SetKind(TOperationId::PREPARED_QUERY_ID);
                    AddOptionalValue(opId, "id", kqpResponse.GetPreparedQuery());
                    queryMeta.set_id(ProtoToString(opId));

                    const auto& queryParameters = kqpResponse.GetQueryParameters();
                    for (const auto& queryParameter: queryParameters) {
                        Ydb::Type parameterType;
                        ConvertMiniKQLTypeToYdbType(queryParameter.GetType(), parameterType);
                        queryMeta.mutable_parameters_types()->insert({queryParameter.GetName(), parameterType});
                    }
                }
            } catch (const std::exception& ex) {
                NYql::TIssues issues;
                issues.AddIssue(NYql::ExceptionToIssue(ex));
                return Reply(Ydb::StatusIds::INTERNAL_ERROR, issues, ctx);
            }

            ReplyWithResult(Ydb::StatusIds::SUCCESS, issueMessage, *queryResult, ctx);
        } else {
            return OnQueryResponseErrorWithTxMeta(record, ctx);
        }
    }
};

void DoExecuteDataQueryRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider &) {
    TActivationContext::AsActorContext().Register(new TExecuteDataQueryRPC(p.release()));
}

template<>
IActor* TEvExecuteDataQueryRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
    return new TExecuteDataQueryRPC(msg);
}

} // namespace NGRpcService
} // namespace NKikimr
