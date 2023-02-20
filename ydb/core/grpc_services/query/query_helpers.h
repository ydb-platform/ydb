#pragma once
#include <ydb/core/base/kikimr_issue.h>
#include <ydb/core/grpc_services/rpc_kqp_base.h>
#include <ydb/public/api/protos/draft/ydb_query.pb.h>

#include <utility>

namespace NKikimr::NGRpcService {

namespace NQueryHelpersPrivate {

inline bool FillQueryContent(const Ydb::Query::ExecuteQueryRequest& req, NKikimrKqp::TEvQueryRequest& kqpRequest, NYql::TIssues& issues) {
    switch (req.query_case()) {
        case Ydb::Query::ExecuteQueryRequest::kQueryContent:
            if (!CheckQuery(req.query_content().text(), issues)) {
                return false;
            }

            kqpRequest.MutableRequest()->SetQuery(req.query_content().text());
            return true;

        default:
            issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, "Unexpected query option"));
            return false;
    }
}

inline bool FillQueryContent(const Ydb::Query::ExecuteScriptRequest& req, NKikimrKqp::TEvQueryRequest& kqpRequest, NYql::TIssues& issues) {
    switch (req.script_case()) {
        case Ydb::Query::ExecuteScriptRequest::kScriptContent:
            if (!CheckQuery(req.script_content().text(), issues)) {
                return false;
            }

            kqpRequest.MutableRequest()->SetQuery(req.script_content().text());
            return true;

        case Ydb::Query::ExecuteScriptRequest::kScriptId:
            issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, "Execution by script id is not supported yet"));
            return false;

        default:
            issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, "Unexpected query option"));
            return false;
    }
}

inline NKikimrKqp::EQueryType GetQueryType(const Ydb::Query::ExecuteQueryRequest&) {
    return NKikimrKqp::QUERY_TYPE_SQL_QUERY;
}

inline NKikimrKqp::EQueryType GetQueryType(const Ydb::Query::ExecuteScriptRequest&) {
    return NKikimrKqp::QUERY_TYPE_SQL_QUERY; // TODO: make new query type
}

} // namespace NQueryHelpersPrivate

template <class TRequestProto>
std::tuple<Ydb::StatusIds::StatusCode, NYql::TIssues> FillKqpRequest(
    const TRequestProto& req, NKikimrKqp::TEvQueryRequest& kqpRequest)
{
    kqpRequest.MutableRequest()->MutableYdbParameters()->insert(req.parameters().begin(), req.parameters().end());
    switch (req.exec_mode()) {
        case Ydb::Query::EXEC_MODE_EXECUTE:
            kqpRequest.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
            break;
        default: {
            NYql::TIssues issues;
            issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, "Unexpected query mode"));
            return {Ydb::StatusIds::BAD_REQUEST, issues};
        }
    }

    kqpRequest.MutableRequest()->SetType(NQueryHelpersPrivate::GetQueryType(req));
    kqpRequest.MutableRequest()->SetKeepSession(false);

    // TODO: Use tx control from request.
    kqpRequest.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
    kqpRequest.MutableRequest()->MutableTxControl()->set_commit_tx(true);

    NYql::TIssues issues;
    if (!NQueryHelpersPrivate::FillQueryContent(req, kqpRequest, issues)) {
        return {Ydb::StatusIds::BAD_REQUEST, issues};
    }

    return {Ydb::StatusIds::SUCCESS, {}};
}

} // namespace NKikimr::NGRpcService
