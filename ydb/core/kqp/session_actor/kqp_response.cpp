#include "kqp_worker_common.h"

#include <ydb/core/ydb_convert/ydb_convert.h>

#include <ydb/library/yql/public/issue/yql_issue_message.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;

namespace {

void CollectYdbStatuses(const TIssue& issue, TSet<Ydb::StatusIds::StatusCode>& statuses) {
    if (issue.GetSeverity() == TSeverityIds::S_WARNING) {
        return;
    }

    if (auto status = GetYdbStatus(issue)) {
        statuses.insert(*status);
        return;
    }

    const auto& subIssues = issue.GetSubIssues();
    if (subIssues.empty()) {
        statuses.insert(Ydb::StatusIds::GENERIC_ERROR);
    }

    for (auto& subIssue : subIssues) {
        CollectYdbStatuses(*subIssue, statuses);
    }
}

bool HasSchemeOrFatalIssues(const TIssue& issue) {
    if (issue.GetSeverity() == TSeverityIds::S_FATAL) {
        return true;
    }

    switch (issue.GetCode()) {
        case TIssuesIds::KIKIMR_SCHEME_MISMATCH:
        case TIssuesIds::KIKIMR_SCHEME_ERROR:
            return true;
    }

    for (auto& subIssue : issue.GetSubIssues()) {
        if (HasSchemeOrFatalIssues(*subIssue)) {
            return true;
        }
    }

    return false;
}

} // namespace

TMaybe<Ydb::StatusIds::StatusCode> GetYdbStatus(const TIssue& issue) {
    if (issue.GetSeverity() == TSeverityIds::S_FATAL) {
        return Ydb::StatusIds::INTERNAL_ERROR;
    }

    switch (issue.GetCode()) {
        case TIssuesIds::KIKIMR_LOCKS_INVALIDATED:
        case TIssuesIds::KIKIMR_LOCKS_ACQUIRE_FAILURE:
        case TIssuesIds::KIKIMR_OPERATION_ABORTED:
        case TIssuesIds::KIKIMR_SCHEME_MISMATCH:
            return Ydb::StatusIds::ABORTED;

        case TIssuesIds::KIKIMR_SCHEME_ERROR:
            return Ydb::StatusIds::SCHEME_ERROR;

        case TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE:
            return Ydb::StatusIds::UNAVAILABLE;

        case TIssuesIds::KIKIMR_OVERLOADED:
        case TIssuesIds::KIKIMR_MULTIPLE_SCHEME_MODIFICATIONS:
            return Ydb::StatusIds::OVERLOADED;

        case TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION:
            return Ydb::StatusIds::PRECONDITION_FAILED;

        case TIssuesIds::KIKIMR_TRANSACTION_NOT_FOUND:
            return Ydb::StatusIds::NOT_FOUND;

        case TIssuesIds::KIKIMR_BAD_REQUEST:
        case TIssuesIds::KIKIMR_BAD_COLUMN_TYPE:
        case TIssuesIds::KIKIMR_NO_COLUMN_DEFAULT_VALUE:
            return Ydb::StatusIds::BAD_REQUEST;

        case TIssuesIds::CORE_GC_STRINGS_LIMIT_EXCEEDED:
        case TIssuesIds::KIKIMR_TOO_MANY_TRANSACTIONS:
            return Ydb::StatusIds::BAD_SESSION;

        case TIssuesIds::KIKIMR_ACCESS_DENIED:
            return Ydb::StatusIds::UNAUTHORIZED;

        case TIssuesIds::KIKIMR_TIMEOUT:
            return Ydb::StatusIds::TIMEOUT;

        case TIssuesIds::KIKIMR_OPERATION_CANCELLED:
            return Ydb::StatusIds::CANCELLED;

        case TIssuesIds::KIKIMR_RESULT_UNAVAILABLE:
        case TIssuesIds::KIKIMR_OPERATION_STATE_UNKNOWN:
            return Ydb::StatusIds::UNDETERMINED;

        case TIssuesIds::KIKIMR_PRECONDITION_FAILED:
            return Ydb::StatusIds::PRECONDITION_FAILED;

        case TIssuesIds::KIKIMR_UNSUPPORTED:
            return Ydb::StatusIds::UNSUPPORTED;

        default:
            break;
    }

    return TMaybe<Ydb::StatusIds::StatusCode>();
}

Ydb::StatusIds::StatusCode GetYdbStatus(const NYql::NCommon::TOperationResult& queryResult) {
    if (queryResult.Success()) {
        return Ydb::StatusIds::SUCCESS;
    }
    return GetYdbStatus(queryResult.Issues());
}

Ydb::StatusIds::StatusCode GetYdbStatus(const TIssues& issues) {
    TSet<Ydb::StatusIds::StatusCode> statuses;
    for (const auto& topIssue : issues) {
        CollectYdbStatuses(topIssue, statuses);
    }

    if (statuses.contains(Ydb::StatusIds::UNAUTHORIZED)) {
        return Ydb::StatusIds::UNAUTHORIZED;
    }

    if (statuses.contains(Ydb::StatusIds::INTERNAL_ERROR)) {
        return Ydb::StatusIds::INTERNAL_ERROR;
    }

    if (statuses.contains(Ydb::StatusIds::GENERIC_ERROR)) {
        return Ydb::StatusIds::GENERIC_ERROR;
    }

    if (statuses.size() != 1) {
        return Ydb::StatusIds::GENERIC_ERROR;
    }

    return *statuses.begin();
}

void AddQueryIssues(NKikimrKqp::TQueryResponse& response, const TIssues& issues) {
    for (auto& issue : issues) {
        IssueToMessage(issue, response.AddQueryIssues());
    }
}

bool HasSchemeOrFatalIssues(const TIssues& issues) {
    for (auto& issue : issues) {
        if (HasSchemeOrFatalIssues(issue)) {
            return true;
        }
    }

    return false;
}

} // namespace NKqp
} // namespace NKikimr
