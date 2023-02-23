#include "kqp_compute_actor.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/tx/datashard/range_ops.h>
#include <ydb/library/yql/core/issue/yql_issue.h>

namespace NKikimr::NKqp::NComputeActor {

bool FindSchemeErrorInIssues(const Ydb::StatusIds::StatusCode& status, const NYql::TIssues& issues) {
    bool schemeError = false;
    if (status == Ydb::StatusIds::SCHEME_ERROR) {
        schemeError = true;
    } else if (status == Ydb::StatusIds::ABORTED) {
        for (auto& issue : issues) {
            WalkThroughIssues(issue, false, [&schemeError](const NYql::TIssue& x, ui16) {
                if (x.IssueCode == NYql::TIssuesIds::KIKIMR_SCHEME_MISMATCH) {
                    schemeError = true;
                }
            });
            if (schemeError) {
                break;
            }
        }
    }
    return schemeError;
}

} // namespace NKikimr::NKqp::NComputeActor
