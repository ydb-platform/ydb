#include "common.h"

#include <util/system/env.h>

namespace NFqRun {

TExternalDatabase TExternalDatabase::Parse(const TString& optionValue) {
    TStringBuf database, endpoint;
    TStringBuf(optionValue).Split('@', database, endpoint);
    if (database.empty() || endpoint.empty()) {
        ythrow yexception() << "Incorrect external database mapping, expected form database@endpoint";
    }

    if (!GetEnv(YQL_TOKEN_VARIABLE)) {
        ythrow yexception() << "Cannot use external databases without token, please specify environment variable " << YQL_TOKEN_VARIABLE;
    }

    return {
        .Endpoint = TString(endpoint),
        .Database = TString(database)
    };
}

void SetupAcl(FederatedQuery::Acl* acl) {
    if (acl->visibility() == FederatedQuery::Acl::VISIBILITY_UNSPECIFIED) {
        acl->set_visibility(FederatedQuery::Acl::SCOPE);
    }
}

NYql::TIssue GroupIssues(NYql::TIssue rootIssue, const NYql::TIssues& childrenIssues) {
    for (const auto& issue : childrenIssues) {
        rootIssue.AddSubIssue(MakeIntrusive<NYql::TIssue>(issue));
    }
    return rootIssue;
}

bool IsFinalStatus(FederatedQuery::QueryMeta::ComputeStatus status) {
    using EStatus = FederatedQuery::QueryMeta;
    return IsIn({EStatus::FAILED, EStatus::COMPLETED, EStatus::ABORTED_BY_USER, EStatus::ABORTED_BY_SYSTEM}, status);
}

}  // namespace NFqRun
