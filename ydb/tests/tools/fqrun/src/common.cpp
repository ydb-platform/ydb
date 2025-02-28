#include "common.h"

#include <util/system/env.h>

#include <ydb/library/aclib/aclib.h>

namespace NFqRun {

TExternalDatabase TExternalDatabase::Parse(const TString& optionValue, const TString& tokenVar) {
    TStringBuf database, endpoint;
    TStringBuf(optionValue).Split('@', database, endpoint);
    if (database.empty() || endpoint.empty()) {
        ythrow yexception() << "Incorrect external database mapping, expected form database@endpoint";
    }

    TExternalDatabase result = {
        .Endpoint = TString(endpoint),
        .Database = TString(database),
        .Token = GetEnv(tokenVar)
    };

    if (!result.Token) {
        result.Token = GetEnv(YQL_TOKEN_VARIABLE);
        if (!result.Token) {
            result.Token = BUILTIN_ACL_ROOT;
        }
    }

    return result;
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
