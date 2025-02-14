#pragma once

#include "common.h"

#include <ydb/tests/tools/kqprun/runlib/utils.h>

namespace NFqRun {

struct TExecutionMeta {
    FederatedQuery::QueryMeta::ComputeStatus Status;
    NYql::TIssues Issues;
    NYql::TIssues TransientIssues;
    std::vector<i64> ResultSetSizes;
};

class TFqSetup {
    using TRequestResult = NKikimrRun::TRequestResult;

public:
    explicit TFqSetup(const TFqSetupSettings& settings);

    TRequestResult StreamRequest(const TRequestOptions& query, TString& queryId) const;

    TRequestResult DescribeQuery(const TString& queryId, TExecutionMeta& meta) const;

    TRequestResult FetchQueryResults(const TString& queryId, i32 resultSetId, Ydb::ResultSet& resultSet) const;

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl;
};

}  // namespace NFqRun
