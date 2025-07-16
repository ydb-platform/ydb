#pragma once

#include "common.h"

#include <ydb/tests/tools/kqprun/runlib/utils.h>

namespace NFqRun {

struct TExecutionMeta {
    TString Ast;
    TString Plan;
    TString Statistics;

    FederatedQuery::QueryMeta::ComputeStatus Status;
    NYql::TIssues Issues;
    NYql::TIssues TransientIssues;
    std::vector<i64> ResultSetSizes;
};

class TFqSetup {
    using TRequestResult = NKikimrRun::TRequestResult;

public:
    explicit TFqSetup(const TFqSetupSettings& settings);

    TRequestResult QueryRequest(const TRequestOptions& query, TString& queryId) const;

    TRequestResult DescribeQuery(const TString& queryId, const TFqOptions& options, TExecutionMeta& meta) const;

    TRequestResult FetchQueryResults(const TString& queryId, i32 resultSetId, const TFqOptions& options, Ydb::ResultSet& resultSet) const;

    TRequestResult CreateConnection(const FederatedQuery::ConnectionContent& connection, const TFqOptions& options, TString& connectionId) const;

    TRequestResult CreateBinding(const FederatedQuery::BindingContent& binding, const TFqOptions& options) const;

    void QueryRequestAsync(const TRequestOptions& query, TDuration pingPeriod) const;

    void WaitAsyncQueries() const;

    void StartTraceOpt() const;

    static void StopTraceOpt();

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl;
};

}  // namespace NFqRun
