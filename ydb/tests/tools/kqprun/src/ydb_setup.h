#pragma once

#include "common.h"
#include "actors.h"

#include <ydb/public/sdk/cpp/client/ydb_query/query.h>


namespace NKqpRun {

struct TSchemeMeta {
    TString Ast;
};


struct TQueryMeta {
    TString Ast;
    TString Plan;
    TDuration TotalDuration;
};


struct TExecutionMeta : public TQueryMeta {
    bool Ready = false;
    NYdb::NQuery::EExecStatus ExecutionStatus = NYdb::NQuery::EExecStatus::Unspecified;

    i32 ResultSetsCount = 0;
};


struct TRequestResult {
    Ydb::StatusIds::StatusCode Status;
    NYql::TIssues Issues;

    TRequestResult();

    TRequestResult(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues);

    TRequestResult(Ydb::StatusIds::StatusCode status, const google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>& issues);

    bool IsSuccess() const;

    TString ToString() const;
};


class TYdbSetup {
public:
    explicit TYdbSetup(const TYdbSetupSettings& settings);

    TRequestResult SchemeQueryRequest(const TRequestOptions& query, TSchemeMeta& meta) const;

    TRequestResult ScriptRequest(const TRequestOptions& script, TString& operation) const;

    TRequestResult QueryRequest(const TRequestOptions& query, TQueryMeta& meta, std::vector<Ydb::ResultSet>& resultSets, TProgressCallback progressCallback) const;

    TRequestResult YqlScriptRequest(const TRequestOptions& query, TQueryMeta& meta, std::vector<Ydb::ResultSet>& resultSets) const;

    TRequestResult GetScriptExecutionOperationRequest(const TString& operation, TExecutionMeta& meta) const;

    TRequestResult FetchScriptExecutionResultsRequest(const TString& operation, i32 resultSetId, Ydb::ResultSet& resultSet) const;

    TRequestResult ForgetScriptExecutionOperationRequest(const TString& operation) const;

    void QueryRequestAsync(const TRequestOptions& query) const;

    void WaitAsyncQueries() const;

    void StartTraceOpt() const;

    static void StopTraceOpt();

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

}  // namespace NKqpRun
