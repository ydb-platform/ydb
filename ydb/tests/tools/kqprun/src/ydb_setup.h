#pragma once

#include "common.h"
#include "actors.h"

#include <ydb/tests/tools/kqprun/runlib/utils.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/query.h>


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

    TString Database = "";

    i32 ResultSetsCount = 0;
};


class TYdbSetup {
    using TRequestResult = NKikimrRun::TRequestResult;

public:
    explicit TYdbSetup(const TYdbSetupSettings& settings);

    TRequestResult SchemeQueryRequest(const TRequestOptions& query, TSchemeMeta& meta) const;

    TRequestResult ScriptRequest(const TRequestOptions& script, TString& operation) const;

    TRequestResult QueryRequest(const TRequestOptions& query, TQueryMeta& meta, std::vector<Ydb::ResultSet>& resultSets, TProgressCallback progressCallback) const;

    TRequestResult YqlScriptRequest(const TRequestOptions& query, TQueryMeta& meta, std::vector<Ydb::ResultSet>& resultSets) const;

    TRequestResult GetScriptExecutionOperationRequest(const TString& database, const TString& operation, TExecutionMeta& meta) const;

    TRequestResult FetchScriptExecutionResultsRequest(const TString& database, const TString& operation, i32 resultSetId, Ydb::ResultSet& resultSet) const;

    TRequestResult ForgetScriptExecutionOperationRequest(const TString& database, const TString& operation) const;

    TRequestResult CancelScriptExecutionOperationRequest(const TString& database, const TString& operation) const;

    void QueryRequestAsync(const TRequestOptions& query) const;

    void WaitAsyncQueries() const;

    void CloseSessions() const;

    void StartTraceOpt() const;

    static void StopTraceOpt();

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

}  // namespace NKqpRun
