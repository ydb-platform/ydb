#pragma once

#include "common.h"
#include "actors.h"

#include <ydb/core/protos/kqp.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_query/query.h>


namespace NKqpRun {

struct TSchemeMeta {
    TString Ast;
};


struct TExecutionMeta {
    bool Ready = false;
    NYdb::NQuery::EExecStatus ExecutionStatus = NYdb::NQuery::EExecStatus::Unspecified;

    i32 ResultSetsCount = 0;

    TString Ast;
    TString Plan;
};


struct TQueryMeta {
    TString Ast;
    TString Plan;
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

    TRequestResult SchemeQueryRequest(const TString& query, const TString& traceId, TSchemeMeta& meta) const;

    TRequestResult ScriptRequest(const TString& script, NKikimrKqp::EQueryAction action, const TString& traceId, TString& operation) const;

    TRequestResult QueryRequest(const TString& query, NKikimrKqp::EQueryAction action, const TString& traceId, TQueryMeta& meta, std::vector<Ydb::ResultSet>& resultSets, TProgressCallback progressCallback) const;

    TRequestResult YqlScriptRequest(const TString& query, NKikimrKqp::EQueryAction action, const TString& traceId, TQueryMeta& meta, std::vector<Ydb::ResultSet>& resultSets) const;

    TRequestResult GetScriptExecutionOperationRequest(const TString& operation, TExecutionMeta& meta) const;

    TRequestResult FetchScriptExecutionResultsRequest(const TString& operation, i32 resultSetId, Ydb::ResultSet& resultSet) const;

    TRequestResult ForgetScriptExecutionOperationRequest(const TString& operation) const;

    void StartTraceOpt() const;

    static void StopTraceOpt();

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

}  // namespace NKqpRun
