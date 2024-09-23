#pragma once

#include "common.h"

#include <ydb/core/protos/kqp.pb.h>

namespace NKqpRun {

class TKqpRunner {
public:
    explicit TKqpRunner(const TRunnerOptions& options);

    bool ExecuteSchemeQuery(const TString& query, const TString& traceId) const;

    bool ExecuteScript(const TString& script, NKikimrKqp::EQueryAction action, const TString& traceId) const;

    bool ExecuteQuery(const TString& query, NKikimrKqp::EQueryAction action, const TString& traceId) const;

    bool ExecuteYqlScript(const TString& query, NKikimrKqp::EQueryAction action, const TString& traceId) const;

    void ExecuteQueryAsync(const TString& query, NKikimrKqp::EQueryAction action, const TString& traceId) const;

    void WaitAsyncQueries() const;

    bool FetchScriptResults();

    bool ForgetExecutionOperation();

    void PrintScriptResults() const;

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

}  // namespace NKqpRun
