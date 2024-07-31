#pragma once

#include "common.h"


namespace NKqpRun {

class TKqpRunner {
public:
    explicit TKqpRunner(const TRunnerOptions& options);

    bool ExecuteSchemeQuery(const TRequestOptions& query) const;

    bool ExecuteScript(const TRequestOptions& script) const;

    bool ExecuteQuery(const TRequestOptions& query) const;

    bool ExecuteYqlScript(const TRequestOptions& query) const;

    void ExecuteQueryAsync(const TRequestOptions& query) const;

    void WaitAsyncQueries() const;

    bool FetchScriptResults();

    bool ForgetExecutionOperation();

    void PrintScriptResults() const;

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

}  // namespace NKqpRun
