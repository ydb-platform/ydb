#pragma once

#include "common.h"


namespace NKqpRun {

class TKqpRunner {
public:
    explicit TKqpRunner(const TRunnerOptions& options);

    bool ExecuteSchemeQuery(const TRequestOptions& query) const;

    bool ExecuteScript(const TRequestOptions& script, TDuration& duration) const;

    bool ExecuteQuery(const TRequestOptions& query, TDuration& duration) const;

    bool ExecuteYqlScript(const TRequestOptions& query, TDuration& duration) const;

    void ExecuteQueryAsync(const TRequestOptions& query) const;

    void FinalizeRunner() const;

    bool FetchScriptResults(const TString& userSID);

    bool ForgetExecutionOperation(const TString& userSID);

    void PrintScriptResults() const;

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

}  // namespace NKqpRun
