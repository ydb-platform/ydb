#pragma once

#include "common.h"


namespace NKqpRun {

class TKqpRunner {
public:
    explicit TKqpRunner(const TRunnerOptions& options);

    bool ExecuteSchemeQuery(const TString& query) const;

    bool ExecuteScript(const TString& script, NKikimrKqp::EQueryAction action, const TString& traceId) const;

    bool ExecuteQuery(const TString& query, NKikimrKqp::EQueryAction action, const TString& traceId) const;

    bool FetchScriptResults();

    void PrintScriptResults() const;

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

}  // namespace NKqpRun
