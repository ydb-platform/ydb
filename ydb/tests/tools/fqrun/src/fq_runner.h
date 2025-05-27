#pragma once

#include "common.h"

#include <memory>

namespace NFqRun {

class TFqRunner {
public:
    explicit TFqRunner(const TRunnerOptions& options);

    bool ExecuteQuery(const TRequestOptions& query) const;

    bool FetchQueryResults() const;

    void PrintQueryResults() const;

    bool CreateConnections(const std::vector<FederatedQuery::ConnectionContent>& connections, const TFqOptions& options) const;

    bool CreateBindings(const std::vector<FederatedQuery::BindingContent>& bindings, const TFqOptions& options) const;

    void ExecuteQueryAsync(const TRequestOptions& query) const;

    void FinalizeRunner() const;

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl;
};

}  // namespace NFqRun
