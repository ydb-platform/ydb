#pragma once

#include "common.h"

#include <memory>

namespace NFqRun {

class TFqRunner {
public:
    explicit TFqRunner(const TRunnerOptions& options);

    bool ExecuteStreamQuery(const TRequestOptions& query) const;

    bool FetchQueryResults() const;

    void PrintQueryResults() const;

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl;
};

}  // namespace NFqRun
