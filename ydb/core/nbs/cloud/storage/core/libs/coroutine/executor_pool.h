#pragma once

#include "executor.h"

#include <util/generic/vector.h>

#include <memory>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

using TExecutorPtr = std::shared_ptr<TExecutor>;

class TExecutorPool
{
public:
    explicit TExecutorPool(ui32 executorCount);

    ~TExecutorPool();

    // Stops every executor and joins its thread. A later call does nothing.
    void Stop();

    [[nodiscard]] TVector<TExecutorPtr> GetExecutors(ui32 count) const;

private:
    TVector<TExecutorPtr> Executors;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS
