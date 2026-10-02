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

    [[nodiscard]] TVector<TExecutorPtr> GetExecutors(ui32 count) const;

private:
    TVector<TExecutorPtr> Executors;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS
