#pragma once

#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

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

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
