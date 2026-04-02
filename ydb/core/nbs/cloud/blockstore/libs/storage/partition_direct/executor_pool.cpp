#include "executor_pool.h"

#include <util/random/random.h>
#include <util/string/builder.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TExecutorPool::TExecutorPool(ui32 executorCount)
{
    for (ui32 i = 0; i < executorCount; i++) {
        Executors.emplace_back(
            TExecutor::Create(TStringBuilder() << "NBS_" << i));
        Executors.back()->Start();
    }
}

TExecutorPool::~TExecutorPool()
{
    for (const auto& executor: Executors) {
        executor->Stop();
    }
}

////////////////////////////////////////////////////////////////////////////////

TVector<TExecutorPtr> TExecutorPool::GetExecutors(ui32 count) const
{
    const ui32 startIndex = RandomNumber<ui32>(Executors.size());
    TVector<TExecutorPtr> executors;
    for (ui32 i = 0; i < count; i++) {
        executors.push_back(Executors[(startIndex + i) % Executors.size()]);
    }

    return executors;
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
