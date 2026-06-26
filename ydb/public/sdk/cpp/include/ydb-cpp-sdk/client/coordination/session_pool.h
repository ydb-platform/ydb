#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/distributed_lock.h>

#include <memory>

namespace NYdb {
namespace NCoordination {

class TCoordinationSessionPool {
public:
    TCoordinationSessionPool();
    ~TCoordinationSessionPool();

    TDistributedLock CreateDistributedLock(const TDistributedLockSettings& settings);

private:
    friend class TClient;
    struct TImpl;

    explicit TCoordinationSessionPool(std::shared_ptr<TImpl> impl);

private:
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NCoordination
} // namespace NYdb
