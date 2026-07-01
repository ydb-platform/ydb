#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/distributed_lock.h>

#include <functional>
#include <memory>
#include <optional>

namespace NYdb {
namespace NCoordination {

class TCoordinationSessionPool {
public:
    using TSessionLostCallback = std::function<void()>;

    TCoordinationSessionPool();
    ~TCoordinationSessionPool();

    // Returns nullopt when no idle session is available.
    // Throws TYdbException if a popped idle session cannot be refreshed after retries.
    std::optional<TSession> GetAny(TSessionLostCallback onLost = {});
    // Throws TYdbException if a lost returned session cannot be replaced after retries.
    void Return(TSession session);
    // Returns false if the session is not checked out by this pool.
    // Throws TYdbException if replacement cannot be started after retries.
    bool Replace(TSession session);
    size_t Size() const;

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
