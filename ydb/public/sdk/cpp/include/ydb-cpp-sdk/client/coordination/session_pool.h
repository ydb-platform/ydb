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
    // Throws TYdbException if a popped slot cannot be replaced after retries
    // (the slot is restored to the idle pool before throwing).
    std::optional<TSession> GetAny(TSessionLostCallback onLost = {});
    void Return(TSession session);
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
