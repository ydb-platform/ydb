#pragma once
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/coordination.h>
#include <stop_token>
namespace NYdb {
namespace NCoordination {
    struct TYdbLockException : public TYdbException {
        TYdbLockException(const std::string& message) : TYdbException(message) {}
    };
    class TDistributedMutex {
    public:
        TDistributedMutex(TClient& client, std::string_view path, std::string_view name, TDuration timeout);
        void lock();
        void unlock() noexcept;
        bool try_lock() noexcept;
        std::stop_token getStopToken() const;
    private:
        struct TImpl;
        std::unique_ptr<TImpl> impl_;
    };
}
}