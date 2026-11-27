#pragma once

#include <util/system/types.h>
#include <functional>
#include <memory>

namespace NKikimr::NMiniKQL::NAsync {

class TMemoryReservation;
// Shared quota, usable by task/node adapters. Waiters are keyed by execution
// generation. Release can run on a callback thread: wakeups only enqueue work.
class TMemoryQuota: public std::enable_shared_from_this<TMemoryQuota> {
public:
    explicit TMemoryQuota(ui64 limit);
    ~TMemoryQuota();
    std::shared_ptr<TMemoryReservation> TryReserve(ui64 bytes, ui64 waiter = 0, std::function<void()> wakeup = {});
    bool CanReserve(ui64 bytes, ui64 waiter, std::function<void()> wakeup);
    void RemoveWaiter(ui64 waiter);
    ui64 GetUsed() const;
    ui64 GetLimit() const;

private:
    friend class TMemoryReservation;
    bool Resize(ui64 oldBytes, ui64 newBytes);
    struct TImpl;
    std::unique_ptr<TImpl> Impl_;
};

// Sharing a reservation transfers ownership without charging the same buffer
// again. In particular, ready results and physical cleanup retain this lease.
class TMemoryReservation {
public:
    ~TMemoryReservation();
    bool Resize(ui64 bytes); // owner only, never concurrently with destruction
    ui64 GetBytes() const {
        return Bytes_;
    }

private:
    friend class TMemoryQuota;
    TMemoryReservation(std::shared_ptr<TMemoryQuota> quota, ui64 bytes);
    const std::shared_ptr<TMemoryQuota> Quota_;
    ui64 Bytes_;
};

} // namespace NKikimr::NMiniKQL::NAsync
