#include "resources.h"
#include <util/generic/yexception.h>
#include <map>
#include <algorithm>
#include <mutex>

namespace NKikimr::NMiniKQL::NAsync {
struct TMemoryQuota::TImpl {
    const ui64 Limit;
    ui64 Used = 0;
    mutable std::mutex Mutex;
    struct TWaiter {
        ui64 Bytes = 0;
        std::function<void()> Wakeup;
    };
    std::map<ui64, TWaiter> Waiters;
    explicit TImpl(ui64 limit)
        : Limit(limit)
    {
    }
};
TMemoryQuota::TMemoryQuota(ui64 limit)
    : Impl_(std::make_unique<TImpl>(limit))
{
}
TMemoryQuota::~TMemoryQuota() = default;
std::shared_ptr<TMemoryReservation> TMemoryQuota::TryReserve(ui64 bytes, ui64 waiter, std::function<void()> wakeup) {
    auto lease = std::shared_ptr<TMemoryReservation>(new TMemoryReservation(shared_from_this(), 0));
    std::function<void()> removed;
    {
        std::lock_guard lock(Impl_->Mutex);
        if (bytes > Impl_->Limit - Impl_->Used) {
            if (wakeup) {
                auto& slot = Impl_->Waiters[waiter];
                slot.Bytes = slot.Wakeup ? std::min(slot.Bytes, bytes) : bytes;
                removed.swap(slot.Wakeup);
                slot.Wakeup = std::move(wakeup);
            }
        } else {
            Impl_->Used += bytes;
            lease->Bytes_ = bytes;
            return lease;
        }
    }
    return {};
}
bool TMemoryQuota::CanReserve(ui64 bytes, ui64 waiter, std::function<void()> wakeup) {
    std::function<void()> removed;
    {
        std::lock_guard lock(Impl_->Mutex);
        if (bytes <= Impl_->Limit - Impl_->Used) {
            return true;
        }
        if (wakeup) {
            auto& slot = Impl_->Waiters[waiter];
            slot.Bytes = slot.Wakeup ? std::min(slot.Bytes, bytes) : bytes;
            removed.swap(slot.Wakeup);
            slot.Wakeup = std::move(wakeup);
        }
    }
    return false;
}
void TMemoryQuota::RemoveWaiter(ui64 waiter) {
    std::function<void()> removed;
    {
        std::lock_guard lock(Impl_->Mutex);
        auto it = Impl_->Waiters.find(waiter);
        if (it != Impl_->Waiters.end()) {
            removed = std::move(it->second.Wakeup);
            Impl_->Waiters.erase(it);
        }
    }
}
bool TMemoryQuota::Resize(ui64 oldBytes, ui64 newBytes) {
    std::map<ui64, TImpl::TWaiter> wakeups;
    {
        std::lock_guard lock(Impl_->Mutex);
        Y_ENSURE(oldBytes <= Impl_->Used);
        if (newBytes > Impl_->Limit - (Impl_->Used - oldBytes)) {
            return false;
        }
        Impl_->Used = Impl_->Used - oldBytes + newBytes;
        if (newBytes < oldBytes) {
            // A rejected submission can acquire then roll back a temporary lease.
            // Wake only generations whose smallest blocked reservation now fits.
            for (auto it = Impl_->Waiters.begin(); it != Impl_->Waiters.end();) {
                if (it->second.Bytes <= Impl_->Limit - Impl_->Used) {
                    wakeups.insert(Impl_->Waiters.extract(it++));
                } else {
                    ++it;
                }
            }
        }
    }
    for (auto& [id, wakeup] : wakeups) {
        wakeup.Wakeup();
    }
    return true;
}
ui64 TMemoryQuota::GetUsed() const {
    std::lock_guard lock(Impl_->Mutex);
    return Impl_->Used;
}
ui64 TMemoryQuota::GetLimit() const {
    return Impl_->Limit;
}
TMemoryReservation::TMemoryReservation(std::shared_ptr<TMemoryQuota> quota, ui64 bytes)
    : Quota_(std::move(quota))
    , Bytes_(bytes)
{
}
TMemoryReservation::~TMemoryReservation() {
    Quota_->Resize(Bytes_, 0);
}
bool TMemoryReservation::Resize(ui64 bytes) {
    if (!Quota_->Resize(Bytes_, bytes)) {
        return false;
    }
    Bytes_ = bytes;
    return true;
}
} // namespace NKikimr::NMiniKQL::NAsync
