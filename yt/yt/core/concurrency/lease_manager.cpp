#include "lease_manager.h"
#include "delayed_executor.h"
#include "thread_affinity.h"

#include <yt/yt/core/actions/bind.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

const TLease NullLease;

////////////////////////////////////////////////////////////////////////////////

struct TLeaseEntry
    : public TRefCounted
{
    bool IsValid = true;
    TDuration Timeout;
    TClosure OnExpired;
    NConcurrency::TDelayedExecutorCookie Cookie;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock);

    TLeaseEntry(TDuration timeout, TClosure onExpired)
        : Timeout(timeout)
        , OnExpired(std::move(onExpired))
    { }
};

DEFINE_REFCOUNTED_TYPE(TLeaseEntry)

////////////////////////////////////////////////////////////////////////////////

class TLeaseManager::TImpl
    : private TNonCopyable
{
public:
    static TLease CreateLease(TDuration timeout, TClosure onExpired)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_ASSERT(onExpired);

        auto lease = New<TLeaseEntry>(timeout, std::move(onExpired));
        auto guard = Guard(lease->SpinLock);
        lease->Cookie = TDelayedExecutor::Submit(
            BIND(&TImpl::OnLeaseExpired, lease),
            timeout);
        return lease;
    }

    static bool RenewLease(TLease lease, std::optional<TDuration> timeout)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YT_ASSERT(lease);

        auto guard = Guard(lease->SpinLock);

        if (!lease->IsValid) {
            return false;
        }

        if (timeout) {
            lease->Timeout = *timeout;
        }

        TDelayedExecutor::Cancel(lease->Cookie);
        lease->Cookie = TDelayedExecutor::Submit(
            BIND(&TImpl::OnLeaseExpired, lease),
            lease->Timeout);

        return true;
    }

    static bool CloseLease(TLease lease)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!lease) {
            return false;
        }

        auto guard = Guard(lease->SpinLock);

        if (!lease->IsValid) {
            return false;
        }

        InvalidateLease(lease);
        return true;
    }

private:
    static void OnLeaseExpired(TLease lease, bool aborted)
    {
        if (aborted) {
            return;
        }

        auto guard = Guard(lease->SpinLock);

        if (!lease->IsValid) {
            return;
        }

        auto onExpired = lease->OnExpired;
        InvalidateLease(lease);
        guard.Release();

        onExpired();
    }

    static void InvalidateLease(TLease lease)
    {
        VERIFY_SPINLOCK_AFFINITY(lease->SpinLock);

        TDelayedExecutor::CancelAndClear(lease->Cookie);
        lease->IsValid = false;
        lease->OnExpired.Reset();
    }
};

////////////////////////////////////////////////////////////////////////////////

TLease TLeaseManager::CreateLease(TDuration timeout, TClosure onExpired)
{
    return TImpl::CreateLease(timeout, std::move(onExpired));
}

bool TLeaseManager::RenewLease(TLease lease, std::optional<TDuration> timeout)
{
    return TImpl::RenewLease(std::move(lease), timeout);
}

bool TLeaseManager::CloseLease(TLease lease)
{
    return TImpl::CloseLease(std::move(lease));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency::NYT


