#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/ring_queue.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <library/cpp/yt/misc/strong_typedef.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! An opaque handle to an item in TAsyncBarrier.
YT_DEFINE_STRONG_TYPEDEF(TAsyncBarrierCookie, i64);

//! A sentinel value that can never be returned by TAsyncBarrier::Insert.
inline const TAsyncBarrierCookie InvalidAsyncBarrierCookie = TAsyncBarrierCookie(0);

//! Maintains (but does not store) a set of items and enables tracking moments
//! when all items that are currently present in the set become removed.
/*!
 *  \note
 *  Thread affinity: any
 */
class TAsyncBarrier final
{
public:
    //! Inserts a new item into the set.
    //! Returns the cookie to be used later for removal.
    TAsyncBarrierCookie Insert();

    //! Removes an existing item from the set.
    void Remove(TAsyncBarrierCookie cookie);

    //! Returns a future that becomes set when all items currently present in the
    //! set are removed.
    TFuture<void> GetBarrierFuture();

    //! Clears the state and also propagates #error to all subscribers.
    void Clear(const TError& error);

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);

    i64 FirstSlotCookie_ = 1;
    TRingQueue<bool> SlotOccupied_;

    struct TSubscription
    {
        i64 BarrierCookie;
        TPromise<void> Promise;
        TFuture<void> Future;
    };

    TRingQueue<TSubscription> Subscriptions_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
