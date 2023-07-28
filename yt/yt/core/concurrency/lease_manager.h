#pragma once

#include "public.h"

#include <yt/yt/core/actions/callback.h>

#include <optional>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Manages lease expiration.
/*!
 *  A lease is an opaque entity.
 *  It is assigned a timeout and an expiration handler upon creation.
 *  The lease must be continuously renewed by calling #Renew.
 *  If #Renew is not called during the timeout, the lease expires and the handler is invoked.
 *  Closing the lease releases resources and cancels expiration notification.
 */
class TLeaseManager
    : public TNonCopyable
{
public:
    TLeaseManager() = delete;

    //! Creates a new lease with a given timeout and a given expiration callback.
    static TLease CreateLease(TDuration timeout, TClosure onExpired);

    //! Renews the lease.
    /*!
     *  \param lease A lease to renew.
     *  \param timeout A new timeout (if |std::nullopt| then the old one is preserved).
     *  \returns True iff the lease is still valid (i.e. not expired).
     */
    static bool RenewLease(TLease lease, std::optional<TDuration> timeout = std::nullopt);

    //! Closes the lease.
    /*!
     *  \returns True iff the lease is still valid (i.e. not expired).
     */
    static bool CloseLease(TLease lease);

private:
    class TImpl;
};

//! An invalid lease.
extern const TLease NullLease;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
