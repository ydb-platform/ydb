#pragma once

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NCypressClient {

////////////////////////////////////////////////////////////////////////////////

using NObjectClient::TObjectId;
using NObjectClient::TTransactionId;
using NObjectClient::NullObjectId;
using NObjectClient::NullTransactionId;
using NObjectClient::EObjectType;
using NObjectClient::TVersionedObjectId;

using TNodeId = TObjectId;
using TLockId = TObjectId;
using TCypressShardId = TObjectId;
using TVersionedNodeId = TVersionedObjectId;

extern const TLockId NullLockId;

// NB: The order is from weakest to strongest.
DEFINE_ENUM(ELockMode,
    ((None)      (0))
    ((Snapshot)  (1))
    ((Shared)    (2))
    ((Exclusive) (3))
);

DEFINE_ENUM(ELockState,
    ((Pending)   (0))
    ((Acquired)  (1))
);

YT_DEFINE_ERROR_ENUM(
    ((SameTransactionLockConflict)         (400))
    ((DescendantTransactionLockConflict)   (401))
    ((ConcurrentTransactionLockConflict)   (402))
    ((PendingLockConflict)                 (403))
    ((LockDestroyed)                       (404))
    ((TooManyLocksOnTransaction)           (405))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressClient
