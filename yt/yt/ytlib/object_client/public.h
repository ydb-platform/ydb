#pragma once

#include <yt/yt/client/object_client/public.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NObjectClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TReqExecuteBatchWithRetriesConfig)

DECLARE_REFCOUNTED_CLASS(TObjectAttributeCacheConfig)
DECLARE_REFCOUNTED_CLASS(TObjectAttributeCache)

DECLARE_REFCOUNTED_CLASS(TObjectServiceCacheConfig)
DECLARE_REFCOUNTED_CLASS(TObjectServiceCacheDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TCachingObjectServiceConfig)
DECLARE_REFCOUNTED_CLASS(TCachingObjectServiceDynamicConfig)

DECLARE_REFCOUNTED_CLASS(TObjectServiceCacheEntry)
DECLARE_REFCOUNTED_CLASS(TObjectServiceCache)

DECLARE_REFCOUNTED_CLASS(TAbcConfig)

DECLARE_REFCOUNTED_STRUCT(ICachingObjectService)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMasterFeature,
    ((OverlayedJournals)            (0))
    ((Portals)                      (1))
    ((PortalExitSynchronization)    (2))
);

// Some objects must be created and removed atomically.
//
// Let's consider accounts. In the absence of an atomic commit, it's possible
// that some cell knows about an account, and some other cell doesn't. Then, the
// former cell sending a chunk requisition update to the latter will cause
// trouble.
//
// Removal also needs two-phase (and even more!) locking since otherwise a primary master
// is unable to command the destruction of an object to its secondaries without risking
// that some secondary still holds a reference to the object.
DEFINE_ENUM_WITH_UNDERLYING_TYPE(EObjectLifeStage, ui8,
     // Creation workflow
     ((CreationStarted)         (0))
     ((CreationPreCommitted)    (1))
     ((CreationCommitted)       (2))

     // Removal workflow
     ((RemovalStarted)          (3))
     ((RemovalPreCommitted)     (4))
     ((RemovalAwaitingCellsSync)(5))
     ((RemovalCommitted)        (6))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient
