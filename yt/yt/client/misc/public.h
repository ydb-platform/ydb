#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TWorkloadDescriptor;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

//! The type of the workload.
//! Order is given by |GetBasicPriority| function and not by concrete enum values.
/*!
 *  This is the most fine-grained categorization available.
 *  Most subsystems will map EWorkloadCategory to their own coarser categories.
 *
 *  NB: This enum is serializable, so please keep values intact or advance
 *  protocol version where appropriate.
 */
DEFINE_ENUM(EWorkloadCategory,
    ((Idle)                        (0))
    ((SystemArtifactCacheDownload) (9))
    ((SystemRepair)                (2))
    ((SystemReincarnation)        (17))
    ((SystemReplication)           (1))
    ((SystemMerge)                (16))
    ((SystemTabletCompaction)      (6))
    ((SystemTabletLogging)         (5))
    ((SystemTabletPartitioning)    (7))
    ((SystemTabletPreload)         (8))
    ((SystemTabletRecovery)       (10))
    ((SystemTabletReplication)    (14))
    ((SystemTabletSnapshot)       (13))
    ((SystemTabletStoreFlush)     (12))
    ((UserBatch)                   (3))
    ((UserInteractive)            (11))
    ((UserRealtime)                (4))
    ((UserDynamicStoreRead)       (15))
);

struct TWorkloadDescriptor;

DECLARE_REFCOUNTED_CLASS(TWorkloadConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
