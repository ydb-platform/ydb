#pragma once

#include <yt/yt/core/misc/public.h>

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/client/object_client/public.h>

namespace NYT::NTabletClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TLockMask;

} // namespace NProto

DEFINE_ENUM(ETabletState,
    // Individual states
    ((Mounting)        (0))
    ((Mounted)         (1))
    ((Unmounting)      (2))
    ((Unmounted)       (3))
    ((Freezing)        (4))
    ((Frozen)          (5))
    ((Unfreezing)      (6))
    ((FrozenMounting)  (7))

    // Special states
    ((None)          (100))
    ((Mixed)         (101))
    ((Transient)     (102))
);

constexpr ETabletState MinValidTabletState = ETabletState::Mounting;
constexpr ETabletState MaxValidTabletState = ETabletState::FrozenMounting;

// Keep in sync with NRpcProxy::NProto::ETableReplicaMode.
DEFINE_ENUM(ETableReplicaMode,
    ((Sync)           (0))
    ((Async)          (1))
    ((AsyncToSync)    (2))
    ((SyncToAsync)    (3))
);

DEFINE_ENUM(ETableReplicaContentType,
    ((Data)     (0))
    ((Queue)    (1))
    ((External) (2))
);

YT_DEFINE_ERROR_ENUM(
    ((TransactionLockConflict)                (1700))
    ((NoSuchTablet)                           (1701))
    ((NoSuchCell)                             (1721))
    ((TabletNotMounted)                       (1702))
    ((AllWritesDisabled)                      (1703))
    ((InvalidMountRevision)                   (1704))
    ((TableReplicaAlreadyExists)              (1705))
    ((InvalidTabletState)                     (1706))
    ((TableMountInfoNotReady)                 (1707))
    ((TabletSnapshotExpired)                  (1708))
    ((QueryInputRowCountLimitExceeded)        (1709))
    ((QueryOutputRowCountLimitExceeded)       (1710))
    ((QueryExpressionDepthLimitExceeded)      (1711))
    ((RowIsBlocked)                           (1712))
    ((BlockedRowWaitTimeout)                  (1713))
    ((NoSyncReplicas)                         (1714))
    ((TableMustNotBeReplicated)               (1715))
    ((TableMustBeSorted)                      (1716))
    ((TooManyRowsInTransaction)               (1717))
    ((UpstreamReplicaMismatch)                (1718))
    ((NoSuchDynamicStore)                     (1719))
    ((BundleResourceLimitExceeded)            (1720))
    ((SyncReplicaIsNotKnown)                  (1722))
    ((SyncReplicaIsNotInSyncMode)             (1723))
    ((SyncReplicaIsNotWritten)                (1724))
    ((RequestThrottled)                       (1725))
    ((ColumnNotFound)                         (1726))
    ((ReplicatorWriteBlockedByUser)           (1727))
    ((UserWriteBlockedByReplicator)           (1728))
    ((CannotCheckConflictsAgainstChunkStore)  (1729))
    ((InvalidBackupState)                     (1730))
    ((WriteRetryIsImpossible)                 (1731))
    ((SyncReplicaNotInSync)                   (1732))
    ((BackupCheckpointRejected)               (1733))
    ((BackupInProgress)                       (1734))
    ((ChunkIsNotPreloaded)                    (1735))
    ((NoInSyncReplicas)                       (1736))
    ((CellHasNoAssignedPeers)                 (1737))
    ((TableSchemaIncompatible)                (1738))
    ((BundleIsBanned)                         (1739))
    ((TabletServantIsNotActive)               (1740))
    ((UniqueIndexConflict)                    (1741))
);

DEFINE_ENUM(EInMemoryMode,
    ((None)        (0))
    ((Compressed)  (1))
    ((Uncompressed)(2))
);

DEFINE_ENUM(EOrderedTableBackupMode,
    ((Exact)       (0))
    ((AtLeast)     (1))
    ((AtMost)      (2))
);

using TTabletCellId = NHydra::TCellId;
extern const TTabletCellId NullTabletCellId;

using TTabletId = NObjectClient::TObjectId;
extern const TTabletId NullTabletId;

using TStoreId = NObjectClient::TObjectId;
extern const TStoreId NullStoreId;

using TPartitionId = NObjectClient::TObjectId;
extern const TPartitionId NullPartitionId;

using TTabletCellBundleId = NObjectClient::TObjectId;
extern const TTabletCellBundleId NullTabletCellBundleId;

using THunkStorageId = NObjectClient::TObjectId;
extern const THunkStorageId NullHunkStorageId;

constexpr int TypicalTableReplicaCount = 6;

using TTableReplicaId = NObjectClient::TObjectId;
using TTabletActionId = NObjectClient::TObjectId;
using TDynamicStoreId = NObjectClient::TObjectId;
using TTabletOwnerId = NObjectClient::TObjectId;
using TAreaId = NObjectClient::TObjectId;

DEFINE_BIT_ENUM(EReplicationLogDataFlags,
    ((None)      (0x0000))
    ((Missing)   (0x0001))
    ((Aggregate) (0x0002))
);

struct TReplicationLogTable
{
    static const TString ChangeTypeColumnName;
    static const TString KeyColumnNamePrefix;
    static const TString ValueColumnNamePrefix;
    static const TString FlagsColumnNamePrefix;
};

DEFINE_BIT_ENUM(EUnversionedUpdateDataFlags,
    ((None)      (0x0000))
    ((Missing)   (0x0001))
    ((Aggregate) (0x0002))
);

constexpr EUnversionedUpdateDataFlags MinValidUnversionedUpdateDataFlags = EUnversionedUpdateDataFlags::None;
constexpr EUnversionedUpdateDataFlags MaxValidUnversionedUpdateDataFlags =
    EUnversionedUpdateDataFlags::Missing | EUnversionedUpdateDataFlags::Aggregate;

struct TUnversionedUpdateSchema
{
    static const TString ChangeTypeColumnName;
    static const TString ValueColumnNamePrefix;
    static const TString FlagsColumnNamePrefix;
};

DEFINE_ENUM(ETabletCellHealth,
    (Initializing)
    (Good)
    (Degraded)
    (Failed)
);

DEFINE_ENUM(ETableReplicaState,
    ((None)                     (0))
    ((Disabling)                (1))
    ((Disabled)                 (2))
    ((Enabling)                 (4))
    ((Enabled)                  (3))
);

DEFINE_ENUM(ETableReplicaStatus,
    ((Unknown)                  (0))

    ((SyncInSync)               (1))
    ((SyncCatchingUp)           (2))
    ((SyncNotWritable)          (3))

    ((AsyncInSync)              (4))
    ((AsyncCatchingUp)          (5))
    ((AsyncNotWritable)         (6))
);

DEFINE_ENUM(ETabletActionKind,
    ((Move)                     (0))
    ((Reshard)                  (1))
    ((SmoothMove)               (2))
);

DEFINE_ENUM(ETabletActionState,
    ((Preparing)                (0))
    ((Freezing)                 (1))
    ((Frozen)                   (2))
    ((Unmounting)               (3))
    ((Unmounted)                (4))
    ((Orphaned)                (10))
    ((Mounting)                 (5))
    ((Mounted)                  (6))
    ((Completed)                (7))
    ((Failing)                  (8))
    ((Failed)                   (9))

    ((MountingAuxiliary)        (11))
    ((WaitingForSmoothMove)     (12))
    ((AbortingSmoothMove)       (13))
);

DEFINE_ENUM(ETabletServiceFeatures,
    ((WriteGenerations)         (0))
    ((SharedWriteLocks)         (1))
);

DEFINE_ENUM(ESecondaryIndexKind,
    ((FullSync)                 (0))
    ((Unfolding)                (1))
    ((Unique)                   (2))
);

DEFINE_ENUM(ERowMergerType,
    ((Legacy)               (0))
    ((Watermark)            (1))
    ((New)                  (2))
);

extern const TString CustomRuntimeDataWatermarkKey;
struct TWatermarkRuntimeDataConfig;
struct TWatermarkRuntimeData;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTableMountCacheConfig)
DECLARE_REFCOUNTED_CLASS(TTableMountCacheDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TRemoteDynamicStoreReaderConfig)
DECLARE_REFCOUNTED_CLASS(TRetryingRemoteDynamicStoreReaderConfig)
DECLARE_REFCOUNTED_CLASS(TReplicatedTableOptions)
DECLARE_REFCOUNTED_CLASS(TReplicationCollocationOptions)

DECLARE_REFCOUNTED_STRUCT(TTableMountInfo)
DECLARE_REFCOUNTED_STRUCT(TTabletInfo)
DECLARE_REFCOUNTED_STRUCT(TTableReplicaInfo)
DECLARE_REFCOUNTED_STRUCT(ITableMountCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
