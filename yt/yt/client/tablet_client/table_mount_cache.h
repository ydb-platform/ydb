#pragma once

#include "public.h"

#include <yt/yt/client/hive/public.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>
#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/client/chaos_client/replication_card.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

#include <util/datetime/base.h>

namespace NYT::NTabletClient {

////////////////////////////////////////////////////////////////////////////////

struct TTabletInfo final
{
    TTabletId TabletId;
    NHydra::TRevision MountRevision = NHydra::NullRevision;
    ETabletState State;
    EInMemoryMode InMemoryMode;
    NTableClient::TLegacyOwningKey PivotKey;
    TTabletCellId CellId;
    NObjectClient::TObjectId TableId;
    TInstant UpdateTime;

    NTableClient::TKeyBound GetLowerKeyBound() const;

    TTabletInfoPtr Clone() const;
};

DEFINE_REFCOUNTED_TYPE(TTabletInfo)

////////////////////////////////////////////////////////////////////////////////

struct TTableReplicaInfo final
{
    TTableReplicaId ReplicaId;
    TString ClusterName;
    NYPath::TYPath ReplicaPath;
    ETableReplicaMode Mode;
};

DEFINE_REFCOUNTED_TYPE(TTableReplicaInfo)

////////////////////////////////////////////////////////////////////////////////

struct TIndexInfo
{
    NObjectClient::TObjectId TableId;
    ESecondaryIndexKind Kind;
    std::optional<TString> Predicate;
};

////////////////////////////////////////////////////////////////////////////////

//! Describes the primary and the auxiliary schemas derived from the table schema.
//! Cf. TTableSchema::ToXXX methods.
DEFINE_ENUM(ETableSchemaKind,
    // Schema assigned to Cypress node, as is.
    (Primary)
    // Schema used for inserting rows.
    (Write)
    // Schema used for querying rows.
    (Query)
    // Schema used for deleting rows.
    (Delete)
    // Schema used for writing versioned rows (during replication).
    (VersionedWrite)
    // Schema used for looking up rows.
    (Lookup)
    // Schema used for locking rows.
    (Lock)
    // For sorted schemas, coincides with primary.
    // For ordered, contains an additional tablet index columns.
    (PrimaryWithTabletIndex)
    // Schema used for replication log rows.
    (ReplicationLog)
    // Schema used for inserting rows into ordered tables via queue producer.
    (WriteViaQueueProducer)
);

struct TTableMountInfo final
{
    NYPath::TYPath Path;
    NObjectClient::TObjectId TableId;
    TEnumIndexedArray<ETableSchemaKind, NTableClient::TTableSchemaPtr> Schemas;

    // PhysicalPath points to a physical object, if current object is linked to some other object, then this field will point to the source.
    // When this field is not supported on the server-side, this path will be equal to object path.
    NYPath::TYPath PhysicalPath;

    NObjectClient::TObjectId HunkStorageId;

    bool Dynamic;
    TTableReplicaId UpstreamReplicaId;
    bool NeedKeyEvaluation;

    NChaosClient::TReplicationCardId ReplicationCardId;

    std::vector<TTabletInfoPtr> Tablets;
    std::vector<TTabletInfoPtr> MountedTablets;

    std::vector<TTableReplicaInfoPtr> Replicas;

    std::vector<TIndexInfo> Indices;

    //! For sorted tables, these are -infinity and +infinity.
    //! For ordered tablets, these are |[0]| and |[tablet_count]| resp.
    NTableClient::TLegacyOwningKey LowerCapBound;
    NTableClient::TLegacyOwningKey UpperCapBound;

    // Master reply revision for master service cache invalidation.
    NHydra::TRevision PrimaryRevision;
    NHydra::TRevision SecondaryRevision;

    bool EnableDetailedProfiling = false;

    bool IsSorted() const;
    bool IsOrdered() const;
    bool IsReplicated() const;
    bool IsChaosReplicated() const;
    bool IsReplicationLog() const;
    bool IsPhysicallyLog() const;
    bool IsChaosReplica() const;
    bool IsHunkStorage() const;

    TTabletInfoPtr GetTabletByIndexOrThrow(int tabletIndex) const;
    int GetTabletIndexForKey(NTableClient::TUnversionedValueRange key) const;
    int GetTabletIndexForKey(NTableClient::TLegacyKey key) const;
    TTabletInfoPtr GetTabletForKey(NTableClient::TUnversionedValueRange key) const;
    TTabletInfoPtr GetTabletForRow(NTableClient::TUnversionedRow row) const;
    TTabletInfoPtr GetTabletForRow(NTableClient::TVersionedRow row) const;
    int GetRandomMountedTabletIndex() const;
    TTabletInfoPtr GetRandomMountedTablet() const;

    void ValidateTabletOwner() const;
    void ValidateDynamic() const;
    void ValidateSorted() const;
    void ValidateOrdered() const;
    void ValidateNotPhysicallyLog() const;
    void ValidateReplicated() const;
    void ValidateReplicationLog() const;

    TTableMountInfoPtr Clone() const;
};

DEFINE_REFCOUNTED_TYPE(TTableMountInfo)

////////////////////////////////////////////////////////////////////////////////

struct ITableMountCache
    : public virtual TRefCounted
{
    virtual TFuture<TTableMountInfoPtr> GetTableInfo(const NYPath::TYPath& path) = 0;

    //! Invalidates cached table info for all table infos owning this tablet.
    virtual void InvalidateTablet(TTabletId tabletId) = 0;

    struct TInvalidationResult
    {
        bool Retryable = false;
        //! Actual retryable error code.
        TErrorCode ErrorCode;
        //! Tablet info for the tablet mentioned in the |error|. Note that it may belong
        //! to the would-be invalidated table.
        TTabletInfoPtr TabletInfo;
        //! True if table info was patched by info from the error and update is not required.
        bool TableInfoUpdatedFromError = false;
    };

    //! If #error is unretryable, returns the result with |Retryable| = false.
    //! Otherwise either patches cached tablet info with hints provided within
    //! |error| attributes or invalidates cached tablet info. In the former
    //! case |TableInfoUpdatedFromError| flag will be set.
    virtual TInvalidationResult InvalidateOnError(
        const TError& error,
        bool forceRetry) = 0;

    virtual void Clear() = 0;

    virtual void Reconfigure(TTableMountCacheConfigPtr config) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITableMountCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
