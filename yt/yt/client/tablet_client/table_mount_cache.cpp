#include "table_mount_cache.h"

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/key_bound.h>

namespace NYT::NTabletClient {

using namespace NTableClient;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TKeyBound TTabletInfo::GetLowerKeyBound() const
{
    return TKeyBound::FromRow() >= PivotKey;
}

TTabletInfoPtr TTabletInfo::Clone() const
{
    return New<TTabletInfo>(*this);
}

////////////////////////////////////////////////////////////////////////////////

bool TTableMountInfo::IsSorted() const
{
    return Schemas[ETableSchemaKind::Primary]->IsSorted();
}

bool TTableMountInfo::IsOrdered() const
{
    return !IsSorted();
}

bool TTableMountInfo::IsReplicated() const
{
    return TypeFromId(TableId) == EObjectType::ReplicatedTable;
}

bool TTableMountInfo::IsChaosReplicated() const
{
    return TypeFromId(TableId) == EObjectType::ChaosReplicatedTable;
}

bool TTableMountInfo::IsReplicationLog() const
{
    return TypeFromId(TableId) == EObjectType::ReplicationLogTable;
}

bool TTableMountInfo::IsHunkStorage() const
{
    return TypeFromId(TableId) == EObjectType::HunkStorage;
}

bool TTableMountInfo::IsPhysicallyLog() const
{
    return IsReplicated() || IsReplicationLog();
}

bool TTableMountInfo::IsChaosReplica() const
{
    return TypeFromId(UpstreamReplicaId) == EObjectType::ChaosTableReplica;
}

TTabletInfoPtr TTableMountInfo::GetTabletByIndexOrThrow(int tabletIndex) const
{
    if (tabletIndex < 0 || tabletIndex >= std::ssize(Tablets)) {
        THROW_ERROR_EXCEPTION(EErrorCode::NoSuchTablet,
            "Invalid tablet index for table %v: expected in range [0,%v], got %v",
            Path,
            Tablets.size() - 1,
            tabletIndex);
    }
    return Tablets[tabletIndex];
}

int TTableMountInfo::GetTabletIndexForKey(TUnversionedValueRange key) const
{
    ValidateDynamic();
    auto it = std::upper_bound(
        Tablets.begin(),
        Tablets.end(),
        key,
        [&] (TUnversionedValueRange key, const TTabletInfoPtr& rhs) {
            return CompareValueRanges(key, rhs->PivotKey.Elements()) < 0;
        });
    YT_VERIFY(it != Tablets.begin());
    return std::distance(Tablets.begin(), it - 1);
}

int TTableMountInfo::GetTabletIndexForKey(TUnversionedRow key) const
{
    return GetTabletIndexForKey(key.Elements());
}

TTabletInfoPtr TTableMountInfo::GetTabletForKey(TUnversionedValueRange key) const
{
    auto index = GetTabletIndexForKey(key);
    return Tablets[index];
}

TTabletInfoPtr TTableMountInfo::GetTabletForRow(TUnversionedRow row) const
{
    int keyColumnCount = Schemas[ETableSchemaKind::Primary]->GetKeyColumnCount();
    YT_VERIFY(static_cast<int>(row.GetCount()) >= keyColumnCount);
    return GetTabletForKey(row.FirstNElements(keyColumnCount));
}

TTabletInfoPtr TTableMountInfo::GetTabletForRow(TVersionedRow row) const
{
    int keyColumnCount = Schemas[ETableSchemaKind::Primary]->GetKeyColumnCount();
    YT_VERIFY(row.GetKeyCount() == keyColumnCount);
    return GetTabletForKey(row.Keys());
}

int TTableMountInfo::GetRandomMountedTabletIndex() const
{
    ValidateTabletOwner();

    if (MountedTablets.empty()) {
        THROW_ERROR_EXCEPTION(EErrorCode::TabletNotMounted,
            "Table %v has no mounted tablets",
            Path);
    }

    return RandomNumber(MountedTablets.size());
}

TTabletInfoPtr TTableMountInfo::GetRandomMountedTablet() const
{
    return MountedTablets[GetRandomMountedTabletIndex()];
}

void TTableMountInfo::ValidateTabletOwner() const
{
    if (!Dynamic && !IsHunkStorage()) {
        THROW_ERROR_EXCEPTION("Table %v is neither dynamic nor a hunk storage", Path);
    }
}

void TTableMountInfo::ValidateDynamic() const
{
    if (!Dynamic) {
        THROW_ERROR_EXCEPTION("Table %v is not dynamic", Path);
    }
}

void TTableMountInfo::ValidateSorted() const
{
    if (!IsSorted()) {
        THROW_ERROR_EXCEPTION("Table %v is not sorted", Path);
    }
}

void TTableMountInfo::ValidateOrdered() const
{
    if (!IsOrdered()) {
        THROW_ERROR_EXCEPTION("Table %v is not ordered", Path);
    }
}

void TTableMountInfo::ValidateNotPhysicallyLog() const
{
    if (IsPhysicallyLog()) {
        THROW_ERROR_EXCEPTION("Table %v physically contains replication log", Path);
    }
}

void TTableMountInfo::ValidateReplicated() const
{
    if (!IsReplicated()) {
        THROW_ERROR_EXCEPTION("Table %v is not replicated", Path);
    }
}

void TTableMountInfo::ValidateReplicationLog() const
{
    if (!IsReplicationLog()) {
        THROW_ERROR_EXCEPTION("Table %v is not replication log", Path);
    }
}

TTableMountInfoPtr TTableMountInfo::Clone() const
{
    return New<TTableMountInfo>(*this);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
