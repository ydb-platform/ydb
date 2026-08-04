#include "public.h"
#include "replication_card_serialization.h"

#include <yt/yt/client/object_client/helpers.h>

#include <library/cpp/yt/assert/assert.h>

namespace NYT::NChaosClient {

using namespace NObjectClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TReplicationCardId MakeReplicationCardId(TObjectId randomId)
{
    return MakeId(
        EObjectType::ReplicationCard,
        CellTagFromId(randomId),
        CounterFromId(randomId),
        EntropyFromId(randomId) & 0xffff0000);
}

TReplicationCardId MakeChaosLeaseId(TObjectId randomId)
{
    return MakeId(
        EObjectType::ChaosLease,
        CellTagFromId(randomId),
        CounterFromId(randomId),
        EntropyFromId(randomId) & 0xffff0000);
}

TReplicaId MakeReplicaId(TReplicationCardId replicationCardId, TReplicaIdIndex index)
{
    return MakeId(
        EObjectType::ChaosTableReplica,
        CellTagFromId(replicationCardId),
        CounterFromId(replicationCardId),
        EntropyFromId(replicationCardId) | index);
}

TReplicationCardId ReplicationCardIdFromReplicaId(TReplicaId replicaId)
{
    return MakeId(
        EObjectType::ReplicationCard,
        CellTagFromId(replicaId),
        CounterFromId(replicaId),
        EntropyFromId(replicaId) & 0xffff0000);
}

TReplicationCardId ReplicationCardIdFromUpstreamReplicaIdOrNull(TReplicaId upstreamReplicaId)
{
    return IsChaosTableReplicaType(TypeFromId(upstreamReplicaId))
        ? ReplicationCardIdFromReplicaId(upstreamReplicaId)
        : TReplicationCardId();
}

TReplicationCardId MakeReplicationCardCollocationId(TObjectId randomId)
{
    return MakeId(
        EObjectType::ReplicationCardCollocation,
        CellTagFromId(randomId),
        CounterFromId(randomId),
        EntropyFromId(randomId) & 0xffff0000);
}

TCellTag GetSiblingChaosCellTag(TCellTag cellTag)
{
    return TCellTag(cellTag.Underlying() ^ 1);
}

bool IsValidReplicationProgress(const TReplicationProgress& progress)
{
    const auto& segments = progress.Segments;

    if (segments.empty()) {
        return false;
    }

    for (int segmentIndex = 1; segmentIndex < std::ssize(segments); ++segmentIndex) {
        if (segments[segmentIndex - 1].LowerKey >= segments[segmentIndex].LowerKey) {
            return false;
        }
    }

    return segments.back().LowerKey < progress.UpperKey;
}

bool IsOrderedTableReplicationProgress(const TReplicationProgress& progress, int tabletCount)
{
    const auto& segments = progress.Segments;
    const auto& upper = progress.UpperKey;

    int segmentCount = std::ssize(segments);
    if (segmentCount == 0 || segmentCount > tabletCount) {
        return false;
    }

    for (int segmentIndex = 0; segmentIndex < segmentCount; ++segmentIndex) {
        const auto& segment = segments[segmentIndex];
        if (segment.LowerKey.GetCount() != 0 &&
            (segment.LowerKey.GetCount() != 1 || segment.LowerKey[0].Type != EValueType::Int64))
        {
            return false;
        }
    }

    if (upper.GetCount() != 1 || (upper[0].Type != EValueType::Int64 && upper[0].Type != EValueType::Max)) {
        return false;
    }

    return true;
}

bool IsOrderedTabletReplicationProgress(const TReplicationProgress& progress)
{
    return IsOrderedTableReplicationProgress(progress, /*tabletCount*/ 1);
}

void ValidateOrderedTabletReplicationProgress(const TReplicationProgress& progress)
{
    if (!IsOrderedTabletReplicationProgress(progress)) {
        THROW_ERROR_EXCEPTION("Invalid replication progress for ordered table tablet")
            << TErrorAttribute("replication_progress", progress);
    }
}

void ValidateOrderedTableReplicationProgress(const TReplicationProgress& progress, int tabletCount)
{
    if (!IsOrderedTableReplicationProgress(progress, tabletCount)) {
        THROW_ERROR_EXCEPTION("Invalid replication progress for ordered table")
            << TErrorAttribute("replication_progress", progress)
            << TErrorAttribute("tablet_count", tabletCount);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
