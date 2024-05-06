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

bool IsOrderedTabletReplicationProgress(const TReplicationProgress& progress)
{
    const auto& segments = progress.Segments;
    const auto& upper = progress.UpperKey;

    if (segments.size() != 1) {
        return false;
    }

    if (segments[0].LowerKey.GetCount() != 0 &&
        (segments[0].LowerKey.GetCount() != 1 || segments[0].LowerKey[0].Type != EValueType::Int64))
    {
        return false;
    }

    if (upper.GetCount() != 1 || (upper[0].Type != EValueType::Int64 && upper[0].Type != EValueType::Max)) {
        return false;
    }

    return true;
}

void ValidateOrderedTabletReplicationProgress(const TReplicationProgress& progress)
{
    if (!IsOrderedTabletReplicationProgress(progress)) {
        THROW_ERROR_EXCEPTION("Invalid replication progress for ordered table")
            << TErrorAttribute("replication_progress", progress);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
