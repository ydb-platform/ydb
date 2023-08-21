#include "helpers.h"

#include "read_limit.h"

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_spec.pb.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NChunkClient {

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void PrintTo(const TReadRange& readRange, std::ostream* os)
{
    *os << ToString(readRange);
}

NObjectClient::TObjectId GetObjectIdFromChunkSpec(const NProto::TChunkSpec& chunkSpec)
{
    return FromProto<NObjectClient::TObjectId>(chunkSpec.chunk_id());
}

NObjectClient::TCellId GetCellIdFromChunkSpec(const NProto::TChunkSpec& chunkSpec)
{
    return FromProto<NObjectClient::TCellId>(chunkSpec.cell_id());
}

NObjectClient::TObjectId GetTabletIdFromChunkSpec(const NProto::TChunkSpec& chunkSpec)
{
    return FromProto<NTabletClient::TTabletId>(chunkSpec.tablet_id());
}

TChunkReplicaWithMediumList GetReplicasFromChunkSpec(const NProto::TChunkSpec& chunkSpec)
{
    if (chunkSpec.replicas_size() == 0) {
        auto legacyReplicas = FromProto<TChunkReplicaList>(chunkSpec.legacy_replicas());
        TChunkReplicaWithMediumList replicas;
        replicas.reserve(legacyReplicas.size());
        for (auto legacyReplica : legacyReplicas) {
            replicas.emplace_back(legacyReplica);
        }
        return replicas;
    } else {
        return FromProto<TChunkReplicaWithMediumList>(chunkSpec.replicas());
    }
}

void SetTabletId(NProto::TChunkSpec* chunkSpec, NTabletClient::TTabletId tabletId)
{
    ToProto(chunkSpec->mutable_tablet_id(), tabletId);
}

void SetObjectId(NProto::TChunkSpec* chunkSpec, NObjectClient::TObjectId objectId)
{
    ToProto(chunkSpec->mutable_chunk_id(), objectId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
