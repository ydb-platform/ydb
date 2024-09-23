#include "chunk_replica.h"

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/library/erasure/public.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/confirm_chunk_replica_info.pb.h>

namespace NYT::NChunkClient {

using namespace NNodeTrackerClient;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

void TChunkIdWithIndex::Save(TStreamSaveContext& context) const
{
    using NYT::Save;

    Save(context, Id);
    Save(context, ReplicaIndex);
}

void TChunkIdWithIndex::Load(TStreamLoadContext& context)
{
    using NYT::Load;

    Load(context, Id);
    Load(context, ReplicaIndex);
}

////////////////////////////////////////////////////////////////////////////////

void TChunkIdWithIndexes::Save(TStreamSaveContext& context) const
{
    using NYT::Save;

    Save(context, Id);
    Save(context, ReplicaIndex);
    Save(context, MediumIndex);
}

void TChunkIdWithIndexes::Load(TStreamLoadContext& context)
{
    using NYT::Load;

    Load(context, Id);
    Load(context, ReplicaIndex);
    Load(context, MediumIndex);
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TConfirmChunkReplicaInfo* value, TChunkReplicaWithLocation replica)
{
    using NYT::ToProto;

    value->set_replica(replica.Value_);
    ToProto(value->mutable_location_uuid(), replica.ChunkLocationUuid_);
}

void FromProto(TChunkReplicaWithLocation* replica, NProto::TConfirmChunkReplicaInfo value)
{
    using NYT::FromProto;

    replica->Value_ = value.replica();
    replica->ChunkLocationUuid_ = FromProto<TGuid>(value.location_uuid());
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, TChunkReplicaWithLocation replica, TStringBuf /*spec*/)
{
    builder->AppendFormat("%v", replica.GetNodeId());
    if (replica.GetReplicaIndex() != GenericChunkReplicaIndex) {
        builder->AppendFormat("/%v", replica.GetReplicaIndex());
    }
    builder->AppendFormat("@%v", replica.GetChunkLocationUuid());
}

void FormatValue(TStringBuilderBase* builder, TChunkReplicaWithMedium replica, TStringBuf /*spec*/)
{
    builder->AppendFormat("%v", replica.GetNodeId());
    if (replica.GetReplicaIndex() != GenericChunkReplicaIndex) {
        builder->AppendFormat("/%v", replica.GetReplicaIndex());
    }
    if (replica.GetMediumIndex() == AllMediaIndex) {
        builder->AppendString("@all");
    } else if (replica.GetMediumIndex() != GenericMediumIndex) {
        builder->AppendFormat("@%v", replica.GetMediumIndex());
    }
}

void FormatValue(TStringBuilderBase* builder, TChunkReplica replica, TStringBuf /*spec*/)
{
    builder->AppendFormat("%v", replica.GetNodeId());
    if (replica.GetReplicaIndex() != GenericChunkReplicaIndex) {
        builder->AppendFormat("/%v", replica.GetReplicaIndex());
    }
}

void FormatValue(TStringBuilderBase* builder, const TChunkIdWithIndex& id, TStringBuf /*spec*/)
{
    builder->AppendFormat("%v", id.Id);
    if (id.ReplicaIndex != GenericChunkReplicaIndex) {
        builder->AppendFormat("/%v", id.ReplicaIndex);
    }
}

void FormatValue(TStringBuilderBase* builder, const TChunkIdWithIndexes& id, TStringBuf /*spec*/)
{
    builder->AppendFormat("%v", id.Id);
    if (id.ReplicaIndex != GenericChunkReplicaIndex) {
        builder->AppendFormat("/%v", id.ReplicaIndex);
    }
    if (id.MediumIndex == AllMediaIndex) {
        builder->AppendString("@all");
    } else if (id.MediumIndex != GenericMediumIndex) {
        builder->AppendFormat("@%v", id.MediumIndex);
    }
}

////////////////////////////////////////////////////////////////////////////////

TChunkReplicaAddressFormatter::TChunkReplicaAddressFormatter(TNodeDirectoryPtr nodeDirectory)
    : NodeDirectory_(std::move(nodeDirectory))
{ }

void TChunkReplicaAddressFormatter::operator()(TStringBuilderBase* builder, TChunkReplicaWithMedium replica) const
{
    if (const auto* descriptor = NodeDirectory_->FindDescriptor(replica.GetNodeId())) {
        builder->AppendFormat("%v", *descriptor);
    } else {
        builder->AppendFormat("<unresolved-%v>", replica.GetNodeId());
    }
    if (replica.GetReplicaIndex() != GenericChunkReplicaIndex) {
        builder->AppendFormat("/%v", replica.GetReplicaIndex());
    }
    if (replica.GetMediumIndex() == AllMediaIndex) {
        builder->AppendString("@all");
    } else if (replica.GetMediumIndex() != GenericMediumIndex) {
        builder->AppendFormat("@%v", replica.GetMediumIndex());
    }
}

void TChunkReplicaAddressFormatter::operator()(TStringBuilderBase* builder, TChunkReplica replica) const
{
    const auto* descriptor = NodeDirectory_->FindDescriptor(replica.GetNodeId());
    if (descriptor) {
        builder->AppendFormat("%v", descriptor->GetDefaultAddress());
    } else {
        builder->AppendFormat("<unresolved-%v>", replica.GetNodeId());
    }
    if (replica.GetReplicaIndex() != GenericChunkReplicaIndex) {
        builder->AppendFormat("/%v", replica.GetReplicaIndex());
    }
}

////////////////////////////////////////////////////////////////////////////////

TChunkReplicaList TChunkReplicaWithMedium::ToChunkReplicas(const TChunkReplicaWithMediumList& replicasWithMedia)
{
    TChunkReplicaList replicas;
    replicas.reserve(replicasWithMedia.size());
    for (auto replicaWithMedium : replicasWithMedia) {
        replicas.push_back(replicaWithMedium.ToChunkReplica());
    }
    return replicas;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

EObjectType BaseErasurePartTypeFromPartId(TChunkId id)
{
    auto type = TypeFromId(id);
    if (type >= MinErasureChunkPartType && type <= MaxErasureChunkPartType) {
        return EObjectType::ErasureChunkPart_0;
    } else if (type >= MinErasureJournalChunkPartType && type <= MaxErasureJournalChunkPartType) {
        return EObjectType::ErasureJournalChunkPart_0;
    } else {
        YT_ABORT();
    }
}

EObjectType BaseErasurePartTypeFromWholeId(TChunkId id)
{
    switch (TypeFromId(id)) {
        case EObjectType::ErasureChunk:        return EObjectType::ErasureChunkPart_0;
        case EObjectType::ErasureJournalChunk: return EObjectType::ErasureJournalChunkPart_0;
        default:                               YT_ABORT();
    }
}

EObjectType WholeErasureTypeFromPartId(TChunkId id)
{
    auto type = TypeFromId(id);
    if (type >= MinErasureChunkPartType && type <= MaxErasureChunkPartType) {
        return EObjectType::ErasureChunk;
    } else if (type >= MinErasureJournalChunkPartType && type <= MaxErasureJournalChunkPartType) {
        return EObjectType::ErasureJournalChunk;
    } else {
        YT_ABORT();
    }
}

} // namespace

TChunkId ErasurePartIdFromChunkId(TChunkId id, int index)
{
    return ReplaceTypeInId(id, static_cast<EObjectType>(static_cast<int>(BaseErasurePartTypeFromWholeId(id)) + index));
}

TChunkId ErasureChunkIdFromPartId(TChunkId id)
{
    return ReplaceTypeInId(id, WholeErasureTypeFromPartId(id));
}

int ReplicaIndexFromErasurePartId(TChunkId id)
{
    int index = static_cast<int>(TypeFromId(id)) - static_cast<int>(BaseErasurePartTypeFromPartId(id));
    YT_VERIFY(index >= 0 && index < ChunkReplicaIndexBound);
    return index;
}

TChunkId EncodeChunkId(const TChunkIdWithIndex& idWithIndex)
{
    return IsErasureChunkId(idWithIndex.Id)
        ? ErasurePartIdFromChunkId(idWithIndex.Id, idWithIndex.ReplicaIndex)
        : idWithIndex.Id;
}

TChunkIdWithIndex DecodeChunkId(TChunkId id)
{
    return IsErasureChunkPartId(id)
        ? TChunkIdWithIndex(ErasureChunkIdFromPartId(id), ReplicaIndexFromErasurePartId(id))
        : TChunkIdWithIndex(id, GenericChunkReplicaIndex);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
