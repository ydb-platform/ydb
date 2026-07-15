#include "destination_blob.h"

namespace NKikimr::NPQ::NDeferredPublish {

TString SerializeDestinationBlob(const NKikimrPQ::TDeferredPublishDestinationBlob& blob) {
    TString bytes;
    Y_PROTOBUF_SUPPRESS_NODISCARD blob.SerializeToString(&bytes);
    return bytes;
}

bool ParseDestinationBlob(TStringBuf bytes, NKikimrPQ::TDeferredPublishDestinationBlob* blob) {
    Y_ABORT_UNLESS(blob != nullptr);
    return blob->ParseFromArray(bytes.data(), bytes.size());
}

NKikimrPQ::TTopicPartitionDestination MakeTopicPartitionDestination(
    ui32 partitionId,
    ui64 tabletId)
{
    NKikimrPQ::TTopicPartitionDestination partition;
    partition.SetPartitionId(partitionId);
    partition.SetTabletId(tabletId);
    return partition;
}

NKikimrPQ::TDeferredPublishDestinationBlob MakeDestinationBlob(
    ui32 partitionId,
    ui64 tabletId)
{
    NKikimrPQ::TDeferredPublishDestinationBlob blob;
    AddOrUpdateTopicPartition(&blob, partitionId, tabletId);
    return blob;
}

void AddOrUpdateTopicPartition(
    NKikimrPQ::TDeferredPublishDestinationBlob* blob,
    ui32 partitionId,
    ui64 tabletId)
{
    Y_ABORT_UNLESS(blob != nullptr);

    for (auto& partition : *blob->MutablePartitions()) {
        if (partition.GetPartitionId() == partitionId) {
            partition.SetTabletId(tabletId);
            return;
        }
    }

    *blob->AddPartitions() = MakeTopicPartitionDestination(partitionId, tabletId);
}

const NKikimrPQ::TTopicPartitionDestination* FindTopicPartitionDestination(
    const NKikimrPQ::TDeferredPublishDestinationBlob& blob,
    ui32 partitionId)
{
    for (const auto& partition : blob.GetPartitions()) {
        if (partition.GetPartitionId() == partitionId) {
            return &partition;
        }
    }
    return nullptr;
}

} // namespace NKikimr::NPQ::NDeferredPublish
