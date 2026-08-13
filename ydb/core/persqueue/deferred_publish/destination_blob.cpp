#include "destination_blob.h"
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NPQ::NDeferredPublish {

TString SerializeDestinationBlob(const NKikimrPQ::TDeferredPublishDestinationBlob& blob) {
    TString bytes;
    Y_PROTOBUF_SUPPRESS_NODISCARD blob.SerializeToString(&bytes);
    return bytes;
}

bool ParseDestinationBlob(TStringBuf bytes, NKikimrPQ::TDeferredPublishDestinationBlob* blob) {
    AFL_ENSURE(blob != nullptr)("reason", "destination blob is null");
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
    AFL_ENSURE(blob != nullptr)("reason", "destination blob is null");

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
