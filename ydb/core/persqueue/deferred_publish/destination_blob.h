#pragma once

#include <ydb/core/protos/pqdata_deferred_publish_destination.pb.h>

namespace NKikimr::NPQ::NDeferredPublish {

TString SerializeDestinationBlob(const NKikimrPQ::TDeferredPublishDestinationBlob& blob);
bool ParseDestinationBlob(TStringBuf bytes, NKikimrPQ::TDeferredPublishDestinationBlob* blob);

NKikimrPQ::TTopicPartitionDestination MakeTopicPartitionDestination(
    ui32 partitionId,
    ui64 tabletId);

NKikimrPQ::TDeferredPublishDestinationBlob MakeDestinationBlob(
    ui32 partitionId,
    ui64 tabletId);

void AddOrUpdateTopicPartition(
    NKikimrPQ::TDeferredPublishDestinationBlob* blob,
    ui32 partitionId,
    ui64 tabletId);

const NKikimrPQ::TTopicPartitionDestination* FindTopicPartitionDestination(
    const NKikimrPQ::TDeferredPublishDestinationBlob& blob,
    ui32 partitionId);

} // namespace NKikimr::NPQ::NDeferredPublish
