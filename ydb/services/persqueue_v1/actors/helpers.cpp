#include "helpers.h"

#include <library/cpp/protobuf/util/repeated_field_utils.h>

namespace NKikimr::NGRpcProxy::V1 {

bool RemoveEmptyMessages(PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch& data) {
    auto batchRemover = [&](PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch::Batch& batch) -> bool {
        return batch.message_data_size() == 0;
    };
    auto partitionDataRemover = [&](PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch::PartitionData& partition) -> bool {
        NProtoBuf::RemoveRepeatedFieldItemIf(partition.mutable_batches(), batchRemover);
        return partition.batches_size() == 0;
    };
    NProtoBuf::RemoveRepeatedFieldItemIf(data.mutable_partition_data(), partitionDataRemover);
    return !data.partition_data().empty();
}

// TODO: remove after refactor
bool RemoveEmptyMessages(Topic::StreamReadMessage::ReadResponse& data) {
    auto batchRemover = [&](Topic::StreamReadMessage::ReadResponse::Batch& batch) -> bool {
        return batch.message_data_size() == 0;
    };
    auto partitionDataRemover = [&](Topic::StreamReadMessage::ReadResponse::PartitionData& partition) -> bool {
        NProtoBuf::RemoveRepeatedFieldItemIf(partition.mutable_batches(), batchRemover);
        return partition.batches_size() == 0;
    };
    NProtoBuf::RemoveRepeatedFieldItemIf(data.mutable_partition_data(), partitionDataRemover);
    return !data.partition_data().empty();
}

TMaybe<ui32> GetPartitionFromConfigOptions(
        ui32 preferred, const NPQ::NSourceIdEncoding::TEncodedSourceId& encodedSrcId,
        ui32 partPerTablet, bool firstClass, bool useRoundRobin
) {
    TMaybe<ui32> ret;
    if (preferred < Max<ui32>()) {
        ret = preferred;
    } else if (firstClass) {
        ret = NKikimr::NDataStreams::V1::CalculateShardFromSrcId(encodedSrcId.OriginalSourceId, partPerTablet);
    } else if (!useRoundRobin){
        ret = encodedSrcId.Hash % partPerTablet;
    }
    return ret;
}

}
