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
bool RemoveEmptyMessages(PersQueue::V1::StreamingReadServerMessage::ReadResponse& data) {
    auto batchRemover = [&](PersQueue::V1::StreamingReadServerMessage::ReadResponse::Batch& batch) -> bool {
        return batch.message_data_size() == 0;
    };
    auto partitionDataRemover = [&](PersQueue::V1::StreamingReadServerMessage::ReadResponse::PartitionData& partition) -> bool {
        NProtoBuf::RemoveRepeatedFieldItemIf(partition.mutable_batches(), batchRemover);
        return partition.batches_size() == 0;
    };
    NProtoBuf::RemoveRepeatedFieldItemIf(data.mutable_partition_data(), partitionDataRemover);
    return !data.partition_data().empty();
}

}
