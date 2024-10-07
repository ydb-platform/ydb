#include "helpers.h"

#include <library/cpp/protobuf/util/repeated_field_utils.h>

namespace NKikimr::NGRpcProxy::V1 {

bool HasMessages(const PersQueue::V1::MigrationStreamingReadServerMessage::DataBatch& data) {
    for (const auto& partData : data.partition_data()) {
        for (const auto& batch : partData.batches()) {
            if (batch.message_data_size() > 0) {
                return true;
            }
        }
    }
    return false;
}

// TODO: remove after refactor
bool HasMessages(const Topic::StreamReadMessage::ReadResponse& data) {
    for (const auto& partData : data.partition_data()) {
        for (const auto& batch : partData.batches()) {
            if (batch.message_data_size() > 0) {
                return true;
            }
        }
    }
    return false;
}

}
