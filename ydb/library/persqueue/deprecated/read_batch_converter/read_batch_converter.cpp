#include "read_batch_converter.h"

namespace NPersQueue {

static void Convert(const ReadResponse::BatchedData::PartitionData& partition, ReadResponse::Data::MessageBatch* dstBatch) {
    dstBatch->set_topic(partition.topic());
    dstBatch->set_partition(partition.partition());
    for (const ReadResponse::BatchedData::Batch& batch : partition.batch()) {
        for (const ReadResponse::BatchedData::MessageData& message : batch.message_data()) {
            ReadResponse::Data::Message* const dstMessage = dstBatch->add_message();
            dstMessage->set_data(message.data());
            dstMessage->set_offset(message.offset());

            MessageMeta* const meta = dstMessage->mutable_meta();
            meta->set_source_id(batch.source_id());
            meta->set_seq_no(message.seq_no());
            meta->set_create_time_ms(message.create_time_ms());
            meta->set_write_time_ms(batch.write_time_ms());
            meta->set_codec(message.codec());
            meta->set_ip(batch.ip());
            meta->set_uncompressed_size(message.uncompressed_size());
            if (batch.has_extra_fields()) {
                *meta->mutable_extra_fields() = batch.extra_fields();
            }
        }
    }
}

void ConvertToOldBatch(ReadResponse& response) {
    if (!response.has_batched_data()) {
        return;
    }
    ReadResponse::BatchedData data;
    data.Swap(response.mutable_batched_data());

    ReadResponse::Data& dstData = *response.mutable_data(); // this call will clear BatchedData field
    dstData.set_cookie(data.cookie());
    for (const ReadResponse::BatchedData::PartitionData& partition : data.partition_data()) {
        Convert(partition, dstData.add_message_batch());
    }
}

} // namespace NPersQueue
