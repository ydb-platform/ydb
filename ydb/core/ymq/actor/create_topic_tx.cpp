#include "create_topic_tx.h"

#include <ydb/core/ymq/base/constants.h>

#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>

#include <util/string/builder.h>

namespace NKikimr::NSQS {

Ydb::Topic::CreateTopicRequest BuildCreateTopicTx(
    const TString& queuePath,
    const TString& versionName,
    bool isFifo,
    const TTopicParams& params
) {
    const TString topicPath = TString::Join(queuePath, '/', versionName, "/streamImpl");

    Ydb::Topic::CreateTopicRequest request;
    request.set_path(topicPath);

    request.mutable_retention_period()->set_seconds(params.PartitionLifetimeSeconds);
    request.set_partition_write_speed_bytes_per_second(1048576);
    request.set_partition_write_burst_bytes(1048576);
    if (params.HasContentBasedDeduplication) {
        request.set_content_based_deduplication(params.ContentBasedDeduplication);
    }

    auto* partitioningSettings = request.mutable_partitioning_settings();
    partitioningSettings->set_min_active_partitions(1);
    partitioningSettings->set_max_active_partitions(100);
    auto* autoPartitioningSettings = partitioningSettings->mutable_auto_partitioning_settings();
    autoPartitioningSettings->set_strategy(::Ydb::Topic::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_SCALE_UP_AND_DOWN);
    autoPartitioningSettings->mutable_partition_write_speed()->set_up_utilization_percent(80);
    autoPartitioningSettings->mutable_partition_write_speed()->set_down_utilization_percent(20);
    autoPartitioningSettings->mutable_partition_write_speed()->mutable_stabilization_window()->set_seconds(30);

    auto* consumer = request.add_consumers();
    consumer->set_name(ConsumerName);
    auto* consumerType = consumer->mutable_shared_consumer_type();
    consumerType->set_keep_messages_order(isFifo);
    if (params.DefaultDelayMessageTimeMs) {
        consumerType->mutable_receive_message_delay()->set_seconds(params.DefaultDelayMessageTimeMs / 1000);
        consumerType->mutable_receive_message_delay()->set_nanos(params.DefaultDelayMessageTimeMs % 1000 * 1000000);
    }
    if (params.DefaultProcessingTimeoutSeconds) {
        consumerType->mutable_default_processing_timeout()->set_seconds(params.DefaultProcessingTimeoutSeconds);
    }
    if (params.DefaultReceiveMessageWaitTimeMs) {
        consumerType->mutable_receive_message_wait_time()->set_seconds(params.DefaultReceiveMessageWaitTimeMs / 1000);
        consumerType->mutable_receive_message_wait_time()->set_nanos(params.DefaultReceiveMessageWaitTimeMs % 1000 * 1000000);
    }
    if (params.MaxReceiveCount || params.RedriveTargetQueueName) {
        consumerType->mutable_dead_letter_policy()->set_enabled(true);
        if (params.MaxReceiveCount) {
            consumerType->mutable_dead_letter_policy()->mutable_condition()->set_max_processing_attempts(params.MaxReceiveCount);
        }
        if (params.RedriveTargetQueueName) {
            auto dlq = TStringBuilder() << "sqs://" << params.AccountName << "/" << params.FolderId << "/" << params.RedriveTargetQueueName;
            consumerType->mutable_dead_letter_policy()->mutable_move_action()->set_dead_letter_queue(std::move(dlq));
        } else {
            consumerType->mutable_dead_letter_policy()->mutable_delete_action();
        }
    }

    return request;
}

} // namespace NKikimr::NSQS
