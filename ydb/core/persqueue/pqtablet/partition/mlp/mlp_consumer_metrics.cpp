#include "mlp_consumer.h"
#include "mlp_storage.h"

#include <ydb/core/protos/counters_pq.pb.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>


namespace NKikimr::NPQ::NMLP {

void TConsumerActor::UpdateMetrics() {
    auto& metrics = Storage->GetMetrics();

    NKikimrPQ::TAggregatedCounters::TMLPConsumerCounters counters;
    counters.SetConsumer(Config.GetName());

    auto* values = counters.MutableCountersValues();
    values->Resize(NAux::GetLabeledCounterOpts<EMLPConsumerLabeledCounters_descriptor>()->Size, 0);
    values->Set(EMLPConsumerLabeledCounters::METRIC_INFLIGHT_COMMITTED_COUNT, metrics.CommittedMessageCount);
    values->Set(EMLPConsumerLabeledCounters::METRIC_INFLIGHT_LOCKED_COUNT, metrics.LockedMessageCount);
    values->Set(EMLPConsumerLabeledCounters::METRIC_INFLIGHT_DELAYED_COUNT, metrics.DelayedMessageCount);
    values->Set(EMLPConsumerLabeledCounters::METRIC_INFLIGHT_UNLOCKED_COUNT, metrics.UnprocessedMessageCount);
    values->Set(EMLPConsumerLabeledCounters::METRIC_INFLIGHT_SCHEDULED_TO_DLQ_COUNT, metrics.TotalScheduledToDLQMessageCount);
    values->Set(EMLPConsumerLabeledCounters::METRIC_COMMITTED_COUNT, metrics.TotalCommittedMessageCount);
    values->Set(EMLPConsumerLabeledCounters::METRIC_PURGED_COUNT, metrics.TotalPurgedMessageCount);

    for (size_t i = 0; i < metrics.MessageLocks.GetRangeCount(); ++i) {
        counters.AddMessageLocksValues(metrics.MessageLocks.GetRangeValue(i));
    }

    for (size_t i = 0; i < metrics.MessageLockingDuration.GetRangeCount(); ++i) {
        counters.AddMessageLockingDurationValues(metrics.MessageLockingDuration.GetRangeValue(i));
    }

    counters.SetDeletedByRetentionPolicy(metrics.TotalDeletedByRetentionMessageCount);
    counters.SetDeletedByDeadlinePolicy(metrics.TotalDeletedByDeadlinePolicyMessageCount);
    counters.SetDeletedByMovedToDLQ(metrics.TotalMovedToDLQMessageCount);

    counters.SetCPUUsage(CPUUsageMetric);
    CPUUsageMetric = 0;

    Send(PartitionActorId, new TEvPQ::TEvMLPConsumerState(std::move(counters)));

    if (DetailedMetrics) {
        DetailedMetrics->UpdateMetrics(metrics);
    }
}

TDetailedMetrics::TDetailedMetrics(const NKikimrPQ::TPQTabletConfig::TConsumer& consumerConfig, ::NMonitoring::TDynamicCounterPtr& root) {
    Y_UNUSED(consumerConfig);
    Y_UNUSED(root);

    auto consumerGroup = root->GetSubgroup("consumer", consumerConfig.GetName());

    InflightCommittedCount = consumerGroup->GetExpiringNamedCounter("name", "topic.inflight.committed_messages", false);
    InflightLockedCount = consumerGroup->GetExpiringNamedCounter("name", "topic.inflight.locked_messages", false);
    InflightDelayedCount = consumerGroup->GetExpiringNamedCounter("name", "topic.inflight.delayed_messages", false);
    InflightUnlockedCount = consumerGroup->GetExpiringNamedCounter("name", "topic.inflight.unlocked_messages", false);
    InflightScheduledToDLQCount = consumerGroup->GetExpiringNamedCounter("name", "topic.inflight.scheduled_to_dlq_messages", false);
    CommittedCount = consumerGroup->GetExpiringNamedCounter("name", "topic.committed_messages", false);
    PurgedCount = consumerGroup->GetExpiringNamedCounter("name", "topic.purged_messages", false);
    
    MessageLocks = consumerGroup->GetExpiringNamedCounter("name", "topic.message_locks", false);
    MessageLockingDuration = consumerGroup->GetExpiringNamedCounter("name", "topic.message_locking_duration_milliseconds", false);

    auto deletedMessagesGroup = consumerGroup->GetSubgroup("name", "topic.deleted_messages");
    DeletedByRetentionPolicy = deletedMessagesGroup->GetExpiringNamedCounter("reason", "retention", false);
    DeletedByDeadlinePolicy = consumerGroup->GetExpiringNamedCounter("reason", "delete_policy", false);
    DeletedByMovedToDLQ = consumerGroup->GetExpiringNamedCounter("reason", "move_policy", false);
}

TDetailedMetrics::~TDetailedMetrics() {
}

void TDetailedMetrics::UpdateMetrics(const TMetrics& metrics) {
    InflightCommittedCount->Set(metrics.CommittedMessageCount);
    InflightLockedCount->Set(metrics.LockedMessageCount);
    InflightDelayedCount->Set(metrics.DelayedMessageCount);
    InflightUnlockedCount->Set(metrics.UnprocessedMessageCount);
    InflightScheduledToDLQCount->Set(metrics.TotalScheduledToDLQMessageCount);
    CommittedCount->Set(metrics.TotalCommittedMessageCount);
    PurgedCount->Set(metrics.TotalPurgedMessageCount);
    
    MessageLocks->Set(metrics.MessageLocks.GetRangeCount());
    MessageLockingDuration->Set(metrics.MessageLockingDuration.GetRangeCount());
    DeletedByRetentionPolicy->Set(metrics.TotalDeletedByRetentionMessageCount);
    DeletedByDeadlinePolicy->Set(metrics.TotalDeletedByDeadlinePolicyMessageCount);
    DeletedByMovedToDLQ->Set(metrics.TotalMovedToDLQMessageCount);
}


}
