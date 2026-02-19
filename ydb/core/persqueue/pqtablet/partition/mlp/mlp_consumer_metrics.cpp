#include "mlp_consumer.h"
#include "mlp_storage.h"

#include <ydb/core/persqueue/common/percentiles.h>
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
    values->Set(EMLPConsumerLabeledCounters::METRIC_INFLIGHT_SCHEDULED_TO_DLQ_COUNT, metrics.DLQMessageCount);
    values->Set(EMLPConsumerLabeledCounters::METRIC_COMMITTED_COUNT, metrics.TotalCommittedMessageCount);
    values->Set(EMLPConsumerLabeledCounters::METRIC_PURGED_COUNT, metrics.TotalPurgedMessageCount);

    for (size_t i = 0; i < metrics.MessageLocks.GetRangeCount(); ++i) {
        counters.AddMessageLocksValues(metrics.MessageLocks.GetRangeValue(i));
    }

    for (size_t i = 0; i < metrics.MessageLockingDuration.GetRangeCount(); ++i) {
        counters.AddMessageLockingDurationValues(metrics.MessageLockingDuration.GetRangeValue(i));
    }

    for (size_t i = 0; i < metrics.WaitingLockingDuration.GetRangeCount(); ++i) {
        counters.AddWaitingLockingDurationValues(metrics.WaitingLockingDuration.GetRangeValue(i));
    }

    counters.SetDeletedByRetentionPolicy(metrics.TotalDeletedByRetentionMessageCount);
    counters.SetDeletedByDeadlinePolicy(metrics.TotalDeletedByDeadlinePolicyMessageCount);
    counters.SetDeletedByMovedToDLQ(metrics.TotalMovedToDLQMessageCount);

    counters.SetCPUUsage(CPUUsageMetric);
    CPUUsageMetric = 0;

    Send(PartitionActorId, new TEvPQ::TEvMLPConsumerState(UseForReading(), std::move(counters)));

    if (DetailedMetrics) {
        DetailedMetrics->UpdateMetrics(metrics);
    }
}

TDetailedMetrics::TDetailedMetrics(const NKikimrPQ::TPQTabletConfig::TConsumer& consumerConfig, ::NMonitoring::TDynamicCounterPtr& root) {
    auto consumerGroup = root->GetSubgroup("consumer", consumerConfig.GetName());

    InflightCommittedCount = consumerGroup->GetExpiringNamedCounter("name", "topic.partition.inflight.committed_messages", false);
    InflightLockedCount = consumerGroup->GetExpiringNamedCounter("name", "topic.partition.inflight.locked_messages", false);
    InflightDelayedCount = consumerGroup->GetExpiringNamedCounter("name", "topic.partition.inflight.delayed_messages", false);
    InflightUnlockedCount = consumerGroup->GetExpiringNamedCounter("name", "topic.partition.inflight.unlocked_messages", false);
    InflightScheduledToDLQCount = consumerGroup->GetExpiringNamedCounter("name", "topic.partition.inflight.scheduled_to_dlq_messages", false);
    CommittedCount = consumerGroup->GetExpiringNamedCounter("name", "topic.partition.committed_messages", true);
    PurgedCount = consumerGroup->GetExpiringNamedCounter("name", "topic.partition.purged_messages", true);

    MessageLocks = consumerGroup->GetExpiringNamedHistogram("name", "topic.partition.message_lock_attempts", NMonitoring::ExplicitHistogram(MLP_LOCKS_BOUNDS), true);
    MessageLockingDuration = consumerGroup->GetExpiringNamedHistogram("name", "topic.partition.message_locking_duration_milliseconds", NMonitoring::ExplicitHistogram(SLOW_LATENCY_BOUNDS), true);
    WaitingLockingDuration = consumerGroup->GetExpiringNamedHistogram("name", "topic.partition.waiting_locking_duration_milliseconds", NMonitoring::ExplicitHistogram(SLOW_LATENCY_BOUNDS), true);

    auto deletedMessagesGroup = consumerGroup->GetSubgroup("name", "topic.partition.deleted_messages");
    DeletedByRetentionPolicy = deletedMessagesGroup->GetExpiringNamedCounter("reason", "retention", true);
    DeletedByDeadlinePolicy = deletedMessagesGroup->GetExpiringNamedCounter("reason", "delete_policy", true);
    DeletedByMovedToDLQ = deletedMessagesGroup->GetExpiringNamedCounter("reason", "move_policy", true);
}

TDetailedMetrics::~TDetailedMetrics() = default;

namespace {

void SetCounters(NMonitoring::THistogramPtr& counter, const TTabletPercentileCounter& metrics, const auto& bounds) {
    counter->Reset();
    for (size_t i = 0; i < bounds.size(); ++i) {
        counter->Collect(bounds[i], metrics.GetRangeValue(i));
    }
    counter->Collect(Max<double>(), metrics.GetRangeValue(bounds.size()));
}

}

void TDetailedMetrics::UpdateMetrics(const TMetrics& metrics) {
    InflightCommittedCount->Set(metrics.CommittedMessageCount);
    InflightLockedCount->Set(metrics.LockedMessageCount);
    InflightDelayedCount->Set(metrics.DelayedMessageCount);
    InflightUnlockedCount->Set(metrics.UnprocessedMessageCount);
    InflightScheduledToDLQCount->Set(metrics.DLQMessageCount);
    CommittedCount->Set(metrics.TotalCommittedMessageCount);
    PurgedCount->Set(metrics.TotalPurgedMessageCount);

    SetCounters(MessageLocks, metrics.MessageLocks, MLP_LOCKS_BOUNDS);
    SetCounters(MessageLockingDuration, metrics.MessageLockingDuration, SLOW_LATENCY_BOUNDS);
    SetCounters(WaitingLockingDuration, metrics.WaitingLockingDuration, SLOW_LATENCY_BOUNDS);

    DeletedByRetentionPolicy->Set(metrics.TotalDeletedByRetentionMessageCount);
    DeletedByDeadlinePolicy->Set(metrics.TotalDeletedByDeadlinePolicyMessageCount);
    DeletedByMovedToDLQ->Set(metrics.TotalMovedToDLQMessageCount);
}

}
