#include "read_balancer__sqs_metrics.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/protos/counters_pq.pb.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NPQ {

namespace {

ui64 GetMlpConsumerMetric(const TTabletLabeledCountersBase& mlpConsumerMetrics, EMLPConsumerLabeledCounters index) {
    const auto& aggregatedCounters = mlpConsumerMetrics.GetCounters();
    const size_t idx = static_cast<size_t>(index);
    if (aggregatedCounters.Size() <= idx) {
        return 0;
    }
    return aggregatedCounters[idx].Get();
}

} // namespace

bool TTopicSqsMetricsHandler::IsApplicable(const NKikimrPQ::TPQTabletConfig& tabletConfig) {
    return tabletConfig.GetSqsExportMetrics() && !tabletConfig.GetSqsQueueName().empty();
}

TTopicSqsMetricsHandler::TTopicSqsMetricsHandler(const NKikimrPQ::TPQTabletConfig& tabletConfig, const NActors::TActorContext& ctx) {
    Y_ABORT_UNLESS(IsApplicable(tabletConfig));

    const TString& account = tabletConfig.GetSqsAccountName();
    const TString& queueName = tabletConfig.GetSqsQueueName();
    const TString& folderId = tabletConfig.GetSqsFolderId();

    if (!folderId.empty()) {
        auto ymqCounters = NKikimr::GetServiceCounters(AppData(ctx)->Counters, "ymq_public");
        auto ymqQueueCounters = ymqCounters
            ->GetSubgroup("cloud", account)
            ->GetSubgroup("folder", folderId)
            ->GetSubgroup("queue", queueName);
        MessagesCount_ = ymqQueueCounters->GetExpiringNamedCounter("name", "queue.messages.stored_count", false);
        InflightMessagesCount_ = ymqQueueCounters->GetExpiringNamedCounter("name", "queue.messages.inflight_count", false);
        CountersType_ = ECountersType::Ymq;
        return;
    }

    auto sqsCounters = NKikimr::GetServiceCounters(AppData(ctx)->Counters, "sqs");
    auto sqsQueueCounters = sqsCounters
        ->GetSubgroup("subsystem", "core")
        ->GetSubgroup("user", account)
        ->GetSubgroup("queue", queueName);
    MessagesCount_ = sqsQueueCounters->GetExpiringNamedCounter("sensor", "MessagesCount", false);
    InflightMessagesCount_ = sqsQueueCounters->GetExpiringNamedCounter("sensor", "InflyMessagesCount", false);
    CountersType_ = ECountersType::Sqs;
}

void TTopicSqsMetricsHandler::Update(const TTabletLabeledCountersBase& mlpConsumerMetrics) {
    const ui64 locked = GetMlpConsumerMetric(mlpConsumerMetrics, METRIC_INFLIGHT_LOCKED_COUNT);
    const ui64 delayed = GetMlpConsumerMetric(mlpConsumerMetrics, METRIC_INFLIGHT_DELAYED_COUNT);
    const ui64 unlocked = GetMlpConsumerMetric(mlpConsumerMetrics, METRIC_INFLIGHT_UNLOCKED_COUNT);
    const ui64 scheduledToDlq = GetMlpConsumerMetric(mlpConsumerMetrics, METRIC_INFLIGHT_SCHEDULED_TO_DLQ_COUNT);
    SetMessageCounts(locked + delayed + unlocked + scheduledToDlq, locked);
}

void TTopicSqsMetricsHandler::SetMessageCounts(ui64 storedCount, ui64 inflightCount) {
    if (MessagesCount_) {
        MessagesCount_->Set(storedCount);
    }
    if (InflightMessagesCount_) {
        InflightMessagesCount_->Set(inflightCount);
    }
}

}
