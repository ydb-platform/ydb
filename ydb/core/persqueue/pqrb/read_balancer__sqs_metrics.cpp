#include "read_balancer__sqs_metrics.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/persqueue/common/percentiles.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/counters_pq.pb.h>
#include <ydb/library/actors/core/actor.h>

#include <util/generic/array_ref.h>
#include <util/string/builder.h>

namespace NKikimr::NPQ {

namespace {

constexpr TStringBuf SqsSensorLabel = "sensor";
constexpr TStringBuf YmqNameLabel = "name";
constexpr TStringBuf YmqQueueCounterPrefix = "queue.messages.";
constexpr TStringBuf YmqMethodLabel = "method";
constexpr TStringBuf YmqActionCounterPrefix = "api.http.";

const NMonitoring::TBucketBounds FastActionsDurationBucketsMs = {
    5, 10, 25, 50, 75, 100, 150, 300, 500, 1000, 5000,
};

const NMonitoring::TBucketBounds SlowActionsDurationBucketsMs = {
    100, 150, 300, 500, 1000, 5000,
};

const NMonitoring::TBucketBounds YmqFastActionsDurationBucketsMs = FastActionsDurationBucketsMs;

const NMonitoring::TBucketBounds YmqSlowActionsDurationBucketsMs = {
    100, 150, 300, 500, 1000, 5000, 10'000, 20'000, 50'000,
};

const NMonitoring::TBucketBounds DurationBucketsMs = {
    5, 10, 25, 50, 75, 100, 125, 150, 250, 500, 750, 1'000, 2'500, 5'000, 10'000, 30'000, 50'000,
};

const NMonitoring::TBucketBounds ClientDurationBucketsMs = {
    100, 250, 500, 750, 1'000, 2'500, 5'000, 7'500, 10'000, 25'000, 50'000, 100'000, 250'000,
    500'000, 1'000'000, 2'500'000, 5'000'000, 10'000'000, 25'000'000, 50'000'000,
};

const NMonitoring::TBucketBounds YmqClientDurationBucketsMs = {
    100, 200, 500, 1'000, 2'000, 5'000, 10'000, 20'000, 60'000, 120'000, 300'000, 600'000, 3'600'000, 7'200'000,
};


struct TQueueActionDesc {
    TString Name;
    TString NonBatchName;
    TString CloudMethod;
    bool ForMessage = false;
    bool Fast = false;
};

const TQueueActionDesc SqsQueueProxyActions[] = {
    {"ChangeMessageVisibility", "ChangeMessageVisibility", "change_message_visibility", true, true},
    {"ChangeMessageVisibilityBatch", "ChangeMessageVisibility", "change_message_visibility_batch", true, true},
    {"DeleteMessage", "DeleteMessage", "delete_message", true, true},
    {"DeleteMessageBatch", "DeleteMessage", "delete_message_batch", true, true},
    {"GetQueueAttributes", "GetQueueAttributes", "get_queue_attributes", false, true},
    {"PurgeQueue", "PurgeQueue", "purge_queue", false, true},
    {"ReceiveMessage", "ReceiveMessage", "receive_message", true, true},
    {"SendMessage", "SendMessage", "send_message", true, true},
    {"SendMessageBatch", "SendMessage", "send_message_batch", true, true},
    {"SetQueueAttributes", "SetQueueAttributes", "set_queue_attributes", false, true},
    {"ListDeadLetterSourceQueues", "ListDeadLetterSourceQueues", "list_dead_letter_source_queues", false, true},
    {"ListQueueTags", "ListQueueTags", "list_queue_tags", false, true},
    {"TagQueue", "TagQueue", "tag_queue", false, true},
    {"UntagQueue", "UntagQueue", "untag_queue", false, true},
};

const TQueueActionDesc YmqQueueProxyActions[] = {
    {"ChangeMessageVisibility", "ChangeMessageVisibility", "change_message_visibility", true, true},
    {"ChangeMessageVisibilityBatch", "ChangeMessageVisibility", "change_message_visibility_batch", true, true},
    {"DeleteMessage", "DeleteMessage", "delete_message", true, true},
    {"DeleteMessageBatch", "DeleteMessage", "delete_message_batch", true, true},
    {"GetQueueAttributes", "GetQueueAttributes", "get_queue_attributes", false, true},
    {"GetQueueUrl", "GetQueueUrl", "get_queue_url", false, true},
    {"PurgeQueue", "PurgeQueue", "purge_queue", false, true},
    {"ReceiveMessage", "ReceiveMessage", "receive_message", true, true},
    {"SendMessage", "SendMessage", "send_message", true, true},
    {"SendMessageBatch", "SendMessage", "send_message_batch", true, true},
    {"SetQueueAttributes", "SetQueueAttributes", "set_queue_attributes", false, true},
    {"ListQueueTags", "ListQueueTags", "list_queue_tags", false, true},
    {"TagQueue", "TagQueue", "tag_queue", false, true},
    {"UntagQueue", "UntagQueue", "untag_queue", false, true},
};

template<typename TEnum>
ui64 GetLabeledMetric(const TTabletLabeledCountersBase& metrics, TEnum index) {
    const auto& aggregatedCounters = metrics.GetCounters();
    const size_t idx = static_cast<size_t>(index);
    if (aggregatedCounters.Size() <= idx) {
        return 0;
    }
    return aggregatedCounters[idx].Get();
}

ui64 GetTimelagMetricMs(const TTabletLabeledCountersBase& metrics, EClientLabeledCounters index) {
    const ui64 value = GetLabeledMetric(metrics, index);
    const ui64 now = TAppData::TimeProvider->Now().MilliSeconds();
    return value < now ? now - value : 0;
}

void SetHistogram(
    NMonitoring::THistogramPtr& counter,
    TConstArrayRef<ui64> values,
    const NMonitoring::TBucketBounds& bounds
) {
    if (!counter || values.size() != bounds.size() + 1) {
        return;
    }

    counter->Reset();
    for (size_t i = 0; i < bounds.size(); ++i) {
        counter->Collect(bounds[i], values[i]);
    }
    counter->Collect(Max<double>(), values[values.size() - 1]);
}

NMonitoring::TDynamicCounterPtr GetSqsQueueCountersRoot(
    const NMonitoring::TDynamicCounterPtr& countersRoot,
    const TString& account,
    const TString& queueName
) {
    return NKikimr::GetServiceCounters(countersRoot, "sqs")
        ->GetSubgroup(TString("subsystem"), "core")
        ->GetSubgroup(TString("user"), account)
        ->GetSubgroup(TString("queue"), queueName);
}

NMonitoring::TDynamicCounterPtr GetYmqQueueCountersRoot(
    const NMonitoring::TDynamicCounterPtr& countersRoot,
    const TString& account,
    const TString& folderId,
    const TString& queueName
) {
    return NKikimr::GetServiceCounters(countersRoot, "ymq_public")
        ->GetSubgroup(TString("cloud"), account)
        ->GetSubgroup(TString("folder"), folderId)
        ->GetSubgroup(TString("queue"), queueName);
}

NMonitoring::TDynamicCounters::TCounterPtr MakeSqsCounter(
    const NMonitoring::TDynamicCounterPtr& root,
    const TString& sensor,
    bool derivative,
    bool expiring
) {
    if (expiring) {
        return root->GetExpiringNamedCounter(TString(SqsSensorLabel), sensor, derivative);
    }
    return root->GetNamedCounter(TString(SqsSensorLabel), sensor, derivative);
}

NMonitoring::TDynamicCounters::TCounterPtr MakeYmqCounter(
    const NMonitoring::TDynamicCounterPtr& root,
    const TString& name,
    bool derivative
) {
    return root->GetExpiringNamedCounter(TString(YmqNameLabel), name, derivative);
}

NMonitoring::THistogramPtr MakeSqsHistogram(
    const NMonitoring::TDynamicCounterPtr& root,
    const TString& sensor,
    const NMonitoring::TBucketBounds& buckets,
    bool expiring
) {
    if (expiring) {
        return root->GetExpiringNamedHistogram(TString(SqsSensorLabel), sensor, NMonitoring::ExplicitHistogram(buckets));
    }
    return root->GetNamedHistogram(TString(SqsSensorLabel), sensor, NMonitoring::ExplicitHistogram(buckets));
}

NMonitoring::THistogramPtr MakeYmqHistogram(
    const NMonitoring::TDynamicCounterPtr& root,
    const TString& name,
    const NMonitoring::TBucketBounds& buckets
) {
    return root->GetExpiringNamedHistogram(TString(YmqNameLabel), name, NMonitoring::ExplicitHistogram(buckets));
}

TString YmqQueueCounterName(TStringBuf suffix) {
    return TString(YmqQueueCounterPrefix) + suffix;
}

void InitSqsActionCounters(
    TTopicQueueLeaderCounters& counters,
    const NMonitoring::TDynamicCounterPtr& root,
    const NKikimrConfig::TSqsConfig& cfg,
    const TQueueActionDesc& action
) {
    const bool onStart = !cfg.GetCreateLazyCounters();
    auto& actionCounters = counters.SqsActionCounters[action.Name];
    const auto& nonBatch = action.NonBatchName;
    const bool derivative = true;

    actionCounters.Success = MakeSqsCounter(
        root, TStringBuilder() << nonBatch << "_Success", derivative, true);
    actionCounters.Errors = MakeSqsCounter(
        root, TStringBuilder() << nonBatch << "_Errors", derivative, true);
    actionCounters.Infly = MakeSqsCounter(
        root, TStringBuilder() << nonBatch << "_Infly", false, true);

    const auto& buckets = action.Fast ? FastActionsDurationBucketsMs : SlowActionsDurationBucketsMs;
    actionCounters.Duration = MakeSqsHistogram(root, TStringBuilder() << nonBatch << "_Duration", buckets, true);

    if (action.Name == "ReceiveMessage") {
        actionCounters.WorkingDuration = MakeSqsHistogram(
            root, "ReceiveMessage_WorkingDuration", DurationBucketsMs, true);
    }

    Y_UNUSED(onStart);
}

void InitYmqActionCounters(
    TTopicQueueLeaderCounters& counters,
    const NMonitoring::TDynamicCounterPtr& root,
    const TQueueActionDesc& action
) {
    auto& actionCounters = counters.YmqActionCounters[action.Name];
    actionCounters.SubGroup = root->GetSubgroup(TString(YmqMethodLabel), action.CloudMethod);
    actionCounters.Success = MakeYmqCounter(
        actionCounters.SubGroup, TStringBuilder() << YmqActionCounterPrefix << "requests_count_per_second", true);
    actionCounters.Errors = MakeYmqCounter(
        actionCounters.SubGroup, TStringBuilder() << YmqActionCounterPrefix << "errors_count_per_second", true);

    const auto& buckets = action.Fast ? YmqFastActionsDurationBucketsMs : YmqSlowActionsDurationBucketsMs;
    actionCounters.Duration = MakeYmqHistogram(
        actionCounters.SubGroup, TStringBuilder() << YmqActionCounterPrefix << "request_duration_milliseconds", buckets);
}

void InitSqsLeaderCounters(
    TTopicQueueLeaderCounters& counters,
    const NMonitoring::TDynamicCounterPtr& root,
    const NKikimrConfig::TSqsConfig& cfg
) {
    counters.RequestsThrottled = MakeSqsCounter(root, "RequestsThrottled", true, true);
    counters.QueueMasterStartProblems = MakeSqsCounter(root, "QueueMasterStartProblems", true, false);
    counters.QueueLeaderStartProblems = MakeSqsCounter(root, "QueueLeaderStartProblems", true, false);

    counters.MessagesPurged = MakeSqsCounter(root, "MessagesPurged", true, true);
    counters.MessageReceiveAttempts = MakeSqsHistogram(root, "MessageReceiveAttempts", MLP_LOCKS_BOUNDS, true);
    counters.ClientMessageProcessing_Duration = MakeSqsHistogram(root, "ClientMessageProcessing_Duration", SLOW_LATENCY_BOUNDS, true);
    counters.MessageReside_Duration = MakeSqsHistogram(root, "MessageReside_Duration", SLOW_LATENCY_BOUNDS, true);

    counters.DeleteMessage_Count = MakeSqsCounter(root, "DeleteMessage_Count", true, true);
    counters.ReceiveMessage_EmptyCount = MakeSqsCounter(root, "ReceiveMessage_EmptyCount", true, true);
    counters.ReceiveMessage_Count = MakeSqsCounter(root, "ReceiveMessage_Count", true, true);
    counters.ReceiveMessage_BytesRead = MakeSqsCounter(root, "ReceiveMessage_BytesRead", true, true);

    counters.MessagesMovedToDLQ = MakeSqsCounter(root, "MessagesMovedToDLQ", true, true);

    counters.SendMessage_DeduplicationCount = MakeSqsCounter(root, "SendMessage_DeduplicationCount", true, true);
    counters.SendMessage_Count = MakeSqsCounter(root, "SendMessage_Count", true, true);
    counters.SendMessage_BytesWritten = MakeSqsCounter(root, "SendMessage_BytesWritten", true, true);

    counters.MessagesCount = MakeSqsCounter(root, "MessagesCount", false, true);
    counters.InflyMessagesCount = MakeSqsCounter(root, "InflyMessagesCount", false, true);
    counters.OldestMessageAgeSeconds = MakeSqsCounter(root, "OldestMessageAgeSeconds", false, true);

    counters.ReceiveMessage_KeysInvalidated = MakeSqsCounter(root, "ReceiveMessage_KeysInvalidated", true, true);
    counters.ReceiveMessageImmediate_Duration = MakeSqsHistogram(root, "ReceiveMessageImmediate_Duration", DurationBucketsMs, true);

    for (const auto& action : SqsQueueProxyActions) {
        InitSqsActionCounters(counters, root, cfg, action);
    }
}

void InitYmqLeaderCounters(
    TTopicQueueLeaderCounters& counters,
    const NMonitoring::TDynamicCounterPtr& root
) {
    counters.MessagesPurged = MakeYmqCounter(root, YmqQueueCounterName("purged_count_per_second"), true);
    counters.MessageReceiveAttempts = MakeYmqHistogram(root, YmqQueueCounterName("receive_attempts_count_rate"), MLP_LOCKS_BOUNDS);
    counters.ClientMessageProcessing_Duration = MakeYmqHistogram(
        root, YmqQueueCounterName("client_processing_duration_milliseconds"), SLOW_LATENCY_BOUNDS);
    counters.MessageReside_Duration = MakeYmqHistogram(
        root, YmqQueueCounterName("reside_duration_milliseconds"), SLOW_LATENCY_BOUNDS);

    counters.DeleteMessage_Count = MakeYmqCounter(root, YmqQueueCounterName("deleted_count_per_second"), true);
    counters.ReceiveMessage_EmptyCount = MakeYmqCounter(root, YmqQueueCounterName("empty_receive_attempts_count_per_second"), true);
    counters.ReceiveMessage_Count = MakeYmqCounter(root, YmqQueueCounterName("received_count_per_second"), true);
    counters.ReceiveMessage_BytesRead = MakeYmqCounter(root, YmqQueueCounterName("received_bytes_per_second"), true);

    counters.SendMessage_DeduplicationCount = MakeYmqCounter(root, YmqQueueCounterName("deduplicated_count_per_second"), true);
    counters.SendMessage_Count = MakeYmqCounter(root, YmqQueueCounterName("sent_count_per_second"), true);
    counters.SendMessage_BytesWritten = MakeYmqCounter(root, YmqQueueCounterName("sent_bytes_per_second"), true);

    counters.MessagesCount = MakeYmqCounter(root, YmqQueueCounterName("stored_count"), false);
    counters.InflyMessagesCount = MakeYmqCounter(root, YmqQueueCounterName("inflight_count"), false);
    counters.OldestMessageAgeSeconds = MakeYmqCounter(root, YmqQueueCounterName("oldest_age_milliseconds"), false);

    for (const auto& action : YmqQueueProxyActions) {
        InitYmqActionCounters(counters, root, action);
    }
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
    const auto& cfg = AppData(ctx)->SqsConfig;

    Backend_ = folderId.empty() ? ETopicSqsCountersBackend::Sqs : ETopicSqsCountersBackend::Ymq;

    if (Backend_ == ETopicSqsCountersBackend::Sqs) {
        const auto root = GetSqsQueueCountersRoot(AppData(ctx)->Counters, account, queueName);
        InitSqsLeaderCounters(Counters_, root, cfg);
    } else {
        const auto root = GetYmqQueueCountersRoot(AppData(ctx)->Counters, account, folderId, queueName);
        InitYmqLeaderCounters(Counters_, root);
    }
}

void TTopicSqsMetricsHandler::Update(
    const TTabletLabeledCountersBase& clientLabeledCounters,
    const TTabletLabeledCountersBase& mlpConsumerLabeledCounters,
    TConstArrayRef<ui64> messageLockAttemptsValues,
    TConstArrayRef<ui64> messageLockingDurationValues,
    TConstArrayRef<ui64> waitingLockingDurationValues,
    ui64 deletedByMovedToDlq
) {
    const ui64 messagesCount = GetLabeledMetric(clientLabeledCounters, METRIC_TOTAL_COMMIT_MESSAGE_LAG);
    const ui64 inflightMessagesCount = GetLabeledMetric(mlpConsumerLabeledCounters, METRIC_INFLIGHT_LOCKED_COUNT);
    const ui64 committedReadLagMs = GetTimelagMetricMs(clientLabeledCounters, METRIC_COMMIT_WRITE_TIME);

    if (Counters_.MessagesCount) {
        Counters_.MessagesCount->Set(messagesCount);
    }
    if (Counters_.InflyMessagesCount) {
        Counters_.InflyMessagesCount->Set(inflightMessagesCount);
    }
    if (Counters_.OldestMessageAgeSeconds) {
        const ui64 oldestMessageAge = Backend_ == ETopicSqsCountersBackend::Sqs
            ? committedReadLagMs / 1000
            : committedReadLagMs;
        Counters_.OldestMessageAgeSeconds->Set(oldestMessageAge);
    }
    SetHistogram(Counters_.MessageReceiveAttempts, messageLockAttemptsValues, MLP_LOCKS_BOUNDS);
    SetHistogram(Counters_.ClientMessageProcessing_Duration, messageLockingDurationValues, SLOW_LATENCY_BOUNDS);
    SetHistogram(Counters_.MessageReside_Duration, waitingLockingDurationValues, SLOW_LATENCY_BOUNDS);
    if (Counters_.MessagesMovedToDLQ) {
        Counters_.MessagesMovedToDLQ->Set(deletedByMovedToDlq);
    }
    if (Counters_.MessagesPurged) {
        const ui64 purgedMessageCount = GetLabeledMetric(mlpConsumerLabeledCounters, METRIC_PURGED_COUNT);
        const ui64 delta = purgedMessageCount >= PreviousPurgedMessageCount_
            ? purgedMessageCount - PreviousPurgedMessageCount_
            : purgedMessageCount;
        if (delta > 0) {
            Counters_.MessagesPurged->Add(delta);
        }
        PreviousPurgedMessageCount_ = purgedMessageCount;
    }
    FlushPendingActionMetrics();
}

void TTopicSqsMetricsHandler::AddActionMetrics(const NKikimrPQ::TEvTopicSqsActionMetrics& metrics) {
    PendingSendMessageCount_ += metrics.GetSendMessageCount();
    PendingBytesWritten_ += metrics.GetBytesWritten();
    PendingDeduplicationCount_ += metrics.GetDeduplicationCount();
    PendingDeleteMessageCount_ += metrics.GetDeleteMessageCount();
    PendingReceiveMessageCount_ += metrics.GetReceiveMessageCount();
    PendingReceiveMessageBytesRead_ += metrics.GetReceiveMessageBytesRead();
    PendingReceiveMessageEmptyCount_ += metrics.GetReceiveMessageEmptyCount();
}

void TTopicSqsMetricsHandler::FlushPendingActionMetrics() {
    if (PendingSendMessageCount_ > 0 && Counters_.SendMessage_Count) {
        Counters_.SendMessage_Count->Add(PendingSendMessageCount_);
    }
    if (PendingBytesWritten_ > 0 && Counters_.SendMessage_BytesWritten) {
        Counters_.SendMessage_BytesWritten->Add(PendingBytesWritten_);
    }
    if (PendingDeduplicationCount_ > 0 && Counters_.SendMessage_DeduplicationCount) {
        Counters_.SendMessage_DeduplicationCount->Add(PendingDeduplicationCount_);
    }
    if (PendingDeleteMessageCount_ > 0 && Counters_.DeleteMessage_Count) {
        Counters_.DeleteMessage_Count->Add(PendingDeleteMessageCount_);
    }
    if (PendingReceiveMessageCount_ > 0 && Counters_.ReceiveMessage_Count) {
        Counters_.ReceiveMessage_Count->Add(PendingReceiveMessageCount_);
    }
    if (PendingReceiveMessageBytesRead_ > 0 && Counters_.ReceiveMessage_BytesRead) {
        Counters_.ReceiveMessage_BytesRead->Add(PendingReceiveMessageBytesRead_);
    }
    if (PendingReceiveMessageEmptyCount_ > 0 && Counters_.ReceiveMessage_EmptyCount) {
        Counters_.ReceiveMessage_EmptyCount->Add(PendingReceiveMessageEmptyCount_);
    }

    PendingSendMessageCount_ = 0;
    PendingBytesWritten_ = 0;
    PendingDeduplicationCount_ = 0;
    PendingDeleteMessageCount_ = 0;
    PendingReceiveMessageCount_ = 0;
    PendingReceiveMessageBytesRead_ = 0;
    PendingReceiveMessageEmptyCount_ = 0;
}

}
