#include "read_balancer__sqs_metrics.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/counters_pq.pb.h>
#include <ydb/library/actors/core/actor.h>

#include <util/string/builder.h>

namespace NKikimr::NPQ {

namespace {

constexpr TStringBuf SqsSensorLabel = "sensor";
constexpr TStringBuf YmqNameLabel = "name";
constexpr TStringBuf YmqQueueCounterPrefix = "queue.messages.";
constexpr TStringBuf YmqMethodLabel = "method";
constexpr TStringBuf YmqActionCounterPrefix = "api.http.";
constexpr TStringBuf QueryTypeLabel = "query_type";

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

const NMonitoring::TBucketBounds MessageReceiveAttemptsBuckets = {1, 2, 5};

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

const TString QueryTypes[] = {
    "WRITE_MESSAGE_ID",
    "PURGE_QUEUE_ID",
};

ui64 GetMlpConsumerMetric(const TTabletLabeledCountersBase& mlpConsumerMetrics, EMLPConsumerLabeledCounters index) {
    const auto& aggregatedCounters = mlpConsumerMetrics.GetCounters();
    const size_t idx = static_cast<size_t>(index);
    if (aggregatedCounters.Size() <= idx) {
        return 0;
    }
    return aggregatedCounters[idx].Get();
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

void InitTransactionCounters(TTopicTransactionCounters& counters, const NMonitoring::TDynamicCounterPtr& root) {
    auto transactionsByType = root->GetSubgroup(TString(SqsSensorLabel), "TransactionsByType");
    auto transactionsDurationByType = root->GetSubgroup(TString(SqsSensorLabel), "TransactionsDurationsByType");
    auto transactionsFailedByType = root->GetSubgroup(TString(SqsSensorLabel), "TransactionsFailedByType");

    for (const auto& queryType : QueryTypes) {
        transactionsByType->GetExpiringNamedCounter(TString(QueryTypeLabel), queryType, true);
        transactionsFailedByType->GetExpiringNamedCounter(TString(QueryTypeLabel), queryType, true);
        transactionsDurationByType->GetExpiringNamedHistogram(
            TString(QueryTypeLabel), queryType, NMonitoring::ExplicitHistogram(DurationBucketsMs));
    }

    counters.CompileQueryCount = MakeSqsCounter(root, "CompileQueryCount", true, true);
    counters.TransactionsCount = MakeSqsCounter(root, "TransactionsCount", true, true);
    counters.TransactionsInfly = MakeSqsCounter(root, "TransactionsInfly", false, true);
    counters.TransactionRetryTimeouts = MakeSqsCounter(root, "TransactionRetryTimeouts", true, true);
    counters.TransactionRetries = MakeSqsCounter(root, "TransactionRetries", true, true);
    counters.TransactionsFailed = MakeSqsCounter(root, "TransactionsFailed", true, true);
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
    counters.MessageReceiveAttempts = MakeSqsHistogram(root, "MessageReceiveAttempts", MessageReceiveAttemptsBuckets, true);
    counters.ClientMessageProcessing_Duration = MakeSqsHistogram(root, "ClientMessageProcessing_Duration", ClientDurationBucketsMs, true);
    counters.MessageReside_Duration = MakeSqsHistogram(root, "MessageReside_Duration", ClientDurationBucketsMs, true);

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

    InitTransactionCounters(counters.TransactionCounters, root);

    for (const auto& action : SqsQueueProxyActions) {
        InitSqsActionCounters(counters, root, cfg, action);
    }
}

void InitYmqLeaderCounters(
    TTopicQueueLeaderCounters& counters,
    const NMonitoring::TDynamicCounterPtr& root
) {
    counters.MessagesPurged = MakeYmqCounter(root, YmqQueueCounterName("purged_count_per_second"), true);
    counters.MessageReceiveAttempts = MakeYmqHistogram(root, YmqQueueCounterName("receive_attempts_count_rate"), MessageReceiveAttemptsBuckets);
    counters.ClientMessageProcessing_Duration = MakeYmqHistogram(
        root, YmqQueueCounterName("client_processing_duration_milliseconds"), YmqClientDurationBucketsMs);
    counters.MessageReside_Duration = MakeYmqHistogram(
        root, YmqQueueCounterName("reside_duration_milliseconds"), YmqClientDurationBucketsMs);

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

void TTopicSqsMetricsHandler::Update(const TTabletLabeledCountersBase& mlpConsumerMetrics) {
    const ui64 locked = GetMlpConsumerMetric(mlpConsumerMetrics, METRIC_INFLIGHT_LOCKED_COUNT);
    const ui64 delayed = GetMlpConsumerMetric(mlpConsumerMetrics, METRIC_INFLIGHT_DELAYED_COUNT);
    const ui64 unlocked = GetMlpConsumerMetric(mlpConsumerMetrics, METRIC_INFLIGHT_UNLOCKED_COUNT);
    const ui64 scheduledToDlq = GetMlpConsumerMetric(mlpConsumerMetrics, METRIC_INFLIGHT_SCHEDULED_TO_DLQ_COUNT);
    const ui64 storedCount = locked + delayed + unlocked + scheduledToDlq;

    if (Counters_.MessagesCount) {
        Counters_.MessagesCount->Set(storedCount);
    }
    if (Counters_.InflyMessagesCount) {
        Counters_.InflyMessagesCount->Set(locked);
    }
}

}
