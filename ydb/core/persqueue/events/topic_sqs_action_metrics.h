#pragma once

#include <ydb/core/persqueue/events/internal/protos/events.pb.h>

namespace NKikimr::NPQ {

inline bool HasTopicSqsProxyActionMetrics(const NKikimrPQ::TEvTopicSqsActionMetrics& metrics) {
    switch (metrics.GetActionCase()) {
    case NKikimrPQ::TEvTopicSqsActionMetrics::kChangeMessageVisibility:
    case NKikimrPQ::TEvTopicSqsActionMetrics::kChangeMessageVisibilityBatch:
    case NKikimrPQ::TEvTopicSqsActionMetrics::kGetQueueAttributes:
    case NKikimrPQ::TEvTopicSqsActionMetrics::kGetQueueUrl:
    case NKikimrPQ::TEvTopicSqsActionMetrics::kPurgeQueue:
    case NKikimrPQ::TEvTopicSqsActionMetrics::kSetQueueAttributes:
    case NKikimrPQ::TEvTopicSqsActionMetrics::kListDeadLetterSourceQueues:
    case NKikimrPQ::TEvTopicSqsActionMetrics::kListQueueTags:
    case NKikimrPQ::TEvTopicSqsActionMetrics::kTagQueue:
    case NKikimrPQ::TEvTopicSqsActionMetrics::kUntagQueue:
        return true;
    case NKikimrPQ::TEvTopicSqsActionMetrics::kDeleteMessage: {
        const auto& action = metrics.GetDeleteMessage();
        return action.HasErrorsCount() || action.HasDurationMs();
    }
    case NKikimrPQ::TEvTopicSqsActionMetrics::kDeleteMessageBatch: {
        const auto& action = metrics.GetDeleteMessageBatch();
        return action.HasErrorsCount() || action.HasDurationMs();
    }
    case NKikimrPQ::TEvTopicSqsActionMetrics::kReceiveMessage: {
        const auto& action = metrics.GetReceiveMessage();
        return action.HasErrorsCount() || action.HasDurationMs() || action.HasWorkingDurationMs();
    }
    case NKikimrPQ::TEvTopicSqsActionMetrics::kSendMessage: {
        const auto& action = metrics.GetSendMessage();
        return action.HasErrorsCount() || action.HasDurationMs();
    }
    case NKikimrPQ::TEvTopicSqsActionMetrics::kSendMessageBatch: {
        const auto& action = metrics.GetSendMessageBatch();
        return action.HasErrorsCount() || action.HasDurationMs();
    }
    case NKikimrPQ::TEvTopicSqsActionMetrics::ACTION_NOT_SET:
        return false;
    }
}

inline bool HasTopicSqsMessageMetrics(const NKikimrPQ::TEvTopicSqsActionMetrics& metrics) {
    switch (metrics.GetActionCase()) {
    case NKikimrPQ::TEvTopicSqsActionMetrics::kDeleteMessage:
        return metrics.GetDeleteMessage().GetDeleteMessageCount() > 0;
    case NKikimrPQ::TEvTopicSqsActionMetrics::kDeleteMessageBatch:
        return metrics.GetDeleteMessageBatch().GetDeleteMessageCount() > 0;
    case NKikimrPQ::TEvTopicSqsActionMetrics::kReceiveMessage: {
        const auto& action = metrics.GetReceiveMessage();
        return action.GetReceiveMessageCount() > 0
            || action.GetReceiveMessageBytesRead() > 0
            || action.GetReceiveMessageEmptyCount() > 0;
    }
    case NKikimrPQ::TEvTopicSqsActionMetrics::kSendMessage: {
        const auto& action = metrics.GetSendMessage();
        return action.GetSendMessageCount() > 0
            || action.GetBytesWritten() > 0
            || action.GetDeduplicationCount() > 0;
    }
    case NKikimrPQ::TEvTopicSqsActionMetrics::kSendMessageBatch: {
        const auto& action = metrics.GetSendMessageBatch();
        return action.GetSendMessageCount() > 0
            || action.GetBytesWritten() > 0
            || action.GetDeduplicationCount() > 0;
    }
    default:
        return false;
    }
}

inline bool HasTopicSqsActionMetrics(const NKikimrPQ::TEvTopicSqsActionMetrics& metrics) {
    return HasTopicSqsProxyActionMetrics(metrics) || HasTopicSqsMessageMetrics(metrics);
}

} // namespace NKikimr::NPQ
