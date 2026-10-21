#include <ydb/core/persqueue/events/topic_sqs_action_metrics.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {

Y_UNIT_TEST_SUITE(TopicSqsActionMetricsTest) {
    Y_UNIT_TEST(EmptyMetricsHaveNoPayload) {
        NKikimrPQ::TEvTopicSqsActionMetrics metrics;
        UNIT_ASSERT(!HasTopicSqsActionMetrics(metrics));
        UNIT_ASSERT(!HasTopicSqsProxyActionMetrics(metrics));
        UNIT_ASSERT(!HasTopicSqsMessageMetrics(metrics));
    }

    Y_UNIT_TEST(ProxyOnlyActionsAreDetected) {
        {
            NKikimrPQ::TEvTopicSqsActionMetrics metrics;
            metrics.MutableGetQueueAttributes()->SetDurationMs(10);
            UNIT_ASSERT(HasTopicSqsProxyActionMetrics(metrics));
            UNIT_ASSERT(HasTopicSqsActionMetrics(metrics));
            UNIT_ASSERT(!HasTopicSqsMessageMetrics(metrics));
        }
        {
            NKikimrPQ::TEvTopicSqsActionMetrics metrics;
            metrics.MutablePurgeQueue()->SetDurationMs(5);
            UNIT_ASSERT(HasTopicSqsActionMetrics(metrics));
        }
        {
            NKikimrPQ::TEvTopicSqsActionMetrics metrics;
            metrics.MutableChangeMessageVisibility()->SetDurationMs(1);
            UNIT_ASSERT(HasTopicSqsActionMetrics(metrics));
        }
    }

    Y_UNIT_TEST(SendReceiveMessageMetricsAreDetected) {
        {
            NKikimrPQ::TEvTopicSqsActionMetrics metrics;
            auto* action = metrics.MutableSendMessage();
            action->SetSendMessageCount(1);
            action->SetBytesWritten(42);
            UNIT_ASSERT(HasTopicSqsMessageMetrics(metrics));
            UNIT_ASSERT(HasTopicSqsActionMetrics(metrics));
        }
        {
            NKikimrPQ::TEvTopicSqsActionMetrics metrics;
            auto* action = metrics.MutableReceiveMessage();
            action->SetReceiveMessageEmptyCount(1);
            UNIT_ASSERT(HasTopicSqsMessageMetrics(metrics));
            UNIT_ASSERT(HasTopicSqsActionMetrics(metrics));
        }
        {
            NKikimrPQ::TEvTopicSqsActionMetrics metrics;
            auto* action = metrics.MutableReceiveMessage();
            action->SetDurationMs(3);
            UNIT_ASSERT(HasTopicSqsProxyActionMetrics(metrics));
            UNIT_ASSERT(HasTopicSqsActionMetrics(metrics));
        }
    }

    Y_UNIT_TEST(DeleteMessageRequiresCountOrDuration) {
        {
            NKikimrPQ::TEvTopicSqsActionMetrics metrics;
            metrics.MutableDeleteMessage();
            UNIT_ASSERT(!HasTopicSqsProxyActionMetrics(metrics));
            UNIT_ASSERT(!HasTopicSqsMessageMetrics(metrics));
            UNIT_ASSERT(!HasTopicSqsActionMetrics(metrics));
        }
        {
            NKikimrPQ::TEvTopicSqsActionMetrics metrics;
            metrics.MutableDeleteMessage()->SetDurationMs(7);
            UNIT_ASSERT(HasTopicSqsProxyActionMetrics(metrics));
            UNIT_ASSERT(HasTopicSqsActionMetrics(metrics));
        }
        {
            NKikimrPQ::TEvTopicSqsActionMetrics metrics;
            metrics.MutableDeleteMessage()->SetDeleteMessageCount(1);
            UNIT_ASSERT(HasTopicSqsMessageMetrics(metrics));
            UNIT_ASSERT(HasTopicSqsActionMetrics(metrics));
        }
    }
}

} // namespace NKikimr::NPQ
