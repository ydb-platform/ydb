#include <ydb/core/persqueue/pqrb/read_balancer__metrics.h>
#include <ydb/core/persqueue/pqrb/read_balancer__sqs_metrics.h>
#include <ydb/core/protos/pqconfig.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {

Y_UNIT_TEST_SUITE(TPqrbMetrics) {

Y_UNIT_TEST(SqsMetricsApplicableRequiresExportFlagAndQueueName) {
    NKikimrPQ::TPQTabletConfig config;
    UNIT_ASSERT(!TTopicSqsMetricsHandler::IsApplicable(config));

    config.SetSqsExportMetrics(true);
    UNIT_ASSERT(!TTopicSqsMetricsHandler::IsApplicable(config));

    config.SetSqsQueueName("q");
    UNIT_ASSERT(TTopicSqsMetricsHandler::IsApplicable(config));

    config.SetSqsExportMetrics(false);
    UNIT_ASSERT(!TTopicSqsMetricsHandler::IsApplicable(config));
}

Y_UNIT_TEST(InitializePartitionsAggregatesTopicMetrics) {
    TTopicMetricsHandler handler;
    handler.InitializePartitions(0, 10, 1);
    handler.InitializePartitions(1, 25, 4);

    UNIT_ASSERT_VALUES_EQUAL(handler.GetTopicMetrics().TotalDataSize, 35u);
    UNIT_ASSERT_VALUES_EQUAL(handler.GetTopicMetrics().TotalUsedReserveSize, 5u);
    UNIT_ASSERT_VALUES_EQUAL(handler.GetPartitionMetrics().size(), 2u);
    UNIT_ASSERT_VALUES_EQUAL(handler.GetPartitionMetrics().at(1).DataSize, 25u);
}

} // Y_UNIT_TEST_SUITE(TPqrbMetrics)

} // namespace NKikimr::NPQ
