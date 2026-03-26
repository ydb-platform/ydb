#include <ydb/core/kqp/topics/kqp_topics.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpTopics) {

void AddReadOperation(NTopic::TTopicOperations& ops,
                      const TString& topic, ui32 partition,
                      const ui64 begin, const ui64 end,
                      const TString& consumer)
{
    NKikimrKqp::TTopicOperationsRequest_TopicOffsets_PartitionOffsets_OffsetsRange range;
    range.set_start(begin);
    range.set_end(end);

    ops.AddOperation(topic, partition,
                     consumer,
                     range,
                     false,
                     false,
                     false,
                     "read-session");
}

void AddWriteOperation(NTopic::TTopicOperations& ops,
                       const TString& topic, ui32 partition,
                       const ui32 supportivePartition)
{
    ops.AddOperation(topic, partition,
                     supportivePartition);
}

Y_UNIT_TEST(OnlyReadOperations) {
    const TString TOPIC = "topic";
    const ui32 PARTITION = 0;
    const ui64 TABLETID = 1'000'000;

    NTopic::TTopicOperations topicOps;

    topicOps.SetSkipConflictCheck(false);

    AddReadOperation(topicOps, TOPIC, PARTITION, 100, 105, "consumer");
    topicOps.SetTabletId(TOPIC, PARTITION, TABLETID);

    const auto recvTabletIds = topicOps.GetReceivingTabletIds();
    UNIT_ASSERT_EQUAL(recvTabletIds.size(), 1);
    UNIT_ASSERT(recvTabletIds.contains(TABLETID));

    const auto sendTabletIds = topicOps.GetSendingTabletIds();
    UNIT_ASSERT_EQUAL(sendTabletIds.size(), 1);
    UNIT_ASSERT(sendTabletIds.contains(TABLETID));
}

Y_UNIT_TEST(ReadWriteOperations) {
    const TString TOPIC_A = "topic_A";
    const TString TOPIC_B = "topic_B";
    const ui32 PARTITION = 0;
    const ui64 TABLETID_A = 1'000'000;
    const ui64 TABLETID_B = 2'000'000;

    NTopic::TTopicOperations topicOps;

    topicOps.SetSkipConflictCheck(false);

    AddReadOperation(topicOps, TOPIC_A, PARTITION, 100, 105, "consumer");
    AddWriteOperation(topicOps, TOPIC_B, PARTITION, 100'001);

    topicOps.SetTabletId(TOPIC_A, PARTITION, TABLETID_A);
    topicOps.SetTabletId(TOPIC_B, PARTITION, TABLETID_B);

    const auto recvTabletIds = topicOps.GetReceivingTabletIds();
    UNIT_ASSERT_EQUAL(recvTabletIds.size(), 2);
    UNIT_ASSERT(recvTabletIds.contains(TABLETID_A));
    UNIT_ASSERT(recvTabletIds.contains(TABLETID_B));

    const auto sendTabletIds = topicOps.GetSendingTabletIds();
    UNIT_ASSERT_EQUAL(sendTabletIds.size(), 2);
    UNIT_ASSERT(sendTabletIds.contains(TABLETID_A));
    UNIT_ASSERT(sendTabletIds.contains(TABLETID_B));
}

Y_UNIT_TEST(OnlyWriteOperations) {
    const TString TOPIC = "topic";
    const ui32 PARTITION = 0;
    const ui64 TABLETID = 1'000'000;

    NTopic::TTopicOperations topicOps;

    topicOps.SetSkipConflictCheck(false);

    AddWriteOperation(topicOps, TOPIC, PARTITION, 100'001);

    topicOps.SetTabletId(TOPIC, PARTITION, TABLETID);

    const auto recvTabletIds = topicOps.GetReceivingTabletIds();
    UNIT_ASSERT_EQUAL(recvTabletIds.size(), 1);
    UNIT_ASSERT(recvTabletIds.contains(TABLETID));

    const auto sendTabletIds = topicOps.GetSendingTabletIds();
    UNIT_ASSERT_EQUAL(sendTabletIds.size(), 1);
    UNIT_ASSERT(sendTabletIds.contains(TABLETID));
}

Y_UNIT_TEST(ReadWriteOperations_SkipConflictCheck) {
    const TString TOPIC_A = "topic_A";
    const TString TOPIC_B = "topic_B";
    const ui32 PARTITION = 0;
    const ui64 TABLETID_A = 1'000'000;
    const ui64 TABLETID_B = 2'000'000;

    NTopic::TTopicOperations topicOps;

    topicOps.SetSkipConflictCheck(true);

    AddReadOperation(topicOps, TOPIC_A, PARTITION, 100, 105, "consumer");
    AddWriteOperation(topicOps, TOPIC_B, PARTITION, 100'001);

    topicOps.SetTabletId(TOPIC_A, PARTITION, TABLETID_A);
    topicOps.SetTabletId(TOPIC_B, PARTITION, TABLETID_B);

    const auto recvTabletIds = topicOps.GetReceivingTabletIds();
    UNIT_ASSERT_EQUAL(recvTabletIds.size(), 2);
    UNIT_ASSERT(recvTabletIds.contains(TABLETID_A));
    UNIT_ASSERT(recvTabletIds.contains(TABLETID_B));

    const auto sendTabletIds = topicOps.GetSendingTabletIds();
    UNIT_ASSERT_EQUAL(sendTabletIds.size(), 1);
    UNIT_ASSERT(sendTabletIds.contains(TABLETID_A));
}

Y_UNIT_TEST(OnlyWriteOperations_SkipConflictCheck) {
    const TString TOPIC = "topic";
    const ui32 PARTITION = 0;
    const ui64 TABLETID = 1'000'000;

    NTopic::TTopicOperations topicOps;

    topicOps.SetSkipConflictCheck(true);

    AddWriteOperation(topicOps, TOPIC, PARTITION, 100'001);

    topicOps.SetTabletId(TOPIC, PARTITION, TABLETID);

    const auto recvTabletIds = topicOps.GetReceivingTabletIds();
    UNIT_ASSERT_EQUAL(recvTabletIds.size(), 1);
    UNIT_ASSERT(recvTabletIds.contains(TABLETID));

    const auto sendTabletIds = topicOps.GetSendingTabletIds();
    UNIT_ASSERT(sendTabletIds.empty());
}

Y_UNIT_TEST(ReadWriteOperations_OnePartition) {
    const TString TOPIC = "topic";
    const ui32 PARTITION = 0;
    const ui64 TABLETID = 1'000'000;

    NTopic::TTopicOperations topicOps;

    topicOps.SetSkipConflictCheck(false);

    AddReadOperation(topicOps, TOPIC, PARTITION, 100, 105, "consumer");
    AddWriteOperation(topicOps, TOPIC, PARTITION, 100'001);

    topicOps.SetTabletId(TOPIC, PARTITION, TABLETID);

    const auto recvTabletIds = topicOps.GetReceivingTabletIds();
    UNIT_ASSERT_EQUAL(recvTabletIds.size(), 1);
    UNIT_ASSERT(recvTabletIds.contains(TABLETID));

    const auto sendTabletIds = topicOps.GetSendingTabletIds();
    UNIT_ASSERT_EQUAL(sendTabletIds.size(), 1);
    UNIT_ASSERT(sendTabletIds.contains(TABLETID));
}

Y_UNIT_TEST(ReadWriteOperations_OnePartition_SkipConflictCheck) {
    const TString TOPIC = "topic";
    const ui32 PARTITION = 0;
    const ui64 TABLETID = 1'000'000;

    NTopic::TTopicOperations topicOps;

    topicOps.SetSkipConflictCheck(true);

    AddReadOperation(topicOps, TOPIC, PARTITION, 100, 105, "consumer");
    AddWriteOperation(topicOps, TOPIC, PARTITION, 100'001);

    topicOps.SetTabletId(TOPIC, PARTITION, TABLETID);

    const auto recvTabletIds = topicOps.GetReceivingTabletIds();
    UNIT_ASSERT_EQUAL(recvTabletIds.size(), 1);
    UNIT_ASSERT(recvTabletIds.contains(TABLETID));

    const auto sendTabletIds = topicOps.GetSendingTabletIds();
    UNIT_ASSERT_EQUAL(sendTabletIds.size(), 1);
    UNIT_ASSERT(sendTabletIds.contains(TABLETID));
}

}

}
