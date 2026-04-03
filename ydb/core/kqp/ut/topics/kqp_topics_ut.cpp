#include <ydb/core/kqp/topics/kqp_topics.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpTopics) {

static
bool ShouldSkipConflictCheck(bool skipConflictCheck, bool trackProducerId) {
    return skipConflictCheck && !trackProducerId;
}

static
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

static
void AddWriteOperation(NTopic::TTopicOperations& ops,
                       const TString& topic, ui32 partition,
                       const ui32 supportivePartition)
{
    ops.AddOperation(topic, partition,
                     supportivePartition);
}

static
void AssertReadOperation(const NTopic::TTopicOperationTransactions& txs,
                         const ui64 tabletId,
                         const size_t opsCount,
                         const size_t opIndex)
{
    UNIT_ASSERT(txs.contains(tabletId));
    const auto& t = txs.at(tabletId);
    UNIT_ASSERT_EQUAL(t.tx.OperationsSize(), opsCount);
    UNIT_ASSERT_LT(opIndex, opsCount);
    const auto& readOp = t.tx.GetOperations(opIndex);
    UNIT_ASSERT(not readOp.HasSkipConflictCheck());
}

static
void AssertWriteOperation(const NTopic::TTopicOperationTransactions& txs,
                          const ui64 tabletId,
                          const size_t opsCount,
                          const size_t opIndex,
                          const bool skipConflictCheck)
{
    UNIT_ASSERT(txs.contains(tabletId));
    const auto& t = txs.at(tabletId);
    UNIT_ASSERT(t.hasWrite);
    UNIT_ASSERT_EQUAL(t.tx.OperationsSize(), opsCount);
    UNIT_ASSERT_LT(opIndex, opsCount);
    const auto& writeOp = t.tx.GetOperations(opIndex);
    UNIT_ASSERT(writeOp.HasSkipConflictCheck());
    UNIT_ASSERT_EQUAL(writeOp.GetSkipConflictCheck(), skipConflictCheck);
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

    NTopic::TTopicOperationTransactions txs;
    topicOps.BuildTopicTxs(txs);
    UNIT_ASSERT_EQUAL(txs.size(), 1);

    AssertReadOperation(txs, TABLETID, 1, 0);
}

static
void TestReadWriteOperations(bool skipConflictCheck, bool trackProducerId) {
    const TString TOPIC_A = "topic_A";
    const TString TOPIC_B = "topic_B";
    const ui32 PARTITION = 0;
    const ui64 TABLETID_A = 1'000'000;
    const ui64 TABLETID_B = 2'000'000;

    const bool shouldSkip = ShouldSkipConflictCheck(skipConflictCheck, trackProducerId);

    NTopic::TTopicOperations topicOps;

    topicOps.SetSkipConflictCheck(skipConflictCheck);
    topicOps.SetTrackProducerId(trackProducerId);

    AddReadOperation(topicOps, TOPIC_A, PARTITION, 100, 105, "consumer");
    AddWriteOperation(topicOps, TOPIC_B, PARTITION, 100'001);

    topicOps.SetTabletId(TOPIC_A, PARTITION, TABLETID_A);
    topicOps.SetTabletId(TOPIC_B, PARTITION, TABLETID_B);

    const auto recvTabletIds = topicOps.GetReceivingTabletIds();
    UNIT_ASSERT_EQUAL(recvTabletIds.size(), 2);
    UNIT_ASSERT(recvTabletIds.contains(TABLETID_A));
    UNIT_ASSERT(recvTabletIds.contains(TABLETID_B));

    const auto sendTabletIds = topicOps.GetSendingTabletIds();
    size_t expectedSendCount = shouldSkip ? 1 : 2;
    UNIT_ASSERT_EQUAL(sendTabletIds.size(), expectedSendCount);
    UNIT_ASSERT(sendTabletIds.contains(TABLETID_A));
    if (!shouldSkip) {
        UNIT_ASSERT(sendTabletIds.contains(TABLETID_B));
    }

    NTopic::TTopicOperationTransactions txs;
    topicOps.BuildTopicTxs(txs);
    UNIT_ASSERT_EQUAL(txs.size(), 2);

    AssertReadOperation(txs, TABLETID_A, 1, 0);
    AssertWriteOperation(txs, TABLETID_B, 1, 0, shouldSkip);
}

Y_UNIT_TEST(ReadWriteOperations_NoSkipConflictCheck_NoTrackProducerId) {
    TestReadWriteOperations(false, false);
}

Y_UNIT_TEST(ReadWriteOperations_NoSkipConflictCheck_TrackProducerId) {
    TestReadWriteOperations(false, true);
}

Y_UNIT_TEST(ReadWriteOperations_SkipConflictCheck_NoTrackProducerId) {
    TestReadWriteOperations(true, false);
}

Y_UNIT_TEST(ReadWriteOperations_SkipConflictCheck_TrackProducerId) {
    TestReadWriteOperations(true, true);
}

static
void TestOnlyWriteOperations(bool skipConflictCheck, bool trackProducerId) {
    const TString TOPIC = "topic";
    const ui32 PARTITION = 0;
    const ui64 TABLETID = 1'000'000;

    const bool shouldSkip = ShouldSkipConflictCheck(skipConflictCheck, trackProducerId);

    NTopic::TTopicOperations topicOps;

    topicOps.SetSkipConflictCheck(skipConflictCheck);
    topicOps.SetTrackProducerId(trackProducerId);

    AddWriteOperation(topicOps, TOPIC, PARTITION, 100'001);

    topicOps.SetTabletId(TOPIC, PARTITION, TABLETID);

    const auto recvTabletIds = topicOps.GetReceivingTabletIds();
    UNIT_ASSERT_EQUAL(recvTabletIds.size(), 1);
    UNIT_ASSERT(recvTabletIds.contains(TABLETID));

    const auto sendTabletIds = topicOps.GetSendingTabletIds();
    size_t expectedSendCount = shouldSkip ? 0 : 1;
    UNIT_ASSERT_EQUAL(sendTabletIds.size(), expectedSendCount);
    if (!shouldSkip) {
        UNIT_ASSERT(sendTabletIds.contains(TABLETID));
    }

    NTopic::TTopicOperationTransactions txs;
    topicOps.BuildTopicTxs(txs);
    UNIT_ASSERT_EQUAL(txs.size(), 1);

    AssertWriteOperation(txs, TABLETID, 1, 0, shouldSkip);
}

Y_UNIT_TEST(OnlyWriteOperations_NoSkipConflictCheck_NoTrackProducerId) {
    TestOnlyWriteOperations(false, false);
}

Y_UNIT_TEST(OnlyWriteOperations_NoSkipConflictCheck_TrackProducerId) {
    TestOnlyWriteOperations(false, true);
}

Y_UNIT_TEST(OnlyWriteOperations_SkipConflictCheck_NoTrackProducerId) {
    TestOnlyWriteOperations(true, false);
}

Y_UNIT_TEST(OnlyWriteOperations_SkipConflictCheck_TrackProducerId) {
    TestOnlyWriteOperations(true, true);
}

static
void TestReadWriteOperationsOnePartition(bool skipConflictCheck, bool trackProducerId) {
    const TString TOPIC = "topic";
    const ui32 PARTITION = 0;
    const ui64 TABLETID = 1'000'000;

    const bool shouldSkip = ShouldSkipConflictCheck(skipConflictCheck, trackProducerId);

    NTopic::TTopicOperations topicOps;

    topicOps.SetSkipConflictCheck(skipConflictCheck);
    topicOps.SetTrackProducerId(trackProducerId);

    AddReadOperation(topicOps, TOPIC, PARTITION, 100, 105, "consumer");
    AddWriteOperation(topicOps, TOPIC, PARTITION, 100'001);

    topicOps.SetTabletId(TOPIC, PARTITION, TABLETID);

    const auto recvTabletIds = topicOps.GetReceivingTabletIds();
    UNIT_ASSERT_EQUAL(recvTabletIds.size(), 1);
    UNIT_ASSERT(recvTabletIds.contains(TABLETID));

    const auto sendTabletIds = topicOps.GetSendingTabletIds();
    UNIT_ASSERT_EQUAL(sendTabletIds.size(), 1);
    UNIT_ASSERT(sendTabletIds.contains(TABLETID));

    NTopic::TTopicOperationTransactions txs;
    topicOps.BuildTopicTxs(txs);
    UNIT_ASSERT_EQUAL(txs.size(), 1);

    AssertReadOperation(txs, TABLETID, 2, 0);
    AssertWriteOperation(txs, TABLETID, 2, 1, shouldSkip);
}

Y_UNIT_TEST(ReadWriteOperations_OnePartition_NoSkipConflictCheck_NoTrackProducerId) {
    TestReadWriteOperationsOnePartition(false, false);
}

Y_UNIT_TEST(ReadWriteOperations_OnePartition_NoSkipConflictCheck_TrackProducerId) {
    TestReadWriteOperationsOnePartition(false, true);
}

Y_UNIT_TEST(ReadWriteOperations_OnePartition_SkipConflictCheck_NoTrackProducerId) {
    TestReadWriteOperationsOnePartition(true, false);
}

Y_UNIT_TEST(ReadWriteOperations_OnePartition_SkipConflictCheck_TrackProducerId) {
    TestReadWriteOperationsOnePartition(true, true);
}

}

}
