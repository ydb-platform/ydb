#include <ydb/core/kqp/topics/kqp_topics.h>
#include <ydb/core/kafka_proxy/kafka_producer_instance_id.h>
#include <ydb/core/protos/kqp.pb.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/yexception.h>

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
                         const size_t opIndex,
                         const TString& consumer,
                         ui64 begin,
                         ui64 end,
                         bool forceCommit,
                         bool killReadSession,
                         bool onlyCheckCommitedToFinish,
                         const TString& readSessionId)
{
    UNIT_ASSERT(txs.contains(tabletId));
    const auto& t = txs.at(tabletId);
    UNIT_ASSERT_EQUAL(t.tx.OperationsSize(), opsCount);
    UNIT_ASSERT_LT(opIndex, opsCount);
    const auto& readOp = t.tx.GetOperations(opIndex);
    UNIT_ASSERT(not readOp.HasSkipConflictCheck());

    UNIT_ASSERT(readOp.HasRead());
    UNIT_ASSERT(readOp.GetRead().HasTopic());
    const auto& topicRead = readOp.GetRead().GetTopic();
    UNIT_ASSERT_EQUAL(topicRead.GetConsumer(), consumer);
    UNIT_ASSERT_EQUAL(topicRead.GetCommitOffsetsBegin(), begin);
    UNIT_ASSERT_EQUAL(topicRead.GetCommitOffsetsEnd(), end);
    UNIT_ASSERT_EQUAL(topicRead.GetForceCommit(), forceCommit);
    UNIT_ASSERT_EQUAL(topicRead.GetKillReadSession(), killReadSession);
    UNIT_ASSERT_EQUAL(topicRead.GetOnlyCheckCommitedToFinish(), onlyCheckCommitedToFinish);
    UNIT_ASSERT_EQUAL(topicRead.GetReadSessionId(), readSessionId);

    UNIT_ASSERT_EQUAL(readOp.GetConsumer(), consumer);
    UNIT_ASSERT_EQUAL(readOp.GetCommitOffsetsBegin(), begin);
    UNIT_ASSERT_EQUAL(readOp.GetCommitOffsetsEnd(), end);
    UNIT_ASSERT_EQUAL(readOp.GetForceCommit(), forceCommit);
    UNIT_ASSERT_EQUAL(readOp.GetKillReadSession(), killReadSession);
    UNIT_ASSERT_EQUAL(readOp.GetOnlyCheckCommitedToFinish(), onlyCheckCommitedToFinish);
    UNIT_ASSERT_EQUAL(readOp.GetReadSessionId(), readSessionId);
}

static
void AssertKafkaReadOperation(const NTopic::TTopicOperationTransactions& txs,
                                const ui64 tabletId,
                                const size_t opsCount,
                                const size_t opIndex,
                                const TString& consumer,
                                ui64 commitOffsetEnd)
{
    UNIT_ASSERT(txs.contains(tabletId));
    const auto& t = txs.at(tabletId);
    UNIT_ASSERT_EQUAL(t.tx.OperationsSize(), opsCount);
    UNIT_ASSERT_LT(opIndex, opsCount);
    const auto& readOp = t.tx.GetOperations(opIndex);

    UNIT_ASSERT(readOp.HasRead());
    UNIT_ASSERT(readOp.GetRead().HasKafka());
    const auto& kafkaRead = readOp.GetRead().GetKafka();
    UNIT_ASSERT_EQUAL(kafkaRead.GetConsumer(), consumer);
    UNIT_ASSERT_EQUAL(kafkaRead.GetCommitOffsetsEnd(), commitOffsetEnd);

    UNIT_ASSERT(readOp.GetKafkaTransaction());
    UNIT_ASSERT_EQUAL(readOp.GetConsumer(), consumer);
    UNIT_ASSERT_EQUAL(readOp.GetCommitOffsetsEnd(), commitOffsetEnd);
}

static
void AssertWriteOperation(const NTopic::TTopicOperationTransactions& txs,
                          const ui64 tabletId,
                          const size_t opsCount,
                          const size_t opIndex,
                          const bool skipConflictCheck,
                          TMaybe<ui32> supportivePartition = Nothing())
{
    UNIT_ASSERT(txs.contains(tabletId));
    const auto& t = txs.at(tabletId);
    UNIT_ASSERT(t.hasWrite);
    UNIT_ASSERT_EQUAL(t.tx.OperationsSize(), opsCount);
    UNIT_ASSERT_LT(opIndex, opsCount);
    const auto& writeOp = t.tx.GetOperations(opIndex);
    UNIT_ASSERT(writeOp.HasSkipConflictCheck());
    UNIT_ASSERT_EQUAL(writeOp.GetSkipConflictCheck(), skipConflictCheck);

    UNIT_ASSERT(writeOp.HasWrite());
    UNIT_ASSERT_EQUAL(writeOp.GetWrite().GetSkipConflictCheck(), skipConflictCheck);

    if (supportivePartition.Defined()) {
        UNIT_ASSERT_EQUAL(writeOp.GetSupportivePartition(), *supportivePartition);
        UNIT_ASSERT(writeOp.GetWrite().HasTopic());
        UNIT_ASSERT_EQUAL(writeOp.GetWrite().GetTopic().GetSupportivePartition(), *supportivePartition);
    }
}

static
void AssertKafkaWriteOperation(const NTopic::TTopicOperationTransactions& txs,
                                 const ui64 tabletId,
                                 const size_t opsCount,
                                 const size_t opIndex,
                                 const bool skipConflictCheck,
                                 const NKafka::TProducerInstanceId& producerInstanceId)
{
    UNIT_ASSERT(txs.contains(tabletId));
    const auto& t = txs.at(tabletId);
    UNIT_ASSERT(t.hasWrite);
    UNIT_ASSERT_EQUAL(t.tx.OperationsSize(), opsCount);
    UNIT_ASSERT_LT(opIndex, opsCount);
    const auto& writeOp = t.tx.GetOperations(opIndex);
    UNIT_ASSERT(writeOp.HasSkipConflictCheck());
    UNIT_ASSERT_EQUAL(writeOp.GetSkipConflictCheck(), skipConflictCheck);

    UNIT_ASSERT(writeOp.HasWrite());
    UNIT_ASSERT_EQUAL(writeOp.GetWrite().GetSkipConflictCheck(), skipConflictCheck);
    UNIT_ASSERT(writeOp.GetWrite().HasKafka());

    UNIT_ASSERT(writeOp.GetKafkaTransaction());
    const auto& legacyProducerId = writeOp.GetKafkaProducerInstanceId();
    UNIT_ASSERT_EQUAL(legacyProducerId.GetId(), producerInstanceId.Id);
    UNIT_ASSERT_EQUAL(legacyProducerId.GetEpoch(), producerInstanceId.Epoch);

    const auto& canonicalProducerId = writeOp.GetWrite().GetKafka().GetKafkaProducerInstanceId();
    UNIT_ASSERT_EQUAL(canonicalProducerId.GetId(), producerInstanceId.Id);
    UNIT_ASSERT_EQUAL(canonicalProducerId.GetEpoch(), producerInstanceId.Epoch);
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

    AssertReadOperation(txs, TABLETID, 1, 0, "consumer", 100, 105, false, false, false, "read-session");
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

    AssertReadOperation(txs, TABLETID_A, 1, 0, "consumer", 100, 105, false, false, false, "read-session");
    AssertWriteOperation(txs, TABLETID_B, 1, 0, shouldSkip, 100'001u);
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

    AssertWriteOperation(txs, TABLETID, 1, 0, shouldSkip, 100'001u);
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

    AssertReadOperation(txs, TABLETID, 2, 0, "consumer", 100, 105, false, false, false, "read-session");
    AssertWriteOperation(txs, TABLETID, 2, 1, shouldSkip, 100'001u);
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

Y_UNIT_TEST(ShouldOmitPeerTopicPredicateExchange_WriteOnlySkipConflict) {
    const TString TOPIC = "topic";
    constexpr ui32 SUPPORTIVE_PARTITION_ID_0 = 100'001;
    constexpr ui32 SUPPORTIVE_PARTITION_ID_1 = 100'002;
    NTopic::TTopicOperations topicOps;
    topicOps.SetSkipConflictCheck(true);
    topicOps.SetTrackProducerId(false);

    AddWriteOperation(topicOps, TOPIC, 0, SUPPORTIVE_PARTITION_ID_0);
    topicOps.SetTabletId(TOPIC, 0, 100);
    AddWriteOperation(topicOps, TOPIC, 1, SUPPORTIVE_PARTITION_ID_1);
    topicOps.SetTabletId(TOPIC, 1, 200);

    UNIT_ASSERT(topicOps.ShouldOmitPeerTopicTabletsForPredicateExchange());
}

Y_UNIT_TEST(ShouldOmitPeerTopicPredicateExchange_FalseWhenConsumerReads) {
    const TString TOPIC = "topic";
    constexpr ui32 SUPPORTIVE_PARTITION_ID = 100'001;
    NTopic::TTopicOperations topicOps;
    topicOps.SetSkipConflictCheck(true);
    topicOps.SetTrackProducerId(false);

    AddReadOperation(topicOps, TOPIC, 0, 1, 2, "c");
    topicOps.SetTabletId(TOPIC, 0, 100);
    AddWriteOperation(topicOps, TOPIC, 1, SUPPORTIVE_PARTITION_ID);
    topicOps.SetTabletId(TOPIC, 1, 200);

    UNIT_ASSERT(!topicOps.ShouldOmitPeerTopicTabletsForPredicateExchange());
}

Y_UNIT_TEST(ShouldOmitPeerTopicPredicateExchange_FalseWhenSkipConflictCheckOff) {
    const TString TOPIC = "topic";
    constexpr ui32 SUPPORTIVE_PARTITION_ID_0 = 100'001;
    constexpr ui32 SUPPORTIVE_PARTITION_ID_1 = 100'002;
    NTopic::TTopicOperations topicOps;
    topicOps.SetSkipConflictCheck(false);
    topicOps.SetTrackProducerId(false);

    AddWriteOperation(topicOps, TOPIC, 0, SUPPORTIVE_PARTITION_ID_0);
    topicOps.SetTabletId(TOPIC, 0, 100);
    AddWriteOperation(topicOps, TOPIC, 1, SUPPORTIVE_PARTITION_ID_1);
    topicOps.SetTabletId(TOPIC, 1, 200);

    UNIT_ASSERT(!topicOps.ShouldOmitPeerTopicTabletsForPredicateExchange());
}

Y_UNIT_TEST(ShouldOmitPeerTopicPredicateExchange_FalseWhenTrackProducerId) {
    const TString TOPIC = "topic";
    constexpr ui32 SUPPORTIVE_PARTITION_ID_0 = 100'001;
    constexpr ui32 SUPPORTIVE_PARTITION_ID_1 = 100'002;
    NTopic::TTopicOperations topicOps;
    topicOps.SetSkipConflictCheck(true);
    topicOps.SetTrackProducerId(true);

    AddWriteOperation(topicOps, TOPIC, 0, SUPPORTIVE_PARTITION_ID_0);
    topicOps.SetTabletId(TOPIC, 0, 100);
    AddWriteOperation(topicOps, TOPIC, 1, SUPPORTIVE_PARTITION_ID_1);
    topicOps.SetTabletId(TOPIC, 1, 200);

    UNIT_ASSERT(!topicOps.ShouldOmitPeerTopicTabletsForPredicateExchange());
}

Y_UNIT_TEST(KafkaReadOperation_DualWrite) {
    const TString TOPIC = "topic";
    const ui32 PARTITION = 0;
    const ui64 TABLETID = 1'000'000;
    const TString CONSUMER = "kafka-consumer";
    const ui64 OFFSET = 42;

    NTopic::TTopicOperations topicOps;
    topicOps.AddKafkaApiReadOperation(TOPIC, PARTITION, CONSUMER, OFFSET);
    topicOps.SetTabletId(TOPIC, PARTITION, TABLETID);

    NTopic::TTopicOperationTransactions txs;
    topicOps.BuildTopicTxs(txs);
    UNIT_ASSERT_EQUAL(txs.size(), 1);
    AssertKafkaReadOperation(txs, TABLETID, 1, 0, CONSUMER, OFFSET);
}

Y_UNIT_TEST(KafkaWriteOperation_DualWrite) {
    const TString TOPIC = "topic";
    const ui32 PARTITION = 0;
    const ui64 TABLETID = 1'000'000;
    const NKafka::TProducerInstanceId PRODUCER_ID{7, 3};

    NTopic::TTopicOperations topicOps;
    topicOps.SetSkipConflictCheck(false);
    topicOps.SetTrackProducerId(false);
    topicOps.AddKafkaApiWriteOperation(TOPIC, PARTITION, PRODUCER_ID);
    topicOps.SetTabletId(TOPIC, PARTITION, TABLETID);

    NTopic::TTopicOperationTransactions txs;
    topicOps.BuildTopicTxs(txs);
    UNIT_ASSERT_EQUAL(txs.size(), 1);
    AssertKafkaWriteOperation(txs, TABLETID, 1, 0, false, PRODUCER_ID);
}

static
void AddDeferredPublicationOperation(NTopic::TTopicOperations& ops,
                                     const TString& topic,
                                     ui32 partition,
                                     ui64 tabletId,
                                     NKikimrKqp::TTopicDeferredPublicationRequest::EOp op,
                                     ui64 intPublicationId,
                                     const TString& extPublicationId)
{
    ops.SetTrackProducerId(true);
    ops.AddDeferredPublicationOperation(topic, partition, tabletId, op, intPublicationId, extPublicationId);
}

static
NKikimrPQ::TPartitionOperation::TWriteOp::TDeferredPublicationApi::EOp ExpectedDeferredPublicationOp(
    NKikimrKqp::TTopicDeferredPublicationRequest::EOp op)
{
    switch (op) {
        case NKikimrKqp::TTopicDeferredPublicationRequest::Publish:
            return NKikimrPQ::TPartitionOperation::TWriteOp::TDeferredPublicationApi::Publish;
        case NKikimrKqp::TTopicDeferredPublicationRequest::Cancel:
            return NKikimrPQ::TPartitionOperation::TWriteOp::TDeferredPublicationApi::Cancel;
        case NKikimrKqp::TTopicDeferredPublicationRequest::Unspecified:
            return NKikimrPQ::TPartitionOperation::TWriteOp::TDeferredPublicationApi::Unspecified;
        default:
            return NKikimrPQ::TPartitionOperation::TWriteOp::TDeferredPublicationApi::Unspecified;
    }
}

static
void AssertDeferredPublicationOperation(const NTopic::TTopicOperationTransactions& txs,
                                        ui64 tabletId,
                                        const TString& topic,
                                        ui32 partition,
                                        NKikimrKqp::TTopicDeferredPublicationRequest::EOp op,
                                        bool skipConflictCheck)
{
    UNIT_ASSERT(txs.contains(tabletId));
    const auto& t = txs.at(tabletId);
    UNIT_ASSERT(t.hasWrite);
    UNIT_ASSERT_EQUAL(t.tx.OperationsSize(), 1);

    const auto& writeOp = t.tx.GetOperations(0);
    UNIT_ASSERT_EQUAL(writeOp.GetPath(), topic);
    UNIT_ASSERT_EQUAL(writeOp.GetPartitionId(), partition);
    UNIT_ASSERT(writeOp.HasWrite());
    UNIT_ASSERT(writeOp.GetWrite().HasDeferredPublication());
    UNIT_ASSERT_EQUAL(writeOp.GetWrite().GetDeferredPublication().GetOp(), ExpectedDeferredPublicationOp(op));
    UNIT_ASSERT_EQUAL(writeOp.GetWrite().GetSkipConflictCheck(), skipConflictCheck);
}

Y_UNIT_TEST(DeferredPublication_Publish_SingleDestination) {
    const TString TOPIC = "topic";
    const ui32 PARTITION = 0;
    const ui64 TABLETID = 1'000'000;

    NTopic::TTopicOperations topicOps;
    AddDeferredPublicationOperation(topicOps, TOPIC, PARTITION, TABLETID,
        NKikimrKqp::TTopicDeferredPublicationRequest::Publish, 42, "ext-42");

    UNIT_ASSERT(topicOps.HasDeferredPublicationOperations());
    UNIT_ASSERT(topicOps.HasWriteOperations());
    UNIT_ASSERT(!topicOps.HasWriteId());
    UNIT_ASSERT_VALUES_EQUAL(topicOps.GetDeferredPublicationIntId(), 42u);
    UNIT_ASSERT_VALUES_EQUAL(topicOps.GetDeferredPublicationExtId(), "ext-42");

    NTopic::TTopicOperationTransactions txs;
    topicOps.BuildTopicTxs(txs);
    UNIT_ASSERT_EQUAL(txs.size(), 1);
    AssertDeferredPublicationOperation(txs, TABLETID, TOPIC, PARTITION,
        NKikimrKqp::TTopicDeferredPublicationRequest::Publish, false);
}

Y_UNIT_TEST(DeferredPublication_GetExtIdReturnsEmptyWhenOmitted) {
    NTopic::TTopicOperations topicOps;
    topicOps.AddDeferredPublicationOperation("topic", 0, 1'000'000,
        NKikimrKqp::TTopicDeferredPublicationRequest::Publish, 42, "");

    UNIT_ASSERT(topicOps.HasDeferredPublicationOperations());
    UNIT_ASSERT_VALUES_EQUAL(topicOps.GetDeferredPublicationExtId(), "");
}

Y_UNIT_TEST(DeferredPublication_Cancel_MultipleDestinations) {
    const TString TOPIC_A = "topic_A";
    const TString TOPIC_B = "topic_B";
    const ui64 TABLETID_A = 1'000'000;
    const ui64 TABLETID_B = 2'000'000;

    NTopic::TTopicOperations topicOps;
    AddDeferredPublicationOperation(topicOps, TOPIC_A, 0, TABLETID_A,
        NKikimrKqp::TTopicDeferredPublicationRequest::Cancel, 7, "ext-7");
    AddDeferredPublicationOperation(topicOps, TOPIC_B, 1, TABLETID_B,
        NKikimrKqp::TTopicDeferredPublicationRequest::Cancel, 7, "ext-7");

    const auto recvTabletIds = topicOps.GetReceivingTabletIds();
    UNIT_ASSERT_EQUAL(recvTabletIds.size(), 2);
    UNIT_ASSERT(recvTabletIds.contains(TABLETID_A));
    UNIT_ASSERT(recvTabletIds.contains(TABLETID_B));

    const auto sendTabletIds = topicOps.GetSendingTabletIds();
    UNIT_ASSERT_EQUAL(sendTabletIds.size(), 2);
    UNIT_ASSERT(sendTabletIds.contains(TABLETID_A));
    UNIT_ASSERT(sendTabletIds.contains(TABLETID_B));

    NTopic::TTopicOperationTransactions txs;
    topicOps.BuildTopicTxs(txs);
    UNIT_ASSERT_EQUAL(txs.size(), 2);
    AssertDeferredPublicationOperation(txs, TABLETID_A, TOPIC_A, 0,
        NKikimrKqp::TTopicDeferredPublicationRequest::Cancel, false);
    AssertDeferredPublicationOperation(txs, TABLETID_B, TOPIC_B, 1,
        NKikimrKqp::TTopicDeferredPublicationRequest::Cancel, false);
}

Y_UNIT_TEST(DeferredPublication_GetSendingTabletIds_IncludesWriteOnlyTablet) {
    const TString TOPIC = "topic";
    const ui64 TABLETID = 1'000'000;
    const bool skipConflictCheck = true;
    const bool trackProducerId = false;

    NTopic::TTopicOperations topicOps;
    topicOps.SetSkipConflictCheck(skipConflictCheck);
    topicOps.SetTrackProducerId(trackProducerId);

    topicOps.AddDeferredPublicationOperation(TOPIC, 0, TABLETID,
        NKikimrKqp::TTopicDeferredPublicationRequest::Publish, 1, "ext");

    UNIT_ASSERT(ShouldSkipConflictCheck(skipConflictCheck, trackProducerId));

    const auto sendTabletIds = topicOps.GetSendingTabletIds();
    UNIT_ASSERT_EQUAL(sendTabletIds.size(), 1);
    UNIT_ASSERT(sendTabletIds.contains(TABLETID));
}

Y_UNIT_TEST(DeferredPublication_MergeKeepsPublicationId) {
    NTopic::TTopicOperations lhs;
    AddDeferredPublicationOperation(lhs, "topic_A", 0, 100,
        NKikimrKqp::TTopicDeferredPublicationRequest::Publish, 11, "ext-11");

    NTopic::TTopicOperations rhs;
    AddDeferredPublicationOperation(rhs, "topic_B", 1, 200,
        NKikimrKqp::TTopicDeferredPublicationRequest::Publish, 11, "ext-11");

    lhs.Merge(rhs);

    UNIT_ASSERT(lhs.HasDeferredPublicationOperations());
    UNIT_ASSERT_VALUES_EQUAL(lhs.GetDeferredPublicationIntId(), 11u);
    UNIT_ASSERT_VALUES_EQUAL(lhs.GetDeferredPublicationExtId(), "ext-11");
    UNIT_ASSERT_VALUES_EQUAL(lhs.GetSize(), 2u);
}

Y_UNIT_TEST(DeferredPublication_MergeAllowsMissingExtPublicationId) {
    NTopic::TTopicOperations lhs;
    lhs.AddDeferredPublicationOperation("topic_A", 0, 100,
        NKikimrKqp::TTopicDeferredPublicationRequest::Publish, 11, "");

    NTopic::TTopicOperations rhs;
    AddDeferredPublicationOperation(rhs, "topic_B", 1, 200,
        NKikimrKqp::TTopicDeferredPublicationRequest::Publish, 11, "ext-11");

    lhs.Merge(rhs);

    UNIT_ASSERT_VALUES_EQUAL(lhs.GetDeferredPublicationIntId(), 11u);
    UNIT_ASSERT_VALUES_EQUAL(lhs.GetDeferredPublicationExtId(), "ext-11");

    NTopic::TTopicOperations followUp;
    followUp.AddDeferredPublicationOperation("topic_C", 2, 300,
        NKikimrKqp::TTopicDeferredPublicationRequest::Publish, 11, "");

    lhs.Merge(followUp);
    UNIT_ASSERT_VALUES_EQUAL(lhs.GetDeferredPublicationExtId(), "ext-11");
}

Y_UNIT_TEST(DeferredPublication_MergeConflictsOnDifferentPublicationId) {
    NTopic::TTopicOperations lhs;
    AddDeferredPublicationOperation(lhs, "topic_A", 0, 100,
        NKikimrKqp::TTopicDeferredPublicationRequest::Publish, 11, "ext-11");

    NTopic::TTopicOperations rhs;
    AddDeferredPublicationOperation(rhs, "topic_B", 1, 200,
        NKikimrKqp::TTopicDeferredPublicationRequest::Publish, 22, "ext-22");

    UNIT_ASSERT_EXCEPTION(lhs.Merge(rhs), yexception);
}

Y_UNIT_TEST(DeferredPublication_MergeRejectsTopicWrite) {
    NTopic::TTopicOperations lhs;
    AddDeferredPublicationOperation(lhs, "topic_A", 0, 100,
        NKikimrKqp::TTopicDeferredPublicationRequest::Publish, 11, "ext-11");

    NTopic::TTopicOperations rhs;
    AddWriteOperation(rhs, "topic_B", 1, 100'001);
    rhs.SetTabletId("topic_B", 1, 200);

    UNIT_ASSERT_EXCEPTION(lhs.Merge(rhs), yexception);
}

Y_UNIT_TEST(DeferredPublication_ValidateRejectsUnspecifiedOp) {
    NKikimrKqp::TTopicDeferredPublicationRequest request;
    request.SetOp(NKikimrKqp::TTopicDeferredPublicationRequest::Unspecified);
    request.SetIntPublicationId(1);
    auto* destination = request.AddDestinations();
    destination->SetPath("topic");
    destination->SetPartitionId(0);
    destination->SetTabletId(1);

    UNIT_ASSERT_EXCEPTION(NTopic::ValidateDeferredPublicationRequest(request), yexception);
}

Y_UNIT_TEST(DeferredPublication_ValidateRejectsEmptyDestinations) {
    NKikimrKqp::TTopicDeferredPublicationRequest request;
    request.SetOp(NKikimrKqp::TTopicDeferredPublicationRequest::Publish);
    request.SetIntPublicationId(1);

    UNIT_ASSERT_EXCEPTION(NTopic::ValidateDeferredPublicationRequest(request), yexception);
}

Y_UNIT_TEST(DeferredPublication_ValidateRejectsDestinationWithoutPath) {
    NKikimrKqp::TTopicDeferredPublicationRequest request;
    request.SetOp(NKikimrKqp::TTopicDeferredPublicationRequest::Publish);
    request.SetIntPublicationId(1);
    auto* destination = request.AddDestinations();
    destination->SetPartitionId(0);
    destination->SetTabletId(1);

    UNIT_ASSERT_EXCEPTION(NTopic::ValidateDeferredPublicationRequest(request), yexception);
}

Y_UNIT_TEST(DeferredPublication_ValidateRejectsDestinationWithoutPartitionId) {
    NKikimrKqp::TTopicDeferredPublicationRequest request;
    request.SetOp(NKikimrKqp::TTopicDeferredPublicationRequest::Publish);
    request.SetIntPublicationId(1);
    auto* destination = request.AddDestinations();
    destination->SetPath("topic");
    destination->SetTabletId(1);

    UNIT_ASSERT_EXCEPTION(NTopic::ValidateDeferredPublicationRequest(request), yexception);
}

Y_UNIT_TEST(DeferredPublication_ValidateRejectsDestinationWithoutTabletId) {
    NKikimrKqp::TTopicDeferredPublicationRequest request;
    request.SetOp(NKikimrKqp::TTopicDeferredPublicationRequest::Publish);
    request.SetIntPublicationId(1);
    auto* destination = request.AddDestinations();
    destination->SetPath("topic");
    destination->SetPartitionId(0);

    UNIT_ASSERT_EXCEPTION(NTopic::ValidateDeferredPublicationRequest(request), yexception);
}

}

}
