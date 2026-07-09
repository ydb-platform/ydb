#include <ydb/core/persqueue/public/pqdata_transaction_compat.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {
namespace {

bool IsWriteTxOperation(const NKikimrPQ::TPartitionOperation& operation)
{
    const bool isRead = operation.HasCommitOffsetsBegin()
        || (operation.GetKafkaTransaction() && operation.HasCommitOffsetsEnd());
    return !isRead;
}

void AssertTopicReadDualWrite(const NKikimrPQ::TPartitionOperation& op)
{
    UNIT_ASSERT(op.HasRead());
    UNIT_ASSERT(op.GetRead().HasTopic());
    const auto& topicRead = op.GetRead().GetTopic();

    UNIT_ASSERT_EQUAL(op.GetConsumer(), topicRead.GetConsumer());
    UNIT_ASSERT_EQUAL(op.GetCommitOffsetsBegin(), topicRead.GetCommitOffsetsBegin());
    UNIT_ASSERT_EQUAL(op.GetCommitOffsetsEnd(), topicRead.GetCommitOffsetsEnd());
    UNIT_ASSERT_EQUAL(op.GetForceCommit(), topicRead.GetForceCommit());
    UNIT_ASSERT_EQUAL(op.GetKillReadSession(), topicRead.GetKillReadSession());
    UNIT_ASSERT_EQUAL(op.GetOnlyCheckCommitedToFinish(), topicRead.GetOnlyCheckCommitedToFinish());
    UNIT_ASSERT_EQUAL(op.GetReadSessionId(), topicRead.GetReadSessionId());
    UNIT_ASSERT(!op.GetKafkaTransaction());
}

void AssertKafkaReadDualWrite(const NKikimrPQ::TPartitionOperation& op)
{
    UNIT_ASSERT(op.HasRead());
    UNIT_ASSERT(op.GetRead().HasKafka());
    const auto& kafkaRead = op.GetRead().GetKafka();

    UNIT_ASSERT(op.GetKafkaTransaction());
    UNIT_ASSERT_EQUAL(op.GetConsumer(), kafkaRead.GetConsumer());
    UNIT_ASSERT_EQUAL(op.GetCommitOffsetsEnd(), kafkaRead.GetCommitOffsetsEnd());
    UNIT_ASSERT(!op.HasCommitOffsetsBegin());
    UNIT_ASSERT(!op.HasSupportivePartition());
    UNIT_ASSERT(!op.HasKafkaProducerInstanceId());
}

void AssertTopicWriteDualWrite(const NKikimrPQ::TPartitionOperation& op, bool skipConflictCheck, TMaybe<ui32> supportivePartition)
{
    UNIT_ASSERT(op.HasWrite());
    UNIT_ASSERT_EQUAL(op.GetSkipConflictCheck(), skipConflictCheck);
    UNIT_ASSERT_EQUAL(op.GetWrite().GetSkipConflictCheck(), skipConflictCheck);

    if (supportivePartition.Defined()) {
        UNIT_ASSERT(op.GetWrite().HasTopic());
        UNIT_ASSERT_EQUAL(op.GetSupportivePartition(), *supportivePartition);
        UNIT_ASSERT_EQUAL(op.GetWrite().GetTopic().GetSupportivePartition(), *supportivePartition);
    } else {
        UNIT_ASSERT(!op.HasSupportivePartition());
    }
    UNIT_ASSERT(!op.GetKafkaTransaction());
}

void AssertKafkaWriteDualWrite(const NKikimrPQ::TPartitionOperation& op, bool skipConflictCheck, i64 id, i32 epoch)
{
    UNIT_ASSERT(op.HasWrite());
    UNIT_ASSERT_EQUAL(op.GetSkipConflictCheck(), skipConflictCheck);
    UNIT_ASSERT_EQUAL(op.GetWrite().GetSkipConflictCheck(), skipConflictCheck);
    UNIT_ASSERT(op.GetWrite().HasKafka());
    UNIT_ASSERT(op.GetKafkaTransaction());

    const auto& legacyProducerId = op.GetKafkaProducerInstanceId();
    UNIT_ASSERT_EQUAL(legacyProducerId.GetId(), id);
    UNIT_ASSERT_EQUAL(legacyProducerId.GetEpoch(), epoch);

    const auto& canonicalProducerId = op.GetWrite().GetKafka().GetKafkaProducerInstanceId();
    UNIT_ASSERT_EQUAL(canonicalProducerId.GetId(), id);
    UNIT_ASSERT_EQUAL(canonicalProducerId.GetEpoch(), epoch);
}

Y_UNIT_TEST_SUITE(TPartitionOperationCompat) {

Y_UNIT_TEST(DowngradeToLegacyTopicRead) {
    NKikimrPQ::TPartitionOperation op;
    op.SetPath("topic");
    op.SetPartitionId(3);

    auto* topicRead = op.MutableRead()->MutableTopic();
    topicRead->SetConsumer("consumer");
    topicRead->SetCommitOffsetsBegin(10);
    topicRead->SetCommitOffsetsEnd(20);
    topicRead->SetForceCommit(true);
    topicRead->SetKillReadSession(true);
    topicRead->SetOnlyCheckCommitedToFinish(true);
    topicRead->SetReadSessionId("session");

    DowngradeToLegacy(op);
    AssertTopicReadDualWrite(op);
}

Y_UNIT_TEST(DowngradeToLegacyKafkaRead) {
    NKikimrPQ::TPartitionOperation op;
    op.SetPath("topic");
    op.SetPartitionId(1);

    auto* kafkaRead = op.MutableRead()->MutableKafka();
    kafkaRead->SetConsumer("consumer");
    kafkaRead->SetCommitOffsetsEnd(42);

    DowngradeToLegacy(op);
    AssertKafkaReadDualWrite(op);
}

Y_UNIT_TEST(DowngradeToLegacyTopicWrite) {
    NKikimrPQ::TPartitionOperation op;
    op.SetPath("topic");
    op.SetPartitionId(2);

    auto* write = op.MutableWrite();
    write->SetSkipConflictCheck(true);
    write->MutableTopic()->SetSupportivePartition(100'001);

    DowngradeToLegacy(op);
    AssertTopicWriteDualWrite(op, true, 100'001u);
}

Y_UNIT_TEST(DowngradeToLegacyKafkaWrite) {
    NKikimrPQ::TPartitionOperation op;
    op.SetPath("topic");
    op.SetPartitionId(0);

    auto* producerId = op.MutableWrite()->MutableKafka()->MutableKafkaProducerInstanceId();
    producerId->SetId(7);
    producerId->SetEpoch(3);
    op.MutableWrite()->SetSkipConflictCheck(false);

    DowngradeToLegacy(op);
    AssertKafkaWriteDualWrite(op, false, 7, 3);
}

Y_UNIT_TEST(DowngradeToLegacyDeferredPublicationWriteClearsLegacy) {
    NKikimrPQ::TPartitionOperation op;
    op.SetPath("topic");
    op.SetPartitionId(0);

    auto* write = op.MutableWrite();
    write->SetSkipConflictCheck(true);
    write->MutableDeferredPublication()->SetOp(
        NKikimrPQ::TPartitionOperation::TWriteOp::TDeferredPublicationApi::Publish);

    DowngradeToLegacy(op);

    UNIT_ASSERT(op.HasWrite());
    UNIT_ASSERT(op.GetWrite().HasDeferredPublication());
    UNIT_ASSERT(!op.HasSkipConflictCheck());
    UNIT_ASSERT(!op.GetKafkaTransaction());
    UNIT_ASSERT(!op.HasConsumer());
    UNIT_ASSERT(!op.HasSupportivePartition());
    UNIT_ASSERT(!op.HasKafkaProducerInstanceId());
}

Y_UNIT_TEST(UpgradeFromLegacyTopicReadRoundTrip) {
    NKikimrPQ::TPartitionOperation legacy;
    legacy.SetPath("topic");
    legacy.SetPartitionId(4);
    legacy.SetConsumer("consumer");
    legacy.SetCommitOffsetsBegin(1);
    legacy.SetCommitOffsetsEnd(2);
    legacy.SetForceCommit(true);
    legacy.SetKillReadSession(false);
    legacy.SetOnlyCheckCommitedToFinish(true);
    legacy.SetReadSessionId("session");

    UpgradeFromLegacy(legacy);
    UNIT_ASSERT(HasCanonical(legacy));
    AssertTopicReadDualWrite(legacy);

    DowngradeToLegacy(legacy);
    AssertTopicReadDualWrite(legacy);
}

Y_UNIT_TEST(UpgradeFromLegacyKafkaWriteRoundTrip) {
    NKikimrPQ::TPartitionOperation legacy;
    legacy.SetPath("topic");
    legacy.SetPartitionId(0);
    legacy.SetKafkaTransaction(true);
    legacy.SetSkipConflictCheck(true);
    legacy.MutableKafkaProducerInstanceId()->SetId(11);
    legacy.MutableKafkaProducerInstanceId()->SetEpoch(12);

    UpgradeFromLegacy(legacy);
    UNIT_ASSERT(HasCanonical(legacy));
    AssertKafkaWriteDualWrite(legacy, true, 11, 12);

    DowngradeToLegacy(legacy);
    AssertKafkaWriteDualWrite(legacy, true, 11, 12);
}

Y_UNIT_TEST(DowngradeToLegacyKafkaWriteClearsTopicReadLegacy) {
    NKikimrPQ::TPartitionOperation op;
    op.SetPath("topic");
    op.SetPartitionId(1);
    op.SetConsumer("consumer");
    op.SetCommitOffsetsBegin(10);
    op.SetCommitOffsetsEnd(20);
    UpgradeFromLegacy(op);

    op.ClearOp();
    auto* write = op.MutableWrite()->MutableKafka();
    write->MutableKafkaProducerInstanceId()->SetId(7);
    write->MutableKafkaProducerInstanceId()->SetEpoch(3);
    op.MutableWrite()->SetSkipConflictCheck(false);

    DowngradeToLegacy(op);

    AssertKafkaWriteDualWrite(op, false, 7, 3);
    UNIT_ASSERT(!op.HasCommitOffsetsBegin());
    UNIT_ASSERT(IsWriteTxOperation(op));
}

Y_UNIT_TEST(DowngradeToLegacyTopicWriteClearsSupportivePartition) {
    NKikimrPQ::TPartitionOperation op;
    op.SetPath("topic");
    op.SetPartitionId(2);
    op.SetSupportivePartition(100'001);
    op.SetSkipConflictCheck(true);
    UpgradeFromLegacy(op);

    op.ClearOp();
    op.MutableWrite()->SetSkipConflictCheck(false);
    op.MutableWrite()->MutableTopic();

    DowngradeToLegacy(op);

    AssertTopicWriteDualWrite(op, false, Nothing());
}

Y_UNIT_TEST(DowngradeToLegacyWriteApiNotSetClearsLegacy) {
    NKikimrPQ::TPartitionOperation op;
    op.SetPath("topic");
    op.SetPartitionId(1);
    op.SetConsumer("consumer");
    op.SetCommitOffsetsBegin(10);
    op.SetCommitOffsetsEnd(20);
    UpgradeFromLegacy(op);

    op.ClearOp();
    op.MutableWrite()->SetSkipConflictCheck(true);

    DowngradeToLegacy(op);

    UNIT_ASSERT(op.HasWrite());
    UNIT_ASSERT(op.GetSkipConflictCheck());
    UNIT_ASSERT_EQUAL(op.GetWrite().GetSkipConflictCheck(), true);
    UNIT_ASSERT(!op.HasCommitOffsetsBegin());
    UNIT_ASSERT(!op.HasConsumer());
    UNIT_ASSERT(!op.HasKafkaProducerInstanceId());
    UNIT_ASSERT(!op.HasSupportivePartition());
    UNIT_ASSERT(!op.GetKafkaTransaction());
    UNIT_ASSERT(IsWriteTxOperation(op));
}

} // Y_UNIT_TEST_SUITE(TPartitionOperationCompat)

} // namespace
} // namespace NKikimr::NPQ
