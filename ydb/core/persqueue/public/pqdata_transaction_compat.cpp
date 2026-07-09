#include "pqdata_transaction_compat.h"

namespace NKikimr::NPQ {

namespace {

void CopyKafkaProducerInstanceId(
    const NKikimrPQ::TKafkaProducerInstanceId& src,
    NKikimrPQ::TKafkaProducerInstanceId* dst)
{
    dst->SetId(src.GetId());
    dst->SetEpoch(src.GetEpoch());
}

void UpgradeTopicApiFromLegacy(NKikimrPQ::TWriteId& writeId)
{
    auto* topicApi = writeId.MutableTopicApi();
    topicApi->SetNodeId(writeId.GetNodeId());
    topicApi->SetKeyId(writeId.GetKeyId());
}

void UpgradeKafkaApiFromLegacy(NKikimrPQ::TWriteId& writeId)
{
    auto* kafkaApi = writeId.MutableKafkaApi();
    CopyKafkaProducerInstanceId(writeId.GetKafkaProducerInstanceId(), kafkaApi->MutableKafkaProducerInstanceId());
}

void DowngradeTopicApiToLegacy(const NKikimrPQ::TWriteId::TTopicApi& topicApi, NKikimrPQ::TWriteId& writeId)
{
    writeId.ClearKafkaProducerInstanceId();
    writeId.SetKafkaTransaction(false);
    writeId.SetNodeId(topicApi.GetNodeId());
    writeId.SetKeyId(topicApi.GetKeyId());
}

void DowngradeKafkaApiToLegacy(const NKikimrPQ::TWriteId::TKafkaApi& kafkaApi, NKikimrPQ::TWriteId& writeId)
{
    writeId.ClearNodeId();
    writeId.ClearKeyId();
    writeId.SetKafkaTransaction(true);
    CopyKafkaProducerInstanceId(kafkaApi.GetKafkaProducerInstanceId(), writeId.MutableKafkaProducerInstanceId());
}

bool HasLegacy(const NKikimrPQ::TWriteId& writeId)
{
    return writeId.GetKafkaTransaction() || writeId.HasKafkaProducerInstanceId()
        || writeId.HasNodeId() || writeId.HasKeyId();
}

void ClearLegacyPartitionOpFields(NKikimrPQ::TPartitionOperation& op)
{
    op.ClearCommitOffsetsBegin();
    op.ClearCommitOffsetsEnd();
    op.ClearConsumer();
    op.ClearSupportivePartition();
    op.ClearForceCommit();
    op.ClearKillReadSession();
    op.ClearOnlyCheckCommitedToFinish();
    op.ClearReadSessionId();
    op.SetKafkaTransaction(false);
    op.ClearKafkaProducerInstanceId();
    op.ClearSkipConflictCheck();
}

void DowngradeTopicReadToLegacy(
    const NKikimrPQ::TPartitionOperation::TReadOp::TTopicApi& topicRead,
    NKikimrPQ::TPartitionOperation& op)
{
    ClearLegacyPartitionOpFields(op);
    op.SetConsumer(topicRead.GetConsumer());
    op.SetCommitOffsetsBegin(topicRead.GetCommitOffsetsBegin());
    op.SetCommitOffsetsEnd(topicRead.GetCommitOffsetsEnd());
    op.SetForceCommit(topicRead.GetForceCommit());
    op.SetKillReadSession(topicRead.GetKillReadSession());
    op.SetOnlyCheckCommitedToFinish(topicRead.GetOnlyCheckCommitedToFinish());
    op.SetReadSessionId(topicRead.GetReadSessionId());
    op.SetKafkaTransaction(false);
}

void DowngradeKafkaReadToLegacy(
    const NKikimrPQ::TPartitionOperation::TReadOp::TKafkaApi& kafkaRead,
    NKikimrPQ::TPartitionOperation& op)
{
    ClearLegacyPartitionOpFields(op);
    op.SetConsumer(kafkaRead.GetConsumer());
    op.SetCommitOffsetsEnd(kafkaRead.GetCommitOffsetsEnd());
    op.SetKafkaTransaction(true);
}

void DowngradeTopicWriteToLegacy(
    const NKikimrPQ::TPartitionOperation::TWriteOp& write,
    NKikimrPQ::TPartitionOperation& op)
{
    ClearLegacyPartitionOpFields(op);
    op.SetSkipConflictCheck(write.GetSkipConflictCheck());
    if (write.GetTopic().HasSupportivePartition()) {
        op.SetSupportivePartition(write.GetTopic().GetSupportivePartition());
    }
    op.SetKafkaTransaction(false);
}

void DowngradeKafkaWriteToLegacy(
    const NKikimrPQ::TPartitionOperation::TWriteOp& write,
    NKikimrPQ::TPartitionOperation& op)
{
    ClearLegacyPartitionOpFields(op);
    op.SetSkipConflictCheck(write.GetSkipConflictCheck());
    op.SetKafkaTransaction(true);
    CopyKafkaProducerInstanceId(
        write.GetKafka().GetKafkaProducerInstanceId(),
        op.MutableKafkaProducerInstanceId());
}

void DowngradeDeferredPublicationWriteToLegacy(
    const NKikimrPQ::TPartitionOperation::TWriteOp& /*write*/,
    NKikimrPQ::TPartitionOperation& op)
{
    // Legacy wire has no deferred-publication representation; do not populate legacy write fields.
    ClearLegacyPartitionOpFields(op);
}

bool HasLegacyPartitionOp(const NKikimrPQ::TPartitionOperation& op)
{
    return op.HasConsumer() || op.HasCommitOffsetsBegin() || op.HasCommitOffsetsEnd()
        || op.HasForceCommit() || op.HasKillReadSession() || op.HasOnlyCheckCommitedToFinish()
        || op.HasReadSessionId() || op.GetKafkaTransaction()
        || op.HasKafkaProducerInstanceId() || op.HasSupportivePartition() || op.HasSkipConflictCheck();
}

void UpgradeTopicReadFromLegacy(NKikimrPQ::TPartitionOperation& op)
{
    auto* topicRead = op.MutableRead()->MutableTopic();
    topicRead->SetConsumer(op.GetConsumer());
    topicRead->SetCommitOffsetsBegin(op.GetCommitOffsetsBegin());
    topicRead->SetCommitOffsetsEnd(op.GetCommitOffsetsEnd());
    topicRead->SetForceCommit(op.GetForceCommit());
    topicRead->SetKillReadSession(op.GetKillReadSession());
    topicRead->SetOnlyCheckCommitedToFinish(op.GetOnlyCheckCommitedToFinish());
    topicRead->SetReadSessionId(op.GetReadSessionId());
}

void UpgradeKafkaReadFromLegacy(NKikimrPQ::TPartitionOperation& op)
{
    auto* kafkaRead = op.MutableRead()->MutableKafka();
    kafkaRead->SetConsumer(op.GetConsumer());
    kafkaRead->SetCommitOffsetsEnd(op.GetCommitOffsetsEnd());
}

void UpgradeTopicWriteFromLegacy(NKikimrPQ::TPartitionOperation& op)
{
    auto* write = op.MutableWrite();
    write->SetSkipConflictCheck(op.GetSkipConflictCheck());
    if (op.HasSupportivePartition()) {
        write->MutableTopic()->SetSupportivePartition(op.GetSupportivePartition());
    }
}

void UpgradeKafkaWriteFromLegacy(NKikimrPQ::TPartitionOperation& op)
{
    auto* write = op.MutableWrite();
    write->SetSkipConflictCheck(op.GetSkipConflictCheck());
    CopyKafkaProducerInstanceId(
        op.GetKafkaProducerInstanceId(),
        write->MutableKafka()->MutableKafkaProducerInstanceId());
}

} // namespace

bool HasCanonical(const NKikimrPQ::TWriteId& writeId)
{
    return writeId.Id_case() != NKikimrPQ::TWriteId::ID_NOT_SET;
}

void UpgradeFromLegacy(NKikimrPQ::TWriteId& writeId)
{
    if (HasCanonical(writeId) || !HasLegacy(writeId)) {
        return;
    }

    if (writeId.GetKafkaTransaction()) {
        UpgradeKafkaApiFromLegacy(writeId);
    } else {
        UpgradeTopicApiFromLegacy(writeId);
    }
}

void DowngradeToLegacy(NKikimrPQ::TWriteId& writeId)
{
    if (!HasCanonical(writeId)) {
        return;
    }

    switch (writeId.Id_case()) {
        case NKikimrPQ::TWriteId::kTopicApi:
            DowngradeTopicApiToLegacy(writeId.GetTopicApi(), writeId);
            break;
        case NKikimrPQ::TWriteId::kKafkaApi:
            DowngradeKafkaApiToLegacy(writeId.GetKafkaApi(), writeId);
            break;
        case NKikimrPQ::TWriteId::kDeferredPublicationApi:
            // Legacy wire format has no deferred representation.
            break;
        case NKikimrPQ::TWriteId::ID_NOT_SET:
            break;
    }
}

void EnsureCanonical(NKikimrPQ::TWriteId& writeId)
{
    UpgradeFromLegacy(writeId);
}

bool HasCanonical(const NKikimrPQ::TPartitionOperation& op)
{
    return op.Op_case() != NKikimrPQ::TPartitionOperation::OP_NOT_SET;
}

void DowngradeToLegacy(NKikimrPQ::TPartitionOperation& op)
{
    if (!HasCanonical(op)) {
        return;
    }

    switch (op.Op_case()) {
        case NKikimrPQ::TPartitionOperation::kRead:
            switch (op.GetRead().Api_case()) {
                case NKikimrPQ::TPartitionOperation::TReadOp::kTopic:
                    DowngradeTopicReadToLegacy(op.GetRead().GetTopic(), op);
                    break;
                case NKikimrPQ::TPartitionOperation::TReadOp::kKafka:
                    DowngradeKafkaReadToLegacy(op.GetRead().GetKafka(), op);
                    break;
                case NKikimrPQ::TPartitionOperation::TReadOp::API_NOT_SET:
                    break;
            }
            break;
        case NKikimrPQ::TPartitionOperation::kWrite:
            switch (op.GetWrite().Api_case()) {
                case NKikimrPQ::TPartitionOperation::TWriteOp::kTopic:
                    DowngradeTopicWriteToLegacy(op.GetWrite(), op);
                    break;
                case NKikimrPQ::TPartitionOperation::TWriteOp::kKafka:
                    DowngradeKafkaWriteToLegacy(op.GetWrite(), op);
                    break;
                case NKikimrPQ::TPartitionOperation::TWriteOp::kDeferredPublication:
                    DowngradeDeferredPublicationWriteToLegacy(op.GetWrite(), op);
                    break;
                case NKikimrPQ::TPartitionOperation::TWriteOp::API_NOT_SET:
                    ClearLegacyPartitionOpFields(op);
                    if (op.GetWrite().HasSkipConflictCheck()) {
                        op.SetSkipConflictCheck(op.GetWrite().GetSkipConflictCheck());
                    }
                    break;
            }
            break;
        case NKikimrPQ::TPartitionOperation::OP_NOT_SET:
            break;
    }
}

void UpgradeFromLegacy(NKikimrPQ::TPartitionOperation& op)
{
    if (HasCanonical(op) || !HasLegacyPartitionOp(op)) {
        return;
    }

    if (op.GetKafkaTransaction() && op.HasKafkaProducerInstanceId()) {
        UpgradeKafkaWriteFromLegacy(op);
        return;
    }

    if (op.GetKafkaTransaction() && op.HasConsumer()) {
        UpgradeKafkaReadFromLegacy(op);
        return;
    }

    if (op.HasConsumer()) {
        UpgradeTopicReadFromLegacy(op);
        return;
    }

    UpgradeTopicWriteFromLegacy(op);
}

void EnsureCanonical(NKikimrPQ::TPartitionOperation& op)
{
    UpgradeFromLegacy(op);
}

} // namespace NKikimr::NPQ
