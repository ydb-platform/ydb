#include "create_topic_tx.h"

#include "action.h"

#include <ydb/core/protos/schemeshard/operations.pb.h>

#include <util/generic/ptr.h>
#include <util/string/builder.h>

namespace NKikimr::NSQS {

THolder<TEvTxUserProxy::TEvProposeTransaction> BuildCreateTopicTx(
    const TString& queuePath,
    const TString& versionName,
    bool isFifo,
    const TPersQueueGroupTopicParams& params
) {
    auto ev = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
    auto* trans = ev->Record.MutableTransaction()->MutableModifyScheme();

    const TString topicDir = TString::Join(queuePath, '/', versionName);

    trans->SetWorkingDir(topicDir);
    trans->SetOperationType(NKikimrSchemeOp::ESchemeOpCreatePersQueueGroup);

    auto* pqgroup = trans->MutableCreatePersQueueGroup();
    pqgroup->SetName("streamImpl");
    pqgroup->SetTotalGroupCount(1);

    auto* config = pqgroup->MutablePQTabletConfig();
    config->SetTopicName("streamImpl");
    config->SetTopicPath(TString::Join(topicDir, '/', "streamImpl"));

    config->MutablePartitionConfig()->SetLifetimeSeconds(params.PartitionLifetimeSeconds);
    config->MutablePartitionConfig()->SetWriteSpeedInBytesPerSecond(1048576);
    config->MutablePartitionConfig()->SetBurstSize(1048576);

    if (params.HasContentBasedDeduplication) {
        config->SetContentBasedDeduplication(params.ContentBasedDeduplication);
    }

    auto* partitionStrategy = pqgroup->MutablePQTabletConfig()->MutablePartitionStrategy();
    partitionStrategy->SetPartitionStrategyType(::NKikimrPQ::TPQTabletConfig::CAN_SPLIT_AND_MERGE);
    partitionStrategy->SetMinPartitionCount(1);
    partitionStrategy->SetMaxPartitionCount(100);
    partitionStrategy->SetScaleUpPartitionWriteSpeedThresholdPercent(80);
    partitionStrategy->SetScaleDownPartitionWriteSpeedThresholdPercent(20);
    partitionStrategy->SetScaleThresholdSeconds(30);

    auto* consumer = config->AddConsumers();
    consumer->SetName(ConsumerName);
    consumer->SetType(::NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP);
    consumer->SetKeepMessageOrder(isFifo);
    if (params.DefaultDelayMessageTimeMs) {
        consumer->SetDefaultDelayMessageTimeMs(params.DefaultDelayMessageTimeMs);
    }
    if (params.DefaultProcessingTimeoutSeconds) {
        consumer->SetDefaultProcessingTimeoutSeconds(params.DefaultProcessingTimeoutSeconds);
    }
    if (params.DefaultReceiveMessageWaitTimeMs) {
        consumer->SetDefaultReceiveMessageWaitTimeMs(params.DefaultReceiveMessageWaitTimeMs);
    }
    if (params.MaxReceiveCount) {
        consumer->SetDeadLetterPolicyEnabled(true);
        consumer->SetDeadLetterPolicy(::NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_DELETE);
        consumer->SetMaxProcessingAttempts(params.MaxReceiveCount);
    }
    if (params.RedriveTargetQueueName) {
        consumer->SetDeadLetterPolicyEnabled(true);
        consumer->SetDeadLetterPolicy(::NKikimrPQ::TPQTabletConfig::DEAD_LETTER_POLICY_MOVE);
        consumer->SetDeadLetterQueue(TStringBuilder() << "sqs://" << params.AccountName << "/" << params.FolderId << "/" << params.RedriveTargetQueueName);
    }

    return ev;
}

} // namespace NKikimr::NSQS
