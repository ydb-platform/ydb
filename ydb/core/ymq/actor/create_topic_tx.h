#pragma once

#include <ydb/core/tx/tx_proxy/proxy.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>

namespace NKikimr::NSQS {

struct TPersQueueGroupTopicParams {
    ui64 PartitionLifetimeSeconds = 0;
    bool HasContentBasedDeduplication = false;
    bool ContentBasedDeduplication = false;
    ui64 DefaultDelayMessageTimeMs = 0;
    ui64 DefaultProcessingTimeoutSeconds = 0;
    ui64 DefaultReceiveMessageWaitTimeMs = 0;
    ui64 MaxReceiveCount = 0;
    TString RedriveTargetQueueName;
    TString AccountName;
    TString FolderId;
};

THolder<TEvTxUserProxy::TEvProposeTransaction> BuildCreateTopicTx(
    const TString& queuePath,
    const TString& versionName,
    bool isFifo,
    const TPersQueueGroupTopicParams& params
);

} // namespace NKikimr::NSQS
