#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NKikimrConfig {
    class TSqsConfig;
} // namespace NKikimrConfig

namespace NKikimr::NGrpcService {
    class IRequestOpCtx;
} // namespace NKikimr::NGrpcService

namespace NKikimr::NPQ::NMLP {
    struct TMessageId;
} // namespace NKikimr::NPQ::NMLP

namespace NKikimr::NSqsTopic {

    struct TQueueNameWithConsumer {
        TStringBuf QueueName;
        TStringBuf Consumer;
    };

    TQueueNameWithConsumer SplitExtendedQueueName(TStringBuf queueNameExt Y_LIFETIME_BOUND);

    const NKikimrConfig::TSqsConfig& Cfg();

    TString GetEndpoint(const NKikimrConfig::TSqsConfig& config);

    TString GenerateMessageId(const TString& database, const TString& topicPath, const NPQ::NMLP::TMessageId& pos);

    TVector<std::pair<TString, TString>> GetMetricsLabels(
        const TString& databasePath,
        const TString& topicPath,
        const TString& consumer,
        const TString& method,
        TVector<std::pair<TString, TString>>&& labels
    );

    TVector<std::pair<TString, TString>> GetRequestMessageCountMetricsLabels(
        const TString& databasePath,
        const TString& topicPath,
        const TString& consumer,
        const TString& method
    );

    TVector<std::pair<TString, TString>> GetResponseMessageCountMetricsLabels(
        const TString& databasePath,
        const TString& topicPath,
        const TString& consumer,
        const TString& method,
        const TString& status
    );

    TVector<std::pair<TString, TString>> GetResponseEmptyCountMetricsLabels(
        const TString& databasePath,
        const TString& topicPath,
        const TString& consumer,
        const TString& method
    );

    ui64 SampleIdFromRequestId(const TStringBuf requestId);
} // namespace NKikimr::NSqsTopic
