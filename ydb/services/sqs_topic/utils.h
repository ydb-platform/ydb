#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NKikimrConfig {
    class TSqsConfig;
} // namespace NKikimrConfig

namespace NKikimr::NGrpcService {
    class IRequestOpCtx;
} // namespace NKikimr::NGrpcService

namespace NKikimr::NSqsTopic {

    struct TQueueNameWithConsumer {
        TStringBuf QueueName;
        TStringBuf Consumer;
    };

    TQueueNameWithConsumer SplitExtendedQueueName(TStringBuf queueNameExt Y_LIFETIME_BOUND);

    const NKikimrConfig::TSqsConfig& Cfg();

    TString GetEndpoint(const NKikimrConfig::TSqsConfig& config);
} // namespace NKikimr::NSqsTopic
