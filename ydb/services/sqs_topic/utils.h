#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NKikimrConfig {
    class TSqsConfig;
}

namespace NKikimr::NSqsTopic {

    struct TQueueNameWithConsumer {
        TStringBuf QueueName;
        TStringBuf Consumer;
    };

    TQueueNameWithConsumer SplitExtendedQueueName(TStringBuf queueNameExt Y_LIFETIME_BOUND);


    const NKikimrConfig::TSqsConfig& Cfg();

    TString GetEndpoint(const NKikimrConfig::TSqsConfig& config);
}
