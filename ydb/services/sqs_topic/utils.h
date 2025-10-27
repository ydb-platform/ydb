#pragma once

#include <util/generic/strbuf.h>


namespace NKikimr::NSqsTopic {

    struct TQueueNameWithConsumer {
        TStringBuf QueueName;
        TStringBuf Consumer;
    };

    TQueueNameWithConsumer SplitExtendedQueueName(TStringBuf queueNameExt Y_LIFETIME_BOUND);

}
