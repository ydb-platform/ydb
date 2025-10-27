#include "utils.h"

namespace NKikimr::NSqsTopic {

    TQueueNameWithConsumer SplitExtendedQueueName(TStringBuf queueNameExt) {
        TQueueNameWithConsumer result;
        if (!queueNameExt.TryRSplit('@', result.QueueName, result.Consumer)) {
            result.QueueName = queueNameExt;
        }
        return result;
    }
}
