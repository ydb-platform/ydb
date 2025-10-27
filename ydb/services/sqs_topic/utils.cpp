#include "utils.h"

namespace NKikimr::NSqsTopic {

    TQueueNameWithConsumer SplitExtendedQueueName(TStringBuf queueNameExt) {
        TQueueNameWithConsumer result;
        if (!queueNameExt.TryRSplit('@', result.TopicName, result.Consumer)) {
            result.TopicName = queueNameExt;
        }
        return result;
    }
}
