#pragma once

#include <util/generic/string.h>

namespace NKikimr::NSqsTopic {
    std::pair<TString, TString> CloudIdAndResourceIdFromQueueUrl(const TStringBuf queueUrl);


    struct TRichQueueUrl {
        TString Database;
        TString Consumer;
        TString TopicPath;
        TString QueueName;
    };

    TRichQueueUrl ParseQueueUrl(const TStringBuf queueUrl);

    TString PackQueueUrlPath(const TRichQueueUrl& queueUrl);
}
