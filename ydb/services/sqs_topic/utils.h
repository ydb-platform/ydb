#pragma once

#include <util/generic/string.h>

namespace NKikimr::NSqsTopic {

    struct TRichQueueUrl {
        TString Database;
        TString Consumer;
        TString TopicPath;
        bool Fifo{};
    };

    TRichQueueUrl ParseQueueUrl(const TStringBuf queueUrl);

    TString PackQueueUrlPath(const TRichQueueUrl& queueUrl);
}
