#pragma once

#include <util/generic/string.h>

namespace NKikimr::NYmq {
    std::pair<TString, TString> CloudIdAndResourceIdFromQueueUrl(const TString& queueUrl);
}
