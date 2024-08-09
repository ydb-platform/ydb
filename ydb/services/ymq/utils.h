#pragma once

#include <util/generic/string.h>
#include <util/generic/maybe.h>
#include <util/string/split.h>

namespace NKikimr::NYmq {
    inline static TMaybe<std::pair<TString, TString>> CloudIdAndResourceIdFromQueueUrl(const TString& queueUrl) {
        auto protocolSeparator = queueUrl.find("://");
        if (protocolSeparator == TString::npos) {
            return Nothing();
        }

        auto restOfUrl = queueUrl.substr(protocolSeparator + 3);
        auto parts = StringSplitter(restOfUrl).Split('/').ToList<TString>();
        if (parts.size() != 4) {
            return Nothing();
        }

        return std::pair<TString, TString>(parts[1], parts[2]);
    }
}