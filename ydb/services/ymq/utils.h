#pragma once

#include <util/generic/string.h>
#include <util/generic/maybe.h>
#include <util/string/split.h>
#include <ydb/core/ymq/base/utils.h>

namespace NKikimr::NYmq {
    inline static TMaybe<std::pair<TString, TString>> CloudIdAndResourceIdFromQueueUrl(const TString& queueUrl) {
        auto protocolSeparator = queueUrl.find("://");
        if (protocolSeparator == TString::npos) {
            return Nothing();
        }

        auto restOfUrl = queueUrl.substr(protocolSeparator + 3);
        auto parts = StringSplitter(restOfUrl).Split('/').ToList<TString>();
        if (parts.size() < 3) {
            return Nothing();
        }

        bool isPrivateRequest = NKikimr::NSQS::IsPrivateRequest(restOfUrl);
        TString queueName = NKikimr::NSQS::ExtractQueueNameFromPath(restOfUrl, isPrivateRequest);
        TString accountName = NKikimr::NSQS::ExtractAccountNameFromPath(restOfUrl, isPrivateRequest);
        return std::pair<TString, TString>(std::move(accountName), std::move(queueName));
    }
}
