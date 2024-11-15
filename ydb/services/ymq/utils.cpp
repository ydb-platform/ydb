#include "utils.h"

#include <util/string/split.h>
#include <ydb/core/ymq/base/utils.h>


namespace NKikimr::NYmq {
    std::pair<TString, TString> CloudIdAndResourceIdFromQueueUrl(const TString& queueUrl) {
        auto protocolSeparator = queueUrl.find("://");
        if (protocolSeparator == TString::npos) {
            return {"", ""};
        }

        auto restOfUrl = queueUrl.substr(protocolSeparator + 3);
        auto parts = StringSplitter(restOfUrl).Split('/').ToList<TString>();
        if (parts.size() < 3) {
            return {"", ""};
        }

        bool isPrivateRequest = NKikimr::NSQS::IsPrivateRequest(restOfUrl);
        TString queueName = NKikimr::NSQS::ExtractQueueNameFromPath(restOfUrl, isPrivateRequest);
        TString accountName = NKikimr::NSQS::ExtractAccountNameFromPath(restOfUrl, isPrivateRequest);
        return {accountName, queueName};
    }
}
