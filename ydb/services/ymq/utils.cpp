#include "utils.h"

#include <ydb/core/util/address_classifier.h>
#include <ydb/core/ymq/base/utils.h>

#include <library/cpp/ipv6_address/ipv6_address.h>
#include <library/cpp/string_utils/quote/quote.h>

#include <util/string/split.h>


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

    TString GetAuditRemoteAddress(TString peer) {
        if (peer.StartsWith("ipv4:") || peer.StartsWith("ipv6:")) {
            // gRPC percent-encodes IPv6 brackets and scope IDs in peer URIs.
            CGIUnescape(peer);
            peer = NAddressClassifier::ExtractAddress(peer);
        }
        bool valid = false;
        TIpv6Address::FromString(peer, valid);
        return valid ? peer : TString();
    }
}

