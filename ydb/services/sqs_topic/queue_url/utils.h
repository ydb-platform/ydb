#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/stream/output.h>
#include <util/system/types.h>

#include <expected>

namespace NKikimr::NSqsTopic {

    struct TRichQueueUrl {
        TString Database;
        TString TopicPath;
        TString Consumer;
        bool Fifo{};

        friend bool operator==(const TRichQueueUrl&, const TRichQueueUrl&) noexcept = default;
    };

    std::expected<TRichQueueUrl, TString> ParseQueueUrl(const TStringBuf queueUrl);
    std::expected<TRichQueueUrl, TString> ParseQueueUrlPath(const TStringBuf path);

    TString PackQueueUrlPath(const TRichQueueUrl& queueUrl);

    // Public SQS QueueUrl prefix: scheme://host[:port] without a trailing slash.
    // If requestEndpoint is set (from the incoming HTTP request), it is used as-is.
    // Otherwise scheme://fallbackHost[:httpProxyPort], where scheme is https iff secure.
    // Port 0 omits :port so the URL is not scheme://host:0.
    TString MakeQueueUrlEndpoint(TStringBuf requestEndpoint, TStringBuf fallbackHost, ui16 httpProxyPort, bool secure = true);
    TString MakeQueueUrl(const TRichQueueUrl& queueUrl, TStringBuf requestEndpoint, TStringBuf fallbackHost, ui16 httpProxyPort, bool secure = true);

    void WriteLengthDelimitedString(IOutputStream& os, TStringBuf value);
} // namespace NKikimr::NSqsTopic
