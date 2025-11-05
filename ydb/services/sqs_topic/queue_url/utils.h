#pragma once

#include <util/generic/string.h>

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
}
