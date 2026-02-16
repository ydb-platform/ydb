#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/stream/output.h>

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

    void WriteLengthDelimitedString(IOutputStream& os, TStringBuf value);
} // namespace NKikimr::NSqsTopic
