#include "utils.h"

#include <util/string/builder.h>
#include <util/string/split.h>
#include <ydb/core/ymq/base/utils.h>
#include <library/cpp/string_utils/url/url.h>

#include <expected>

namespace NKikimr::NYmq {
    std::pair<TString, TString> CloudIdAndResourceIdFromQueueUrl(const TStringBuf queueUrl) {
        auto protocolSeparator = queueUrl.find("://");
        if (protocolSeparator == TStringBuf::npos) {
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

    constexpr TStringBuf QUEUE_URL_VERSION1_PREFIX = "/v1";

    struct TParsedString {
        TString String;
        size_t NextPosition = TStringBuf::npos;
    };

    static std::expected<TParsedString, TString> ReadString(TStringBuf part Y_LIFETIME_BOUND) {
        if (part.size() < 3) {
            return std::unexpected("Part too short");
        }
        const size_t index0 = part.find('/');
        if (index0 != 0) {
            return std::unexpected("Missing first '/'");
        }
        const size_t index1 = part.find('/', 1);
        if (index1 == TString::npos) {
            return std::unexpected("Missing second '/'");
        }
        Y_ASSERT(index0 < index1);
        TStringBuf lengthPart = part.SubString(index0 + 1, index1 - index0 - 1);
        if (lengthPart.empty()) {
            return std::unexpected("Empty length part");
        }
        size_t length = 0;
        if (!TryFromString(lengthPart, length)) {
            return std::unexpected("Invalid length part");
        }
        /*
            /nnn/string/
            ^   ^      ^
            i0  i1     end
        */
        TStringBuf stringValue = part.SubString(index1 + 1, length);
        if (stringValue.size() != length) {
            return std::unexpected("Value too short");
        }
        size_t endPos = index1 + 1 + length;
        Y_ASSERT(endPos <= part.size());
        return TParsedString{ToString(stringValue), endPos};
    }


    static std::expected<TRichQueueUrl, TString> ParsePathV1(const TStringBuf path) {
        TStringBuf part = path;
        bool correctVer = part.SkipPrefix(QUEUE_URL_VERSION1_PREFIX);
        Y_ASSERT(correctVer);

        TRichQueueUrl result;
        if (auto v = ReadString(part); v.has_value()) {
            result.Database = std::move(v.value().String);
            part.Skip(v.value().NextPosition);
        } else {
            return std::unexpected("Database " + std::move(v.error()));
        }

        if (auto v = ReadString(part); v.has_value()) {
            result.Consumer = std::move(v.value().String);
            part.Skip(v.value().NextPosition);
        } else {
            return std::unexpected("Consumer " + std::move(v.error()));
        }

        if (auto v = ReadString(part); v.has_value()) {
            result.TopicPath = std::move(v.value().String);
            part.Skip(v.value().NextPosition);
        } else {
            return std::unexpected("TopicPath " + std::move(v.error()));
        }

        if (auto v = ReadString(part); v.has_value()) {
            result.QueueName = std::move(v.value().String);
            part.Skip(v.value().NextPosition);
        } else {
            return std::unexpected("QueueName " + std::move(v.error()));
        }

        return result;
    }

    TRichQueueUrl ParseQueueUrl(const TStringBuf queueUrl) {
        const auto [host, path] = NUrl::SplitUrlToHostAndPath(queueUrl);
        if (path.StartsWith(QUEUE_URL_VERSION1_PREFIX)) {
            if (auto r = ParsePathV1(path); r.has_value()) {
                return std::move(r).value();
            } else {
                return {};
            }
        }
        auto prev = CloudIdAndResourceIdFromQueueUrl(queueUrl);
        TRichQueueUrl result{
            .Database = std::move(prev.first),
            .TopicPath = prev.second,
            .QueueName = prev.second,
        };
        return result;
    }


    static void WriteLengthDelimitedString(IOutputStream& os, TStringBuf value) {
        os << '/' << value.size() << '/' << value;
    }

    TString PackQueueUrlPath(const TRichQueueUrl& queueUrl) {
        TStringBuilder result;
        result << QUEUE_URL_VERSION1_PREFIX;
        WriteLengthDelimitedString(result.Out, queueUrl.Database);
        WriteLengthDelimitedString(result.Out, queueUrl.Consumer);
        WriteLengthDelimitedString(result.Out, queueUrl.TopicPath);
        WriteLengthDelimitedString(result.Out, queueUrl.QueueName);
        return result;
    }
}
