#include "utils.h"

#include <library/cpp/string_utils/url/url.h>

#include <util/string/ascii.h>
#include <util/string/builder.h>
#include <util/string/split.h>

#include <expected>

namespace NKikimr::NSqsTopic {

    constexpr TStringBuf QUEUE_URL_VERSION1_PREFIX = "/v1";

    struct TParsedString {
        TString String;
        size_t NextPosition = TStringBuf::npos;
    };

    static std::expected<TParsedString, TString> ReadLengthDelimitedString(TStringBuf part Y_LIFETIME_BOUND) {
        if (part.size() < 3) {
            return std::unexpected("part too short");
        }
        const size_t index0 = part.find('/');
        if (index0 != 0) {
            return std::unexpected("missing first '/'");
        }
        const size_t index1 = part.find('/', 1);
        if (index1 == TString::npos) {
            return std::unexpected("missing second '/'");
        }
        Y_ASSERT(index0 < index1);
        TStringBuf lengthPart = part.SubString(index0 + 1, index1 - index0 - 1);
        if (lengthPart.empty()) {
            return std::unexpected("empty length part");
        }
        size_t length = 0;
        if (!TryFromString(lengthPart, length)) {
            return std::unexpected("invalid length part");
        }
        /*
            /nnn/string/
            ^   ^      ^
            i0  i1     end
        */
        TStringBuf stringValue = part.SubString(index1 + 1, length);
        if (stringValue.size() != length) {
            return std::unexpected("value too short");
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

        auto consumePart = [&part](TStringBuf name, TString& dst) -> std::expected<void, TString> {
            if (auto v = ReadLengthDelimitedString(part); v.has_value()) {
                dst = std::move(v.value().String);
                part.Skip(v.value().NextPosition);
            } else {
                return std::unexpected(TString::Join(name, " part ", v.error()));
            }
            return {};
        };

        if (auto r = consumePart("Database", result.Database); !r) {
            return std::unexpected{std::move(r).error()};
        }
        if (auto r = consumePart("TopicPath", result.TopicPath); !r) {
            return std::unexpected{std::move(r).error()};
        }
        if (auto r = consumePart("Consumer", result.Consumer); !r) {
            return std::unexpected{std::move(r).error()};
        }
        result.Fifo = AsciiHasSuffixIgnoreCase(part, ".fifo");

        return result;
    }

    std::expected<TRichQueueUrl, TString> ParseQueueUrl(const TStringBuf queueUrl) {
        const auto [host, path] = NUrl::SplitUrlToHostAndPath(queueUrl);
        return ParseQueueUrlPath(path);
    }

    std::expected<TRichQueueUrl, TString> ParseQueueUrlPath(const TStringBuf path) {
        if (path.StartsWith(QUEUE_URL_VERSION1_PREFIX)) {
            if (auto r = ParsePathV1(path); r.has_value()) {
                return std::move(r).value();
            } else {
                return std::unexpected{"Invalid QueueUrl: " + std::move(r.error())};
            }
        }
        if (path.empty()) {
            return std::unexpected{"Invalid QueueUrl: empty path"};
        }
        return std::unexpected{"Invalid QueueUrl: unsupported version"};
    }

    static void WriteLengthDelimitedString(IOutputStream& os, TStringBuf value) {
        os << '/' << value.size() << '/' << value;
    }

    TString PackQueueUrlPath(const TRichQueueUrl& queueUrl) {
        TStringBuilder result;
        result << QUEUE_URL_VERSION1_PREFIX;
        WriteLengthDelimitedString(result.Out, queueUrl.Database);
        WriteLengthDelimitedString(result.Out, queueUrl.TopicPath);
        WriteLengthDelimitedString(result.Out, queueUrl.Consumer);
        if (queueUrl.Fifo) {
            result << ".fifo";
        }
        return result;
    }
} // namespace NKikimr::NSqsTopic
