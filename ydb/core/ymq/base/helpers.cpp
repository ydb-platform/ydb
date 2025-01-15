#include "helpers.h"

#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <util/charset/utf8.h>
#include <util/generic/array_size.h>
#include <util/generic/yexception.h>
#include <util/stream/format.h>
#include <util/string/ascii.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <ydb/core/ymq/base/limits.h>

namespace NKikimr::NSQS {

static bool AlphaNumAndPunctuation[256] = {};

static bool MakeAlphaNumAndPunctuation() {
    char src[] = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~";
    for (size_t i = 0; i < Y_ARRAY_SIZE(src) - 1; ++i) {
        AlphaNumAndPunctuation[static_cast<unsigned char>(src[i])] = true;
    }
    return true;
}

static const bool AlphaNumAndPunctuationMade = MakeAlphaNumAndPunctuation();

bool IsAlphaNumAndPunctuation(TStringBuf str) {
    for (char c : str) {
        if (!AlphaNumAndPunctuation[static_cast<unsigned char>(c)]) {
            return false;
        }
    }
    return true;
}


static bool MessageAttributesCharacters[256] = {};
constexpr TStringBuf AWS_RESERVED_PREFIX = "AWS.";
constexpr TStringBuf AMAZON_RESERVED_PREFIX = "Amazon.";
constexpr TStringBuf YA_RESERVED_PREFIX = "Ya.";
constexpr TStringBuf YC_RESERVED_PREFIX = "YC.";
constexpr TStringBuf YANDEX_RESERVED_PREFIX = "Yandex.";
constexpr TStringBuf FIFO_SUFFIX = ".fifo";

static bool MakeMessageAttributesCharacters() {
    char src[] = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_-.";
    for (size_t i = 0; i < Y_ARRAY_SIZE(src) - 1; ++i) {
        MessageAttributesCharacters[static_cast<unsigned char>(src[i])] = true;
    }
    return true;
}

static const bool MessageAttributesCharactersAreMade = MakeMessageAttributesCharacters();

// https://docs.aws.amazon.com/en_us/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-attributes.html
bool ValidateMessageAttributeName(TStringBuf str, bool& hasYandexPrefix, bool allowYandexPrefix) {
    if (!str || str.size() > 256) {
        return false;
    }

    if (str[0] == '.' || str[str.size() - 1] == '.') {
        return false;
    }

    for (size_t i = 0; i < str.size() - 1; ++i) {
        if (!MessageAttributesCharacters[static_cast<unsigned char>(str[i])]) {
            return false;
        }
        if (str[i] == '.' && str[i + 1] == '.') {
            return false;
        }
    }

    if (!MessageAttributesCharacters[static_cast<unsigned char>(str[str.size() - 1])]) {
        return false;
    }

    // AWS reserved prefixes:
    if (AsciiHasPrefixIgnoreCase(str, AWS_RESERVED_PREFIX) || AsciiHasPrefixIgnoreCase(str, AMAZON_RESERVED_PREFIX)) {
        return false;
    }

    // Yandex reserved prefixes:
    if (AsciiHasPrefixIgnoreCase(str, YA_RESERVED_PREFIX) || AsciiHasPrefixIgnoreCase(str, YC_RESERVED_PREFIX) || AsciiHasPrefixIgnoreCase(str, YANDEX_RESERVED_PREFIX)) {
        hasYandexPrefix = true;
        if (!allowYandexPrefix) {
            return false;
        }
    }

    return true;
}

static bool QueueNameCharacters[256] = {};

static bool MakeQueueNameCharacters() {
    char src[] = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-_";
    for (size_t i = 0; i < Y_ARRAY_SIZE(src) - 1; ++i) {
        QueueNameCharacters[static_cast<unsigned char>(src[i])] = true;
    }
    return true;
}

static const bool QueueNameCharactersAreMade = MakeQueueNameCharacters();

bool ValidateQueueNameOrUserName(TStringBuf name) {
    if (name.size() > 80) {
        return false;
    }
    if (AsciiHasSuffixIgnoreCase(name, FIFO_SUFFIX)) {
        name = name.SubStr(0, name.size() - FIFO_SUFFIX.size());
    }
    if (name.empty()) {
        return false;
    }
    for (size_t i = 0; i < name.size(); ++i) {
        if (!QueueNameCharacters[static_cast<unsigned char>(name[i])]) {
            return false;
        }
    }
    return true;
}

static TString ProtobufToString(const NProtoBuf::Message& proto) {
    TString ret;
    Y_PROTOBUF_SUPPRESS_NODISCARD proto.SerializeToString(&ret);
    return ret;
}

static TString EncodeString(const TString& value) {
    TString result = Base64EncodeUrl(value);
    // Remove these symbols from the end of the string to avoid problems
    // with cgi escaping.
    while (!result.empty() && (result.back() == ',' || result.back() == '=')) {
        result.pop_back();
    }

    return result;
}

TString EncodeReceiptHandle(const TReceipt& receipt) {
    return EncodeString(ProtobufToString(receipt));
}

TReceipt DecodeReceiptHandle(const TString& receipt) {
    TString decoded = Base64DecodeUneven(receipt);
    TReceipt ret;
    Y_ENSURE(!decoded.empty());
    Y_ENSURE(ret.ParseFromString(decoded));
    return ret;
}

// https://docs.aws.amazon.com/en_us/AWSSimpleQueueService/latest/APIReference/API_SendMessage.html
static bool IsValidMessageBodyCharacter(wchar32 c) {
    if (c < 0x20u) {
        return c == 0x9u || c == 0xAu || c == 0xDu;
    } else {
        if (c <= 0xD7FFu) {
            return true;
        } else if (c >= 0xE000u && c <= 0xFFFDu) {
            return true;
        } else if (c >= 0x10000u && c <= 0x10FFFFu) {
            return true;
        }
    }
    return false;
}

bool ValidateMessageBody(TStringBuf body, TString& errorDescription) {
    const unsigned char* s = reinterpret_cast<const unsigned char*>(body.data());
    const unsigned char* const end = s + body.size();
    while (s != end) {
        wchar32 c;
        size_t clen;
        const RECODE_RESULT result = SafeReadUTF8Char(c, clen, s, end);
        if (result != RECODE_OK) {
            errorDescription = TStringBuilder() << "nonunicode characters are not allowed";
            return false;
        }
        if (!IsValidMessageBodyCharacter(c)) {
            errorDescription = TStringBuilder() << "character " << Hex(c) << " is not allowed";
            return false;
        }

        s += clen;
    }
    return true;
}

bool TTagValidator::ValidateString(const TString& str, const bool isKey) {
    if (str.empty()) {
        Error = isKey ? "Tag key must not be empty."
                      : "Tag value must not be empty.";
        return false;
    }

    if (isKey && !IsAsciiLower(str[0])) {
        Error = "Tag key must start with a lowercase letter (a-z).";
        return false;
    }

    constexpr size_t maxSize = 63;
    if (str.size() > maxSize) {
        Error = isKey ? "Tag key must not be longer than 63 characters."
                      : "Tag value must not be longer than 63 characters.";
        return false;
    }

    for (char c : str) {
        bool ok = IsAsciiLower(c) || IsAsciiDigit(c) || c == '-' || c == '_';
        if (!ok) {
            Error = isKey ? "Tag key can only consist of ASCII lowercase letters, digits, dashes and underscores."
                          : "Tag value can only consist of ASCII lowercase letters, digits, dashes and underscores.";
            return false;
        }
    }

    return true;
}

TString TagsToJson(NJson::TJsonMap tags) {
    TStringStream json;
    NJson::WriteJson(&json, &tags, /*formatOutput=*/false, /*sortkeys=*/true);
    return json.Str();
}

TTagValidator::TTagValidator(const TMaybe<NJson::TJsonMap>& currentTags, const NJson::TJsonMap& newTags)
    : CurrentTags(currentTags)
    , NewTags(newTags)
{
    if (newTags.GetMapSafe().size() > TLimits::MaxTagCount) {
        Error = "Too many tags added for queue";
        return;
    }

    for (const auto& [k, v] : newTags.GetMapSafe()) {
        if (!ValidateString(k, true) || !ValidateString(v.GetStringSafe(), false)) {
            return;
        }
    }

    PrepareJson();
}

bool TTagValidator::Validate() const {
    return Error.empty();
}

void TTagValidator::PrepareJson() {
    auto tags = CurrentTags.GetOrElse(NJson::TJsonMap());
    auto& map = tags.GetMapSafe();

    for (const auto& [k, v] : NewTags.GetMapSafe()) {
        map.insert_or_assign(k, v);
        if (map.size() > TLimits::MaxTagCount) {
            Error = "Too many tags added for queue";
            return;
        }
    }

    Json = TagsToJson(tags);
}

TString TTagValidator::GetJson() const {
    return Json;
}

TString TTagValidator::GetError() const {
    return Error;
}

} // namespace NKikimr::NSQS
