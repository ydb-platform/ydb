#include "attribute_name.h"
#include <util/generic/algorithm.h>
#include <util/string/ascii.h>

namespace {
    using namespace NKikimr::NSQS;

    constexpr TStringBuf AWS_RESERVED_PREFIX = "AWS.";
    constexpr TStringBuf AMAZON_RESERVED_PREFIX = "Amazon.";
    constexpr TStringBuf YA_RESERVED_PREFIX = "Ya.";
    constexpr TStringBuf YC_RESERVED_PREFIX = "YC.";
    constexpr TStringBuf YANDEX_RESERVED_PREFIX = "Yandex.";

    TMessageAttributeCheckResult CheckMessageAttributeNameImpl(TStringBuf name, bool wildcard, bool allowYandexPrefix, bool allowAmazonPrefix) {
        TMessageAttributeCheckResult result{
            .IsWildcard = wildcard,
            .HasYandexPrefix = name.StartsWith(YA_RESERVED_PREFIX) || name.StartsWith(YC_RESERVED_PREFIX) || name.StartsWith(YANDEX_RESERVED_PREFIX),
            .HasAmazonPrefix = name.StartsWith(AMAZON_RESERVED_PREFIX) || name.StartsWith(AWS_RESERVED_PREFIX),
        };

        /*
            The name of the message attribute,
            • The name can contain alphanumeric characters and the underscore (_), hyphen (-), and
            period (.).
            • The name is case-sensitive and must be unique among all attribute names for the message.
            • The name must not start with AWS-reserved preﬁxes such as AWS. or Amazon. (or any casing
            variants).
            • The name must not start or end with a period (.), and it should not have periods in succession
            (..).
        • The name can be up to 256 characters long.
        */

        do {
            if (name.empty() && !wildcard) {
                result.ErrorMessage = "Message attribute name is empty";
                break;
            }

            if (name.size() > 256) {
                result.ErrorMessage = "Message attribute name is too long";
                break;
            }

            if (name.EndsWith('.') || name.StartsWith('.')) {
                result.ErrorMessage = "Message attribute name cannot start or end with a period";
                break;
            }

            if (name.Contains(".."sv)) {
                result.ErrorMessage = "Message attribute name cannot have two consecutive periods";
                break;
            }

            if (result.HasYandexPrefix && !allowYandexPrefix) {
                result.ErrorMessage = "Message attribute name cannot start with reserved prefix";
                break;
            }

            if (result.HasAmazonPrefix && !allowAmazonPrefix) {
                result.ErrorMessage = "Message attribute name cannot start with reserved prefix";
                break;
            }

            auto validChar = [](char c) -> bool {
                return (IsAsciiAlnum(c) || EqualToOneOf(c, '_', '-', '.'));
            };
            if (!AllOf(name, validChar)) {
                result.ErrorMessage = "Message attribute name contains invalid characters";
                break;
            }

            result.IsValid = true;
        } while (false);

        return result;
    }

} // namespace

namespace NKikimr::NSQS {

    TMessageAttributeCheckResult CheckMessageAttributeName(TStringBuf name) {
        return CheckMessageAttributeNameImpl(name, false, false, false);
    }

    TMessageAttributeCheckResult CheckMessageAttributeNameRequest(TStringBuf name) {
        TStringBuf part = name;
        bool isWildcard = name.BeforeSuffix(".*", part);
        return CheckMessageAttributeNameImpl(part, isWildcard, false, false);
    }

} // namespace NKikimr::NSQS
