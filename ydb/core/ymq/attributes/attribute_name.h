#pragma once

#include <util/generic/strbuf.h>


namespace NKikimr::NSQS {

struct TMessageAttributeCheckResult {
    bool IsValid : 1 = false;
    bool IsWildcard : 1 = false;
    bool HasYandexPrefix : 1 = false;
    bool HasAmazonPrefix : 1 = false;
    std::string_view ErrorMessage;

    explicit operator bool() const {
        return IsValid;
    }
};

TMessageAttributeCheckResult CheckMessageAttributeName(TStringBuf name);
TMessageAttributeCheckResult CheckMessageAttributeNameRequest(TStringBuf name);

} // namespace NKikimr::NSQS
