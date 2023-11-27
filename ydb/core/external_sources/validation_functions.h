#pragma once

#include <library/cpp/regex/pcre/regexp.h>


namespace NKikimr::NExternalSource {

void ValidateHostname(const std::vector<TRegExMatch>& hostnamePatterns, const TString& url);

}  // NKikimr::NExternalSource
