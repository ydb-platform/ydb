#pragma once

#include <util/generic/string.h>

namespace NYql::NS3 {

/// remove duplicate slashes
TString NormalizePath(const TString& path, char slash = '/');

/// supported wildcards are: *, ?, {alt1, alt2, ...}
size_t GetFirstWildcardPos(const TString& path);

inline bool HasWildcards(const TString& path) {
    return GetFirstWildcardPos(path) != TString::npos;
}

/// quotes regex meta characters
TString EscapeRegex(const TString& str);
TString EscapeRegex(const std::string_view& str);

TString RegexFromWildcards(const std::string_view& pattern);
TString ValidateWildcards(const std::string_view& pattern);

}
