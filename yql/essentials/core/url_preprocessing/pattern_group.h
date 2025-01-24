#pragma once

#include <library/cpp/regex/pcre/regexp.h>
#include <util/generic/map.h>
#include <util/generic/vector.h>
#include <util/generic/maybe.h>

#include <utility>

namespace NYql {

class TPatternGroup {
public:
    TPatternGroup() = default;
    void Add(const TString& pattern, const TString& alias);
    bool IsEmpty() const;
    TMaybe<TString> Match(const TString& s) const;

private:
    TMap<TString, std::pair<TRegExMatch, TString>> CompiledPatterns;
};

}
