#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

#include <memory>


class TRegExMatch;


namespace NKikimr::NResourcePool {

///
/// A compiled predicate for classifier matching. Built from a user-facing
/// glob (`*`, `?` wildcards; other characters — including regex metachars —
/// are literal).
///
struct TRegexPredicate {
    TString Pattern;
    std::shared_ptr<TRegExMatch> Compiled;

    static TRegexPredicate FromGlob(TStringBuf glob);

    bool Match(const TString& value) const;
};

}  // namespace NKikimr::NResourcePool
