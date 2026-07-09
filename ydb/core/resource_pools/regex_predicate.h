#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

#include <memory>


class TRegExMatch;


namespace NKikimr::NResourcePool {

///
/// A compiled regex predicate for classifier matching.
///
/// Stores both the original user pattern and the compiled regex
/// (anchored as ^(?:pattern)$). Reusable for any regex-typed
/// classifier field (HAS_APP_NAME and future matchers).
///
struct TRegexPredicate {
    TString Pattern;
    std::shared_ptr<TRegExMatch> Compiled;

    static TRegexPredicate Compile(TStringBuf pattern);

    bool Match(const TString& value) const;
};

}  // namespace NKikimr::NResourcePool
