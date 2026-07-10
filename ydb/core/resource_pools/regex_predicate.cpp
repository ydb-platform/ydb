#include "regex_predicate.h"

#include <library/cpp/regex/pcre/regexp.h>

#include <util/generic/yexception.h>
#include <util/string/builder.h>


namespace NKikimr::NResourcePool {

TRegexPredicate TRegexPredicate::Compile(TStringBuf pattern) {
    if (pattern.empty()) {
        throw yexception() << "TRegexPredicate::Compile called with empty pattern; "
                           << "caller must treat empty as nullopt (no filter)";
    }
    TString anchored = TStringBuilder() << "^(?:" << pattern << ")$";
    try {
        auto compiled = std::make_shared<TRegExMatch>(anchored, REG_EXTENDED | REG_NOSUB);
        return TRegexPredicate{TString(pattern), std::move(compiled)};
    } catch (...) {
        throw yexception() << "Invalid regex pattern '" << pattern << "': " << CurrentExceptionMessage();
    }
}

bool TRegexPredicate::Match(const TString& value) const {
    if (!Compiled) {
        return false;
    }
    return Compiled->Match(value.c_str());
}

}  // namespace NKikimr::NResourcePool
