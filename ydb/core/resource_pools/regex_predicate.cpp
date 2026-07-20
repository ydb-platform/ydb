#include "regex_predicate.h"

#include <library/cpp/regex/pcre/regexp.h>

#include <util/generic/yexception.h>
#include <util/string/builder.h>


namespace NKikimr::NResourcePool {

namespace {

TRegexPredicate CompileAnchored(TStringBuf originalPattern, TStringBuf translated) {
    TString anchored = TStringBuilder() << "^(?:" << translated << ")$";
    try {
        auto compiled = std::make_shared<TRegExMatch>(anchored, REG_EXTENDED | REG_NOSUB);
        return TRegexPredicate{TString(originalPattern), std::move(compiled)};
    } catch (...) {
        throw yexception() << "Failed to compile pattern '" << originalPattern << "': " << CurrentExceptionMessage();
    }
}

}  // namespace

TRegexPredicate TRegexPredicate::FromGlob(TStringBuf glob) {
    if (glob.empty()) {
        throw yexception() << "TRegexPredicate::FromGlob called with empty value; "
                           << "caller must treat empty as nullopt (no filter)";
    }
    TString translated;
    translated.reserve(glob.size() * 2);
    for (char c : glob) {
        switch (c) {
            case '*':
                translated += ".*";
                break;
            case '?':
                translated += '.';
                break;
            case '.': case '\\': case '+': case '(': case ')':
            case '[': case ']': case '{': case '}':
            case '|': case '^': case '$':
                translated += '\\';
                translated += c;
                break;
            default:
                translated += c;
        }
    }
    return CompileAnchored(glob, translated);
}

bool TRegexPredicate::Match(const TString& value) const {
    if (!Compiled) {
        return false;
    }
    return Compiled->Match(value.c_str());
}

}  // namespace NKikimr::NResourcePool
