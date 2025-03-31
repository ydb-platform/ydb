#pragma once

#include <yql/essentials/sql/v1/reflect/sql_reflect.h>

#include <util/generic/hash.h>

namespace NSQLTranslationV1 {

    // Makes regexes only for tokens from OtherNames,
    // as keywords and punctuation are trivially matched.
    THashMap<TString, TString> MakeRegexByOtherNameMap(
        const NSQLReflect::TLexerGrammar& grammar, bool ansi);

} // namespace NSQLTranslationV1
