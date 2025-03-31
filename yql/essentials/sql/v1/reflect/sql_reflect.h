#pragma once

#include <util/generic/string.h>
#include <util/generic/hash_set.h>
#include <util/generic/hash.h>

namespace NSQLReflect {

    struct TLexerGrammar {
        THashSet<TString> KeywordNames;
        THashSet<TString> PunctuationNames;
        THashSet<TString> OtherNames;
        THashMap<TString, TString> BlockByName;
    };

    TLexerGrammar LoadLexerGrammar();

} // namespace NSQLReflect
