#pragma once

#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

namespace NSQLReflect {

    struct TLexerGrammar {
        THashSet<TString> KeywordNames;
        THashSet<TString> PunctuationNames;
        TVector<TString> OtherNames;
        THashMap<TString, TString> BlockByName;

        static const TStringBuf KeywordBlock(const TStringBuf name);
    };

    TLexerGrammar LoadLexerGrammar();

} // namespace NSQLReflect
