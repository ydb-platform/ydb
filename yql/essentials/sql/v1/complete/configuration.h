#pragma once

#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

namespace NSQLComplete {

    struct TConfiguration {
        friend class TSqlCompletionEngine;
        friend TConfiguration MakeYDBConfiguration();
        friend TConfiguration MakeYQLConfiguration();
        friend TConfiguration MakeConfiguration(THashSet<TString> allowedStmts);

    public:
        size_t Limit = 256;

    private:
        THashSet<TString> IgnoredRules_;
        THashMap<TString, THashSet<TString>> DisabledPreviousByToken_;
        THashMap<TString, THashSet<TString>> ForcedPreviousByToken_;
    };

    TConfiguration MakeYDBConfiguration();

    TConfiguration MakeYQLConfiguration();

} // namespace NSQLComplete
