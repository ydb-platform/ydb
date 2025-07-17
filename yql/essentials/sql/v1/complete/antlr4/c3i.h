#pragma once

#include "defs.h"

#include <yql/essentials/sql/v1/complete/core/input.h>

#include <util/generic/fwd.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <unordered_set>

namespace NSQLComplete {

    // std::vector is used to prevent copying a C3 output
    struct TSuggestedToken {
        TTokenId Number;
        std::vector<TTokenId> Following;
    };

    struct TMatchedRule {
        TRuleId Index;
        TParserCallStack ParserCallStack;
    };

    struct TC3Candidates {
        TVector<TSuggestedToken> Tokens;
        TVector<TMatchedRule> Rules;
    };

    class IC3Engine {
    public:
        using TPtr = THolder<IC3Engine>;

        // std::unordered_set is used to prevent copying into c3 core
        struct TConfig {
            std::unordered_set<TTokenId> IgnoredTokens;
            std::unordered_set<TRuleId> PreferredRules;
            std::unordered_set<TRuleId> IgnoredRules;
            std::unordered_map<TTokenId, std::unordered_set<TTokenId>> DisabledPreviousByToken;
            std::unordered_map<TTokenId, std::unordered_set<TTokenId>> ForcedPreviousByToken;
        };

        virtual ~IC3Engine() = default;
        virtual TC3Candidates Complete(TStringBuf text, size_t caretTokenIndex) = 0;
    };

} // namespace NSQLComplete
