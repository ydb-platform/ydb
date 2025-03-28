#pragma once

#include "sql_antlr4.h"
#include "string_util.h"

#include <contrib/libs/antlr4_cpp_runtime/src/ANTLRInputStream.h>
#include <contrib/libs/antlr4_cpp_runtime/src/BufferedTokenStream.h>
#include <contrib/libs/antlr4_cpp_runtime/src/Vocabulary.h>
#include <contrib/libs/antlr4-c3/src/CodeCompletionCore.hpp>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <unordered_set>

namespace NSQLComplete {

    struct TSuggestedToken {
        TTokenId Number;
    };

    struct TMatchedRule {
        TRuleId Index;
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
        };

        virtual TC3Candidates Complete(TStringBuf queryPrefix) = 0;
        virtual const antlr4::dfa::Vocabulary& GetVocabulary() const = 0;
        virtual ~IC3Engine() = default;
    };

    template <class Lexer, class Parser>
    struct TAntlrGrammar {
        using TLexer = Lexer;
        using TParser = Parser;

        TAntlrGrammar() = delete;
    };

    template <class G>
    class TC3Engine: public IC3Engine {
    public:
        explicit TC3Engine(TConfig config)
            : Chars()
            , Lexer(&Chars)
            , Tokens(&Lexer)
            , Parser(&Tokens)
            , CompletionCore(&Parser)
        {
            Lexer.removeErrorListeners();
            Parser.removeErrorListeners();

            CompletionCore.ignoredTokens = std::move(config.IgnoredTokens);
            CompletionCore.preferredRules = std::move(config.PreferredRules);
        }

        TC3Candidates Complete(TStringBuf queryPrefix) override {
            Assign(queryPrefix);
            const auto caretTokenIndex = CaretTokenIndex(queryPrefix);
            auto candidates = CompletionCore.collectCandidates(caretTokenIndex);
            return Converted(std::move(candidates));
        }

        const antlr4::dfa::Vocabulary& GetVocabulary() const override {
            return Lexer.getVocabulary();
        }

    private:
        void Assign(TStringBuf queryPrefix) {
            Chars.load(queryPrefix.Data(), queryPrefix.Size(), /* lenient = */ false);
            Lexer.reset();
            Tokens.setTokenSource(&Lexer);

            Tokens.fill();
        }

        size_t CaretTokenIndex(TStringBuf queryPrefix) {
            const auto tokensCount = Tokens.size();
            if (2 <= tokensCount && !LastWord(queryPrefix).Empty()) {
                return tokensCount - 2;
            }
            return tokensCount - 1;
        }

        static TC3Candidates Converted(c3::CandidatesCollection candidates) {
            TC3Candidates converted;
            for (const auto& [token, _] : candidates.tokens) {
                converted.Tokens.emplace_back(token);
            }
            for (const auto& [rule, _] : candidates.rules) {
                converted.Rules.emplace_back(rule);
            }
            return converted;
        }

        antlr4::ANTLRInputStream Chars;
        G::TLexer Lexer;
        antlr4::BufferedTokenStream Tokens;
        G::TParser Parser;
        c3::CodeCompletionCore CompletionCore;
    };

} // namespace NSQLComplete
