#pragma once

#include "c3i.h"
#include "pipeline.h"

#include <yql/essentials/sql/v1/complete/text/word.h>

#include <contrib/libs/antlr4_cpp_runtime/src/ANTLRInputStream.h>
#include <contrib/libs/antlr4_cpp_runtime/src/BufferedTokenStream.h>
#include <contrib/libs/antlr4_cpp_runtime/src/Vocabulary.h>
#include <contrib/libs/antlr4-c3/src/CodeCompletionCore.hpp>

#include <util/generic/fwd.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NSQLComplete {

    template <class G>
    class TC3Engine: public IC3Engine {
    public:
        explicit TC3Engine(TConfig config)
            : Chars_()
            , Lexer_(&Chars_)
            , Tokens_(&Lexer_)
            , Parser_(&Tokens_)
            , CompletionCore_(&Parser_)
        {
            Lexer_.removeErrorListeners();
            Parser_.removeErrorListeners();

            CompletionCore_.ignoredTokens = std::move(config.IgnoredTokens);
            CompletionCore_.preferredRules = std::move(config.PreferredRules);
        }

        TC3Candidates Complete(TStringBuf text, size_t caretTokenIndex) override {
            Assign(text);
            auto candidates = CompletionCore_.collectCandidates(caretTokenIndex);
            return Converted(std::move(candidates));
        }

    private:
        void Assign(TStringBuf prefix) {
            Chars_.load(prefix.Data(), prefix.Size(), /* lenient = */ false);
            Lexer_.reset();
            Tokens_.setTokenSource(&Lexer_);
            Tokens_.fill();
        }

        static TC3Candidates Converted(c3::CandidatesCollection candidates) {
            TC3Candidates converted;
            for (auto& [token, following] : candidates.tokens) {
                converted.Tokens.emplace_back(token, std::move(following));
            }
            for (auto& [rule, data] : candidates.rules) {
                converted.Rules.emplace_back(rule, std::move(data.ruleList));
                converted.Rules.back().ParserCallStack.emplace_back(rule);
            }
            return converted;
        }

        antlr4::ANTLRInputStream Chars_;
        G::TLexer Lexer_;
        antlr4::BufferedTokenStream Tokens_;
        G::TParser Parser_;
        c3::CodeCompletionCore CompletionCore_;
    };

} // namespace NSQLComplete
