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
#include <util/generic/yexception.h>

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
            , IgnoredRules_(std::move(config.IgnoredRules))
            , DisabledPreviousByToken_(std::move(config.DisabledPreviousByToken))
            , ForcedPreviousByToken_(std::move(config.ForcedPreviousByToken))
        {
            Lexer_.removeErrorListeners();
            Parser_.removeErrorListeners();

            CompletionCore_.ignoredTokens = std::move(config.IgnoredTokens);
            CompletionCore_.preferredRules = std::move(config.PreferredRules);

            for (TRuleId rule : IgnoredRules_) {
                CompletionCore_.preferredRules.emplace(rule);
            }

            PurifyForcedTokens();
        }

        TC3Candidates Complete(TStringBuf text, size_t caretTokenIndex) override {
            Assign(text);
            auto candidates = CompletionCore_.collectCandidates(caretTokenIndex);
            return Converted(std::move(candidates), caretTokenIndex);
        }

    private:
        void PurifyForcedTokens() {
            for (auto it = ForcedPreviousByToken_.begin(); it != ForcedPreviousByToken_.end();) {
                const auto& [token, previous] = *it;
                if (previous.empty()) {
                    CompletionCore_.ignoredTokens.emplace(token);
                    it = ForcedPreviousByToken_.erase(it);
                } else {
                    it = std::next(it);
                }
            }
        }

        void Assign(TStringBuf prefix) {
            Chars_.load(prefix.Data(), prefix.Size(), /* lenient = */ false);
            Lexer_.reset();
            Tokens_.setTokenSource(&Lexer_);
            Tokens_.fill();
        }

        TC3Candidates Converted(c3::CandidatesCollection candidates, size_t caretTokenIndex) {
            TC3Candidates converted;

            for (auto& [token, following] : candidates.tokens) {
                if (IsIgnored(token, caretTokenIndex)) {
                    continue;
                }

                converted.Tokens.emplace_back(token, std::move(following));
            }

            for (auto& [rule, data] : candidates.rules) {
                if (IsIgnored(rule, data.ruleList)) {
                    continue;
                }

                converted.Rules.emplace_back(rule, std::move(data.ruleList));
                converted.Rules.back().ParserCallStack.emplace_back(rule);
            }

            return converted;
        }

        bool IsIgnored(TTokenId token, size_t caretTokenIndex) {
            auto previous = PreviousToken(caretTokenIndex);

            auto disabled = DisabledPreviousByToken_.find(token);
            auto forced = ForcedPreviousByToken_.find(token);

            return (disabled != DisabledPreviousByToken_.end() && disabled->second.contains(previous)) ||
                   (forced != ForcedPreviousByToken_.end() && !forced->second.contains(previous));
        }

        bool IsIgnored(TRuleId head, const std::vector<TRuleId> tail) const {
            return IgnoredRules_.contains(head) ||
                   AnyOf(tail, [this](TRuleId r) { return IgnoredRules_.contains(r); });
        }

        TTokenId PreviousToken(size_t caretTokenIndex) {
            ssize_t index = static_cast<ssize_t>(caretTokenIndex) - 1;
            while (0 <= index && Tokens_.get(index)->getChannel() == antlr4::Token::HIDDEN_CHANNEL) {
                --index;
            }

            if (index < 0) {
                return antlr4::Token::INVALID_TYPE;
            }

            return Tokens_.get(index)->getType();
        }

        antlr4::ANTLRInputStream Chars_;
        G::TLexer Lexer_;
        antlr4::BufferedTokenStream Tokens_;
        G::TParser Parser_;
        c3::CodeCompletionCore CompletionCore_;

        std::unordered_set<TRuleId> IgnoredRules_;
        std::unordered_map<TTokenId, std::unordered_set<TTokenId>> DisabledPreviousByToken_;
        std::unordered_map<TTokenId, std::unordered_set<TTokenId>> ForcedPreviousByToken_;
    };

} // namespace NSQLComplete
