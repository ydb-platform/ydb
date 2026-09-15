#pragma once

#include "c3i.h"
#include "pipeline.h"

#include <yql/essentials/sql/v1/ide/completion/text/word.h>

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
class TC3Engine: public IC3Engine, private IC3Engine::TConfig {
public:
    explicit TC3Engine(TConfig config)
        : IC3Engine::TConfig(std::move(config))
    {
        for (TRuleId rule : IgnoredRules) {
            PreferredRules.emplace(rule);
        }

        PurifyForcedTokens();
    }

    TC3Candidates Complete(TStringBuf text, size_t caretTokenIndex) const override {
        antlr4::ANTLRInputStream chars(text);
        typename G::TLexer lexer(&chars);
        antlr4::BufferedTokenStream tokens(&lexer);
        typename G::TParser parser(&tokens);

        lexer.removeErrorListeners();
        parser.removeErrorListeners();

        tokens.fill();

        c3::CodeCompletionCore c3(&parser);
        c3.ignoredTokens = IgnoredTokens;
        c3.preferredRules = PreferredRules;

        c3::CandidatesCollection candidates = c3.collectCandidates(caretTokenIndex);
        return Converted(std::move(candidates), caretTokenIndex, tokens);
    }

private:
    void PurifyForcedTokens() {
        for (auto it = ForcedPreviousByToken.begin(); it != ForcedPreviousByToken.end();) {
            const auto& [token, previous] = *it;
            if (previous.empty()) {
                IgnoredTokens.emplace(token);
                it = ForcedPreviousByToken.erase(it);
            } else {
                it = std::next(it);
            }
        }
    }

    TC3Candidates Converted(
        c3::CandidatesCollection candidates,
        size_t caretTokenIndex,
        const antlr4::BufferedTokenStream& tokens) const {
        TC3Candidates converted;

        for (auto& [token, following] : candidates.tokens) {
            if (IsIgnored(token, caretTokenIndex, tokens)) {
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

    bool IsIgnored(
        TTokenId token,
        size_t caretTokenIndex,
        const antlr4::BufferedTokenStream& tokens) const {
        auto previous = PreviousToken(caretTokenIndex, tokens);

        auto disabled = DisabledPreviousByToken.find(token);
        auto forced = ForcedPreviousByToken.find(token);

        return (disabled != DisabledPreviousByToken.end() && disabled->second.contains(previous)) ||
               (forced != ForcedPreviousByToken.end() && !forced->second.contains(previous));
    }

    [[nodiscard]] bool IsIgnored(TRuleId head, const std::vector<TRuleId> tail) const {
        return IgnoredRules.contains(head) ||
               AnyOf(tail, [this](TRuleId r) { return IgnoredRules.contains(r); });
    }

    static TTokenId PreviousToken(size_t caretTokenIndex, const antlr4::BufferedTokenStream& tokens) {
        ssize_t index = static_cast<ssize_t>(caretTokenIndex) - 1;
        while (0 <= index && tokens.get(index)->getChannel() == antlr4::Token::HIDDEN_CHANNEL) {
            --index;
        }

        if (index < 0) {
            return antlr4::Token::INVALID_TYPE;
        }

        return tokens.get(index)->getType();
    }
};

} // namespace NSQLComplete
