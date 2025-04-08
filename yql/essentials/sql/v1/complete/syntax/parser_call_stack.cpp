#include "parser_call_stack.h"

#include "grammar.h"

#include <util/generic/vector.h>
#include <util/generic/algorithm.h>
#include <util/generic/yexception.h>

#define DEBUG_SYMBOLIZE_STACK(stack) \
    auto debug_symbolized_##stack = Symbolized(stack)

namespace NSQLComplete {

    const TVector<TRuleId> KeywordRules = {
        RULE(Keyword),
        RULE(Keyword_expr_uncompat),
        RULE(Keyword_table_uncompat),
        RULE(Keyword_select_uncompat),
        RULE(Keyword_alter_uncompat),
        RULE(Keyword_in_uncompat),
        RULE(Keyword_window_uncompat),
        RULE(Keyword_hint_uncompat),
        RULE(Keyword_as_compat),
        RULE(Keyword_compat),
    };

    const TVector<TRuleId> TypeNameRules = {
        RULE(Type_name_simple),
        RULE(An_id_or_type),
    };

    const TVector<TRuleId> FunctionNameRules = {
        RULE(Id_expr),
        RULE(An_id_or_type),
        RULE(Id_or_type),
    };

    TVector<std::string> Symbolized(const TParserCallStack& stack) {
        const ISqlGrammar& grammar = GetSqlGrammar();

        TVector<std::string> symbolized;
        symbolized.reserve(stack.size());
        for (const TRuleId& rule : stack) {
            symbolized.emplace_back(grammar.SymbolizedRule(rule));
        }
        return symbolized;
    }

    bool EndsWith(const TParserCallStack& suffix, const TParserCallStack& stack) {
        if (stack.size() < suffix.size()) {
            return false;
        }
        const size_t prefixSize = stack.size() - suffix.size();
        return Equal(std::begin(stack) + prefixSize, std::end(stack), std::begin(suffix));
    }

    bool Contains(const TParserCallStack& sequence, const TParserCallStack& stack) {
        return !std::ranges::search(stack, sequence).empty();
    }

    bool ContainsRule(TRuleId rule, const TParserCallStack& stack) {
        return Find(stack, rule) != std::end(stack);
    }

    bool IsLikelyTypeStack(const TParserCallStack& stack) {
        return EndsWith({RULE(Type_name_simple)}, stack) ||
               (Contains({RULE(Invoke_expr),
                          RULE(Named_expr_list),
                          RULE(Named_expr),
                          RULE(Expr)}, stack) &&
                EndsWith({RULE(Atom_expr), RULE(An_id_or_type)}, stack));
    }

    bool IsLikelyFunctionStack(const TParserCallStack& stack) {
        return EndsWith({RULE(Unary_casual_subexpr), RULE(Id_expr)}, stack) ||
               EndsWith({RULE(Unary_casual_subexpr),
                         RULE(Atom_expr),
                         RULE(An_id_or_type)}, stack);
    }

    std::unordered_set<TRuleId> GetC3PreferredRules() {
        std::unordered_set<TRuleId> preferredRules;
        preferredRules.insert(std::begin(KeywordRules), std::end(KeywordRules));
        preferredRules.insert(std::begin(TypeNameRules), std::end(TypeNameRules));
        preferredRules.insert(std::begin(FunctionNameRules), std::end(FunctionNameRules));
        return preferredRules;
    }

} // namespace NSQLComplete
