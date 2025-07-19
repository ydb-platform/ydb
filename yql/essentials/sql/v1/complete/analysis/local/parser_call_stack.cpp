#include "parser_call_stack.h"

#include <yql/essentials/sql/v1/complete/syntax/grammar.h>

#include <util/generic/vector.h>
#include <util/generic/algorithm.h>
#include <util/generic/yexception.h>

#include <ranges>

#define DEBUG_SYMBOLIZE_STACK(stack) \
    auto debug_symbolized_##stack = Symbolized(stack)

namespace NSQLComplete {

    const TVector<TRuleId> PreferredRules = {
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
        RULE(Type_id),

        RULE(An_id_or_type),
        RULE(An_id),
        RULE(Id_expr),
        RULE(Id_or_type),
        RULE(Id_hint),
        RULE(Opt_id_prefix_or_type),
        RULE(Type_name_simple),
        RULE(Type_name_composite),
        RULE(Type_name_decimal),
        RULE(Value_constructor),
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

    bool IsLikelyPragmaStack(const TParserCallStack& stack) {
        return EndsWith({RULE(Pragma_stmt), RULE(Opt_id_prefix_or_type)}, stack) ||
               EndsWith({RULE(Pragma_stmt), RULE(An_id)}, stack);
    }

    bool IsLikelyTypeStack(const TParserCallStack& stack) {
        return EndsWith({RULE(Type_name_simple)}, stack) ||
               EndsWith({RULE(Type_name_composite)}, stack) ||
               EndsWith({RULE(Type_name_decimal)}, stack) ||
               (Contains({RULE(Invoke_expr),
                          RULE(Named_expr_list),
                          RULE(Named_expr),
                          RULE(Expr)}, stack) &&
                (EndsWith({RULE(Atom_expr),
                           RULE(An_id_or_type)}, stack) ||
                 EndsWith({RULE(Atom_expr),
                           RULE(Bind_parameter),
                           RULE(An_id_or_type)}, stack)));
    }

    bool IsLikelyFunctionStack(const TParserCallStack& stack) {
        return EndsWith({RULE(Unary_casual_subexpr), RULE(Id_expr)}, stack) ||
               EndsWith({RULE(Unary_casual_subexpr),
                         RULE(Atom_expr),
                         RULE(An_id_or_type)}, stack) ||
               EndsWith({RULE(Unary_casual_subexpr),
                         RULE(Atom_expr),
                         RULE(Bind_parameter),
                         RULE(An_id_or_type)}, stack) ||
               EndsWith({RULE(Atom_expr), RULE(Id_or_type)}, stack) ||
               EndsWith({RULE(Value_constructor)}, stack);
    }

    bool IsLikelyTableFunctionStack(const TParserCallStack& stack) {
        return EndsWith({RULE(Table_ref), RULE(An_id_expr), RULE(Id_expr)}, stack);
    }

    bool IsLikelyHintStack(const TParserCallStack& stack) {
        return ContainsRule(RULE(Id_hint), stack) ||
               Contains({RULE(External_call_param), RULE(An_id)}, stack);
    }

    bool IsLikelyObjectRefStack(const TParserCallStack& stack) {
        return Contains({RULE(Object_ref)}, stack);
    }

    bool IsLikelyExistingTableStack(const TParserCallStack& stack) {
        return !Contains({RULE(Create_table_stmt),
                          RULE(Simple_table_ref)}, stack) &&
               (Contains({RULE(Simple_table_ref),
                          RULE(Simple_table_ref_core),
                          RULE(Object_ref)}, stack) ||
                Contains({RULE(Single_source),
                          RULE(Table_ref),
                          RULE(Table_key),
                          RULE(Id_table_or_type)}, stack));
    }

    bool IsLikelyTableArgStack(const TParserCallStack& stack) {
        return Contains({RULE(Table_arg)}, stack);
    }

    bool IsLikelyClusterStack(const TParserCallStack& stack) {
        return Contains({RULE(Cluster_expr)}, stack);
    }

    bool IsLikelyColumnStack(const TParserCallStack& stack) {
        return Contains({RULE(Select_core)}, stack) &&
               (Contains({RULE(Result_column)}, stack) ||
                Contains({RULE(Expr)}, stack) ||
                Contains({RULE(Sort_specification)}, stack));
    }

    bool IsLikelyBindingStack(const TParserCallStack& stack) {
        return EndsWith({RULE(Bind_parameter), RULE(An_id_or_type)}, stack);
    }

    TMaybe<EStatementKind> StatementKindOf(const TParserCallStack& stack) {
        for (TRuleId rule : std::ranges::views::reverse(stack)) {
            if (rule == RULE(Process_core) || rule == RULE(Reduce_core) || rule == RULE(Select_core)) {
                return EStatementKind::Select;
            }
            if (rule == RULE(Into_table_stmt)) {
                return EStatementKind::Insert;
            }
        }
        return Nothing();
    }

    std::unordered_set<TRuleId> GetC3PreferredRules() {
        std::unordered_set<TRuleId> preferredRules;
        preferredRules.insert(std::begin(PreferredRules), std::end(PreferredRules));
        return preferredRules;
    }

} // namespace NSQLComplete
