#pragma once

#include <yql/essentials/sql/v1/complete/antlr4/defs.h>
#include <yql/essentials/sql/v1/complete/core/statement.h>

#include <util/generic/maybe.h>

namespace NSQLComplete {

    bool IsLikelyPragmaStack(const TParserCallStack& stack);

    bool IsLikelyTypeStack(const TParserCallStack& stack);

    bool IsLikelyFunctionStack(const TParserCallStack& stack);

    bool IsLikelyTableFunctionStack(const TParserCallStack& stack);

    bool IsLikelyHintStack(const TParserCallStack& stack);

    bool IsLikelyObjectRefStack(const TParserCallStack& stack);

    bool IsLikelyExistingTableStack(const TParserCallStack& stack);

    bool IsLikelyTableArgStack(const TParserCallStack& stack);

    bool IsLikelyClusterStack(const TParserCallStack& stack);

    bool IsLikelyColumnStack(const TParserCallStack& stack);

    bool IsLikelyBindingStack(const TParserCallStack& stack);

    TMaybe<EStatementKind> StatementKindOf(const TParserCallStack& stack);

    std::unordered_set<TRuleId> GetC3PreferredRules();

} // namespace NSQLComplete
