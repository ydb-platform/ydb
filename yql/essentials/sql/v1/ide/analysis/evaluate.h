#pragma once

#include <yql/essentials/sql/v1/ide/analysis/named_node_resolution.h>

#include <yql/essentials/sql/v1/ide/core/environment.h>
#include <yql/essentials/sql/v1/ide/pure_ast/parse_tree.h>

namespace NSQLPureAST {

using TIdentifier = TString;

using TPartialValue = std::variant<
    NYT::TNode,
    TIdentifier,
    std::monostate>;

bool IsDefined(const TPartialValue& value);

TMaybe<TString> ToObjectRef(const TPartialValue& value);

NYT::TNode Evaluate(SQLv1::Bind_parameterContext* ctx, const INamedNodes& nodes);

TPartialValue PartiallyEvaluate(antlr4::ParserRuleContext* ctx, const INamedNodes& nodes);

} // namespace NSQLPureAST
