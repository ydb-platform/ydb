#pragma once

#include "named_node_resolution.h"
#include "parse_tree.h"

#include <yql/essentials/sql/v1/complete/core/environment.h>

namespace NSQLComplete {

using TIdentifier = TString;

using TPartialValue = std::variant<
    NYT::TNode,
    TIdentifier,
    std::monostate>;

bool IsDefined(const TPartialValue& value);

TMaybe<TString> ToObjectRef(const TPartialValue& value);

NYT::TNode Evaluate(SQLv1::Bind_parameterContext* ctx, const INamedNodes& nodes);

TPartialValue PartiallyEvaluate(antlr4::ParserRuleContext* ctx, const INamedNodes& nodes);

} // namespace NSQLComplete
