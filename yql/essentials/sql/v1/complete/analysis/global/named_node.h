#pragma once

#include "input.h"

#include <yql/essentials/sql/v1/complete/core/environment.h>

#include <library/cpp/yson/node/node.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NSQLComplete {

    using TNamedNode = std::variant<
        SQLv1::ExprContext*,
        SQLv1::Subselect_stmtContext*,
        NYT::TNode,
        std::monostate>;

    using TNamedNodes = THashMap<TString, TNamedNode>;

    TNamedNodes CollectNamedNodes(TParsedInput input, const TEnvironment& env);

} // namespace NSQLComplete
