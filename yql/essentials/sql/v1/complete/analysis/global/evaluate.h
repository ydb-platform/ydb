#pragma once

#include "parse_tree.h"

#include <yql/essentials/sql/v1/complete/core/environment.h>

namespace NSQLComplete {

    NYT::TNode Evaluate(SQLv1::Bind_parameterContext* ctx, const TEnvironment& env);

} // namespace NSQLComplete
