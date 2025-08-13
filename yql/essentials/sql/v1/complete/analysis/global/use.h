#pragma once

#include "global.h"
#include "input.h"
#include "named_node.h"

#include <util/generic/ptr.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NSQLComplete {

    TMaybe<TClusterContext> ParseClusterContext(SQLv1::Cluster_exprContext* ctx, const TNamedNodes& nodes);

    TMaybe<TClusterContext> FindUseStatement(TParsedInput input, const TNamedNodes& nodes);

} // namespace NSQLComplete
