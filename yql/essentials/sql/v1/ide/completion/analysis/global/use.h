#pragma once

#include "global.h"
#include "input.h"
#include <yql/essentials/sql/v1/ide/analysis/named_node_resolution.h>

#include <util/generic/ptr.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NSQLComplete {

TMaybe<TClusterContext> ParseClusterContext(SQLv1::Cluster_exprContext* ctx, const INamedNodes& nodes);

TMaybe<TClusterContext> FindUseStatement(TParsedInput input, const INamedNodes& nodes);

} // namespace NSQLComplete
