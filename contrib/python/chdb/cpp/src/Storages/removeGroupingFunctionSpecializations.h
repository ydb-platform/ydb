#pragma once

#include <Analyzer/IQueryTreeNode.h>

namespace DB_CHDB
{

void removeGroupingFunctionSpecializations(QueryTreeNodePtr & node);

}
