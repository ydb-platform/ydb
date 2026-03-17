#pragma once

#include <Interpreters/ActionsMatcher.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

using ActionsVisitor = ConstInDepthNodeVisitor<ActionsMatcher, true>;

// Needed for CHYT build.
ColumnsWithTypeAndName createBlockForSet(
    const DataTypePtr & left_arg_type,
    const std::shared_ptr<ASTFunction> & right_arg,
    const DataTypes & set_element_types,
    ContextPtr context);

}
