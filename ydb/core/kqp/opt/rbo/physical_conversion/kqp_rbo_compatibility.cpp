#include "kqp_rbo_compatibility.h"
#include "kqp_rbo_physical_strict_cast.h"

#include <yql/essentials/core/yql_expr_optimize.h>

namespace NKikimr::NKqp {
namespace {

using namespace NYql;

TExprNode::TPtr FindCompatibilityNode(const TExprNode::TPtr& root) {
    return FindNode(root, [](const TExprNode::TPtr& node) {
        return node->IsCallable("StrictCast");
    });
}

} // namespace

bool NeedsRboCompatibilityLowering(const NYql::TExprNode::TPtr& root) {
    return !!FindCompatibilityNode(root);
}

NYql::TExprNode::TPtr RewriteRboCompatibilityNode(
    const NYql::TExprNode::TPtr& node,
    NYql::TExprContext& ctx,
    const NYql::TTypeAnnotationContext&) {
    if (node->IsCallable("StrictCast")) {
        return NPhysicalConvertionUtils::ExpandScalarStrictCast(node, ctx);
    }
    return node;
}

void EnsureRboCompatibilityLowered(const NYql::TExprNode::TPtr& root) {
    if (const auto unsupported = FindCompatibilityNode(root)) {
        YQL_ENSURE(false, "Focused RBO compatibility lowering failed on "
                              << unsupported->Content());
    }
}

} // namespace NKikimr::NKqp
