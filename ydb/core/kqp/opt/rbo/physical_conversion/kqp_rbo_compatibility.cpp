#include "kqp_rbo_compatibility.h"

namespace NKikimr::NKqp {

bool NeedsRboCompatibilityLowering(const NYql::TExprNode::TPtr&) {
    return false;
}

NYql::TExprNode::TPtr RewriteRboCompatibilityNode(
    const NYql::TExprNode::TPtr& node,
    NYql::TExprContext&,
    const NYql::TTypeAnnotationContext&) {
    return node;
}

void EnsureRboCompatibilityLowered(const NYql::TExprNode::TPtr&) {
}

} // namespace NKikimr::NKqp
