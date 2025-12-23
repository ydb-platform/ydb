#include "yql_ytflow_optimization.h"

#include <util/system/yassert.h>


namespace NYql {

namespace {

[[noreturn]] void AbortUnimplemented(const char* methodName) {
    Y_ABORT(
        "Method %s is not implemented in IYtflowOptimization descendant",
        methodName);
}

} // anonymous namespace

TExprNode::TPtr TEmptyYtflowOptimization::ApplyExtractMembers(
    const TExprNode::TPtr& /*read*/,
    const TExprNode::TPtr& /*members*/,
    TExprContext& /*ctx*/
) {
    AbortUnimplemented(__FUNCTION__);
}

TExprNode::TPtr TEmptyYtflowOptimization::ApplyUnordered(
    const TExprNode::TPtr& /*read*/,
    TExprContext& /*ctx*/
) {
    AbortUnimplemented(__FUNCTION__);
}

TExprNode::TPtr TEmptyYtflowOptimization::TrimWriteContent(
    const TExprNode::TPtr& /*write*/,
    TExprContext& /*ctx*/
) {
    AbortUnimplemented(__FUNCTION__);
}

} // namespace NYql
