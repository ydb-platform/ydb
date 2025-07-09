#include "yql_func_stack.h"
#include "yql_expr_type_annotation.h"

#include <util/generic/scope.h>

namespace NYql {

void TFunctionStack::EnterFrame(const TExprNode& input, TExprContext& ctx) {
    if (input.Type() == TExprNode::Callable) {
        CurrentFunctions_.push(std::make_pair(input.Content(), false));
    }

    ctx.IssueManager.AddScope([this, &input, &ctx]() -> TIssuePtr {
        if (!CurrentFunctions_.empty() && CurrentFunctions_.top().second) {
            return nullptr;
        }

        if (input.Type() == TExprNode::Callable && input.Content().EndsWith('!')) {
            return nullptr;
        }

        TStringBuilder str;
        str << "At ";
        switch (input.Type()) {
        case TExprNode::Callable:
            str << "function: " << NormalizeCallableName(input.Content());
            break;
        case TExprNode::List:
            str << "tuple";
            break;
        case TExprNode::Lambda:
            str << "lambda";
            break;
        default:
            str << "unknown";
        }

        return MakeIntrusive<TIssue>(ctx.GetPosition(input.Pos()), str);
    });
}

void TFunctionStack::LeaveFrame(const TExprNode& input, TExprContext& ctx) {
    if (input.Type() == TExprNode::Callable) {
        CurrentFunctions_.pop();
    }

    ctx.IssueManager.LeaveScope();
}

void TFunctionStack::Reset() {
    CurrentFunctions_ = {};
}

void TFunctionStack::MarkUsed() {
    if (!CurrentFunctions_.empty()) {
        CurrentFunctions_.top().second = true;
    }
}

}
