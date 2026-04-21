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

        auto pos = ctx.GetPosition(input.Pos());
        TStringBuilder str;
        str << "At ";
        switch (input.Type()) {
        case TExprNode::Callable:
            str << "function: " << NormalizeCallableName(input.Content());
            if (input.Content() == "WithIssue") {
                if (input.ChildrenSize() != 5) {
                    str << ", expected 5 arguments";
                    break;
                }

                if (input.Child(1)->IsAtom()) {
                    pos.File = input.Child(1)->Content();
                } else {
                    str << ", invalid file";
                }

                if (!input.Child(2)->IsAtom() || !TryFromString(input.Child(2)->Content(), pos.Row)) {
                    str << ", invalid row";
                }

                if (!input.Child(3)->IsAtom() || !TryFromString(input.Child(3)->Content(), pos.Column)) {
                    str << ", invalid column";
                }

                if (input.Child(4)->IsAtom()) {
                    str << ", " << input.Child(4)->Content();
                } else {
                    str << ", invalid message";
                }
            }
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

        return MakeIntrusive<TIssue>(pos, str);
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
