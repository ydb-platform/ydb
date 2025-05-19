#pragma once
#include <yql/essentials/ast/yql_expr.h>
#include <util/generic/stack.h>
#include <util/generic/strbuf.h>

namespace NYql {

class TFunctionStack {
public:
    void EnterFrame(const TExprNode& input, TExprContext& ctx);
    void LeaveFrame(const TExprNode& input, TExprContext& ctx);
    void Reset();
    void MarkUsed();

private:
    TStack<std::pair<TStringBuf, bool>> CurrentFunctions_;
};

}
