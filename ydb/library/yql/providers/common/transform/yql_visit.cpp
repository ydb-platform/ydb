#include "yql_visit.h"

#include <ydb/library/yql/utils/yql_panic.h>

#include <util/string/builder.h>

namespace NYql {

void TVisitorTransformerBase::AddHandler(std::initializer_list<TStringBuf> names, THandler handler) {
    for (auto name: names) {
        YQL_ENSURE(Handlers.emplace(name, handler).second, "Duplicate handler for " << name);
    }
}

IGraphTransformer::TStatus TVisitorTransformerBase::DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
    YQL_ENSURE(input->Type() == TExprNode::Callable);
    output = input;

    if (auto handler = Handlers.FindPtr(input->Content())) {
        return (*handler)(input, output, ctx);
    }
    if (FailOnUnknown) {
        ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "Unsupported callable: " << input->Content()));
        return TStatus::Error;
    }
    return TStatus::Ok;
}

}
