#include "depth_limiting_listener.h"

#include <util/string/cast.h>
#include <util/system/compiler.h>

namespace NAntlrAST {

TDepthLimitingListener::TDepthLimitingListener(size_t maxDepth)
    : MaxDepth_(maxDepth)
{
}

void TDepthLimitingListener::Reset() {
    CurrentDepth_ = 0;
}

void TDepthLimitingListener::enterEveryRule(antlr4::ParserRuleContext* ctx) {
    Y_UNUSED(ctx);

    ++CurrentDepth_;
    if (MaxDepth_ < CurrentDepth_) {
        throw antlr4::ParseCancellationException(
            "Maximum parse tree depth exceeded: " + ToString(MaxDepth_));
    }
}

void TDepthLimitingListener::exitEveryRule(antlr4::ParserRuleContext* ctx) {
    Y_UNUSED(ctx);

    Y_ENSURE(0 < CurrentDepth_);
    --CurrentDepth_;
}

void TDepthLimitingListener::visitTerminal(antlr4::tree::TerminalNode* node) {
    Y_UNUSED(node);
}

void TDepthLimitingListener::visitErrorNode(antlr4::tree::ErrorNode* node) {
    Y_UNUSED(node);
}

} // namespace NAntlrAST
