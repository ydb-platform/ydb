#pragma once

#ifdef ERROR
    #undef ERROR
#endif
#include <contrib/libs/antlr4_cpp_runtime/src/antlr4-runtime.h>

#include <util/generic/maybe.h>

namespace NAntlrAST {

class TDepthLimitingListener: public antlr4::tree::ParseTreeListener {
public:
    explicit TDepthLimitingListener(size_t maxDepth);

    void Reset();

    void enterEveryRule(antlr4::ParserRuleContext* ctx) override;
    void exitEveryRule(antlr4::ParserRuleContext* ctx) override;
    void visitTerminal(antlr4::tree::TerminalNode* node) override;
    void visitErrorNode(antlr4::tree::ErrorNode* node) override;

private:
    const size_t MaxDepth_;
    size_t CurrentDepth_ = 0;
};

} // namespace NAntlrAST
