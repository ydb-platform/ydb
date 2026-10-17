#pragma once

#include "parse_tree.h"

namespace NSQLPureAST {

class TSQLv1BaseVisitor: public SQLv1Antlr4BaseVisitor {
protected:
    // TODO(YQL-21439): remove Nullable suffix.
    std::any VisitNullable(antlr4::ParserRuleContext* ctx);
    std::any aggregateResult(std::any aggregate, std::any nextResult) override;
};

} // namespace NSQLPureAST
