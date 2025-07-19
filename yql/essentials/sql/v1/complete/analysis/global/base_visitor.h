#pragma once

#include "parse_tree.h"

namespace NSQLComplete {

    class TSQLv1BaseVisitor: public SQLv1Antlr4BaseVisitor {
    protected:
        std::any VisitNullable(antlr4::ParserRuleContext* ctx);
        std::any aggregateResult(std::any aggregate, std::any nextResult) override;
    };

} // namespace NSQLComplete
