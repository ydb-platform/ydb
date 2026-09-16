#pragma once

#include "base_visitor.h"

namespace NSQLPureAST {

class TSQLv1PathVisitor: public TSQLv1BaseVisitor {
public:
    std::any visitUnary_casual_subexpr(SQLv1::Unary_casual_subexprContext* ctx) override;

protected:
    std::any aggregateResult(std::any aggregate, std::any nextResult) override;
};

} // namespace NSQLPureAST
