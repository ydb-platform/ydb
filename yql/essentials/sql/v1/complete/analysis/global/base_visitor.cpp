#include "base_visitor.h"

namespace NSQLComplete {

    std::any TSQLv1BaseVisitor::VisitNullable(antlr4::ParserRuleContext* ctx) {
        if (ctx == nullptr) {
            return {};
        }
        return visit(ctx);
    }

    std::any TSQLv1BaseVisitor::aggregateResult(std::any aggregate, std::any nextResult) {
        if (nextResult.has_value()) {
            return nextResult;
        }
        return aggregate;
    }

} // namespace NSQLComplete
