#include "base_visitor.h"

namespace NSQLComplete {

    std::any TSQLv1BaseVisitor::aggregateResult(std::any aggregate, std::any nextResult) {
        if (nextResult.has_value()) {
            return nextResult;
        }
        return aggregate;
    }

} // namespace NSQLComplete
