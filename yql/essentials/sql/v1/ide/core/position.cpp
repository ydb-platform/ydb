#include "position.h"

namespace NSQLPureAST {

bool operator<(const TPosition& lhs, const TPosition& rhs) {
    return std::tie(lhs.Line, lhs.Column) < std::tie(rhs.Line, rhs.Column);
}

} // namespace NSQLPureAST

Y_DECLARE_OUT_SPEC(, NSQLPureAST::TPosition, out, value) {
    out << value.Line << ":" << value.Column;
}
