#include "position.h"

namespace NSQLPureAST {

bool operator<(const TPosition& lhs, const TPosition& rhs) {
    return std::tie(lhs.Line, lhs.Column) < std::tie(rhs.Line, rhs.Column);
}

} // namespace NSQLPureAST

template <>
void Out<NSQLPureAST::TPosition>(IOutputStream& out, const NSQLPureAST::TPosition& value) {
    out << value.Line << ":" << value.Column;
}
