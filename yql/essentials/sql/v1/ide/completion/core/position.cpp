#include "position.h"

namespace NSQLComplete {

bool operator<(const TPosition& lhs, const TPosition& rhs) {
    return std::tie(lhs.Line, lhs.Column) < std::tie(rhs.Line, rhs.Column);
}

} // namespace NSQLComplete

template <>
void Out<NSQLComplete::TPosition>(IOutputStream& out, const NSQLComplete::TPosition& value) {
    out << value.Line << ":" << value.Column;
}
