#include "name.h"

#include <util/stream/output.h>

namespace NSQLComplete {

    bool operator<(const TTableId& lhs, const TTableId& rhs) {
        return std::tie(lhs.Cluster, lhs.Path) < std::tie(rhs.Cluster, rhs.Path);
    }

} // namespace NSQLComplete

template <>
void Out<NSQLComplete::TTableId>(IOutputStream& out, const NSQLComplete::TTableId& value) {
    out << value.Cluster << ".`" << value.Path << "`";
}
