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

template <>
void Out<NSQLComplete::TAliased<NSQLComplete::TTableId>>(IOutputStream& out, const NSQLComplete::TAliased<NSQLComplete::TTableId>& value) {
    Out<NSQLComplete::TTableId>(out, value);
    out << " AS " << value.Alias;
}

template <>
void Out<NSQLComplete::TColumnId>(IOutputStream& out, const NSQLComplete::TColumnId& value) {
    out << value.TableAlias << "." << value.Name;
}
