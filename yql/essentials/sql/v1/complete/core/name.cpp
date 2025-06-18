#include "name.h"

#include <util/stream/output.h>

template <>
void Out<NSQLComplete::TTableId>(IOutputStream& out, const NSQLComplete::TTableId& value) {
    out << value.Cluster << ".`" << value.Path << "`";
}
