#include "name.h"

#include <yql/essentials/core/sql_types/normalize_name.h>

#include <util/stream/output.h>
#include <util/charset/utf8.h>

namespace NSQLComplete {

bool operator<(const TTableId& lhs, const TTableId& rhs) {
    return std::tie(lhs.Cluster, lhs.Path) < std::tie(rhs.Cluster, rhs.Path);
}

TString LowerizeName(TStringBuf name) {
    return ToLowerUTF8(name);
}

TString NormalizeName(TStringBuf name) {
    TString normalized(name);
    TMaybe<NYql::TIssue> error = NYql::NormalizeName(NYql::TPosition(), normalized);
    if (!error.Empty()) {
        return LowerizeName(name);
    }
    return normalized;
}

} // namespace NSQLComplete

Y_DECLARE_OUT_SPEC(, NSQLComplete::TTableId, out, value) {
    out << value.Cluster << ".`" << value.Path << "`";
}

Y_DECLARE_OUT_SPEC(, NSQLComplete::TAliased<NSQLComplete::TTableId>, out, value) {
    Out<NSQLComplete::TTableId>(out, value);
    out << " AS " << value.Alias;
}

Y_DECLARE_OUT_SPEC(, NSQLComplete::TColumnId, out, value) {
    out << value.TableAlias << "." << value.Name;
}
