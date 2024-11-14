#include "utils.h"
#include <yql/essentials/ast/yql_expr.h>

namespace NSQLTranslationPG {

TString NormalizeName(TStringBuf name) {
    return NYql::NormalizeName(name);
}

}
