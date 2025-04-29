#include "name_index.h"

#include <yql/essentials/core/sql_types/normalize_name.h>

#include <util/charset/utf8.h>

namespace NSQLComplete {

    TString NormalizeName(const TString& name) {
        if (name.find("::") != TString::npos) {
            return name;
        }

        return NYql::NormalizeName(name);
    }

    TString LowerizeName(const TString& name) {
        return ToLowerUTF8(name);
    }

    TString UnchangedName(const TString& name) {
        return name;
    }

} // namespace NSQLComplete
