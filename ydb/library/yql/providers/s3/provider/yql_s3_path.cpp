#include "yql_s3_path.h"

#include <ydb/library/yql/utils/yql_panic.h>

#include <contrib/libs/re2/re2/re2.h>

namespace NYql::NS3 {

TString NormalizePath(const TString& path, char slash) {
    YQL_ENSURE(!path.empty());
    TString result;
    bool start = true;
    for (char c : path) {
        if (start && c == slash) {
            continue;
        }
        start = false;
        if (c != slash || result.back() != slash) {
            result.push_back(c);
        }
    }

    if (result.empty()) {
        YQL_ENSURE(start);
        result.push_back(slash);
    }

    return result;
}

size_t GetFirstWildcardPos(const TString& path) {
    return path.find_first_of("*?{");
}

TString EscapeRegex(const TString& str) {
    return RE2::QuoteMeta(re2::StringPiece(str));
}

}
