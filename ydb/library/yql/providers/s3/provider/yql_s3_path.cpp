#include "yql_s3_path.h"

#include <ydb/library/yql/utils/yql_panic.h>

namespace NYql {

TString NormalizeS3Path(const TString& path, char slash) {
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

}
