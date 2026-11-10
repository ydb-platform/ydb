#include "linter.h"

#include <util/string/split.h>

namespace NYql::NFastCheck {

TVector<TCheckFilter> ParseChecks(const TString& checks) {
    TVector<TCheckFilter> res;
    for (TStringBuf one : StringSplitter(checks).SplitByString(",")) {
        TCheckFilter f;
        TStringBuf afterPrefix = one;
        if (one.AfterPrefix("-", afterPrefix)) {
            f.Include = false;
        }

        f.CheckNameGlob = afterPrefix;
        res.push_back(f);
    }

    return res;
}

} // namespace NYql::NFastCheck
