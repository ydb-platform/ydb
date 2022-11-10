#include "util.h"

namespace NYql::NS3Util {

TIssues AddParentIssue(const TStringBuilder& prefix, TIssues&& issues) {
    if (!issues) {
        return TIssues{};
    }
    TIssue result(prefix);
    for (auto& issue: issues) {
        result.AddSubIssue(MakeIntrusive<TIssue>(issue));
    }
    return TIssues{result};
}

}
