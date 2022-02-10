#include <ydb/library/yql/core/file_storage/http_download/pattern_group.h> 

namespace NYql {

TPatternGroup::TPatternGroup(const TVector<TString>& patterns) {
    for (auto& p : patterns) {
        Add(p);
    }
}

void TPatternGroup::Add(const TString& pattern) {
    auto it = CompiledPatterns.find(pattern);
    if (it != CompiledPatterns.end()) {
        return;
    }

    CompiledPatterns.emplace(pattern, TRegExMatch(pattern));
}

bool TPatternGroup::IsEmpty() const {
    return CompiledPatterns.empty();
}

bool TPatternGroup::Match(const TString& s) const {
    for (auto& p : CompiledPatterns) {
        if (p.second.Match(s.c_str())) {
            return true;
        }
    }

    return false;
}

}
