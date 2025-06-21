#include "pattern_group.h"

namespace NYql {

void TPatternGroup::Add(const TString& pattern, const TString& alias) {
    auto it = CompiledPatterns_.find(pattern);
    if (it != CompiledPatterns_.end()) {
        return;
    }

    CompiledPatterns_.emplace(pattern, std::make_pair(TRegExMatch(pattern), alias));
}

bool TPatternGroup::IsEmpty() const {
    return CompiledPatterns_.empty();
}

TMaybe<TString> TPatternGroup::Match(const TString& s) const {
    for (auto& p : CompiledPatterns_) {
        if (p.second.first.Match(s.c_str())) {
            return p.second.second;
        }
    }

    return Nothing();
}

}
