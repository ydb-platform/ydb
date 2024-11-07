#include "pattern_group.h"

namespace NYql {

void TPatternGroup::Add(const TString& pattern, const TString& alias) {
    auto it = CompiledPatterns.find(pattern);
    if (it != CompiledPatterns.end()) {
        return;
    }

    CompiledPatterns.emplace(pattern, std::make_pair(TRegExMatch(pattern), alias));
}

bool TPatternGroup::IsEmpty() const {
    return CompiledPatterns.empty();
}

TMaybe<TString> TPatternGroup::Match(const TString& s) const {
    for (auto& p : CompiledPatterns) {
        if (p.second.first.Match(s.c_str())) {
            return p.second.second;
        }
    }

    return Nothing();
}

}
