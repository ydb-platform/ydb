#include "cbo_optimizer_hints.h"

namespace NKikimr::NKqp {

THashSet<TString> BuildTableHintCandidates(const TString& alias, const TString& path) {
    THashSet<TString> candidates;
    if (!alias.empty()) {
        candidates.insert(alias);
    }
    candidates.insert(path);
    if (auto pos = path.rfind('/'); pos != TString::npos && pos + 1 < path.size()) {
        candidates.insert(path.substr(pos + 1));
    }
    return candidates;
}

void ApplySingleLabelHint(TCardinalityHints& hints, const THashSet<TString>& candidates, double& target) {
    for (auto& hint : hints.Hints) {
        if (hint.JoinLabels.size() == 1 && candidates.contains(hint.JoinLabels.front())) {
            target = hint.ApplyHint(target);
            break;
        }
    }
}

} // namespace NKikimr::NKqp
