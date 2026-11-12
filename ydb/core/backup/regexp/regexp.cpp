#include "regexp.h"

namespace NKikimr::NBackup {

std::vector<TRegExMatch> CombineRegexps(const google::protobuf::RepeatedPtrField<TString>& regexps) {
    std::vector<TRegExMatch> compiled;
    compiled.reserve(regexps.size());

    size_t estimatedCombinedSize = 0;
    for (const auto& regexp : regexps) {
        if (regexp) {
            compiled.emplace_back(regexp); // can throw
            estimatedCombinedSize += regexp.size() + 5; // wrap each regexp with (?:...)
        }
    }

    if (compiled.empty()) {
        return compiled;
    }

    TString combined;
    combined.reserve(estimatedCombinedSize);

    for (size_t i = 0; i < static_cast<size_t>(regexps.size()); ++i) {
        if (!regexps[i]) {
            continue;
        }
        if (i) {
            combined.append("|(?:");
        } else {
            combined.append("(?:");
        }
        combined.append(regexps[i]);
        combined.append(")");
    }

    try {
        std::vector<TRegExMatch> result;
        result.emplace_back(combined);
        return result;
    } catch (const std::exception&) { // combined expression can't be compiled - fallback to separate regexps
        return compiled;
    }
}

} // namespace NKikimr::NBackup
