#include "common.h"

#include <util/generic/string.h>

namespace NFq {

TString CleanupCounterValueString(const TString& value) {
    TString clean;
    constexpr auto valueLenghtLimit = 200;

    for (auto c : value) {
        switch (c) {
        case '|':
        case '*':
        case '?':
        case '"':
        case '\'':
        case '`':
        case '\\':
            continue;
        default:
            clean.push_back(c);
            if (clean.size() == valueLenghtLimit) {
                break;
            }
        }
    }
    return clean;
}

} // namespace NFq
