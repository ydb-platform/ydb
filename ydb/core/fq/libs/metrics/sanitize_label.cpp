#include "sanitize_label.h"

#include <util/generic/string.h>

namespace NFq {

TString SanitizeLabel(const TString& value) {
    TString result;
    result.reserve(value.size());
    constexpr auto labelLengthLimit = 200;

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
            result.push_back(c);
            if (result.size() == labelLengthLimit) {
                return result;
            }
        }
    }
    return result;
}

} // namespace NFq
