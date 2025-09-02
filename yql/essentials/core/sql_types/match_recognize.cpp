#include "match_recognize.h"
#include <util/generic/string.h>
#include <util/generic/hash_set.h>

namespace NYql::NMatchRecognize {

THashSet<TString> GetPatternVars(const TRowPattern& pattern) {
    THashSet<TString> result;
    for (const auto& term: pattern) {
        for (const auto& factor: term) {
            const auto& primary = factor.Primary;
            if (primary.index() == 0){
                result.insert(std::get<0>(primary));
            } else {
                const auto& vars = GetPatternVars(std::get<1>(primary));
                result.insert(vars.begin(), vars.end());
            }
        }
    }
    return result;
}

}//namespace NYql::NMatchRecognize
