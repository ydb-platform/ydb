#pragma once

#include <util/generic/string.h>

namespace NSQLTranslationV1 {

struct TTieringRule {
    TString TierName;
    TString SerializedDurationForEvict;
};

TString SerializeTieringRules(const std::vector<TTieringRule>& input);

}
