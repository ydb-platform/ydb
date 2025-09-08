#pragma once

#include <util/generic/vector.h>

namespace NPersQueue {

struct TPQLabelsInfo {
    TVector<std::pair<TString, TString>> Labels;
    TVector<TString> AggrNames;
};

} // namespace NPersQueue
