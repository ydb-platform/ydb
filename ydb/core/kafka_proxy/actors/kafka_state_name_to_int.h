#pragma once

#include <util/generic/cast.h>
#include <map>

namespace NKafka {
    extern const std::map<int, TString> numbersToStatesMapping;

    extern const std::map<TString, int> statesToNumbersMapping;
} // namespace NKafka
