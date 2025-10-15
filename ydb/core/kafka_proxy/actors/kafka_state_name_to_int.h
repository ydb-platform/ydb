#pragma once

#include <util/generic/cast.h>
#include <map>

namespace NKafka {
    namespace NConsumer {
        extern const std::map<int, TString> NumbersToStatesMapping;

        extern const std::map<TString, int> StatesToNumbersMapping;
    }
} // namespace NKafka::NConsumer
