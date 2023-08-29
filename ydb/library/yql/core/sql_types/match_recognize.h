#pragma once
#include <util/generic/string.h>
#include <util/string/cast.h>
#include <stddef.h>

namespace NYql::NMatchRecognize {

constexpr size_t MaxPatternNesting = 20; //Limit recursion for patterns

//Mixin columns for calculating measures
enum class MeasureInputDataSpecialColumns {
    Classifier = 0,
    MatchNumber = 1,
    Last
};

inline TString MeasureInputDataSpecialColumnName(MeasureInputDataSpecialColumns c) {
    return TString("_yql_") + ToString(c);
}

}//namespace NYql::NMatchRecognize
