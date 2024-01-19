#pragma once
#include <util/generic/string.h>
#include <util/string/cast.h>
#include <util/generic/vector.h>
#include <util/generic/hash_set.h>
#include <variant>
#include <stddef.h>

namespace NYql::NMatchRecognize {

constexpr size_t MaxPatternNesting = 20; //Limit recursion for patterns
constexpr size_t MaxPermutedItems = 6;

//Mixin columns for calculating measures
enum class EMeasureInputDataSpecialColumns {
    Classifier = 0,
    MatchNumber = 1,
    Last
};

inline TString MeasureInputDataSpecialColumnName(EMeasureInputDataSpecialColumns c) {
    return TString("_yql_") + ToString(c);
}

struct TRowPatternFactor;

using TRowPatternTerm = TVector<TRowPatternFactor>;

using TRowPattern = TVector<TRowPatternTerm>;

using TRowPatternPrimary = std::variant<TString, TRowPattern>;

struct TRowPatternFactor {
    TRowPatternPrimary Primary;
    uint64_t QuantityMin;
    uint64_t QuantityMax;
    bool Greedy;
    bool Output; //include in output with ALL ROW PER MATCH
    bool Unused; // optimization flag; is true when the variable is not used in defines and measures
};

THashSet<TString> GetPatternVars(const TRowPattern&);

}//namespace NYql::NMatchRecognize
