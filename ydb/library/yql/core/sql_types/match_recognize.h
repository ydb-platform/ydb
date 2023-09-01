#pragma once
#include <util/generic/string.h>
#include <util/string/cast.h>
#include <util/generic/vector.h>
#include <util/generic/hash_set.h>
#include <variant>
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
};

THashSet<TString> GetPatternVars(const TRowPattern&);

}//namespace NYql::NMatchRecognize
