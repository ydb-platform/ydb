#pragma once
#include <util/generic/string.h>
#include <util/string/cast.h>
#include <util/generic/vector.h>
#include <util/generic/hash_set.h>
#include <variant>
#include <stddef.h>

namespace NYql::NMatchRecognize {

enum class EAfterMatchSkipTo {
    NextRow,
    PastLastRow,
    ToFirst,
    ToLast,
    To
};

struct TAfterMatchSkipTo {
    EAfterMatchSkipTo To;
    TString Var;

    [[nodiscard]] bool operator==(const TAfterMatchSkipTo&) const noexcept = default;
};

enum class ERowsPerMatch {
    OneRow,
    AllRows
};
enum class EOutputColumnSource {
    PartitionKey,
    Measure,
    Other,
};

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
    ui64 QuantityMin;
    ui64 QuantityMax;
    bool Greedy;
    bool Output; //include in output with ALL ROW PER MATCH
    bool Unused; // optimization flag; is true when the variable is not used in defines and measures
};

THashSet<TString> GetPatternVars(const TRowPattern&);

}//namespace NYql::NMatchRecognize
