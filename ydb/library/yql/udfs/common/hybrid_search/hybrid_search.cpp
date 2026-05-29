#include <yql/essentials/public/udf/udf_helpers.h>

using namespace NYql;
using namespace NYql::NUdf;

// HybridSearch::RRF(ftRank: Uint64, vecRank: Uint64, k, wFt, wVec: Double) -> Double
//
// Reciprocal Rank Fusion score contributed by a single document:
//     wFt / (k + ftRank) + wVec / (k + vecRank)
//
// Ranks are 1-based positions inside each per-index result set. A document that is
// absent from one result set is passed the penalty rank (N_internal + 1) by the KQP
// optimizer (RewriteHybridRankTopSort) before this UDF is called, so both rank
// arguments are always concrete (the optimizer Coalesces them ahead of the call).
// wFt/wVec are the per-direction weights (default 1.0 each); since a missing branch
// already gets the penalty rank, its ~0 contribution is unaffected by its weight.
SIMPLE_STRICT_UDF(TRRF, double(ui64, ui64, double, double, double)) {
    Y_UNUSED(valueBuilder);
    const ui64 ftRank = args[0].Get<ui64>();
    const ui64 vecRank = args[1].Get<ui64>();
    const double k = args[2].Get<double>();
    const double wFt = args[3].Get<double>();
    const double wVec = args[4].Get<double>();
    const double score = wFt / (k + static_cast<double>(ftRank))
                       + wVec / (k + static_cast<double>(vecRank));
    return TUnboxedValuePod(score);
}

// HybridSearch::LinearFuse(ftScore, ftMin, ftMax, vecScore, vecMin, vecMax, wFt, wVec: Double,
//                          normalize, vecSimilarity: Bool) -> Double
//
// Weighted linear fusion (the 'linear' alternative to RRF): wFt*normFt + wVec*normVec, where both branches
// are mapped to "higher = better".
//   vecSimilarity tells whether the vector score is a similarity (larger = better, e.g. CosineSimilarity)
//     or a distance (smaller = better, e.g. CosineDistance).
//   normalize = true  (default): each branch score is min-max scaled to [0,1] over the observed candidate
//     min/max; a distance is additionally inverted (1 - scaled) so smaller distances score higher. When a
//     branch's max == min (no spread) that branch contributes 0 rather than dividing by zero.
//   normalize = false: raw scores. normFt = ftScore as-is; normVec = vecScore for a similarity, -vecScore
//     for a distance (negated so a larger fused score still means "better"). Raw fulltext and vector
//     magnitudes are not comparable, which is exactly why normalization is the default.
// The optimizer passes a document absent from a branch the branch min/max that maps to a 0 contribution
// (fulltext min; vector max for a distance / min for a similarity).
SIMPLE_STRICT_UDF(TLinearFuse, double(double, double, double, double, double, double, double, double, bool, bool)) {
    Y_UNUSED(valueBuilder);
    const double ftScore       = args[0].Get<double>();
    const double ftMin         = args[1].Get<double>();
    const double ftMax         = args[2].Get<double>();
    const double vecScore      = args[3].Get<double>();
    const double vecMin        = args[4].Get<double>();
    const double vecMax        = args[5].Get<double>();
    const double wFt           = args[6].Get<double>();
    const double wVec          = args[7].Get<double>();
    const bool   normalize     = args[8].Get<bool>();
    const bool   vecSimilarity = args[9].Get<bool>();
    double normFt, normVec;
    if (normalize) {
        normFt = (ftMax > ftMin) ? (ftScore - ftMin) / (ftMax - ftMin) : 0.0;
        const double vecSpan = vecMax - vecMin;
        normVec = (vecSpan <= 0.0) ? 0.0
                : vecSimilarity    ? (vecScore - vecMin) / vecSpan          // larger similarity = better
                                   : 1.0 - (vecScore - vecMin) / vecSpan;    // smaller distance = better
    } else {
        normFt  = ftScore;
        normVec = vecSimilarity ? vecScore : -vecScore;
    }
    return TUnboxedValuePod(wFt * normFt + wVec * normVec);
}

SIMPLE_MODULE(THybridSearchModule,
              TRRF,
              TLinearFuse)

REGISTER_MODULES(THybridSearchModule)
