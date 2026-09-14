#include <yql/essentials/public/udf/udf_helpers.h>

using namespace NYql;
using namespace NYql::NUdf;

// HybridSearch::RRF(ranks: List<Uint64>, weights: List<Double>, k: Double) -> Double
//
// Reciprocal Rank Fusion score contributed by a single document across an arbitrary number
// of ranked input lists (branches):
//     sum_i  weight_i / (k + rank_i)
//
// `ranks[i]` is the document's 1-based position inside branch i's (already-sorted) result
// set. A document that is absent from branch i is passed a large penalty rank by the KQP
// optimizer (RewriteHybridRankTopSort) so its contribution from that branch is ~0; the ranks
// are therefore always concrete (the optimizer Coalesces them ahead of the call).
//
// `weights` is parallel to `ranks`; any branch beyond the end of the weights list defaults to
// weight 1.0 (so an empty weights list means "all branches equal"). The UDF is not specialised
// to a fulltext+vector pair -- the optimizer may pass any mix (vector+vector, fulltext+fulltext,
// or more than two branches); it just sums one term per rank.
SIMPLE_STRICT_UDF(TRRF, double(TListType<ui64>, TListType<double>, double)) {
    Y_UNUSED(valueBuilder);
    const double k = args[2].Get<double>();

    // Collect per-branch weights up front so ranks can be indexed against them.
    TVector<double, TStdAllocatorForUdf<double>> weights;
    if (const auto elems = args[1].GetElements()) {
        const ui64 count = args[1].GetListLength();
        weights.reserve(count);
        for (ui64 i = 0; i < count; ++i) {
            weights.push_back(elems[i].Get<double>());
        }
    } else {
        const auto it = args[1].GetListIterator();
        for (TUnboxedValue w; it.Next(w);) {
            weights.push_back(w.Get<double>());
        }
    }

    double score = 0.0;
    ui64 i = 0;
    const auto accumulate = [&](ui64 rank) {
        const double weight = (i < weights.size()) ? weights[i] : 1.0;
        score += weight / (k + static_cast<double>(rank));
        ++i;
    };
    if (const auto elems = args[0].GetElements()) {
        const ui64 count = args[0].GetListLength();
        for (ui64 j = 0; j < count; ++j) {
            accumulate(elems[j].Get<ui64>());
        }
    } else {
        const auto it = args[0].GetListIterator();
        for (TUnboxedValue r; it.Next(r);) {
            accumulate(r.Get<ui64>());
        }
    }
    return TUnboxedValuePod(score);
}

// HybridSearch::LinearFuse(scores, mins, maxs, weights: List<Double>, similarities: List<Bool>,
//                          normalize: Bool) -> Double
//
// Weighted linear fusion (the 'linear' alternative to RRF) across an arbitrary number of branches:
//     sum_i  weight_i * norm_i
// where each branch is mapped to "higher = better". The lists are parallel (one element per branch);
// `weights` defaults to 1.0 and `similarities` to true for any branch beyond the end of its list.
//   similarities[i] tells whether branch i's score is a similarity (larger = better, e.g.
//     CosineSimilarity, or a fulltext relevance) or a distance (smaller = better, e.g. CosineDistance).
//   normalize = true  (default): each branch score is min-max scaled to [0,1] over the observed
//     candidate min/max; a distance is additionally inverted (1 - scaled) so smaller distances score
//     higher. When a branch's max == min (no spread) that branch contributes 0 rather than dividing by
//     zero.
//   normalize = false: raw scores. norm_i = score_i for a similarity, -score_i for a distance (negated
//     so a larger fused score still means "better"). Raw scores from different branches are generally
//     not comparable, which is exactly why normalization is the default.
// The optimizer passes a document absent from a branch the branch end that maps to a 0 contribution
// (min for a similarity, max for a distance).
SIMPLE_STRICT_UDF(TLinearFuse, double(TListType<double>, TListType<double>, TListType<double>,
                                      TListType<double>, TListType<bool>, bool)) {
    Y_UNUSED(valueBuilder);
    const auto readDoubles = [](const TUnboxedValuePod& list) {
        TVector<double, TStdAllocatorForUdf<double>> out;
        if (const auto elems = list.GetElements()) {
            const ui64 count = list.GetListLength();
            out.reserve(count);
            for (ui64 i = 0; i < count; ++i) {
                out.push_back(elems[i].Get<double>());
            }
        } else {
            const auto it = list.GetListIterator();
            for (TUnboxedValue v; it.Next(v);) {
                out.push_back(v.Get<double>());
            }
        }
        return out;
    };

    const auto scores  = readDoubles(args[0]);
    const auto mins    = readDoubles(args[1]);
    const auto maxs    = readDoubles(args[2]);
    const auto weights = readDoubles(args[3]);

    TVector<bool, TStdAllocatorForUdf<bool>> similarities;
    if (const auto elems = args[4].GetElements()) {
        const ui64 count = args[4].GetListLength();
        similarities.reserve(count);
        for (ui64 i = 0; i < count; ++i) {
            similarities.push_back(elems[i].Get<bool>());
        }
    } else {
        const auto it = args[4].GetListIterator();
        for (TUnboxedValue s; it.Next(s);) {
            similarities.push_back(s.Get<bool>());
        }
    }

    const bool normalize = args[5].Get<bool>();

    double fused = 0.0;
    for (size_t i = 0; i < scores.size(); ++i) {
        const double score = scores[i];
        const double mn = (i < mins.size()) ? mins[i] : 0.0;
        const double mx = (i < maxs.size()) ? maxs[i] : 0.0;
        const double weight = (i < weights.size()) ? weights[i] : 1.0;
        const bool isSimilarity = (i < similarities.size()) ? similarities[i] : true;

        double norm;
        if (normalize) {
            const double span = mx - mn;
            if (span <= 0.0) {
                norm = 0.0;
            } else {
                const double scaled = (score - mn) / span;
                norm = isSimilarity ? scaled : (1.0 - scaled);  // distance: smaller = better
            }
        } else {
            norm = isSimilarity ? score : -score;  // distance negated so a larger fused score is better
        }
        fused += weight * norm;
    }
    return TUnboxedValuePod(fused);
}

SIMPLE_MODULE(THybridSearchModule,
              TRRF,
              TLinearFuse)

REGISTER_MODULES(THybridSearchModule)
