#pragma once
#include "abstract.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NArrow::NSSA {

// One index probe implied by a LIKE/ILIKE match: every matching input contains (or starts with) Value.
// Value is a literal substring of the pattern, not an ngram; the index decides how to check it.
struct TPatternIndexHint {
    ui32 InputIndex = 0;
    TIndexCheckOperation Operation;
    TString Value;

    TPatternIndexHint(const ui32 inputIndex, const TIndexCheckOperation operation, TString value)
        : InputIndex(inputIndex)
        , Operation(operation)
        , Value(std::move(value)) {
    }
};

struct TPatternHintPolicy {
    // A Contains shorter than the smallest bloom ngram size probes nothing. The caller sets this from the index.
    ui32 MinContainsLength = 3;
    // StartsWith of one byte matches the stored prefix window.
    ui32 MinAnchoredLength = 1;
    // Phase 1 does not interpret ESCAPE. A pattern that has one produces no hints.
    bool AllowEscape = false;
};

// Literal runs of a SQL LIKE pattern that every matching string must contain.
// ILIKE keeps ASCII pieces, and drops k/K/s/S. Re2 folds U+212A to 'k' and U+017F to 's'.
// The bloom folds ASCII only, so a probe that contains those letters can skip a matching portion.
std::vector<TPatternIndexHint> BuildLikeIndexHints(const TStringBuf pattern, const TStringBuf escape, const bool caseSensitive,
    const ui32 inputIndex, const TPatternHintPolicy& policy);

}   // namespace NKikimr::NArrow::NSSA
