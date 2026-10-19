#pragma once
#include "abstract.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NArrow::NSSA {

// One bloom-ngram probe implied by a LIKE/ILIKE match: the matched input contains (or starts/ends with) Value.
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
    // EndsWith is emitted only when the suffix is at least this long. Set it to the largest compatible
    // ngram size: a shorter suffix does not match that index's stored suffix window.
    ui32 MinEndsWithLength = 3;
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
