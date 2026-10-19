#include "pattern_hints.h"

#include <optional>

#include <util/generic/hash_set.h>

namespace NKikimr::NArrow::NSSA {

namespace {

struct TLiteralRun {
    TString Bytes;
    bool AtStart = false;
    bool AtEnd = false;
};

// '%' and '_' end a run. An escaped byte is literal. Nothing when the pattern ends on the escape byte.
std::optional<std::vector<TLiteralRun>> SplitLiteralRuns(const TStringBuf pattern, const TStringBuf escape) {
    if (escape.size() > 1) {
        return std::nullopt;
    }
    const bool hasEscape = !escape.empty();
    const char escapeChar = hasEscape ? escape.front() : '\0';
    std::vector<TLiteralRun> runs;
    TString current;
    bool atStart = true;
    bool escaped = false;
    for (const char c : pattern) {
        if (escaped) {
            current.push_back(c);
            escaped = false;
            continue;
        }
        if (hasEscape && c == escapeChar) {
            escaped = true;
            continue;
        }
        if (c == '%' || c == '_') {
            if (!current.empty()) {
                runs.push_back(TLiteralRun{current, atStart, false});
                current.clear();
            }
            atStart = false;
            continue;
        }
        current.push_back(c);
    }
    if (escaped) {
        return std::nullopt;
    }
    if (!current.empty()) {
        runs.push_back(TLiteralRun{std::move(current), atStart, true});
    }
    return runs;
}

struct TPiece {
    TString Bytes;
    bool AtStart = false;
    bool AtEnd = false;
};

// Re2's case fold maps these ASCII letters onto bytes the bloom does not fold.
bool BreaksCaseInsensitiveProbe(const unsigned char c) {
    return c >= 128 || c == 'k' || c == 'K' || c == 's' || c == 'S';
}

// Case-sensitive: the whole run. Case-insensitive: ASCII pieces, split where a probe would not be
// implied. A piece keeps an anchor only when it is the corresponding end of the run.
std::vector<TPiece> PiecesOf(const TLiteralRun& run, const bool caseSensitive) {
    if (caseSensitive) {
        return {TPiece{run.Bytes, run.AtStart, run.AtEnd}};
    }
    std::vector<TPiece> pieces;
    TString current;
    bool atStart = run.AtStart;
    for (const char c : run.Bytes) {
        if (BreaksCaseInsensitiveProbe(static_cast<unsigned char>(c))) {
            if (!current.empty()) {
                pieces.push_back(TPiece{std::move(current), atStart, false});
                current.clear();
            }
            atStart = false;
            continue;
        }
        current.push_back(c);
    }
    if (!current.empty()) {
        pieces.push_back(TPiece{std::move(current), atStart, run.AtEnd});
    }
    return pieces;
}

}   // namespace

std::vector<TPatternIndexHint> BuildLikeIndexHints(const TStringBuf pattern, const TStringBuf escape, const bool caseSensitive,
    const ui32 inputIndex, const TPatternHintPolicy& policy) {
    if (escape.size() > 1 || (!escape.empty() && !policy.AllowEscape)) {
        return {};
    }
    const auto runs = SplitLiteralRuns(pattern, escape);
    if (!runs) {
        return {};
    }
    std::vector<TPatternIndexHint> hints;
    THashSet<TString> seen;
    for (const auto& run : *runs) {
        for (const auto& piece : PiecesOf(run, caseSensitive)) {
            using EOperation = TIndexCheckOperation::EOperation;
            // A piece that fills the whole pattern is a prefix. One StartsWith probe covers it.
            EOperation operation = piece.AtStart ? EOperation::StartsWith : (piece.AtEnd ? EOperation::EndsWith : EOperation::Contains);
            // EndsWith("bbb") on a size-5 index probes bbb\0\0. Indexing "aaabbb" stores abbb\0.
            // The cutoff is the largest compatible ngram size, because scan may select that index.
            if (operation == EOperation::EndsWith && piece.Bytes.size() < policy.MinEndsWithLength) {
                operation = EOperation::Contains;
            }
            const ui32 minLength = operation == EOperation::Contains ? policy.MinContainsLength : policy.MinAnchoredLength;
            if (piece.Bytes.size() < minLength) {
                continue;
            }
            const TString key = TStringBuilder() << static_cast<int>(operation) << '\n' << piece.Bytes << '\n' << caseSensitive;
            if (!seen.insert(key).second) {
                continue;
            }
            hints.emplace_back(inputIndex, TIndexCheckOperation(operation, caseSensitive), piece.Bytes);
        }
    }
    return hints;
}

}   // namespace NKikimr::NArrow::NSSA
