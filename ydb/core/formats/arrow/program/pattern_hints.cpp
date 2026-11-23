#include "pattern_hints.h"

#include <optional>

#include <util/generic/hash_set.h>

namespace NKikimr::NArrow::NSSA {

namespace {

// A literal fragment of the pattern. AtStart and AtEnd say whether it is anchored to the pattern's
// start or end. SplitLiteralRuns cuts the pattern on wildcards; PiecesOf cuts a run again on bytes
// the index cannot probe, keeping an anchor only while the fragment is still that end of the run.
struct TLiteralRun {
    TString Bytes;
    bool AtStart = false;
    bool AtEnd = false;
};

// SQL LIKE wildcards. LikeAnyString is any string, LikeAnyChar is one character.
constexpr char LikeAnyString = '%';
constexpr char LikeAnyChar = '_';

// LikeAnyString and LikeAnyChar end a run. An escaped byte is literal. Nothing when the pattern ends on the escape byte.
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
        if (c == LikeAnyString || c == LikeAnyChar) {
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

// RE2 cycle-folds 'k' to U+212A (Kelvin) and 's' to U+017F (long s): CycleFoldRune in
// contrib/libs/re2/re2/parse.cc, table in contrib/libs/re2/re2/unicode_casefold.h.
// The bloom folds ASCII only, so a probe that contains these letters can miss a matching portion.
bool BreaksCaseInsensitiveProbe(const unsigned char c) {
    return c >= 128 || c == 'k' || c == 'K' || c == 's' || c == 'S';
}

// Case-sensitive: the whole run. Case-insensitive: ASCII pieces, split on bytes the bloom cannot probe.
std::vector<TLiteralRun> PiecesOf(const TLiteralRun& run, const bool caseSensitive) {
    if (caseSensitive) {
        return {run};
    }
    std::vector<TLiteralRun> pieces;
    TString current;
    bool atStart = run.AtStart;
    for (const char c : run.Bytes) {
        if (BreaksCaseInsensitiveProbe(static_cast<unsigned char>(c))) {
            if (!current.empty()) {
                pieces.push_back(TLiteralRun{std::move(current), atStart, false});
                current.clear();
            }
            atStart = false;
            continue;
        }
        current.push_back(c);
    }
    if (!current.empty()) {
        pieces.push_back(TLiteralRun{std::move(current), atStart, run.AtEnd});
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
            // The suffix window is not a slide, and a portion can still carry an older, larger ngram
            // than the current schema. EndsWith("bbb") then probes bbb\0\0 while "aaabbb" stored abbb\0.
            // Contains uses the inner grams, which every ngram size either checks correctly or ignores.
            if (operation == EOperation::EndsWith) {
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
