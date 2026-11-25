#include "fulltext_regex.h"
#include "fulltext_query.h"

#include <re2/re2.h>
#include <re2/regexp.h>
#include <util/charset/utf8.h>
#include <util/generic/algorithm.h>

#include <iterator>

namespace NKikimr::NFulltext {
namespace {

using EOp = TRegexCandidates::EOp;

TRegexCandidates Combine(EOp op, TVector<TRegexCandidates> children) {
    TRegexCandidates result{.Op = op};
    for (auto& child : children) {
        if (child.Op == EOp::Any) {
            if (op == EOp::Or) {
                return {};
            }
        } else if (child.Op == op) {
            result.Children.insert(result.Children.end(), std::make_move_iterator(child.Children.begin()),
                std::make_move_iterator(child.Children.end()));
        } else {
            result.Children.push_back(std::move(child));
        }
    }
    if (result.Children.empty()) {
        return {};
    }
    if (result.Children.size() == 1) {
        return std::move(result.Children.front());
    }
    return result;
}

// Retain short finite alternatives until their surrounding literals are known.
// RE2 parses "cat|car" as "ca[rt]", whose two parts alone have no trigram.
struct TFragment {
    TVector<TString> Literals;
    TRegexCandidates Filter;
};

struct TExtractor {
    const Ydb::Table::FulltextIndexSettings::Analyzers& Settings;
    bool Utf8;
    static constexpr size_t MaxAlternatives = 16;
    size_t RemainingNodes = 256;
    size_t RemainingTerms = 256;

    TRegexCandidates Filter(TFragment fragment) {
        if (fragment.Literals.empty()) {
            return std::move(fragment.Filter);
        }
        TVector<TRegexCandidates> alternatives;
        for (const auto& literal : fragment.Literals) {
            auto grams = BuildStringMatchTerms(literal, "CONTAINS", {}, Settings);
            SortUnique(grams);
            grams.crop(RemainingTerms);
            if (grams.empty()) {
                return {}; // Every alternative must constrain the index read.
            }
            RemainingTerms -= grams.size();
            TVector<TRegexCandidates> terms;
            for (auto& gram : grams) {
                terms.push_back({.Op = EOp::Term, .Term = std::move(gram)});
            }
            alternatives.push_back(Combine(EOp::And, std::move(terms)));
        }
        return Combine(EOp::Or, std::move(alternatives));
    }

    bool AppendRune(TString& literal, re2::Rune rune) const {
        // In Latin-1 mode non-ASCII runes match individual bytes. Postings are UTF-8.
        if (!Utf8 && rune >= 128) {
            return false;
        }
        unsigned char data[4];
        size_t size;
        WriteUTF8Char(rune, size, data);
        literal.append(reinterpret_cast<const char*>(data), size);
        return true;
    }

    TFragment Concat(re2::Regexp* re, size_t depth) {
        TFragment run;
        TVector<TRegexCandidates> filters;
        auto flush = [&] {
            if (!run.Literals.empty()) {
                filters.push_back(Filter(std::move(run)));
                run = {};
            }
        };
        for (int i = 0; i < re->nsub(); ++i) {
            auto child = Extract(re->sub()[i], depth + 1);
            if (child.Literals.empty()) {
                flush();
                filters.push_back(std::move(child.Filter));
            } else {
                if (run.Literals.size() * child.Literals.size() > MaxAlternatives) {
                    flush(); // Each side is still independently required.
                }
                if (run.Literals.empty()) {
                    run = std::move(child);
                } else {
                    TVector<TString> joined;
                    for (const auto& prefix : run.Literals) {
                        for (const auto& suffix : child.Literals) {
                            joined.push_back(prefix + suffix);
                        }
                    }
                    run.Literals = std::move(joined);
                }
            }
        }
        if (filters.empty()) {
            return run;
        }
        flush();
        return {.Filter = Combine(EOp::And, std::move(filters))};
    }

    TFragment Extract(re2::Regexp* re, size_t depth) {
        if (depth >= 64 || !RemainingNodes) {
            return {};
        }
        --RemainingNodes;
        switch (re->op()) {
            case re2::kRegexpLiteral:
            case re2::kRegexpLiteralString: {
                // RE2 literal instructions fold only ASCII. The parser represents
                // other Unicode fold equivalents (e.g. S and ſ) as character classes.
                if ((re->parse_flags() & re2::Regexp::FoldCase) && !Settings.use_filter_lowercase()) {
                    return {};
                }
                TVector<TString> runs(1);
                const bool single = re->op() == re2::kRegexpLiteral;
                const int count = single ? 1 : re->nrunes();
                for (int i = 0; i < count; ++i) {
                    if (!AppendRune(runs.back(), single ? re->rune() : re->runes()[i])) {
                        runs.emplace_back();
                    }
                }
                if (runs.size() == 1) {
                    return {.Literals = std::move(runs)};
                }
                // A Latin-1 byte can be part of a UTF-8 code point. Only the
                // ASCII runs on either side supply independently required grams.
                TVector<TRegexCandidates> filters;
                for (auto& run : runs) {
                    if (!run.empty()) {
                        filters.push_back(Filter({.Literals = {std::move(run)}}));
                    }
                }
                return {.Filter = Combine(EOp::And, std::move(filters))};
            }
            case re2::kRegexpCharClass: {
                if ((re->parse_flags() & re2::Regexp::FoldCase) || re->cc()->size() > static_cast<int>(MaxAlternatives)) {
                    return {};
                }
                TFragment result;
                for (auto range : *re->cc()) {
                    for (auto rune = range.lo; rune <= range.hi; ++rune) {
                        if (!AppendRune(result.Literals.emplace_back(), rune)) {
                            return {};
                        }
                    }
                }
                return result;
            }
            case re2::kRegexpConcat:
                return Concat(re, depth);
            case re2::kRegexpAlternate: {
                TVector<TFragment> alternatives;
                size_t count = 0;
                bool finite = true;
                for (int i = 0; i < re->nsub(); ++i) {
                    auto& child = alternatives.emplace_back(Extract(re->sub()[i], depth + 1));
                    count += child.Literals.size();
                    finite &= !child.Literals.empty() && count <= MaxAlternatives;
                }
                TFragment result;
                TVector<TRegexCandidates> filters;
                for (auto& child : alternatives) {
                    if (finite) {
                        result.Literals.insert(result.Literals.end(), std::make_move_iterator(child.Literals.begin()),
                            std::make_move_iterator(child.Literals.end()));
                    } else {
                        filters.push_back(Filter(std::move(child)));
                    }
                }
                result.Filter = Combine(EOp::Or, std::move(filters));
                return result;
            }
            case re2::kRegexpCapture:
                return Extract(re->sub()[0], depth + 1);
            case re2::kRegexpPlus:
            case re2::kRegexpRepeat:
                if (re->op() == re2::kRegexpPlus || re->min() > 0) {
                    return {.Filter = Filter(Extract(re->sub()[0], depth + 1))};
                }
                [[fallthrough]];
            default:
                return {};
        }
    }
};

} // namespace

TRegexCandidates BuildRegexCandidates(TStringBuf pattern, const Ydb::Table::FulltextIndexSettings::Analyzers& settings) {
    // Limit work on the optional optimization independently of RE2's matching limits.
    if (pattern.size() > 1024 || (!settings.use_filter_ngram() && !settings.use_filter_edge_ngram())) {
        return {};
    }
    const bool utf8 = UTF8Detect(pattern) == UTF8; // Same default encoding as Re2.Grep/Match.
    re2::RE2::Options options;
    options.set_encoding(utf8 ? re2::RE2::Options::EncodingUTF8 : re2::RE2::Options::EncodingLatin1);
    options.set_log_errors(false);
    re2::RE2 re({pattern.data(), pattern.size()}, options);
    if (!re.ok()) {
        return {}; // The residual matcher owns invalid-pattern diagnostics.
    }
    TExtractor extractor{settings, utf8};
    return extractor.Filter(extractor.Extract(re.Regexp(), 0));
}

} // namespace NKikimr::NFulltext
