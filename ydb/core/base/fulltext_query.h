#pragma once

#include <ydb/public/api/protos/ydb_table.pb.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>

namespace NKikimr::NFulltext {

// A single search term together with its query-syntax modifier.
// Required == true marks a `+term` (Lucene MUST): every match must contain it.
struct TSearchTerm {
    TString Token;
    bool Required = false;

    bool operator==(const TSearchTerm& other) const = default;
};

inline IOutputStream& operator<<(IOutputStream& out, const TSearchTerm& term) {
    return out << (term.Required ? "+" : "") << term.Token;
}

// Like BuildSearchTerms, but parses the `+term` required-term syntax: a `+` that
// begins a whitespace-delimited term marks every token analyzed from that term as
// required. Queries without that syntax tokenize identically to BuildSearchTerms,
// with every term optional.
//
// Kept in its own header (not fulltext.h) so that adding query-syntax support does
// not rebuild every consumer of the widely-included fulltext.h.
TVector<TSearchTerm> BuildSearchTermsStructured(const TString& query, const Ydb::Table::FulltextIndexSettings::Analyzers& settings);

}
