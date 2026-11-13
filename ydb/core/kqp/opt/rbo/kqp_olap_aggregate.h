#pragma once

#include "kqp_operator.h"

#include <optional>

namespace NKikimr::NKqp {

// An aggregation that column shards compute over a column of the read.
struct TOlapAggregation {
    TInfoUnitId Output;
    TString Function;
    // The aggregated read column, std::nullopt for COUNT(*).
    std::optional<TInfoUnitId> Column;
};

// An intermediate aggregate that column shards compute in the program of a
// column table read. Its outputs are the partial states the final aggregate merges.
struct TOlapAggregate {
    TOpRead* Read = nullptr;
    // An optional Map between the read and the aggregate. The aggregate uses
    // only its copies of read columns and the literals COUNT(*) counts.
    TOpMap* Map = nullptr;
    // Distinct read columns to group by.
    TVector<TInfoUnitId> Keys;
    TVector<TOlapAggregation> Aggregations;
    // Whether the read program casts the input of an AVG to Double before summing it, which needs scalar apply.
    // Otherwise, as in the old optimizer, column shards sum the stored type and compute converts the sum.
    bool CastAverageInput = true;
};

// Match an aggregate whose keys and aggregations column shards can compute over
// the read below it. The aggregate phase and pushdown settings are the caller's concern.
std::optional<TOlapAggregate> MatchOlapAggregate(const TOpAggregate& aggregate, TExprContext& ctx);

// AVG accumulates in Double. The read program casts an AVG column of any other
// type with a YQL kernel, which column shards run only with scalar apply enabled.
bool IsOlapAverageCast(const TTypeAnnotationNode& columnType);

// A trivial DISTINCT whose key the program of a column table read marks. Column
// shards then skip repeated keys and stop after a forced number of distinct
// keys. The aggregate still removes duplicates.
struct TOlapDistinct {
    TOpRead* Read = nullptr;
    // The read column holding the key.
    TInfoUnitId Key;
};

// Match a single key trivial DISTINCT over a column table read, whose key PRAGMA
// OptForceOlapPushdownDistinct names as `forcedKey`. Throws when the pragma
// names another key. Uses the liveness of the final plan.
std::optional<TOlapDistinct> MatchOlapDistinct(const TOpAggregate& aggregate, const TString& forcedKey,
                                               const TInfoUnitRegistry& registry);

// The keys of an aggregate over a column table read. As in the old optimizer, PRAGMA
// OptEnableOlapProvideComputeSharding passes them to the read, so column shards send
// all rows of a key to one compute task. The aggregate itself may stay in compute.
struct TOlapComputeSharding {
    TOpRead* Read = nullptr;
    // Distinct storage columns of the keys.
    TVector<TString> Columns;
};

// Match an intermediate aggregate with keys over a column table read in its stage,
// when all the keys are read columns.
std::optional<TOlapComputeSharding> MatchOlapComputeSharding(const TOpAggregate& aggregate,
                                                             const TInfoUnitRegistry& registry);

} // namespace NKikimr::NKqp
