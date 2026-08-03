#include "has_path_matcher.h"

#include <ydb/core/base/path.h>

#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>

#include <google/protobuf/any.pb.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>


namespace NKikimr::NWorkloadManager {

namespace {

bool MatchesPqTopicSource(const NResourcePool::TRegexPredicate& predicate,
                          const NKqpProto::TKqpExternalSource& source) {
    if (!source.GetSettings().Is<NYql::NPq::NProto::TDqPqTopicSource>()) {
        return false;
    }
    NYql::NPq::NProto::TDqPqTopicSource pqSource;
    if (!source.GetSettings().UnpackTo(&pqSource)) {
        return false;
    }
    return predicate.Match(CanonizePath(pqSource.GetTopicPath()));
}

bool MatchesPqTopicSink(const NResourcePool::TRegexPredicate& predicate,
                        const NKqpProto::TKqpExternalSink& sink) {
    if (!sink.GetSettings().Is<NYql::NPq::NProto::TDqPqTopicSink>()) {
        return false;
    }
    NYql::NPq::NProto::TDqPqTopicSink pqSink;
    if (!sink.GetSettings().UnpackTo(&pqSink)) {
        return false;
    }
    return predicate.Match(CanonizePath(pqSink.GetTopicPath()));
}

}  // anonymous namespace


bool MatchesPath(const std::optional<NResourcePool::TRegexPredicate>& predicate,
                 const TVector<TString>& queryTables,
                 const NKqpProto::TKqpPhyQuery& phyQuery) {
    if (!predicate) {
        return true;
    }

    // (A) Stage-side table paths pre-aggregated by TPreparedQueryHolder::FillTables.
    // Defensive coverage for TableOps + StreamLookup + Sequencer + ReadRangesSource
    // + FullTextSource + InternalSink (with index Docs/Dict/Stats sub-tables) +
    // OutputTransforms. Mostly overlaps with (B), retained as a safety net.
    for (const auto& path : queryTables) {
        if (predicate->Match(CanonizePath(path))) {
            return true;
        }
    }

    // (B) Per-tx table list — the primary workhorse. Populated by the compiler at
    // kqp_query_compiler.cpp:1288-1305. Covers every kind we've verified: regular
    // tables (DS + OLAP), sysviews, EDS + underlying EDS via ET's
    // UnderlyingExternalSourceMetadata unroll, direct topics, CDC changefeeds.
    for (const auto& tx : phyQuery.GetTransactions()) {
        for (const auto& table : tx.GetTables()) {
            if (predicate->Match(CanonizePath(table.GetId().GetPath()))) {
                return true;
            }
        }
    }

    // (D) Streaming reads/writes over PQ. Additive granularity only for
    // EDS-mediated remote topics — TDqPqTopicSource.TopicPath carries the bare
    // remote topic name that appears nowhere else in the phy plan. For local
    // topics and CDC changefeeds, TopicPath duplicates (B) — walk is uniform
    // and short-circuits on first match.
    //
    // Empirical evidence: see wm-predicates/has-path.md appendix section 7.
    for (const auto& tx : phyQuery.GetTransactions()) {
        for (const auto& stage : tx.GetStages()) {
            for (const auto& source : stage.GetSources()) {
                if (source.HasExternalSource() && MatchesPqTopicSource(*predicate, source.GetExternalSource())) {
                    return true;
                }
            }
            for (const auto& sink : stage.GetSinks()) {
                if (sink.HasExternalSink() && MatchesPqTopicSink(*predicate, sink.GetExternalSink())) {
                    return true;
                }
            }
        }
    }

    return false;
}

}  // namespace NKikimr::NWorkloadManager
