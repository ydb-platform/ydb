#pragma once

#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/core/resource_pools/regex_predicate.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <optional>


namespace NKikimr::NWorkloadManager {

///
/// Returns true if the classifier should accept the query based on the HAS_PATH filter.
///
/// - When `predicate` is unset, there is no filter — always accepts (returns true).
/// - Otherwise, returns true iff the query touches any object at a path matching
///   `predicate`. Coverage: regular tables (row + column), secondary-index sub-tables,
///   system views, external tables and data sources, views, topics, CDC streams.
///
/// Callers pass `queryTables` from `TPreparedQueryHolder::GetQueryTables()` and
/// `phyQuery` from `TPreparedQueryHolder::GetPhysicalQuery()`.
///
bool MatchesPath(const std::optional<NResourcePool::TRegexPredicate>& predicate,
                 const TVector<TString>& queryTables,
                 const NKqpProto::TKqpPhyQuery& phyQuery);

}  // namespace NKikimr::NWorkloadManager
