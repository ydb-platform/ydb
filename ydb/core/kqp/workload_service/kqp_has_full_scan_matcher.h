#pragma once

#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/core/resource_pools/regex_predicate.h>

#include <optional>


namespace NKikimr::NKqp::NWorkload {

///
/// Returns true if the classifier should accept the query based on the HAS_FULL_SCAN filter.
///
/// - When `predicate` is unset, there is no filter — always accepts (returns true).
/// - Otherwise, returns true iff at least one operation in `phyQuery`
///   performs a full scan on a table whose path matches `predicate`.
///
bool MatchesFullScan(const std::optional<NResourcePool::TRegexPredicate>& predicate,
                   const NKqpProto::TKqpPhyQuery& phyQuery);

}  // namespace NKikimr::NKqp::NWorkload
