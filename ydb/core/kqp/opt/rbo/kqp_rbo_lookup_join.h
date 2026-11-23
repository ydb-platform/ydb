#pragma once

#include "kqp_operator.h"

namespace NKikimr::NKqp {

// A table which a lookup join probes instead of the right side read: the read table itself, or the main
// table or an index the read is redirected to.
struct TLookupJoinTarget {
    TIntrusivePtr<NYql::TKikimrTableMetadata> Metadata;
    // Leading key columns pinned by the read predicate, they become a constant prefix of the lookup keys.
    const TOpRead::TPointPrefix* PointPrefix = nullptr;
    // Set when the target is a non-covering index of this main table: rows found in the index are fetched
    // from the main table by its primary key.
    TIntrusivePtr<NYql::TKikimrTableMetadata> MainTable;
};

// The right side of a lookup join: a read and an optional filter above it.
struct TLookupJoinRightSide {
    TIntrusivePtr<TOpRead> Read;
    TIntrusivePtr<TOpFilter> Filter;
};

// Matches the right side of a lookup join: Read or Filter -> Read. A read redirected to a non-covering
// index, TableLookup -> Filter -> Read(index), is matched as the read it replaced, if the lookup kept
// its description in SourceRead.
std::optional<TLookupJoinRightSide> MatchLookupJoinRightSide(const TIntrusivePtr<IOperator>& input);

// Maps the right side join keys to physical columns of the read. Read output units carry the physical
// source column as their name, so the mapping is a registry lookup.
std::optional<THashSet<TString>> GetLookupJoinKeyColumns(const TOpRead& read, const TUnorderedIUs& rightJoinKeys,
                                                         const TInfoUnitRegistry& registry);

// Chooses a table to probe for the read: the one whose key has the longest prefix of point prefix columns
// followed by join keys. A non-covering index of the read table can be chosen for an inner join only.
// CBO and the rewrite rule both use it, so the rule can rewrite every lookup join the CBO chose.
// readColumns are the physical columns the read produces; callers name them from whatever they have
// at hand - the unit registry in the rewrite rule, the column lineage in the CBO.
std::optional<TLookupJoinTarget> ChooseLookupJoinTarget(const TOpRead& read, const TVector<TString>& readColumns,
                                                        const THashSet<TString>& joinKeyColumns, bool innerJoin,
                                                        const NOpt::TKqpOptimizeContext& kqpCtx);

TExprNode::TPtr BuildTableCallable(const NYql::TKikimrTableMetadata& meta, NYql::TPositionHandle pos, NYql::TExprContext& ctx);

} // namespace NKikimr::NKqp
