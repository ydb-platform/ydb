#pragma once

#include <ydb/core/kqp/host/new_parser/kqp_type_annotation.h>
#include <ydb/core/kqp/opt/rbo/kqp_operator.h>

namespace NKikimr::NKqp::NNewYql {

// The host provides the provider-specific table node only after validating
// that its catalog identity still matches the snapshot used for resolution.
// The lowerer needs storage kind for the read but does not load metadata itself.
struct TRboTable {
    NYql::TExprNode::TPtr Table; // TKqpTable, including path ID and schema version.
    NYql::EStorageType StorageType = NYql::EStorageType::RowStorage;
};

// The callback may request frontend fallback; returning a node for a different
// schema would invalidate both resolved column identities and annotated types.
using TRboTableLookup = std::function<TRboTable(const TTableMetadata&)>;

// Direct import into logical RBO operators, using annotated/re-written scalar
// expressions. The returned root owns its IU registry; expression context must
// outlive it. Syntax, names and annotations are unchanged and may be reused.
// Throws TNewYqlParserFallback for unsupported relational semantics. No partial
// plan escapes. Table lookup must reject unavailable or changed metadata.
TIntrusivePtr<TOpRoot> LowerToRbo(const TQuery& query, const TScript& script,
    const TResolvedNames& names, const TAnnotatedAst& annotations,
    NYql::TExprContext& ctx, const TRboTableLookup& tables, bool orderedColumns = false);

TIntrusivePtr<TOpRoot> LowerWriteToRbo(const TWriteStatement& write, const TScript& script,
    const TResolvedNames& names, const TAnnotatedAst& annotations,
    NYql::TExprContext& ctx, const TRboTableLookup& tables);

} // namespace NKikimr::NKqp::NNewYql
