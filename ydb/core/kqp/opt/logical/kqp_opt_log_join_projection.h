#pragma once

#include <cstddef>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>

// Visible for unit tests
namespace NKikimr::NKqp::NOpt::NPrivate {

struct TJoinColumnUsage {
    THashSet<TString> OutputFieldNames;
    bool WholeRow = false;
};

struct TJoinOutputColumn {
    size_t InputIndex;
    // e.g. `labels`
    TString SourceColumnName;
    // e.g. `r.labels`
    TString OriginalName;
};

// Mapped by join output names, that may differ from both `RemappedName` and `OriginalName` because of remapping
using TJoinOutputColumns = THashMap<TString, TJoinOutputColumn>;

TJoinOutputColumns BuildJoinOutputColumns(const NYql::NNodes::TCoEquiJoin& join);
TJoinColumnUsage CollectJoinColumnUsage(const NYql::TExprNode::TPtr& body, const NYql::TExprNode& joinedRow);
THashSet<TString> CollectRequiredJoinSources(const TJoinColumnUsage& usage, const TJoinOutputColumns& outputColumns);
bool HasMultipleParents(const NYql::TExprNode& node, const NYql::TParentsMap& parents);

} // namespace NKikimr::NKqp::NOpt::NPrivate
