#pragma once

#include "kqp_ast.h"

#include <yql/essentials/public/issue/yql_issue.h>
#include <util/generic/hash.h>

#include <functional>

namespace NKikimr::NKqp::NNewYql {

// Catalog snapshots, independent of both the syntax tree and ExprNodes.
// Types and nullability describe stored columns, before relational operations.
struct TColumnMetadata {
    TString Name;
    ui32 Id = 0;
    TString Type;
    bool NotNull = false;
};

struct TTableMetadata {
    TString Cluster;
    TString Path;
    ui64 OwnerId = 0;
    ui64 TableId = 0;
    ui64 SchemaVersion = 0;
    TVector<TColumnMetadata> Columns; // Catalog column order.
};

using TTableMetadataPtr = std::shared_ptr<const TTableMetadata>;
// A null result means a confirmed missing table. An unavailable or unsupported
// catalog must throw TNewYqlParserFallback instead of reporting a missing table.
using TTableLookup = std::function<TTableMetadataPtr(const TString& cluster, const TString& path)>;

struct TNameResolutionSettings {
    TString DefaultCluster;
    TString TablePathPrefix;
    THashMap<TString, TString> ClusterPathPrefixes;
};

using TColumnId = size_t;

struct TResolvedColumn {
    TString Name;
    const TSource* Source = nullptr; // Distinguishes occurrences in self joins.
    TTableMetadataPtr Table;
    std::optional<ui32> MetadataColumnId;
    const TExpression* Expression = nullptr;
    std::optional<TColumnId> Input; // Projection or derived-table column.
};

// Pointer keys refer to the unchanged input AST, which must outlive this result.
// Column IDs are local to this result; RBO lowering can allocate IUs for them.
struct TResolvedNames {
    TVector<TResolvedColumn> Columns;
    THashMap<const TSource*, TTableMetadataPtr> Tables;
    THashMap<const TExpression*, TColumnId> ColumnReferences;
    THashMap<const TExpression*, TVector<TColumnId>> Stars;
    THashMap<const TQuery*, TVector<TColumnId>> QueryOutputs;
    THashMap<const TWindowReference*, const TWindowDefinition*> WindowReferences;
};

struct TNameResolutionResult {
    std::unique_ptr<TResolvedNames> Names;
    NYql::TIssues Issues;

    bool IsOk() const { return bool(Names); }
};

TNameResolutionResult ResolveNames(const TScript& script, TNameResolutionSettings settings,
    const TTableLookup& lookup);

} // namespace NKikimr::NKqp::NNewYql
