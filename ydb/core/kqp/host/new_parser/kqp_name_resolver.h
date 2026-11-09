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

// These IDs identify logical column occurrences, not catalog column numbers.
// Aliases, projections and repeated CTE references may allocate fresh IDs while
// retaining an Input link to the value they expose.
using TColumnId = size_t;

// Table/MetadataColumnId describe stored provenance; Expression describes a
// projection producer. Input links renames and direct projections back to their
// producer without merging independent source occurrences such as self joins.
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
    // SQL projection order, independent of attribute order in YQL struct types.
    THashMap<const TQuery*, TVector<TColumnId>> QueryOutputs;
    // GROUP BY aliases are visible after the grouping expression is resolved.
    THashMap<const TExpression*, TColumnId> GroupAliases;
    THashMap<const TWindowReference*, const TWindowDefinition*> WindowReferences;
    // Lexical identities distinguish lambda arguments and successive named
    // definitions. Unbound parameter references remain external SDK parameters.
    THashMap<const TExpression*, const TParameter*> BindingReferences;
    THashMap<const TParameter*, const TExpression*> Definitions;
    THashMap<const TParameter*, const TQuery*> NamedQueries;
    THashMap<const TSource*, const TQuery*> NamedSources;
};

struct TNameResolutionResult {
    std::unique_ptr<TResolvedNames> Names;
    NYql::TIssues Issues;

    bool IsOk() const { return bool(Names); }
};

// Resolution reads catalog snapshots but does not construct ExprNodes or infer
// types. A user name error returns Issues and no partial Names; unsupported
// binding rules throw TNewYqlParserFallback for the entire script.
TNameResolutionResult ResolveNames(const TScript& script, TNameResolutionSettings settings,
    const TTableLookup& lookup);

} // namespace NKikimr::NKqp::NNewYql
