#pragma once

#include <ydb/library/generated_columns/generated_column_dependencies.h>

#include <ydb/library/generated_columns/expr/generated_column_expr.h>

#include <util/generic/vector.h>

namespace NYql {

struct TKikimrColumnMetadata;
struct TKikimrTableMetadata;

bool IsGeneratedColumn(const TKikimrColumnMetadata* column);
bool IsStoredGeneratedColumn(const TKikimrColumnMetadata* column);
bool IsVirtualGeneratedColumn(const TKikimrColumnMetadata* column);
bool BuildGeneratedColumnGraph(const TKikimrTableMetadata& metadata,
    NKikimr::NGeneratedColumns::TGeneratedColumnDependencyGraph& graph, TString& error);

}   // namespace NYql
