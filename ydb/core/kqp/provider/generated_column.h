#pragma once

#include <ydb/library/generated_columns/generated_column_dependencies.h>

#include <ydb/library/generated_columns/expr/generated_column_expr.h>

#include <util/generic/vector.h>

namespace NKikimr::NKqp {
class TKqpTranslationSettingsBuilder;
}

namespace NYql {

struct TKikimrColumnMetadata;
struct TKikimrTableMetadata;

bool IsGeneratedColumn(const TKikimrColumnMetadata* column);
bool IsStoredGeneratedColumn(const TKikimrColumnMetadata* column);
bool IsVirtualGeneratedColumn(const TKikimrColumnMetadata* column);
bool BuildGeneratedColumnGraph(const TKikimrTableMetadata& metadata,
    NKikimr::NGeneratedColumns::TGeneratedColumnDependencyGraph& graph, TString& error);


// Compiles the stored SQL text of a GENERATED column into
// a normalized `(lambda '(row) <expr>)` whose single argument is the table row
TExprNode::TPtr CompileGeneratedExpr(const TString& sqlText, const TString& columnName, TExprContext& ctx,
    NKikimr::NKqp::TKqpTranslationSettingsBuilder& settingsBuilder, const IModuleResolver::TPtr& moduleResolver);

}   // namespace NYql
