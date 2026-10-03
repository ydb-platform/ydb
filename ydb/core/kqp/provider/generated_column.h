#pragma once

#include <ydb/core/kqp/host/kqp_translate.h>

#include <yql/essentials/ast/yql_expr.h>

namespace NYql {

// Wraps a raw GENERATED expression body in the SELECT envelope
TString AssembleGeneratedQuery(const TString& exprBody);

// Compiles the stored SQL text of a GENERATED column into
// a normalized `(lambda '(row) <expr>)` whose single argument is the table row
TExprNode::TPtr CompileGeneratedExpr(const TString& sqlText, const TString& columnName, TExprContext& ctx,
    NKikimr::NKqp::TKqpTranslationSettingsBuilder& settingsBuilder, const IModuleResolver::TPtr& moduleResolver);

// Validates a type-annotated generated-column lambda. Every callable in the
// expression must be explicitly known to be deterministic and non-throwing
bool ValidateGeneratedExpr(const TExprNode& lambda, const TString& columnName, TExprContext& ctx);

}   // namespace NYql
