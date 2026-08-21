#pragma once

#include <ydb/core/kqp/host/kqp_translate.h>

#include <yql/essentials/ast/yql_expr.h>

namespace NYql {

// Wraps a raw GENERATED expression body in the SELECT envelope
TString AssembleGeneratedQuery(const TString& context, const TString& exprBody);

// Wraps a raw DEFAULT expression body in a FROM-less SELECT envelope
TString AssembleDefaultQuery(const TString& context, const TString& exprBody);

// Compiles the stored SQL text of a GENERATED column into
// a normalized `(lambda '(row) <expr>)` whose single argument is the table row
TExprNode::TPtr CompileGeneratedExpr(const TString& sqlText, const TString& columnName, TExprContext& ctx,
    NKikimr::NKqp::TKqpTranslationSettingsBuilder& settingsBuilder, const IModuleResolver::TPtr& moduleResolver);

// Compiles the stored SQL text of a DEFAULT column into `(lambda '(row) <expr>)`.
// The lambda argument is an unused empty struct: a DEFAULT expression cannot reference columns
TExprNode::TPtr CompileDefaultExpr(const TString& sqlText, const TString& columnName, TExprContext& ctx,
    NKikimr::NKqp::TKqpTranslationSettingsBuilder& settingsBuilder, const IModuleResolver::TPtr& moduleResolver);

}   // namespace NYql
