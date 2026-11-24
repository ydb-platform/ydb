#pragma once

#include <ydb/core/kqp/host/kqp_translate.h>

#include <yql/essentials/ast/yql_expr.h>

#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/generic/strbuf.h>

namespace NYql {

// Wraps a raw GENERATED expression body in the SELECT envelope
TString AssembleGeneratedQuery(const TString& exprBody);

// Wraps a raw DEFAULT expression body in a FROM-less SELECT envelope
TString AssembleDefaultQuery(const TString& exprBody);

// Converts a captured DEFAULT expression to SQL for the standard lexer, using the original query's lexer mode.
TMaybe<TString> NormalizeDefaultExprText(TStringBuf exprBody, bool ansiLexer, TExprContext& ctx);

// Checks whether a closed DEFAULT expression can be evaluated as a schema literal.
bool CanEvaluateDefaultAtCompileTime(const TExprNode& expression);

// Identifies callables that require table, file or session context.
bool IsNonRowColumnNode(const TExprNode& node);

// Collects named reads from a scalar column expression's row argument.
void CollectColumnRefs(const TExprNode& body, const TExprNode* rowArg, THashSet<TString>& refs);

// Compiles the stored SQL text of a GENERATED column into
// a normalized `(lambda '(row) <expr>)` whose single argument is the table row
TExprNode::TPtr CompileGeneratedExpr(const TString& sqlText, const TString& columnName, TExprContext& ctx,
    NKikimr::NKqp::TKqpTranslationSettingsBuilder& settingsBuilder, const IModuleResolver::TPtr& moduleResolver);

// Compiles the stored SQL text of a DEFAULT column into `(lambda '(row) <expr>)`.
// The lambda argument is an unused empty struct: a DEFAULT expression cannot reference columns
TExprNode::TPtr CompileDefaultExpr(const TString& sqlText, const TString& columnName, TExprContext& ctx,
    NKikimr::NKqp::TKqpTranslationSettingsBuilder& settingsBuilder, const IModuleResolver::TPtr& moduleResolver);

}   // namespace NYql
