#pragma once

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/sql/settings/translation_settings.h>
#include <util/generic/vector.h>

namespace NYql {

TVector<TString> CollectGeneratedColumnRefs(const TExprNode& lambda);
TString AssembleGeneratedQuery(const TString& expression);
TExprNode::TPtr CompileGeneratedExpr(const TString& sqlText, const TString& columnName, TExprContext& ctx,
    NSQLTranslation::TTranslationSettings settings, const IModuleResolver::TPtr& moduleResolver, bool ambiguityError = false);

// Structural schema validation. Type/coercion checks remain with query compilation.
// Successful validation canonicalizes the direct dependency set.
bool ValidateGeneratedExpressionDependencies(const TString& expression, const TString& columnName,
    TVector<TString>& dependencies, TString& error);

} // namespace NYql
