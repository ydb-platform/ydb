#pragma once

#include <yql/essentials/ast/yql_ast.h>
#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/sql/settings/translation_settings.h>

namespace NSQLFormat {

TMaybe<TString> CheckedFormat(
    const TString& query,
    const NYql::TAstNode* ast,
    const NSQLTranslation::TTranslationSettings& settings,
    NYql::TIssues& issues,
    bool isIdempotencyChecked = true);

} // namespace NSQLFormat
