#pragma once

#include <yql/essentials/ast/yql_ast.h>
#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/sql/settings/translation_settings.h>

namespace NSQLFormat {

enum class EConvergenceRequirement: ui8 {
    None,
    Triple, // format(format(input)) == format(format(format(input)))
    Double, // .      format(input)  ==        format(format(input))
};

TMaybe<TString> CheckedFormat(
    const TString& query,
    const NYql::TAstNode* ast,
    const NSQLTranslation::TTranslationSettings& settings,
    NYql::TIssues& issues,
    EConvergenceRequirement convergence = EConvergenceRequirement::Double);

} // namespace NSQLFormat
