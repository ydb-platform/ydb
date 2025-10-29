#pragma once

#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/sql/settings/translation_settings.h>

namespace NSQLFormat {

TMaybe<TString> CheckedFormat(
    const TString& query,
    const NSQLTranslation::TTranslationSettings& settings,
    NYql::TIssues& issues,
    bool isIdempotencyChecked = true);

} // namespace NSQLFormat
