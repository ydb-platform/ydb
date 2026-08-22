#pragma once
#include <yql/essentials/core/issue/yql_issue.h>
#include <util/generic/string.h>

namespace NSQLTranslationV1 {

bool CheckLexers(NYql::TPosition pos, const TString& query, NYql::TIssues& issues);

} // namespace NSQLTranslationV1
