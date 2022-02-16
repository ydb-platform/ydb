#pragma once
#include <ydb/library/yql/ast/yql_ast.h>
#include <ydb/library/yql/sql/settings/translation_settings.h>

namespace NSQLTranslationPG {

NYql::TAstParseResult PGToYql(const TString& query, const NSQLTranslation::TTranslationSettings& settings);

}  // NSQLTranslationPG
