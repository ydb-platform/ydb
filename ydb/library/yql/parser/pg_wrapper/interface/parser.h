#pragma once

#include <ydb/library/yql/ast/yql_ast.h>

namespace NSQLTranslation {

struct TTranslationSettings;

} // NSQLTranslation

namespace NSQLTranslationPG {

NYql::TAstParseResult PGToYql(const TString& query, const NSQLTranslation::TTranslationSettings& settings);

} // NSQLTranslationPG
