#include <ydb/library/yql/sql/pg_sql.h>

namespace NSQLTranslationPG {

NYql::TAstParseResult PGToYql(const TString& query, const NSQLTranslation::TTranslationSettings& settings) {
    Y_UNUSED(query);
    Y_UNUSED(settings);
    NYql::TAstParseResult result;
    result.Issues.AddIssue(NYql::TIssue("PostgreSQL parser is not available"));
    return result;
}

}  // NSQLTranslationPG
