#include <ydb/library/yql/sql/pg_sql.h>
#include <ydb/library/yql/providers/common/codec/yql_pg_codec.h>

namespace NSQLTranslationPG {

NYql::TAstParseResult PGToYql(const TString& query, const NSQLTranslation::TTranslationSettings& settings) {
    Y_UNUSED(query);
    Y_UNUSED(settings);
    NYql::TAstParseResult result;
    result.Issues.AddIssue(NYql::TIssue("PostgreSQL parser is not available"));
    return result;
}

}  // NSQLTranslationPG

namespace NYql {
namespace NCommon {

void WriteYsonValuePg(TYsonResultWriter& writer, const NUdf::TUnboxedValuePod& value, NKikimr::NMiniKQL::TPgType* type,
    const TVector<ui32>* structPositions) {
    Y_UNUSED(writer);
    Y_UNUSED(value);
    Y_UNUSED(type);
    Y_UNUSED(structPositions);
    throw yexception() << "PG types are not supported";
}

} // namespace NCommon
} // NYql

