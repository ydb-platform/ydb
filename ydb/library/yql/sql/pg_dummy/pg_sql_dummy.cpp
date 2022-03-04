#include <ydb/library/yql/sql/pg_sql.h>
#include <ydb/library/yql/providers/common/codec/yql_pg_codec.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_pack_impl.h>

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

namespace NKikimr {
namespace NMiniKQL {

void PGPackImpl(const TPgType* type, const NUdf::TUnboxedValuePod& value, TBuffer& buf) {
   Y_UNUSED(type);
   Y_UNUSED(value);
   Y_UNUSED(buf);
   throw yexception() << "PG types are not supported";
}

NUdf::TUnboxedValue PGUnpackImpl(const TPgType* type, TStringBuf& buf) {
   Y_UNUSED(type);
   Y_UNUSED(buf);
   throw yexception() << "PG types are not supported";
}

} // namespace NMiniKQL
} // namespace NKikimr
