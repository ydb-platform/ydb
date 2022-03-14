#include <ydb/library/yql/sql/pg_sql.h>
#include <ydb/library/yql/providers/common/codec/yql_pg_codec.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_pack_impl.h>
#include <ydb/library/yql/core/yql_pg_utils.h>

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

void WriteYsonValueInTableFormatPg(TOutputBuf& buf, NKikimr::NMiniKQL::TPgType* type, const NKikimr::NUdf::TUnboxedValuePod& value) {
    Y_UNUSED(buf);
    Y_UNUSED(type);
    Y_UNUSED(value);
    throw yexception() << "PG types are not supported";
}

NUdf::TUnboxedValue ReadYsonValuePg(NKikimr::NMiniKQL::TPgType* type, char cmd, TInputBuf& buf) {
    Y_UNUSED(type);
    Y_UNUSED(cmd);
    Y_UNUSED(buf);
    throw yexception() << "PG types are not supported";
}

NKikimr::NUdf::TUnboxedValue ReadSkiffPg(NKikimr::NMiniKQL::TPgType* type, NCommon::TInputBuf& buf) {
    Y_UNUSED(type);
    Y_UNUSED(buf);
    throw yexception() << "PG types are not supported";
}

void WriteSkiffPg(NKikimr::NMiniKQL::TPgType* type, const NKikimr::NUdf::TUnboxedValuePod& value, NCommon::TOutputBuf& buf) {
    Y_UNUSED(type);
    Y_UNUSED(value);
    Y_UNUSED(buf);
    throw yexception() << "PG types are not supported";
}

extern "C" void ReadSkiffPgValue(NKikimr::NMiniKQL::TPgType* type, NKikimr::NUdf::TUnboxedValue& value, NCommon::TInputBuf& buf) {
    Y_UNUSED(type);
    Y_UNUSED(value);
    Y_UNUSED(buf);
    throw yexception() << "PG types are not supported";
}

extern "C" void WriteSkiffPgValue(NKikimr::NMiniKQL::TPgType* type, const NKikimr::NUdf::TUnboxedValuePod& value, NCommon::TOutputBuf& buf) {
    Y_UNUSED(type);
    Y_UNUSED(value);
    Y_UNUSED(buf);
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

namespace NYql {

TMaybe<ui32> ConvertToPgType(NKikimr::NUdf::EDataSlot slot) {
    Y_UNUSED(slot);
    return Nothing();
}

TMaybe<NKikimr::NUdf::EDataSlot> ConvertFromPgType(ui32 typeId) {
    Y_UNUSED(typeId);
    return Nothing();
}

} // NYql
