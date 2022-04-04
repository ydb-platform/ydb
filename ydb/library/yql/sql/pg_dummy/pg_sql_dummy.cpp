#include <ydb/library/yql/sql/pg_sql.h>
#include <ydb/library/yql/providers/common/codec/yql_pg_codec.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_pack_impl.h>
#include <ydb/library/yql/minikql/computation/presort_impl.h>
#include <ydb/library/yql/core/yql_pg_utils.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>

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

void* PgInitializeMainContext() {
    return nullptr;
}

void PgDestroyMainContext(void* ctx) {
    Y_UNUSED(ctx);
}

void PgAcquireThreadContext(void* ctx) {
    Y_UNUSED(ctx);
}

void PgReleaseThreadContext(void* ctx) {
    Y_UNUSED(ctx);
}

void PGPackImpl(bool stable, const TPgType* type, const NUdf::TUnboxedValuePod& value, TBuffer& buf) {
   Y_UNUSED(stable);
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

void EncodePresortPGValue(TPgType* type, const NUdf::TUnboxedValue& value, TVector<ui8>& output) {
    Y_UNUSED(type);
    Y_UNUSED(value);
    Y_UNUSED(output);
    throw yexception() << "PG types are not supported";
}

NUdf::TUnboxedValue DecodePresortPGValue(TPgType* type, TStringBuf& input, TVector<ui8>& buffer) {
    Y_UNUSED(type);
    Y_UNUSED(input);
    Y_UNUSED(buffer);
    throw yexception() << "PG types are not supported";
}

void* PgInitializeContext(const std::string_view& contextType) {
   Y_UNUSED(contextType);
   return nullptr;
}

void PgDestroyContext(const std::string_view& contextType, void* ctx) {
   Y_UNUSED(contextType);
   Y_UNUSED(ctx);
}

NUdf::IHash::TPtr MakePgHash(const NMiniKQL::TPgType* type) {
    Y_UNUSED(type);
    throw yexception() << "PG types are not supported";
}

NUdf::ICompare::TPtr MakePgCompare(const NMiniKQL::TPgType* type) {
    Y_UNUSED(type);
    throw yexception() << "PG types are not supported";
}

NUdf::IEquate::TPtr MakePgEquate(const NMiniKQL::TPgType* type) {
    Y_UNUSED(type);
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
