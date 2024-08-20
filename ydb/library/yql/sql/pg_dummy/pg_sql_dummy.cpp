#include <ydb/library/yql/parser/pg_wrapper/interface/interface.h>

#include <ydb/library/yql/minikql/computation/mkql_computation_node_pack_impl.h>
#include <ydb/library/yql/minikql/mkql_buffer.h>

namespace NSQLTranslationPG {

NYql::TAstParseResult PGToYql(const TString& query, const NSQLTranslation::TTranslationSettings& settings, NYql::TStmtParseInfo* stmtParseInfo) {
    Y_UNUSED(query);
    Y_UNUSED(settings);
    Y_UNUSED(stmtParseInfo);
    NYql::TAstParseResult result;
    result.Issues.AddIssue(NYql::TIssue("PostgreSQL parser is not available"));
    return result;
}

TVector<NYql::TAstParseResult> PGToYqlStatements(const TString& query, const NSQLTranslation::TTranslationSettings& settings, TVector<NYql::TStmtParseInfo>* stmtParseInfo) {
    Y_UNUSED(query);
    Y_UNUSED(settings);
    Y_UNUSED(stmtParseInfo);
    return {};
}

std::unique_ptr<NYql::NPg::IExtensionSqlParser> CreateExtensionSqlParser() {
    throw yexception() << "CreateExtensionSqlParser: PG types are not supported";
}

} // NSQLTranslationPG

namespace NYql {
namespace NCommon {

TString PgValueToString(const NUdf::TUnboxedValuePod& value, ui32 pgTypeId) {
    Y_UNUSED(value);
    Y_UNUSED(pgTypeId);
    throw yexception() << "PgValueToString: PG types are not supported";
}

NUdf::TUnboxedValue PgValueFromString(const TStringBuf text, ui32 pgTypeId) {
    Y_UNUSED(text);
    Y_UNUSED(pgTypeId);
    throw yexception() << "PgValueFromString: PG types are not supported";
}

TString PgValueToNativeText(const NUdf::TUnboxedValuePod& value, ui32 pgTypeId) {
    Y_UNUSED(value);
    Y_UNUSED(pgTypeId);
    throw yexception() << "PgValueToNativeText: PG types are not supported";
}

NUdf::TUnboxedValue PgValueFromNativeText(const TStringBuf text, ui32 pgTypeId) {
    Y_UNUSED(text);
    Y_UNUSED(pgTypeId);
    throw yexception() << "PgValueFromNativeText: PG types are not supported";
}

TString PgValueToNativeBinary(const NUdf::TUnboxedValuePod& value, ui32 pgTypeId) {
    Y_UNUSED(value);
    Y_UNUSED(pgTypeId);
    throw yexception() << "PgValueToNativeBinary: PG types are not supported";
}

NUdf::TUnboxedValue PgValueFromNativeBinary(const TStringBuf binary, ui32 pgTypeId) {
    Y_UNUSED(binary);
    Y_UNUSED(pgTypeId);
    throw yexception() << "PgValueFromNativeBinary: PG types are not supported";
}

TString PgValueCoerce(const NUdf::TUnboxedValuePod& value, ui32 pgTypeId, i32 typMod, TMaybe<TString>* error) {
    Y_UNUSED(value);
    Y_UNUSED(pgTypeId);
    Y_UNUSED(typMod);
    Y_UNUSED(error);
    throw yexception() << "PgValueCoerce: PG types are not supported";
}

void WriteYsonValuePg(TYsonResultWriter& writer, const NUdf::TUnboxedValuePod& value, NKikimr::NMiniKQL::TPgType* type,
    const TVector<ui32>* structPositions) {
    Y_UNUSED(writer);
    Y_UNUSED(value);
    Y_UNUSED(type);
    Y_UNUSED(structPositions);
    throw yexception() << "WriteYsonValuePg: PG types are not supported";
}

void WriteYsonValueInTableFormatPg(TOutputBuf& buf, NKikimr::NMiniKQL::TPgType* type, const NKikimr::NUdf::TUnboxedValuePod& value, bool topLevel) {
    Y_UNUSED(buf);
    Y_UNUSED(type);
    Y_UNUSED(value);
    Y_UNUSED(topLevel);
    throw yexception() << "WriteYsonValueInTableFormatPg: PG types are not supported";
}

NUdf::TUnboxedValue ReadYsonValueInTableFormatPg(NKikimr::NMiniKQL::TPgType* type, char cmd, TInputBuf& buf) {
    Y_UNUSED(type);
    Y_UNUSED(cmd);
    Y_UNUSED(buf);
    throw yexception() << "ReadYsonValueInTableFormatPg: PG types are not supported";
}

NUdf::TUnboxedValue ReadYsonValuePg(NKikimr::NMiniKQL::TPgType* type, char cmd, TInputBuf& buf) {
    Y_UNUSED(type);
    Y_UNUSED(cmd);
    Y_UNUSED(buf);
    throw yexception() << "ReadYsonValuePg: PG types are not supported";
}

void SkipSkiffPg(NKikimr::NMiniKQL::TPgType* type, NCommon::TInputBuf& buf) {
    Y_UNUSED(type);
    Y_UNUSED(buf);
    throw yexception() << "SkipSkiffPg: PG types are not supported";
}

NKikimr::NUdf::TUnboxedValue ReadSkiffPg(NKikimr::NMiniKQL::TPgType* type, NCommon::TInputBuf& buf) {
    Y_UNUSED(type);
    Y_UNUSED(buf);
    throw yexception() << "ReadSkiffPg: PG types are not supported";
}

void WriteSkiffPg(NKikimr::NMiniKQL::TPgType* type, const NKikimr::NUdf::TUnboxedValuePod& value, NCommon::TOutputBuf& buf) {
    Y_UNUSED(type);
    Y_UNUSED(value);
    Y_UNUSED(buf);
    throw yexception() << "WriteSkiffPg: PG types are not supported";
}

extern "C" void ReadSkiffPgValue(NKikimr::NMiniKQL::TPgType* type, NKikimr::NUdf::TUnboxedValue& value, NCommon::TInputBuf& buf) {
    Y_UNUSED(type);
    Y_UNUSED(value);
    Y_UNUSED(buf);
    throw yexception() << "ReadSkiffPgValue: PG types are not supported";
}

extern "C" void WriteSkiffPgValue(NKikimr::NMiniKQL::TPgType* type, const NKikimr::NUdf::TUnboxedValuePod& value, NCommon::TOutputBuf& buf) {
    Y_UNUSED(type);
    Y_UNUSED(value);
    Y_UNUSED(buf);
    throw yexception() << "WriteSkiffPgValue: PG types are not supported";
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

void PgSetGUCSettings(void* ctx, const TGUCSettings::TPtr& GUCSettings) {
    Y_UNUSED(ctx);
    Y_UNUSED(GUCSettings);
}

std::unique_ptr<NYql::NPg::IExtensionLoader> CreateExtensionLoader() {
    throw yexception() << "PG types are not supported";
}

std::optional<std::string> PGGetGUCSetting(const std::string& key) {
    Y_UNUSED(key);
    throw yexception() << "PG types are not supported";
}

ui64 PgValueSize(const NUdf::TUnboxedValuePod& value, i32 typeLen) {
    Y_UNUSED(typeLen);
    Y_UNUSED(value);
    throw yexception() << "PG types are not supported";
}

ui64 PgValueSize(ui32 type, const NUdf::TUnboxedValuePod& value) {
    Y_UNUSED(type);
    Y_UNUSED(value);
    throw yexception() << "PG types are not supported";
}

ui64 PgValueSize(const TPgType* type, const NUdf::TUnboxedValuePod& value) {
    Y_UNUSED(type);
    Y_UNUSED(value);
    throw yexception() << "PG types are not supported";
}

void PGPackImpl(bool stable, const TPgType* type, const NUdf::TUnboxedValuePod& value, TBuffer& buf) {
   Y_UNUSED(stable);
   Y_UNUSED(type);
   Y_UNUSED(value);
   Y_UNUSED(buf);
   throw yexception() << "PG types are not supported";
}

void PGPackImpl(bool stable, const TPgType* type, const NUdf::TUnboxedValuePod& value, NKikimr::NMiniKQL::TPagedBuffer& buf) {
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

NUdf::TUnboxedValue PGUnpackImpl(const TPgType* type, NDetails::TChunkedInputBuffer& buf) {
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

NUdf::IBlockItemComparator::TPtr MakePgItemComparator(ui32 typeId) {
    Y_UNUSED(typeId);
    throw yexception() << "PG types are not supported";
}

NUdf::IBlockItemHasher::TPtr MakePgItemHasher(ui32 typeId) {
    Y_UNUSED(typeId);
    throw yexception() << "PG types are not supported";
}

void RegisterPgBlockAggs(THashMap<TString, std::unique_ptr<IBlockAggregatorFactory>>& registry) {
    Y_UNUSED(registry);
}

} // namespace NMiniKQL
} // namespace NKikimr

namespace NYql {

arrow::Datum MakePgScalar(NKikimr::NMiniKQL::TPgType* type, const NKikimr::NUdf::TUnboxedValuePod& value, arrow::MemoryPool& pool) {
    Y_UNUSED(type);
    Y_UNUSED(value);
    Y_UNUSED(pool);
    return arrow::Datum();
}

arrow::Datum MakePgScalar(NKikimr::NMiniKQL::TPgType* type, const NUdf::TBlockItem& value, arrow::MemoryPool& pool) {
    Y_UNUSED(type);
    Y_UNUSED(value);
    Y_UNUSED(pool);
    return arrow::Datum();
}

TColumnConverter BuildPgColumnConverter(const std::shared_ptr<arrow::DataType>& originalType, NKikimr::NMiniKQL::TPgType* targetType) {
    Y_UNUSED(originalType);
    Y_UNUSED(targetType);
    return {};
}

ui32 ConvertToPgType(NKikimr::NUdf::EDataSlot slot) {
    Y_UNUSED(slot);
    throw yexception() << "PG types are not supported";
}

TMaybe<NKikimr::NUdf::EDataSlot> ConvertFromPgType(ui32 typeId) {
    Y_UNUSED(typeId);
    return Nothing();
}

bool ParsePgIntervalModifier(const TString& str, i32& ret) {
    Y_UNUSED(str);
    Y_UNUSED(ret);
    return false;
}

class TPgDummyBuilder : public NUdf::IPgBuilder {
public:
    NUdf::TUnboxedValue ValueFromText(ui32 typeId, const NUdf::TStringRef& value, NUdf::TStringValue& error) const override {
        Y_UNUSED(typeId);
        Y_UNUSED(value);
        error = NUdf::TStringValue(TStringBuf("TPgDummyBuilder::ValueFromText does nothing"));
        return NUdf::TUnboxedValue();
    }

    NUdf::TUnboxedValue ValueFromBinary(ui32 typeId, const NUdf::TStringRef& value, NUdf::TStringValue& error) const override {
        Y_UNUSED(typeId);
        Y_UNUSED(value);
        error = NUdf::TStringValue(TStringBuf("TPgDummyBuilder::ValueFromBinary does nothing"));
        return NUdf::TUnboxedValue();
    }

    NUdf::TUnboxedValue ConvertFromPg(NUdf::TUnboxedValue source, ui32 sourceTypeId, const NUdf::TType* targetType) const override {
        Y_UNUSED(source);
        Y_UNUSED(sourceTypeId);
        Y_UNUSED(targetType);
        ythrow yexception() << "TPgDummyBuilder::ConvertFromPg does nothing";
    }

    NUdf::TUnboxedValue ConvertToPg(NUdf::TUnboxedValue source, const NUdf::TType* sourceType, ui32 targetTypeId) const override {
        Y_UNUSED(source);
        Y_UNUSED(sourceType);
        Y_UNUSED(targetTypeId);
        ythrow yexception() << "TPgDummyBuilder::ConvertToPg does nothing";
    }

    NUdf::TUnboxedValue NewString(i32 typeLen, ui32 targetTypeId, NUdf::TStringRef data) const override {
        Y_UNUSED(typeLen);
        Y_UNUSED(targetTypeId);
        Y_UNUSED(data);
        ythrow yexception() << "TPgDummyBuilder::NewString does nothing";
    }

    NUdf::TStringRef AsCStringBuffer(const NUdf::TUnboxedValue& value) const override {
        Y_UNUSED(value);
        ythrow yexception() << "TPgDummyBuilder::AsCStringBuffer does nothing";
    }

    NUdf::TStringRef AsTextBuffer(const NUdf::TUnboxedValue& value) const override {
        Y_UNUSED(value);
        ythrow yexception() << "TPgDummyBuilder::AsTextBuffer does nothing";
    }

    NUdf::TUnboxedValue MakeCString(const char* value) const override {
        Y_UNUSED(value);
        ythrow yexception() << "TPgDummyBuilder::MakeCString does nothing";
    }

    NUdf::TUnboxedValue MakeText(const char* value) const override {
        Y_UNUSED(value);
        ythrow yexception() << "TPgDummyBuilder::MakeText does nothing";
    }

    NUdf::TStringRef AsFixedStringBuffer(const NUdf::TUnboxedValue& value, ui32 length) const override {
        Y_UNUSED(value);
        Y_UNUSED(length);
        ythrow yexception() << "TPgDummyBuilder::AsFixedStringBuffer does nothing";
    }
};

std::unique_ptr<NUdf::IPgBuilder> CreatePgBuilder() {
    return std::make_unique<TPgDummyBuilder>();
}

bool HasPgKernel(ui32 procOid) {
    Y_UNUSED(procOid);
    return false;
}

std::function<NKikimr::NMiniKQL::IComputationNode* (NKikimr::NMiniKQL::TCallable&,
    const NKikimr::NMiniKQL::TComputationNodeFactoryContext&)> GetPgFactory()
{
    return [] (
        NKikimr::NMiniKQL::TCallable& callable,
        const NKikimr::NMiniKQL::TComputationNodeFactoryContext& ctx
    ) -> NKikimr::NMiniKQL::IComputationNode* {
        Y_UNUSED(callable);
        Y_UNUSED(ctx);
        return nullptr;
    };
}

IOptimizer* MakePgOptimizerInternal(const IOptimizer::TInput& input, const std::function<void(const TString&)>& log)
{
    Y_UNUSED(input);
    Y_UNUSED(log);
    ythrow yexception() << "PgJoinSearch does nothing";
}

IOptimizerNew* MakePgOptimizerNew(IProviderContext& pctx, TExprContext& ctx, const std::function<void(const TString&)>& log)
{
    Y_UNUSED(pctx);
    Y_UNUSED(ctx);
    Y_UNUSED(log);
    ythrow yexception() << "PgJoinSearch does nothing";
}

} // NYql

namespace NKikimr::NPg {

ui32 PgTypeIdFromTypeDesc(void* typeDesc) {
    Y_UNUSED(typeDesc);
    return 0;
}

void* TypeDescFromPgTypeId(ui32 pgTypeId) {
    Y_UNUSED(pgTypeId);
    return {};
}

TString PgTypeNameFromTypeDesc(void* typeDesc, const TString& typeMod) {
    Y_UNUSED(typeDesc);
    Y_UNUSED(typeMod);
    return "";
}

void* TypeDescFromPgTypeName(const TStringBuf name) {
    Y_UNUSED(name);
    return {};
}

TString TypeModFromPgTypeName(const TStringBuf name) {
    Y_UNUSED(name);
    return {};
}

bool TypeDescIsComparable(void* typeDesc) {
    Y_UNUSED(typeDesc);
    throw yexception() << "PG types are not supported";
}

i32 TypeDescGetTypeLen(void* typeDesc) {
    Y_UNUSED(typeDesc);
    throw yexception() << "PG types are not supported";
}

ui32 TypeDescGetStoredSize(void* typeDesc) {
    Y_UNUSED(typeDesc);
    throw yexception() << "PG types are not supported";
}

bool TypeDescNeedsCoercion(void* typeDesc) {
    Y_UNUSED(typeDesc);
    throw yexception() << "PG types are not supported";
}

int PgNativeBinaryCompare(const char* dataL, size_t sizeL, const char* dataR, size_t sizeR, void* typeDesc) {
    Y_UNUSED(dataL);
    Y_UNUSED(sizeL);
    Y_UNUSED(dataR);
    Y_UNUSED(sizeR);
    Y_UNUSED(typeDesc);
    throw yexception() << "PG types are not supported";
}

ui64 PgNativeBinaryHash(const char* data, size_t size, void* typeDesc) {
    Y_UNUSED(data);
    Y_UNUSED(size);
    Y_UNUSED(typeDesc);
    throw yexception() << "PG types are not supported";
}

TTypeModResult BinaryTypeModFromTextTypeMod(const TString& str, void* typeDesc) {
    Y_UNUSED(str);
    Y_UNUSED(typeDesc);
    throw yexception() << "PG types are not supported";
}

TMaybe<TString> PgNativeBinaryValidate(const TStringBuf binary, void* typeDesc) {
    Y_UNUSED(binary);
    Y_UNUSED(typeDesc);
    throw yexception() << "PG types are not supported";
}

TCoerceResult PgNativeBinaryCoerce(const TStringBuf binary, void* typeDesc, i32 typmod) {
    Y_UNUSED(binary);
    Y_UNUSED(typeDesc);
    Y_UNUSED(typmod);
    throw yexception() << "PG types are not supported";
}

TConvertResult PgNativeBinaryFromNativeText(const TString& str, void* typeDesc) {
    Y_UNUSED(str);
    Y_UNUSED(typeDesc);
    throw yexception() << "PG types are not supported";
}

TConvertResult PgNativeBinaryFromNativeText(const TString& str, ui32 pgTypeId) {
    Y_UNUSED(str);
    Y_UNUSED(pgTypeId);
    throw yexception() << "PG types are not supported";
}

TConvertResult PgNativeTextFromNativeBinary(const TStringBuf binary, void* typeDesc) {
    Y_UNUSED(binary);
    Y_UNUSED(typeDesc);
    throw yexception() << "PG types are not supported";
}

TConvertResult PgNativeTextFromNativeBinary(const TStringBuf binary, ui32 pgTypeId) {
    Y_UNUSED(binary);
    Y_UNUSED(pgTypeId);
    throw yexception() << "PG types are not supported";
}

TString GetPostgresServerVersionNum() {
    return "-1";
}
TString GetPostgresServerVersionStr() {
    return "pg_sql_dummy";
}

} // namespace NKikimr::NPg

namespace NYql {

ui64 HexEncode(const char *src, size_t len, char *dst) {
    Y_UNUSED(src);
    Y_UNUSED(len);
    Y_UNUSED(dst);

    throw yexception() << "HexEncode in pg_dummy does nothing";
}

} // NYql
