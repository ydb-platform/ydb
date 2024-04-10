
#include <ydb/library/yql/utils/backtrace/backtrace.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>
#include <library/cpp/json/writer/json.h>
#include <util/generic/yexception.h>

using namespace NYql;
using namespace NJson;

int Main(int argc, const char *argv[])
{
    Y_UNUSED(argc);
    Y_UNUSED(argv);
    NJsonWriter::TBuf json;
    json.BeginObject();
    json.WriteKey("proc");
    json.BeginList();

    NPg::EnumProc([&](ui32 oid, const NPg::TProcDesc& desc) {
        if (desc.ReturnSet || desc.Kind != NPg::EProcKind::Function) {
            return;
        }

        json.BeginObject();
        json.WriteKey("oid").WriteInt(oid);
        json.WriteKey("src").WriteString(desc.Src);
        json.WriteKey("strict").WriteBool(desc.IsStrict);
        const auto& retTypeDesc = NPg::LookupType(desc.ResultType);
        json.WriteKey("ret_type").WriteString(retTypeDesc.Name);
        json.WriteKey("ret_type_fixed").WriteBool(retTypeDesc.PassByValue &&
            retTypeDesc.TypeLen > 0 && retTypeDesc.TypeLen <= 8);
        if (desc.VariadicType != 0) {
            const auto& varTypeDesc = NPg::LookupType(desc.VariadicType);
            json.WriteKey("var_type").WriteString( varTypeDesc.Name);
            if (varTypeDesc.Name != "any") {
                json.WriteKey("var_type_fixed").WriteBool(varTypeDesc.PassByValue &&
                    varTypeDesc.TypeLen > 0 && varTypeDesc.TypeLen <= 8);
            }
        }
        json.WriteKey("args").BeginList();
        for (const auto& a : desc.ArgTypes) {
            const auto& argTypeDesc = NPg::LookupType(a);
            json.BeginObject();
            json.WriteKey("arg_type").WriteString(argTypeDesc.Name);
            json.WriteKey("arg_type_fixed").WriteBool(argTypeDesc.PassByValue &&
                argTypeDesc.TypeLen > 0 && argTypeDesc.TypeLen <= 8);
            json.EndObject();
        }
        json.EndList();
        json.EndObject();
    });

    json.EndList();
    json.WriteKey("aggregation");
    json.BeginList();
    NPg::EnumAggregation([&](ui32 oid, const NPg::TAggregateDesc& desc) {
        if (desc.Kind != NPg::EAggKind::Normal) {
            return;
        }

        json.BeginObject();
        json.WriteKey("agg_id").WriteInt(oid);
        json.WriteKey("name").WriteString(desc.Name);
        json.WriteKey("args");
        json.BeginList();
        for (const auto& a : desc.ArgTypes) {
            const auto& argTypeDesc = NPg::LookupType(a);
            json.BeginObject();
            json.WriteKey("arg_type").WriteString(argTypeDesc.Name);
            json.WriteKey("arg_type_fixed").WriteBool(argTypeDesc.PassByValue &&
                argTypeDesc.TypeLen > 0 && argTypeDesc.TypeLen <= 8);
            json.EndObject();
        }

        json.EndList();

        json.WriteKey("trans_func_id");
        json.WriteInt(desc.TransFuncId);
        json.WriteKey("serialize_func_id");
        json.WriteInt(desc.SerializeFuncId);
        json.WriteKey("deserialize_func_id");
        json.WriteInt(desc.DeserializeFuncId);
        json.WriteKey("combine_func_id");
        json.WriteInt(desc.CombineFuncId);
        json.WriteKey("final_func_id");
        json.WriteInt(desc.FinalFuncId);

        const auto& transDesc = NPg::LookupType(NPg::LookupProc(desc.TransFuncId).ResultType);
        json.WriteKey("trans_type").WriteString(transDesc.Name);
        json.WriteKey("trans_type_fixed").WriteBool(transDesc.PassByValue &&
            transDesc.TypeLen > 0 && transDesc.TypeLen <= 8);

        const auto& serializedDesc = NPg::LookupType(NPg::LookupProc(desc.SerializeFuncId ?
            desc.SerializeFuncId : desc.TransFuncId).ResultType);
        json.WriteKey("serialized_type").WriteString(serializedDesc.Name);
        json.WriteKey("serialized_type_fixed").WriteBool(serializedDesc.PassByValue &&
            serializedDesc.TypeLen > 0 && serializedDesc.TypeLen <= 8);

        const auto& retDesc = NPg::LookupType(NPg::LookupProc(desc.FinalFuncId ?
            desc.FinalFuncId : desc.TransFuncId).ResultType);
        json.WriteKey("ret_type").WriteString(retDesc.Name);
        json.WriteKey("ret_type_fixed").WriteBool(retDesc.PassByValue &&
            retDesc.TypeLen > 0 && retDesc.TypeLen <= 8);

        json.WriteKey("has_init_value").WriteBool(!desc.InitValue.Empty());
        json.EndObject();
    });

    json.EndList();
    json.EndObject();
    Cout << json.Str();

    return 0;
}

int main(int argc, const char *argv[]) {
    NYql::NBacktrace::RegisterKikimrFatalActions();
    NYql::NBacktrace::EnableKikimrSymbolize();

    try {
        return Main(argc, argv);
    }
    catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
