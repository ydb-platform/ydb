#include "wasm_run.hpp"

#include "wasm_invoke.hpp"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <yql/essentials/public/udf/udf_type_builder.h>

namespace NWasm::NYQL {

using namespace NYql::NUdf;

namespace {

struct TJsonRunRequest
{
    TString ArgsJson;
    bool StringResult = false;
};

TJsonRunRequest ParseJsonRunRequest(TStringBuf requestJson)
{
    NJson::TJsonValue root;
    if (!NJson::ReadJsonTree(requestJson, &root)) {
        ythrow yexception() << "Failed to parse wasm Run JSON: " << requestJson;
    }

    if (root.IsArray()) {
        return {TString(requestJson), false};
    }

    if (!root.IsMap()) {
        ythrow yexception() << "Wasm Run JSON must be an array or object";
    }

    const auto& args = root["args"];
    if (!args.IsDefined()) {
        ythrow yexception() << "Wasm Run JSON object must contain \"args\"";
    }
    if (!args.IsArray()) {
        ythrow yexception() << "Wasm Run JSON object field \"args\" must be an array";
    }

    bool stringResult = false;
    const auto& result = root["result"];
    if (result.IsDefined()) {
        if (!result.IsString()) {
            ythrow yexception() << "Wasm Run JSON object field \"result\" must be a string";
        }
        const auto resultType = result.GetString();
        if (resultType == "string") {
            stringResult = true;
        } else if (resultType != "auto" && resultType != "number") {
            ythrow yexception()
                << "Unsupported wasm Run JSON result type: " << resultType;
        }
    }

    return {NJson::WriteJson(args, false), stringResult};
}

TString MakeNumericJsonResult(const TWasmInvokeResult& invokeResult)
{
    if (!invokeResult.HasValue) {
        return "null";
    }

    if (invokeResult.IsInt) {
        return NJson::WriteJson(NJson::TJsonValue(invokeResult.Value.Get<i64>()), false);
    }
    return NJson::WriteJson(NJson::TJsonValue(invokeResult.Value.Get<double>()), false);
}

TString MakeStringJsonResult(const TWasmStringInvokeResult& invokeResult)
{
    if (!invokeResult.HasValue) {
        return "null";
    }

    return NJson::WriteJson(NJson::TJsonValue(invokeResult.Value), false);
}

} // namespace

TStringRef TRun::Name()
{
    static auto name = TStringRef::Of("Run");
    return name;
}

TType* TRun::BuildFunctionType(IFunctionTypeInfoBuilder& builder)
{
    return builder.Callable()
        ->Returns<char*>()
        .Arg<char*>()
        .Name("FunctionName")
        .Arg<char*>()
        .Name("ArgsJson")
        .Build();
}

TRun::TRun(TWasmRuntimeStatePtr state)
    : State_(std::move(state))
{
}

TUnboxedValue TRun::Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const
{
    try {
        const TString functionName(args[0].AsStringRef());
        const auto request = ParseJsonRunRequest(args[1].AsStringRef());
        const auto& exportInfo = GetWasmExport(State_, functionName);
        const auto wasmArgs = ReadWasmJsonArgs(State_, request.ArgsJson, exportInfo.Signature);

        TString resultJson;
        if (request.StringResult) {
            resultJson = MakeStringJsonResult(
                InvokeWasmStringFunction(State_, functionName, wasmArgs.Values));
        } else {
            resultJson = MakeNumericJsonResult(
                InvokeWasmFunction(State_, functionName, wasmArgs.Values));
        }
        return valueBuilder->NewString(TStringRef(resultJson));
    } catch (const std::exception& ex) {
        WasmError(ex, Name(), valueBuilder);
    }
    return {};
}

} // namespace NWasm::NYQL
