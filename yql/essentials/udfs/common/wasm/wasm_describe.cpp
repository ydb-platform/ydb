#include "wasm_describe.hpp"

#include "wasm_signature.hpp"

#include <yql/essentials/public/udf/udf_type_builder.h>

namespace NWasm::NYQL {

using namespace NYql::NUdf;

TStringRef TDescribe::Name()
{
    static auto name = TStringRef::Of("Describe");
    return name;
}

TType* TDescribe::BuildFunctionType(IFunctionTypeInfoBuilder& builder)
{
    const auto stringType = builder.SimpleType<char*>();
    const auto boolType = builder.SimpleType<bool>();
    const auto argsListType = builder.List()->Item(stringType).Build();

    ui32 nameIdx = 0;
    ui32 argsIdx = 0;
    ui32 returnIdx = 0;
    ui32 supportedIdx = 0;
    const auto describeStructType = builder.Struct(4U)
        ->AddField("Name", stringType, &nameIdx)
        .AddField("Args", argsListType, &argsIdx)
        .AddField("Return", stringType, &returnIdx)
        .AddField("Supported", boolType, &supportedIdx)
        .Build();
    const auto resultListType = builder.List()->Item(describeStructType).Build();

    return builder.Callable()
        ->Returns(resultListType)
        .Build();
}

TDescribe::TDescribe(TWasmRuntimeStatePtr state)
    : State_(std::move(state))
{
}

TUnboxedValue TDescribe::Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* /*args*/) const
{
    try {
        const auto exports = ListWasmExports(State_);
        TVector<TUnboxedValue> items;
        items.reserve(exports.size());

        for (const auto& exportInfo : exports) {
            TVector<TUnboxedValue> argNames;
            argNames.reserve(exportInfo.Signature.Params.size());
            for (const auto param : exportInfo.Signature.Params) {
                const auto paramName = TString(WasmValueTypeToString(param));
                argNames.push_back(valueBuilder->NewString(TStringRef(paramName)));
            }

            TUnboxedValue* structItems = nullptr;
            const auto describeStruct = valueBuilder->NewArray(4U, structItems);
            structItems[0] = valueBuilder->NewString(TStringRef(exportInfo.Name));
            structItems[1] = valueBuilder->NewList(argNames.data(), argNames.size());
            structItems[2] = valueBuilder->NewString(
                TStringRef(TString(WasmValueTypeToString(exportInfo.Signature.Result))));
            structItems[3] = TUnboxedValuePod(exportInfo.Signature.Supported);

            items.push_back(describeStruct);
        }

        return valueBuilder->NewList(items.data(), items.size());
    } catch (const std::exception& ex) {
        WasmError(ex, Name(), valueBuilder);
    }
    return {};
}

} // namespace NWasm::NYQL
