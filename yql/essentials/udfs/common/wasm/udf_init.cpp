#include "udf_init.hpp"

#include "wasm_describe.hpp"
#include "wasm_run.hpp"

#include <yql/essentials/public/udf/udf_type_builder.h>

namespace NWasm::NYQL {

using namespace NYql::NUdf;

TStringRef TInit::Name()
{
    static auto name = TStringRef::Of("Init");
    return name;
}

TInit::TInit(ui32 describeIdx, ui32 runIdx)
    : DescribeIdx_(describeIdx)
    , RunIdx_(runIdx)
{
}

void TInit::Register(IFunctionTypeInfoBuilder& builder, bool typesOnly)
{
    ui32 describeIdx = 0;
    ui32 runIdx = 0;

    const auto describeType = TDescribe::BuildFunctionType(builder);
    const auto runType = TRun::BuildFunctionType(builder);

    const auto outStructType = builder.Struct(2U)
        ->AddField(TDescribe::Name(), describeType, &describeIdx)
        .AddField(TRun::Name(), runType, &runIdx)
        .Build();

    builder
        .Returns(outStructType)
        .Args()
        ->Add<char*>()
        .Name("WasmPath");

    if (!typesOnly) {
        builder.Implementation(new TInit(describeIdx, runIdx));
    }
}

TUnboxedValue TInit::Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const
{
    try {
        const TString path(args[0].AsStringRef());
        const auto state = LoadWasmModule(path);

        TUnboxedValue* structItems = nullptr;
        const auto wasmMethods = valueBuilder->NewArray(2U, structItems);
        structItems[DescribeIdx_] = TUnboxedValuePod(new TDescribe(state));
        structItems[RunIdx_] = TUnboxedValuePod(new TRun(state));
        return wasmMethods;
    } catch (const std::exception& ex) {
        WasmError(ex, Name(), valueBuilder);
    }
    return {};
}

} // namespace NWasm::NYQL
