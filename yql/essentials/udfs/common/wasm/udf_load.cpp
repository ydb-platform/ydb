#include "udf_load.hpp"

#include "wasm_state.hpp"

#include <yql/essentials/public/udf/udf_type_builder.h>

namespace NWasm::NYQL {

using namespace NYql::NUdf;

TStringRef TLoadUdfs::Name()
{
    static auto name = TStringRef::Of("LoadUdfs");
    return name;
}

TLoadUdfs::TLoadUdfs(ui32 describeIdx, ui32 runIdx)
    : DescribeIdx_(describeIdx)
    , RunIdx_(runIdx)
{
}

void TLoadUdfs::Register(IFunctionTypeInfoBuilder& builder, bool typesOnly)
{
    ui32 describeIdx = 0;
    ui32 runIdx = 0;

    const auto describeType = TUdfRegistryDescribe::BuildFunctionType(builder);
    const auto runType = TUdfRegistryRun::BuildFunctionType(builder);

    const auto outStructType = builder.Struct(2U)
        ->AddField(TUdfRegistryDescribe::Name(), describeType, &describeIdx)
        .AddField(TUdfRegistryRun::Name(), runType, &runIdx)
        .Build();

    builder
        .Returns(outStructType)
        .Args()
        ->Add<char*>()
        .Name("RegistryPath");

    if (!typesOnly) {
        builder.Implementation(new TLoadUdfs(describeIdx, runIdx));
    }
}

TUnboxedValue TLoadUdfs::Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const
{
    try {
        const TString path(args[0].AsStringRef());
        const auto state = LoadWasmUdfRegistry(path);

        TUnboxedValue* structItems = nullptr;
        const auto wasmMethods = valueBuilder->NewArray(2U, structItems);
        structItems[DescribeIdx_] = TUnboxedValuePod(new TUdfRegistryDescribe(state));
        structItems[RunIdx_] = TUnboxedValuePod(new TUdfRegistryRun(state));
        return wasmMethods;
    } catch (const std::exception& ex) {
        WasmError(ex, Name(), valueBuilder);
    }
    return {};
}

} // namespace NWasm::NYQL
