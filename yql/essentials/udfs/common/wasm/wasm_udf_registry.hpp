#pragma once

#include <yql/essentials/public/udf/udf_helpers.h>

#include <ydb/library/wasm/api/compartment.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <memory>

namespace NWasm::NYQL {

using namespace NYql::NUdf;

enum class EUdfValueType {
    Null,
    Int64,
    Uint64,
    Double,
    Boolean,
    String,
};

struct TWasmUdfDescriptor
{
    TString Name;
    TVector<EUdfValueType> Args;
    EUdfValueType Result = EUdfValueType::Null;
};

struct TWasmUdfRegistryState
{
    std::unique_ptr<NYdb::NWasm::IWebAssemblyCompartment> Compartment;
    THashMap<TString, TWasmUdfDescriptor> Functions;
};

using TWasmUdfRegistryStatePtr = std::shared_ptr<TWasmUdfRegistryState>;

TWasmUdfRegistryStatePtr LoadWasmUdfRegistry(const TString& path);

TVector<TWasmUdfDescriptor> ListWasmUdfDescriptors(const TWasmUdfRegistryStatePtr& state);

TString InvokeWasmUdfJson(
    const TWasmUdfRegistryStatePtr& state,
    const TString& functionName,
    TStringBuf argsJson);

class TUdfRegistryDescribe: public TBoxedValue
{
public:
    static TStringRef Name();
    static TType* BuildFunctionType(IFunctionTypeInfoBuilder& builder);

    explicit TUdfRegistryDescribe(TWasmUdfRegistryStatePtr state);

private:
    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const override;

    TWasmUdfRegistryStatePtr State_;
};

class TUdfRegistryRun: public TBoxedValue
{
public:
    static TStringRef Name();
    static TType* BuildFunctionType(IFunctionTypeInfoBuilder& builder);

    explicit TUdfRegistryRun(TWasmUdfRegistryStatePtr state);

private:
    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const override;

    TWasmUdfRegistryStatePtr State_;
};

} // namespace NWasm::NYQL
