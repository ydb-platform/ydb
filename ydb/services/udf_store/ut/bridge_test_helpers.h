#pragma once

#include <ydb/services/udf_store/wasm/bridge_node_table.h>
#include <ydb/services/udf_store/wasm/bridge_resident.h>
#include <ydb/services/udf_store/wasm/compartment_manager.h>
#include <ydb/services/udf_store/wasm/compile.h>
#include <ydb/services/udf_store/wasm/host.h>
#include <ydb/services/udf_store/wasm/manifest.h>
#include <ydb/services/udf_store/wasm/registry_helpers.h>
#include <ydb/services/udf_store/wasm/udf_function.h>
#include <ydb/services/udf_store/wasm/udf_configured_callable.h>
#include <yql/essentials/minikql/computation/mkql_value_builder.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/public/udf/udf_type_printer.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NUdfStore::NWasm::NTest {
using namespace NYql::NUdf;
using namespace NKikimr::NMiniKQL;
using namespace NYdb::NWasm;

struct TBridgeEnv {
    TScopedAlloc Alloc{__LOCATION__};
    TTypeEnvironment Env{Alloc};
    TMemoryUsageInfo MemInfo{"bridge_runtime_ut"};
    THolderFactory HolderFactory{Alloc.Ref(), MemInfo};
    TDefaultValueBuilder ValueBuilder{HolderFactory};
    ITypeInfoHelper::TPtr TypeHelper{new TTypeInfoHelper()};
    NYql::TRuntimeSettings::TConstPtr Settings{NYql::MakeRuntimeSettings()};
    TQueryCompartmentHandle Query;
    TCurrentQueryCompartmentGuard Guard{&Query};
    TWasmCompartmentStatePtr State = std::make_shared<TWasmCompartmentState>();

    TBridgeEnv() {
        EnsureUdfHostIntrinsicsRegistered();
        Query.Generation = 1;
        Query.BridgeNodes = std::make_unique<TWasmBridgeNodeTable>(Query.Generation);
        Query.Compartment = CreateRegistryCompartment({});
        State->ModuleName = "Test";
    }

    std::unique_ptr<TFunctionTypeInfoBuilder> Builder() {
        return std::make_unique<TFunctionTypeInfoBuilder>(NYql::UnknownLangVersion, *Settings,
            Env, TypeHelper, "Test", nullptr, TSourcePosition());
    }

    void AddModule(TStringBuf wat, std::initializer_list<TStringBuf> exports) {
        const auto object = CompileModuleObjectCode(wat, EBytecodeFormat::HumanReadable);
        AddPrecompiledModule(Query.Compartment.get(), MakeModuleBytecode(wat, object, EBytecodeFormat::HumanReadable), "Test");
        for (auto name : exports) {
            Query.Exports[MakeExportKey("Test", name)] = Query.Compartment->GetFunction(std::string(name));
        }
    }

    TWasmTypeNodePtr Type(TStringBuf text) {
        return ParseManifest(TStringBuilder() << R"({"module_name":"Test","module_type":"module","module_kind":"wasm",
            "functions":[{"name":"f","result_type":)" << NJson::WriteJson(NJson::TJsonValue(text)) << "}]}").Functions[0].ResultType;
    }

    std::unique_ptr<TWasmBridgeFunction> Function(TStringBuf name, TVector<TWasmTypeNodePtr> args, TWasmTypeNodePtr result) {
        TWasmUdfDescriptor desc;
        desc.Name = name;
        desc.ArgTypes = std::move(args);
        desc.ResultType = std::move(result);
        auto builder = Builder();
        return TWasmBridgeFunction::Create(*builder, State, desc);
    }
};
}
