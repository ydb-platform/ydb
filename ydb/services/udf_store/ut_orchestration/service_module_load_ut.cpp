#include "snapshot_helpers.h"

#include <ydb/core/kqp/common/dynamic_function_registry.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <ydb/services/metadata/request/common.h>
#include <ydb/services/udf_store/events.h>
#include <ydb/services/udf_store/service.h>
#include <ydb/services/udf_store/wasm/compile.h>
#include <ydb/services/udf_store/wasm/module_catalog.h>
#include <ydb/services/udf_store/wasm_artifact_load_actor.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/public/udf/udf_registrator.h>

namespace NKikimr::NUdfStore {
namespace {

constexpr TStringBuf Name = "ExistingModule";
constexpr TStringBuf Manifest = R"json({
    "module_type":"module", "module_kind":"wasm", "module_name":"ExistingModule",
    "module_extension":"wast", "service_abi_version":2,
    "service_methods":[{"name":"Lookup", "batch":false, "max_batch_rows":1,
        "input":[{"name":"id", "type":"Uint64"}],
        "output":[{"name":"value", "type":"Uint64"}], "max_output_row_bytes":8}]
})json";

class TExistingModule: public NUdf::IUdfModule {
public:
    void GetAllFunctions(NUdf::IFunctionsSink& sink) const override {
        sink.Add("ExistingFunction");
    }

    void BuildFunctionTypeInfo(
        const NUdf::TStringRef&, NUdf::TType*, const NUdf::TStringRef&, ui32, NUdf::IFunctionTypeInfoBuilder&) const override {
    }

    void CleanupOnTerminate() const override {
    }
};

Ydb::Table::ExecuteDataQueryResponse Response(Ydb::Table::ExecuteQueryResult&& result) {
    Ydb::Table::ExecuteDataQueryResponse response;
    response.mutable_operation()->set_status(Ydb::StatusIds::SUCCESS);
    response.mutable_operation()->mutable_result()->PackFrom(result);
    return response;
}

Ydb::Table::ExecuteDataQueryResponse ArtifactRow(size_t wasmSize, size_t objectSize) {
    Ydb::Table::ExecuteQueryResult result;
    auto& rows = *result.add_result_sets();
    auto& row = *rows.add_rows();
    const auto text = [&](TStringBuf name, TStringBuf value) {
        rows.add_columns()->set_name(TString(name));
        row.add_items()->set_text_value(TString(value));
    };
    const auto number = [&](TStringBuf name, ui64 value) {
        rows.add_columns()->set_name(TString(name));
        row.add_items()->set_uint64_value(value);
    };
    text("id", Name);
    text("format", "wast");
    number("wasm_data_size", wasmSize);
    number("wasm_data_chunk_count", 1);
    number("object_code_size", objectSize);
    number("object_code_chunk_count", 1);
    return Response(std::move(result));
}

Ydb::Table::ExecuteDataQueryResponse Chunk(TStringBuf bytes) {
    Ydb::Table::ExecuteQueryResult result;
    auto& rows = *result.add_result_sets();
    rows.add_columns()->set_name("chunk_idx");
    rows.add_columns()->set_name("data");
    auto& row = *rows.add_rows();
    row.add_items()->set_uint64_value(0);
    row.add_items()->set_bytes_value(TString(bytes));
    return Response(std::move(result));
}

}   // namespace

Y_UNIT_TEST_SUITE(TServiceModuleLoad) {
    void CheckRegistration(const TString& previousPath, bool mixed) {
        TTestBasicRuntime runtime(1, false);
        TAppPrepare app;
        app.AddDomain(TDomainsInfo::TDomain::ConstructDomainWithExplicitTabletIds(
            "dc-1", 0, 0, 100500, TVector<ui64>{}, TVector<ui64>{}, TVector<ui64>{}, DefaultPoolKinds(2))
                          .Release());
        SetupChannelProfiles(app);
        SetupTabletServices(runtime, &app, true);
        runtime.GetAppData(0).TenantName = "/dc-1";

        auto registry = NKqp::CreateDynamicFunctionRegistry(NMiniKQL::CreateBuiltinRegistry());
        registry->AddModule(previousPath, Name, new TExistingModule());
        const bool owned = previousPath == "wasm:ExistingModule";
        TString manifest(Manifest);
        if (mixed) {
            NJson::TJsonValue root;
            UNIT_ASSERT(NJson::ReadJsonTree(manifest, &root, true));
            NJson::TJsonValue function;
            UNIT_ASSERT(NJson::ReadJsonTree(R"({"name":"f","argument_types":[],"result_type":"Int64"})", &function, true));
            root["functions"].AppendValue(function);
            manifest = NJson::WriteJson(root, false);
        }
        UNIT_ASSERT(registry->IsLoadedUdfModule(Name));

        // The object code is produced by the same compiler as a persisted artifact.
        const TString wasm = "(module)";
        const TString object = NWasm::CompileModuleObjectCode(wasm, NYdb::NWasm::EBytecodeFormat::HumanReadable);
        UNIT_ASSERT(!object.empty());
        const auto edge = runtime.AllocateEdgeActor();
        const auto loader =
            runtime.Register(new TWasmArtifactLoadActor(edge, TString(Name), manifest, "uid", "/artifacts", "/chunks", {}, registry), 0);

        // Only YQL storage responses are replaced; the real load actor, artifact
        // decoding, module catalog and dynamic function registry all run.
        // Drop failures from the absent local KQP service while supplying the
        // artifact rows and chunks through the same response event.
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->Recipient == loader && ev->GetTypeRewrite() == NMetadata::NRequest::EvRequestFailed) {
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        using TResult = NMetadata::NRequest::TEvRequestResult<NMetadata::NRequest::TDialogYQLRequest>;
        runtime.Send(new IEventHandle(loader, edge, new TResult(ArtifactRow(wasm.size(), object.size()))), 0, true);
        runtime.Send(new IEventHandle(loader, edge, new TResult(Chunk(wasm))), 0, true);
        runtime.Send(new IEventHandle(loader, edge, new TResult(Chunk(object))), 0, true);
        auto response = runtime.GrabEdgeEventRethrow<TEvReadBodyResponse>(edge, TDuration::Seconds(10));
        UNIT_ASSERT(response);
        NWasm::GetWasmModuleCatalog().Unregister(TString(Name));
        UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Success, !mixed || owned, response->Get()->ErrorMessage);
        UNIT_ASSERT_VALUES_EQUAL(registry->IsLoadedUdfModule(Name), mixed || !owned);
        if (!owned) {
            UNIT_ASSERT_VALUES_EQUAL(*registry->FindUdfPath(Name), previousPath);
            UNIT_ASSERT(registry->GetModuleFunctions(Name).contains("ExistingFunction"));
        } else if (mixed) {
            UNIT_ASSERT(registry->GetModuleFunctions(Name).contains("f"));
            UNIT_ASSERT(!registry->GetModuleFunctions(Name).contains("ExistingFunction"));
        }

        // Drive the real service's update and unload paths. An unrelated entry
        // must survive both; the module's own synchronous entry must be removed.
        if (mixed && !owned) {
            return;
        }
        NKikimrConfig::TUdfStoreConfig config;
        config.SetEnabled(true);
        config.SetEnableWasmUdf(true);
        config.SetWasmCpuSpecOverride("test-cpu-spec");
        const auto service = runtime.Register(CreateService(config, registry), 0);
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->Recipient == service && ev->GetTypeRewrite() == TEvStoreInitFailed::EventType) {
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        auto snapshot = [&](const TVector<TModuleDesc>& modules) {
            runtime.Send(new IEventHandle(service, edge, new NMetadata::NProvider::TEvRefreshSubscriberData(MakeSnapshot(modules))), 0, true);
            runtime.SimulateSleep(TDuration::MilliSeconds(100));
        };
        snapshot({ { .Name = TString(Name), .Uid = "v1", .Manifest = manifest } });
        snapshot({ { .Name = TString(Name), .Uid = "v2", .Manifest = manifest } });
        UNIT_ASSERT_VALUES_EQUAL(registry->IsLoadedUdfModule(Name), !owned);
        if (owned) {
            // Model the replacement finishing before a later metadata deletion.
            registry->AddModule(previousPath, Name, new TExistingModule());
        }
        snapshot({});
        UNIT_ASSERT_VALUES_EQUAL(registry->IsLoadedUdfModule(Name), !owned);
        if (!owned) {
            UNIT_ASSERT_VALUES_EQUAL(*registry->FindUdfPath(Name), previousPath);
            UNIT_ASSERT(registry->GetModuleFunctions(Name).contains("ExistingFunction"));
        }
    }
    Y_UNIT_TEST(ServiceOnlyModuleDoesNotRemoveAnUnrelatedUdf) {
        CheckRegistration("/native/existing.so", false);
        CheckRegistration("", false);
    }

    Y_UNIT_TEST(ServiceOnlyModuleRemovesOwnedSynchronousRegistration) {
        CheckRegistration("wasm:ExistingModule", false);
    }

    Y_UNIT_TEST(MixedModulePreservesForeignRegistrations) {
        CheckRegistration("/native/existing.so", true);
        CheckRegistration("", true);
    }

    Y_UNIT_TEST(MixedModuleReplacesOwnedRegistration) {
        CheckRegistration("wasm:ExistingModule", true);
    }
}   // Y_UNIT_TEST_SUITE(TServiceModuleLoad)

}   // namespace NKikimr::NUdfStore
