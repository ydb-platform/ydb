#include <ydb/services/udf_store/wasm/compartment_manager.h>
#include <ydb/services/udf_store/wasm/module_catalog.h>
#include <ydb/services/udf_store/wasm/query_compartment_scope.h>
#include <ydb/services/udf_store/wasm/registry_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NUdfStore::NWasm;
using namespace NYdb::NWasm;

namespace {

TWasmModuleArtifactPtr MakeDummyArtifact(const TString& moduleName) {
    auto artifact = std::make_shared<TWasmModuleArtifact>();
    artifact->ModuleName = moduleName;
    artifact->Manifest.ModuleName = moduleName;
    return artifact;
}

} // namespace

Y_UNIT_TEST_SUITE(TWasmCompartmentManagerTest) {
    Y_UNIT_TEST(CatalogRegisterAndResolve) {
        TWasmModuleCatalog catalog;
        catalog.Register(MakeDummyArtifact("ModA"));
        catalog.Register(MakeDummyArtifact("ModB"));

        auto resolved = catalog.ResolveModules({"ModA", "ModB"});
        UNIT_ASSERT_VALUES_EQUAL(resolved.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(resolved[0]->ModuleName, "ModA");
        UNIT_ASSERT_VALUES_EQUAL(resolved[1]->ModuleName, "ModB");

        catalog.Unregister("ModA");
        UNIT_ASSERT(!catalog.FindByModuleName("ModA"));
        UNIT_ASSERT(catalog.FindByModuleName("ModB"));
    }

    Y_UNIT_TEST(CatalogRegisterReplacesSameName) {
        TWasmModuleCatalog catalog;
        auto first = MakeDummyArtifact("Mod");
        auto second = MakeDummyArtifact("Mod");
        catalog.Register(first);
        catalog.Register(second);
        UNIT_ASSERT_EQUAL(catalog.FindByModuleName("Mod").get(), second.get());
        catalog.Unregister("Mod");
        UNIT_ASSERT(!catalog.FindByModuleName("Mod"));
    }

    Y_UNIT_TEST(ExportKey) {
        UNIT_ASSERT_VALUES_EQUAL(MakeExportKey("Mod", "Fn"), "Mod::Fn");
    }

    Y_UNIT_TEST(FilterLoadedWasmUdfModulesSkipsNative) {
        TWasmModuleCatalog catalog;
        catalog.Register(MakeDummyArtifact("ModA"));

        const auto filtered = FilterLoadedWasmUdfModules(
            {"String", "ModA", "Knn", "Missing"},
            catalog);
        UNIT_ASSERT_VALUES_EQUAL(filtered.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(filtered[0], "ModA");
    }

    Y_UNIT_TEST(WasmUdfModulesTaskParamRoundtrip) {
        const TVector<TString> modules = {"LocalUdf", "Trie", "Md5"};
        const TString encoded = SerializeWasmUdfModulesTaskParam(modules);
        UNIT_ASSERT_VALUES_EQUAL(encoded, "LocalUdf\nTrie\nMd5");
        UNIT_ASSERT_VALUES_EQUAL(ParseWasmUdfModulesTaskParam(encoded), modules);
        UNIT_ASSERT(ParseWasmUdfModulesTaskParam(SerializeWasmUdfModulesTaskParam({"Only"})).size() == 1);
        UNIT_ASSERT_VALUES_EQUAL(ParseWasmUdfModulesTaskParam(SerializeWasmUdfModulesTaskParam({"Only"}))[0], "Only");
    }

    Y_UNIT_TEST(QueryCompartmentTlsGuard) {
        TQueryCompartmentHandle handle;
        {
            TCurrentQueryCompartmentGuard guard(&handle);
            UNIT_ASSERT_EQUAL(GetCurrentQueryCompartment(), &handle);
        }
        UNIT_ASSERT_EQUAL(GetCurrentQueryCompartment(), nullptr);
    }
}
