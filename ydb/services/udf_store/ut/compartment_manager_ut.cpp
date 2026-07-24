#include <ydb/services/udf_store/wasm/compartment_manager.h>
#include <ydb/services/udf_store/wasm/module_catalog.h>
#include <ydb/services/udf_store/wasm/registry_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NUdfStore::NWasm;
using namespace NYdb::NWasm;

namespace {

TWasmModuleArtifactPtr MakeDummyArtifact(const TString& md5, const TString& moduleName) {
    auto artifact = std::make_shared<TWasmModuleArtifact>();
    artifact->Md5 = md5;
    artifact->ModuleName = moduleName;
    artifact->Manifest.ModuleName = moduleName;
    return artifact;
}

} // namespace

Y_UNIT_TEST_SUITE(TWasmCompartmentManagerTest) {
    Y_UNIT_TEST(CatalogRegisterAndResolve) {
        TWasmModuleCatalog catalog;
        catalog.Register(MakeDummyArtifact("md5-a", "ModA"));
        catalog.Register(MakeDummyArtifact("md5-b", "ModB"));

        auto resolved = catalog.ResolveModules({"ModA", "ModB"});
        UNIT_ASSERT_VALUES_EQUAL(resolved.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(resolved[0]->Md5, "md5-a");
        UNIT_ASSERT_VALUES_EQUAL(resolved[1]->Md5, "md5-b");

        catalog.Unregister("md5-a");
        UNIT_ASSERT(!catalog.FindByModuleName("ModA"));
        UNIT_ASSERT(catalog.FindByModuleName("ModB"));
    }

    Y_UNIT_TEST(ExportKey) {
        UNIT_ASSERT_VALUES_EQUAL(MakeExportKey("Mod", "Fn"), "Mod::Fn");
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
