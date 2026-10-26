#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/public/udf/udf_static_library.h>
#include <yql/essentials/public/udf/udf_static_registry.h>

#include <library/cpp/testing/unittest/registar.h>

extern "C" const NYql::NUdf::TYqlUdfStaticLibrary* YQL_UDF_STATIC_ENTRY_POINT();

Y_UNIT_TEST_SUITE(TInternalizedStringUdf) {
Y_UNIT_TEST(StableDescriptor) {
    const auto* library = YQL_UDF_STATIC_ENTRY_POINT();
    UNIT_ASSERT(library);
    UNIT_ASSERT_VALUES_EQUAL(library->StructSize, sizeof(*library));
    UNIT_ASSERT_VALUES_EQUAL(library->AbiVersion, NYql::NUdf::MakeAbiVersion(
                                                      STABLE_UDF_ABI_VERSION_MAJOR, STABLE_UDF_ABI_VERSION_MINOR, STABLE_UDF_ABI_VERSION_PATCH));
    UNIT_ASSERT(library->Register);
    UNIT_ASSERT(library->SetBackTraceCallback);
}

Y_UNIT_TEST(CachedAndSourceModulesRegisterInCurrentHost) {
    auto registry = NKikimr::NMiniKQL::CreateFunctionRegistry({})->Clone();
    NKikimr::NMiniKQL::FillStaticModules(*registry);
    UNIT_ASSERT(registry->IsLoadedUdfModule("String"));
    UNIT_ASSERT(registry->IsLoadedUdfModule("Math"));
    UNIT_ASSERT(registry->GetModuleFunctions("String").contains("HexEncode"));
    UNIT_ASSERT(registry->GetModuleFunctions("Math").contains("Pi"));
}

Y_UNIT_TEST(CachedAndSourceDescriptorsShareRegistry) {
    const auto* cached = YQL_UDF_STATIC_ENTRY_POINT();
    bool foundCached = false;
    bool foundSource = false;
    for (const auto* library : NYql::NUdf::GetStaticUdfLibraryList()) {
        foundCached |= library == cached;
        foundSource |= library->AbiVersion == NYql::NUdf::CurrentAbiVersion();
    }
    UNIT_ASSERT(foundCached);
    UNIT_ASSERT(foundSource);
}

Y_UNIT_TEST(SkipAlreadyLoadedModules) {
    auto registry = NKikimr::NMiniKQL::CreateFunctionRegistry({})->Clone();
    NKikimr::NMiniKQL::FillStaticModules(*registry);
    const auto modules = registry->GetAllModuleNames();
    NKikimr::NMiniKQL::FillStaticModules(*registry, /*skipLoaded=*/true);
    UNIT_ASSERT(registry->GetAllModuleNames() == modules);
    UNIT_ASSERT(registry->GetModuleFunctions("String").contains("HexEncode"));
    UNIT_ASSERT(registry->GetModuleFunctions("Math").contains("Pi"));
}
} // Y_UNIT_TEST_SUITE(TInternalizedStringUdf)
