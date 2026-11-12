#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/public/udf/udf_static_library.h>
#include <yql/essentials/public/udf/udf_static_registry.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/scope.h>

extern "C" const NYql::NUdf::TYqlUdfStaticLibrary* YQL_UDF_STATIC_ENTRY_POINT();

namespace {
const TVector<const NYql::NUdf::TYqlUdfStaticLibrary*>* ExternalLibraries = nullptr;

const NYql::NUdf::TYqlUdfStaticLibrary* const* GetExternalLibraries(size_t* count) {
    const auto& libraries = ExternalLibraries ? *ExternalLibraries : NYql::NUdf::GetStaticUdfLibraryList();
    *count = libraries.size();
    return libraries.data();
}
} // namespace

Y_UNIT_TEST_SUITE(TInternalizedStringUdf) {
Y_UNIT_TEST(ExternalStaticLibrariesOverrideLocalRegistry) {
    TVector<const NYql::NUdf::TYqlUdfStaticLibrary*> libraries{YQL_UDF_STATIC_ENTRY_POINT()};
    ExternalLibraries = &libraries;
    Y_DEFER {
        ExternalLibraries = nullptr;
    };
    NKikimr::NMiniKQL::SetStaticUdfLibrariesGetter(&GetExternalLibraries);
    NKikimr::NMiniKQL::SetStaticUdfLibrariesGetter(&GetExternalLibraries);
    auto registry = NKikimr::NMiniKQL::CreateFunctionRegistry({})->Clone();
    NKikimr::NMiniKQL::FillStaticModules(*registry);
    UNIT_ASSERT(registry->IsLoadedUdfModule("String"));
    UNIT_ASSERT_C(!registry->IsLoadedUdfModule("Math"), "Local Math UDF must not be loaded when a host callback is set");
    NKikimr::NMiniKQL::FillStaticModules(*registry, /*skipLoaded=*/true);
    UNIT_ASSERT(registry->GetModuleFunctions("String").contains("HexEncode"));
    libraries.clear();
    auto nextRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry({})->Clone();
    NKikimr::NMiniKQL::FillStaticModules(*nextRegistry);
    UNIT_ASSERT_C(nextRegistry->GetAllModuleNames().empty(), "Each fill must read the current host list");
}

Y_UNIT_TEST(EmptyExternalStaticLibrariesOverrideLocalRegistry) {
    const TVector<const NYql::NUdf::TYqlUdfStaticLibrary*> libraries;
    ExternalLibraries = &libraries;
    Y_DEFER {
        ExternalLibraries = nullptr;
    };
    NKikimr::NMiniKQL::SetStaticUdfLibrariesGetter(&GetExternalLibraries);
    auto registry = NKikimr::NMiniKQL::CreateFunctionRegistry({})->Clone();
    NKikimr::NMiniKQL::FillStaticModules(*registry);
    UNIT_ASSERT_C(registry->GetAllModuleNames().empty(), "An empty host list must not fall back to local UDFs");
}

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
