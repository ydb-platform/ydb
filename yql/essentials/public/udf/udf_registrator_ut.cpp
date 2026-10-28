#include "udf_registrator.h"
#include "udf_static_registry.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/yexception.h>

#include <cstddef>
#include <type_traits>

namespace NYql::NUdf {

namespace {

ui32 CreatedModules = 0;

template <ui32 Index>
class TRegistryTestModule final: public IUdfModule {
public:
    TRegistryTestModule() {
        ++CreatedModules;
    }

    TStringRef Name() const {
        return Index == 0 ? TStringRef::Of("First") : TStringRef::Of("Second");
    }

    void GetAllFunctions(IFunctionsSink&) const override {
    }
    void BuildFunctionTypeInfo(const TStringRef&, TType*, const TStringRef&, ui32, IFunctionTypeInfoBuilder&) const override {
    }
    void CleanupOnTerminate() const override {
    }
};

class TCollectModules final: public IRegistrator {
public:
    TVector<TStringRef> Names;

    void AddModule(const TStringRef& name, TUniquePtr<IUdfModule> module) override {
        UNIT_ASSERT(module);
        Names.push_back(name);
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TUdfRegistrator) {
Y_UNIT_TEST(StaticDescriptorRetainsLazyModuleFactories) {
    const auto& libraries = GetStaticUdfLibraryList();
    const auto previousSize = libraries.size();
    const auto previousCreated = CreatedModules;
    AddToStaticUdfRegistry<TRegistryTestModule<0>, TRegistryTestModule<1>>();
    UNIT_ASSERT_VALUES_EQUAL(libraries.size(), previousSize + 1);
    UNIT_ASSERT_VALUES_EQUAL(CreatedModules, previousCreated);
    const auto* library = libraries.back();
    UNIT_ASSERT_VALUES_EQUAL(library->StructSize, sizeof(*library));
    UNIT_ASSERT_VALUES_EQUAL(library->AbiVersion, CurrentAbiVersion());
    UNIT_ASSERT(!library->SetBackTraceCallback);

    AddToStaticUdfRegistry<TRegistryTestModule<0>, TRegistryTestModule<1>>();
    UNIT_ASSERT_VALUES_EQUAL(libraries.back(), library);
    TCollectModules registrator;
    library->Register(registrator, 0);
    UNIT_ASSERT_VALUES_EQUAL(CreatedModules, previousCreated + 2);
    UNIT_ASSERT_VALUES_EQUAL(registrator.Names.size(), 2);
    UNIT_ASSERT(registrator.Names[0] == TStringRef::Of("First"));
    UNIT_ASSERT(registrator.Names[1] == TStringRef::Of("Second"));
}

Y_UNIT_TEST(RejectInvalidStaticDescriptors) {
    static constexpr TYqlUdfStaticLibrary TooSmall{};
    static constexpr TYqlUdfStaticLibrary MissingRegister{
        .StructSize = sizeof(TYqlUdfStaticLibrary),
        .AbiVersion = CurrentAbiVersion(),
    };
    const auto size = GetStaticUdfLibraryList().size();
    UNIT_ASSERT_EXCEPTION(AddToStaticUdfRegistry(nullptr), yexception);
    UNIT_ASSERT_EXCEPTION(AddToStaticUdfRegistry(&TooSmall), yexception);
    UNIT_ASSERT_EXCEPTION(AddToStaticUdfRegistry(&MissingRegister), yexception);
    UNIT_ASSERT_VALUES_EQUAL(GetStaticUdfLibraryList().size(), size);
}

Y_UNIT_TEST(LockStaticSymbolsLayout) {
    static_assert(std::is_same_v<decltype(TStaticSymbols::Reserved1), void* (*)(ui64)>);
    static_assert(std::is_same_v<decltype(TStaticSymbols::Reserved2), void (*)(const void*)>);
    UNIT_ASSERT_VALUES_EQUAL(offsetof(TStaticSymbols, Reserved1), 0);
    UNIT_ASSERT_VALUES_EQUAL(offsetof(TStaticSymbols, Reserved2), sizeof(void*));
    UNIT_ASSERT_VALUES_EQUAL(offsetof(TStaticSymbols, UdfTerminate), 2 * sizeof(void*));
    UNIT_ASSERT_VALUES_EQUAL(offsetof(TStaticSymbols, UdfRegisterObject), 3 * sizeof(void*));
    UNIT_ASSERT_VALUES_EQUAL(offsetof(TStaticSymbols, UdfUnregisterObject), 4 * sizeof(void*));
    UNIT_ASSERT_VALUES_EQUAL(offsetof(TStaticSymbols, UdfAllocateWithSizeFunc), 5 * sizeof(void*));
    UNIT_ASSERT_VALUES_EQUAL(offsetof(TStaticSymbols, UdfFreeWithSizeFunc), 6 * sizeof(void*));
    UNIT_ASSERT_VALUES_EQUAL(offsetof(TStaticSymbols, UdfArrowAllocateFunc), 7 * sizeof(void*));
    UNIT_ASSERT_VALUES_EQUAL(offsetof(TStaticSymbols, UdfArrowReallocateFunc), 8 * sizeof(void*));
    UNIT_ASSERT_VALUES_EQUAL(offsetof(TStaticSymbols, UdfArrowFreeFunc), 9 * sizeof(void*));
    UNIT_ASSERT_VALUES_EQUAL(sizeof(TStaticSymbols), 10 * sizeof(void*));

    const auto symbols = GetStaticSymbols();
    UNIT_ASSERT(!symbols.Reserved1);
    UNIT_ASSERT(!symbols.Reserved2);
}

} // Y_UNIT_TEST_SUITE(TUdfRegistrator)

} // namespace NYql::NUdf
