#include <ydb/core/kqp/common/dynamic_function_registry.h>

#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/mkql_utils.h>
#include <yql/essentials/minikql/runtime_settings/runtime_settings.h>
#include <yql/essentials/public/udf/udf_registrator.h>
#include <yql/essentials/public/udf/udf_type_builder.h>
#include <yql/essentials/public/udf/udf_value.h>

#include <util/stream/str.h>

#include <atomic>
#include <thread>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;
using namespace NKikimr::NKqp;

namespace {

class TStubUdfModule: public NUdf::IUdfModule {
public:
    explicit TStubUdfModule(
        TString functionName = "Func",
        bool typeAwareness = false,
        TMaybe<TString> polyArgs = Nothing(),
        ui32* cleanupCounter = nullptr)
        : FunctionName_(std::move(functionName))
        , TypeAwareness_(typeAwareness)
        , PolyArgs_(std::move(polyArgs))
        , CleanupCounter_(cleanupCounter)
    {
    }

    void GetAllFunctions(NUdf::IFunctionsSink& sink) const override {
        auto desc = sink.Add(FunctionName_);
        if (TypeAwareness_) {
            desc->SetTypeAwareness();
        }
        if (PolyArgs_) {
            desc->SetPolyArgs(*PolyArgs_);
        }
    }

    void BuildFunctionTypeInfo(
        const NUdf::TStringRef& name,
        NUdf::TType* /*userType*/,
        const NUdf::TStringRef& /*typeConfig*/,
        ui32 /*flags*/,
        NUdf::IFunctionTypeInfoBuilder& builder) const override
    {
        if (name == NUdf::TStringRef(FunctionName_)) {
            builder.SimpleSignature<i32(i32)>();
        }
    }

    void CleanupOnTerminate() const override {
        if (CleanupCounter_) {
            ++(*CleanupCounter_);
        }
    }

private:
    const TString FunctionName_;
    const bool TypeAwareness_;
    const TMaybe<TString> PolyArgs_;
    ui32* CleanupCounter_;
};

TIntrusivePtr<IMutableFunctionRegistry> MakeRegistry() {
    auto registry = CreateDynamicFunctionRegistry(CreateBuiltinRegistry());
    UNIT_ASSERT(AsDynamicFunctionRegistry(registry.Get()));
    return registry;
}

IDynamicFunctionRegistry* Dyn(IMutableFunctionRegistry* registry) {
    auto* dyn = AsDynamicFunctionRegistry(registry);
    UNIT_ASSERT(dyn);
    return dyn;
}

TStatus LookupTypeInfo(
    const IFunctionRegistry& registry,
    const TTypeEnvironment& env,
    NUdf::ITypeInfoHelper::TPtr typeInfoHelper,
    const NYql::TRuntimeSettings& runtimeSettings,
    const TStringBuf& name,
    TFunctionTypeInfo* funcInfo)
{
    return registry.FindFunctionTypeInfo(
        NYql::UnknownLangVersion,
        runtimeSettings,
        env,
        typeInfoHelper,
        /*countersProvider=*/nullptr,
        name,
        /*userType=*/nullptr,
        /*typeConfig=*/TStringBuf(),
        NUdf::IUdfModule::TFlags::TypesOnly,
        NUdf::TSourcePosition(),
        /*secureParamsProvider=*/nullptr,
        /*logProvider=*/nullptr,
        funcInfo);
}

} // namespace

Y_UNIT_TEST_SUITE(TDynamicFunctionRegistryTest) {

Y_UNIT_TEST(RemoveModuleRemovesKnownModule) {
    auto registry = MakeRegistry();
    registry->AddModule("/lib/foo", "Foo", new TStubUdfModule());

    UNIT_ASSERT(registry->IsLoadedUdfModule("Foo"));
    UNIT_ASSERT(registry->GetAllModuleNames().contains("Foo"));
    UNIT_ASSERT(registry->FindUdfPath("Foo").Defined());
    UNIT_ASSERT_VALUES_EQUAL(*registry->FindUdfPath("Foo"), "/lib/foo");

    Dyn(registry.Get())->RemoveModule("Foo");

    UNIT_ASSERT(!registry->IsLoadedUdfModule("Foo"));
    UNIT_ASSERT(!registry->GetAllModuleNames().contains("Foo"));
    UNIT_ASSERT(!registry->FindUdfPath("Foo").Defined());
}

Y_UNIT_TEST(RemoveModuleMissingIsNoOp) {
    auto registry = MakeRegistry();
    registry->AddModule("/lib/foo", "Foo", new TStubUdfModule());

    Dyn(registry.Get())->RemoveModule("Missing");
    Dyn(registry.Get())->RemoveModule("Missing");

    UNIT_ASSERT(registry->IsLoadedUdfModule("Foo"));
    UNIT_ASSERT_VALUES_EQUAL(*registry->FindUdfPath("Foo"), "/lib/foo");
}

Y_UNIT_TEST(RemoveModuleDropsLibraryEntryWhenLastModule) {
    auto registry = MakeRegistry();
    const TString path = "/lib/shared";
    registry->AddModule(path, "Only", new TStubUdfModule());

    Dyn(registry.Get())->RemoveModule("Only");
    UNIT_ASSERT(!registry->IsLoadedUdfModule("Only"));

    // Path must have been dropped from LoadedLibraries_: re-add under the same
    // path must succeed and reappear in FindUdfPath.
    registry->AddModule(path, "Again", new TStubUdfModule("AgainFunc"));
    UNIT_ASSERT(registry->IsLoadedUdfModule("Again"));
    UNIT_ASSERT_VALUES_EQUAL(*registry->FindUdfPath("Again"), path);
}

Y_UNIT_TEST(RemoveModuleKeepsLibraryEntryWhenOtherModulesRemain) {
    auto registry = MakeRegistry();
    const TString path = "/lib/shared";
    registry->AddModule(path, "ModA", new TStubUdfModule("A"));
    registry->AddModule(path, "ModB", new TStubUdfModule("B"));

    Dyn(registry.Get())->RemoveModule("ModA");

    UNIT_ASSERT(!registry->IsLoadedUdfModule("ModA"));
    UNIT_ASSERT(registry->IsLoadedUdfModule("ModB"));
    UNIT_ASSERT_VALUES_EQUAL(*registry->FindUdfPath("ModB"), path);

    // Path still tracked: can register another module under the same path.
    registry->AddModule(path, "ModC", new TStubUdfModule("C"));
    UNIT_ASSERT(registry->IsLoadedUdfModule("ModC"));
    UNIT_ASSERT_VALUES_EQUAL(*registry->FindUdfPath("ModC"), path);
}

Y_UNIT_TEST(CloneIsIndependent) {
    auto registry = MakeRegistry();
    registry->AddModule("/lib/foo", "Foo", new TStubUdfModule());

    auto clone = registry->Clone();
    UNIT_ASSERT(AsDynamicFunctionRegistry(clone.Get()));

    UNIT_ASSERT(clone->IsLoadedUdfModule("Foo"));
    UNIT_ASSERT_VALUES_EQUAL(*clone->FindUdfPath("Foo"), "/lib/foo");

    Dyn(registry.Get())->RemoveModule("Foo");
    UNIT_ASSERT(!registry->IsLoadedUdfModule("Foo"));
    UNIT_ASSERT(clone->IsLoadedUdfModule("Foo"));
    UNIT_ASSERT_VALUES_EQUAL(*clone->FindUdfPath("Foo"), "/lib/foo");

    Dyn(clone.Get())->RemoveModule("Foo");
    UNIT_ASSERT(!clone->IsLoadedUdfModule("Foo"));
}

Y_UNIT_TEST(FindFunctionTypeInfoResolvesRegisteredUdf) {
    auto registry = MakeRegistry();
    registry->AddModule("/lib/foo", "Foo", new TStubUdfModule("Bar"));

    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    NUdf::ITypeInfoHelper::TPtr typeInfoHelper(new TTypeInfoHelper);
    auto runtimeSettings = NYql::MakeRuntimeSettings();

    TFunctionTypeInfo funcInfo;
    auto ok = LookupTypeInfo(*registry, env, typeInfoHelper, *runtimeSettings, "Foo.Bar", &funcInfo);
    UNIT_ASSERT_C(ok.IsOk(), ok.GetError());
    UNIT_ASSERT(funcInfo.FunctionType != nullptr);

    TFunctionTypeInfo missingInfo;
    auto missing = LookupTypeInfo(*registry, env, typeInfoHelper, *runtimeSettings, "Foo.Missing", &missingInfo);
    UNIT_ASSERT(!missing.IsOk());

    TFunctionTypeInfo noModuleInfo;
    auto noModule = LookupTypeInfo(*registry, env, typeInfoHelper, *runtimeSettings, "Unknown.Bar", &noModuleInfo);
    UNIT_ASSERT(!noModule.IsOk());
}

// --- IMutableFunctionRegistry surface (parity with TMutableFunctionRegistry usage) ---

Y_UNIT_TEST(GetBuiltinsAndSupportsSizedAllocators) {
    auto builtins = CreateBuiltinRegistry();
    auto* builtinsRaw = builtins.Get();
    auto registry = CreateDynamicFunctionRegistry(std::move(builtins));

    UNIT_ASSERT(registry->GetBuiltins().Get() == builtinsRaw);
    UNIT_ASSERT(registry->SupportsSizedAllocators());
}

Y_UNIT_TEST(GetModuleFunctionsExposesRegisteredMetadata) {
    auto registry = MakeRegistry();
    registry->AddModule(
        "/lib/foo",
        "Foo",
        new TStubUdfModule("Bar", /*typeAwareness=*/true, TString("poly")));

    auto funcs = registry->GetModuleFunctions("Foo");
    UNIT_ASSERT_VALUES_EQUAL(funcs.size(), 1u);
    UNIT_ASSERT(funcs.contains("Bar"));
    UNIT_ASSERT(funcs.at("Bar").IsTypeAwareness);
    UNIT_ASSERT(funcs.at("Bar").PolyArgs.Defined());
    UNIT_ASSERT_VALUES_EQUAL(*funcs.at("Bar").PolyArgs, "poly");

    UNIT_ASSERT(registry->GetModuleFunctions("Missing").empty());
}

Y_UNIT_TEST(SetSystemModulePathsUsedByFindUdfPath) {
    auto registry = MakeRegistry();
    registry->SetSystemModulePaths({{"Sys", "/system/sys.so"}});

    UNIT_ASSERT(!registry->IsLoadedUdfModule("Sys"));
    UNIT_ASSERT(registry->FindUdfPath("Sys").Defined());
    UNIT_ASSERT_VALUES_EQUAL(*registry->FindUdfPath("Sys"), "/system/sys.so");

    // Loaded module path takes precedence over system path.
    registry->AddModule("/lib/sys", "Sys", new TStubUdfModule());
    UNIT_ASSERT(registry->IsLoadedUdfModule("Sys"));
    UNIT_ASSERT_VALUES_EQUAL(*registry->FindUdfPath("Sys"), "/lib/sys");

    // RemoveModule unloads the dynamic module but keeps the system catalog entry.
    Dyn(registry.Get())->RemoveModule("Sys");
    UNIT_ASSERT(!registry->IsLoadedUdfModule("Sys"));
    UNIT_ASSERT_VALUES_EQUAL(*registry->FindUdfPath("Sys"), "/system/sys.so");
}

Y_UNIT_TEST(CleanupModulesOnTerminateInvokesModules) {
    ui32 cleanups = 0;
    auto registry = MakeRegistry();
    registry->AddModule("/lib/a", "A", new TStubUdfModule("Fa", false, Nothing(), &cleanups));
    registry->AddModule("/lib/b", "B", new TStubUdfModule("Fb", false, Nothing(), &cleanups));

    registry->CleanupModulesOnTerminate();
    UNIT_ASSERT_VALUES_EQUAL(cleanups, 2u);

    registry->CleanupModulesOnTerminate();
    UNIT_ASSERT_VALUES_EQUAL(cleanups, 4u);
}

Y_UNIT_TEST(AddModuleDuplicateNameFails) {
    auto registry = MakeRegistry();
    registry->AddModule("/lib/a", "Foo", new TStubUdfModule("A"));
    UNIT_ASSERT_EXCEPTION(
        registry->AddModule("/lib/b", "Foo", new TStubUdfModule("B")),
        yexception);
}

Y_UNIT_TEST(FindFunctionTypeInfoRejectsInvalidName) {
    auto registry = MakeRegistry();
    registry->AddModule("/lib/foo", "Foo", new TStubUdfModule("Bar"));

    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    NUdf::ITypeInfoHelper::TPtr typeInfoHelper(new TTypeInfoHelper);
    auto runtimeSettings = NYql::MakeRuntimeSettings();

    TFunctionTypeInfo info;
    auto status = LookupTypeInfo(*registry, env, typeInfoHelper, *runtimeSettings, "NoDelimiter", &info);
    UNIT_ASSERT(!status.IsOk());
}

Y_UNIT_TEST(ClonePreservesFindFunctionTypeInfo) {
    auto registry = MakeRegistry();
    registry->AddModule("/lib/foo", "Foo", new TStubUdfModule("Bar"));

    auto clone = registry->Clone();

    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    NUdf::ITypeInfoHelper::TPtr typeInfoHelper(new TTypeInfoHelper);
    auto runtimeSettings = NYql::MakeRuntimeSettings();

    TFunctionTypeInfo info;
    auto status = LookupTypeInfo(*clone, env, typeInfoHelper, *runtimeSettings, "Foo.Bar", &info);
    UNIT_ASSERT_C(status.IsOk(), status.GetError());
    UNIT_ASSERT(info.FunctionType != nullptr);

    auto funcs = clone->GetModuleFunctions("Foo");
    UNIT_ASSERT(funcs.contains("Bar"));
}

Y_UNIT_TEST(PrintInfoToDoesNotCrash) {
    auto registry = MakeRegistry();
    TStringStream out;
    registry->PrintInfoTo(out);
}

Y_UNIT_TEST(ConcurrentLookupDuringRemove) {
    auto registry = MakeRegistry();
    registry->AddModule("/lib/foo", "Foo", new TStubUdfModule("Bar"));

    std::atomic<bool> stop{false};
    std::atomic<ui64> lookups{0};
    std::atomic<ui64> errors{0};
    constexpr size_t readerCount = 8;

    TVector<std::thread> readers;
    readers.reserve(readerCount);
    for (size_t i = 0; i < readerCount; ++i) {
        readers.emplace_back([&] {
            TScopedAlloc alloc(__LOCATION__);
            TTypeEnvironment env(alloc);
            NUdf::ITypeInfoHelper::TPtr typeInfoHelper(new TTypeInfoHelper);
            auto runtimeSettings = NYql::MakeRuntimeSettings();

            while (!stop.load(std::memory_order_relaxed)) {
                TFunctionTypeInfo funcInfo;
                auto status = LookupTypeInfo(
                    *registry, env, typeInfoHelper, *runtimeSettings, "Foo.Bar", &funcInfo);
                lookups.fetch_add(1, std::memory_order_relaxed);
                if (!status.IsOk()) {
                    errors.fetch_add(1, std::memory_order_relaxed);
                }
                Y_UNUSED(registry->IsLoadedUdfModule("Foo"));
            }
        });
    }

    // Let readers warm up, then unload under concurrent lookups.
    while (lookups.load(std::memory_order_relaxed) < 100) {
        std::this_thread::yield();
    }
    Dyn(registry.Get())->RemoveModule("Foo");
    stop.store(true, std::memory_order_relaxed);

    for (auto& t : readers) {
        t.join();
    }

    UNIT_ASSERT(!registry->IsLoadedUdfModule("Foo"));
    UNIT_ASSERT_GT(lookups.load(), 0u);
    // After remove some lookups may fail; before remove they must have succeeded.
    // We only require no crash / use-after-free (survived joins).
    Y_UNUSED(errors);
}

Y_UNIT_TEST(ConcurrentAddAndLookup) {
    auto registry = MakeRegistry();
    constexpr size_t moduleCount = 32;
    constexpr size_t writerCount = 4;
    constexpr size_t readerCount = 4;

    std::atomic<size_t> nextIndex{0};
    std::atomic<bool> writersDone{false};

    TVector<std::thread> writers;
    writers.reserve(writerCount);
    for (size_t w = 0; w < writerCount; ++w) {
        writers.emplace_back([&] {
            for (;;) {
                const size_t i = nextIndex.fetch_add(1, std::memory_order_relaxed);
                if (i >= moduleCount) {
                    break;
                }
                const TString name = TStringBuilder() << "Mod" << i;
                registry->AddModule(
                    TStringBuilder() << "/lib/" << i,
                    name,
                    new TStubUdfModule("Func"));
            }
        });
    }

    TVector<std::thread> readers;
    readers.reserve(readerCount);
    for (size_t r = 0; r < readerCount; ++r) {
        readers.emplace_back([&] {
            TScopedAlloc alloc(__LOCATION__);
            TTypeEnvironment env(alloc);
            NUdf::ITypeInfoHelper::TPtr typeInfoHelper(new TTypeInfoHelper);
            auto runtimeSettings = NYql::MakeRuntimeSettings();

            while (!writersDone.load(std::memory_order_acquire)) {
                for (size_t i = 0; i < moduleCount; ++i) {
                    const TString name = TStringBuilder() << "Mod" << i;
                    if (registry->IsLoadedUdfModule(name)) {
                        TFunctionTypeInfo funcInfo;
                        auto status = LookupTypeInfo(
                            *registry, env, typeInfoHelper, *runtimeSettings,
                            TStringBuilder() << name << ".Func", &funcInfo);
                        UNIT_ASSERT_C(status.IsOk(), status.GetError());
                    }
                }
            }
        });
    }

    for (auto& t : writers) {
        t.join();
    }
    writersDone.store(true, std::memory_order_release);
    for (auto& t : readers) {
        t.join();
    }

    UNIT_ASSERT_VALUES_EQUAL(registry->GetAllModuleNames().size(), moduleCount);
    for (size_t i = 0; i < moduleCount; ++i) {
        const TString name = TStringBuilder() << "Mod" << i;
        UNIT_ASSERT(registry->IsLoadedUdfModule(name));
        UNIT_ASSERT_VALUES_EQUAL(
            *registry->FindUdfPath(name),
            TStringBuilder() << "/lib/" << i);
    }
}

} // Y_UNIT_TEST_SUITE(TDynamicFunctionRegistryTest)
